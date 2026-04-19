"""Pass 1d — canton top-up for rows that went straight to UNKNOWN.

Background. Pass 1a ran once on the original 22,819-row corpus. When the
DB grew to 25,546 rows, the new COMPARIS rows whose `listings.canton` was
NULL never got a reverse-geocoder pass and were sentinel-filled as
`canton_source='UNKNOWN'` by pass 3. This pass closes the gap using two
complementary channels, then cross-checks them.

Channels (in order):

  Stage 1 — reverse_geocoder on original lat/lng.
    Same offline KDTree pass 1a uses. Deterministic.
    Rejects (0, 0) null-island and out-of-CH-bbox coords with [WARN]
    (auditor-flagged risk R1).
    Source tag: `rev_geo_offline`.

  Stage 2 — corpus-derived PLZ → canton majority vote.
    Builds a (postal_code → canton) map from the 22,288 well-known
    rows already in the DB. For each UNKNOWN-canton row with a known
    postal_code NOT resolved by Stage 1, assigns the majority canton.
    Confidence ladder per auditor R2:
      0.85   unanimous AND ≥5 votes
      0.75   majority ≥95% AND ≥5 votes
      0.65   majority ≥95% AND N ≤ 3 (low evidence cap)
      0.60   majority ∈ [75%, 95%)
      0.45   majority < 75% (true tie territory)
    Source tag: `plz_majority_vote`.
    Raw column: `"plz_vote:<votes>|<total>"` for audit.

  Stage 3 — cross-validation (audit only; no writes).
    For every row where Stage 1 and the Stage 2 map both produced an
    answer, compares them. Mismatches logged to
    enrichment/data/pass1d_disagreements.json for QA. Stage 1 always
    wins on conflict (physical lat/lng > postal majority).

Per CLAUDE.md §5, every fallback path emits [WARN] context.

Writes: gated by `WHERE canton_source='UNKNOWN'` explicitly. `write_field`
does NOT enforce no-overwrite itself (auditor R4). Running pass1d twice
is a no-op because the gate no longer matches after the first run.

Usage:
    python -m enrichment.scripts.pass1d_canton_topup --db data/listings.db
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

from enrichment.common.cantons import admin1_to_canton_code
from enrichment.common.db import connect
from enrichment.common.provenance import UNKNOWN_VALUE, write_field
from enrichment.common.sources import (
    PLZ_MAJORITY_VOTE,
    REV_GEO_OFFLINE,
    UNKNOWN,
)

# CH bounding box (same as pass 1a — keep in sync).
CH_LAT_MIN, CH_LAT_MAX = 45.8, 47.9
CH_LNG_MIN, CH_LNG_MAX = 5.9, 10.5

# Minimum votes required before we trust a "unanimous" PLZ as high-confidence.
MIN_VOTES_HIGH_CONF = 5


def _is_null_island(lat: float, lng: float) -> bool:
    return lat == 0.0 and lng == 0.0


def _is_in_ch_bbox(lat: float, lng: float) -> bool:
    return CH_LAT_MIN <= lat <= CH_LAT_MAX and CH_LNG_MIN <= lng <= CH_LNG_MAX


def _parse_float(v: str | None) -> float | None:
    """Parse `latitude_filled` / `longitude_filled` — text column."""
    if v is None or v == UNKNOWN_VALUE:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _build_plz_canton_map(conn) -> dict[str, Counter]:
    """Build {postal_code: Counter({canton: vote_count})} from well-known rows."""
    votes: dict[str, Counter] = defaultdict(Counter)
    rows = conn.execute(
        """
        SELECT postal_code_filled, canton_filled
        FROM listings_enriched
        WHERE canton_source NOT IN ('UNKNOWN', 'DROPPED_bad_data', 'UNKNOWN-pending')
          AND canton_filled != 'UNKNOWN'
          AND postal_code_source NOT IN ('UNKNOWN', 'DROPPED_bad_data', 'UNKNOWN-pending')
          AND postal_code_filled != 'UNKNOWN';
        """
    ).fetchall()
    for r in rows:
        votes[r["postal_code_filled"]][r["canton_filled"]] += 1
    return votes


def _confidence_for(votes: int, total: int) -> float:
    """Apply the auditor-R2 confidence ladder."""
    if total == 0:
        return 0.0
    frac = votes / total
    if frac >= 1.0 and total >= MIN_VOTES_HIGH_CONF:
        return 0.85
    if frac >= 0.95 and total >= MIN_VOTES_HIGH_CONF:
        return 0.75
    if frac >= 0.95 and total <= 3:
        return 0.65  # weak-evidence cap
    if frac >= 0.75:
        return 0.60
    return 0.45  # true-tie territory


def _collect_unknown_canton_rows(conn) -> list[dict]:
    """Pull every UNKNOWN-canton row with everything we need to resolve it."""
    rows = conn.execute(
        """
        SELECT le.listing_id,
               le.latitude_source, le.latitude_filled,
               le.longitude_source, le.longitude_filled,
               le.postal_code_source, le.postal_code_filled
        FROM listings_enriched le
        WHERE le.canton_source = ?;
        """,
        (UNKNOWN,),
    ).fetchall()
    return [dict(r) for r in rows]


def _stage1_reverse_geocode(
    conn, rows: list[dict], stats: dict, disagreement_log: list
) -> dict[str, str]:
    """Run reverse_geocoder on rows with usable original lat/lng.

    Returns a dict {listing_id: canton_code} of successful resolutions —
    used by Stage 3 for cross-validation against Stage 2's PLZ inference.
    """
    import reverse_geocoder as rg  # lazy: KDTree load ~2 s

    stage1_map: dict[str, str] = {}
    candidates: list[tuple[str, float, float]] = []
    for r in rows:
        if r["latitude_source"] != "original" or r["longitude_source"] != "original":
            continue
        lat = _parse_float(r["latitude_filled"])
        lng = _parse_float(r["longitude_filled"])
        if lat is None or lng is None:
            stats["stage1_bad_latlng"] += 1
            print(
                f"[WARN] pass1d.stage1: expected=parseable_latlng, "
                f"got lat_filled={r['latitude_filled']!r} lng={r['longitude_filled']!r} "
                f"listing_id={r['listing_id']}, fallback=skip_stage1",
                flush=True,
            )
            continue
        if _is_null_island(lat, lng):
            stats["stage1_dropped_null_island"] += 1
            print(
                f"[WARN] pass1d.stage1: expected=real_coords, got=(0,0), "
                f"listing_id={r['listing_id']}, fallback=skip_stage1",
                flush=True,
            )
            continue
        if not _is_in_ch_bbox(lat, lng):
            stats["stage1_dropped_oob_ch"] += 1
            print(
                f"[WARN] pass1d.stage1: expected=in_ch_bbox, got=({lat}, {lng}), "
                f"listing_id={r['listing_id']}, fallback=skip_stage1",
                flush=True,
            )
            continue
        candidates.append((r["listing_id"], lat, lng))

    if not candidates:
        return stage1_map

    results = rg.search([(lat, lng) for _, lat, lng in candidates], mode=2)
    for (listing_id, lat, lng), res in zip(candidates, results, strict=True):
        cc = res.get("cc", "")
        if cc != "CH":
            stats["stage1_rg_not_ch"] += 1
            print(
                f"[WARN] pass1d.stage1: expected=rg_cc_CH, got={cc!r}, "
                f"listing_id={listing_id} lat={lat} lng={lng}, fallback=skip",
                flush=True,
            )
            continue
        admin1 = res.get("admin1", "")
        canton = admin1_to_canton_code(admin1)
        if canton is None:
            stats["stage1_unmapped_admin1"] += 1
            print(
                f"[WARN] pass1d.stage1: expected=mapped_canton, got admin1={admin1!r}, "
                f"listing_id={listing_id}, fallback=skip",
                flush=True,
            )
            continue
        stage1_map[listing_id] = canton
        # Write: gated on canton_source='UNKNOWN' (auditor R4).
        conn.execute(
            "UPDATE listings_enriched SET canton_filled=?, canton_source=?, "
            "canton_confidence=?, canton_raw=? "
            "WHERE listing_id=? AND canton_source=?",
            (canton, REV_GEO_OFFLINE, 0.95, admin1, listing_id, UNKNOWN),
        )
        if conn.total_changes % 200 == 0:
            conn.commit()
        stats["stage1_filled"] += 1

    conn.commit()
    return stage1_map


def _stage2_plz_vote(
    conn, rows: list[dict], plz_map: dict[str, Counter], stage1_map: dict[str, str],
    stats: dict, disagreement_log: list
) -> None:
    """For UNKNOWN-canton rows still unresolved by Stage 1, apply PLZ majority."""
    for r in rows:
        lid = r["listing_id"]
        if lid in stage1_map:
            # Stage 3 cross-check: both channels answered — compare.
            plz = r["postal_code_filled"]
            if plz and plz != UNKNOWN_VALUE and plz in plz_map:
                counter = plz_map[plz]
                plz_canton, plz_votes = counter.most_common(1)[0]
                total = sum(counter.values())
                if plz_canton != stage1_map[lid]:
                    disagreement_log.append({
                        "listing_id": lid,
                        "rev_geo_canton": stage1_map[lid],
                        "plz_majority_canton": plz_canton,
                        "plz": plz,
                        "plz_votes": f"{plz_votes}/{total}",
                    })
                    stats["stage3_disagreement"] += 1
                else:
                    stats["stage3_agreement"] += 1
            continue  # already written in Stage 1

        plz = r["postal_code_filled"]
        if not plz or plz == UNKNOWN_VALUE or plz not in plz_map:
            stats["stage2_no_plz_or_unmapped"] += 1
            continue

        counter = plz_map[plz]
        canton, votes = counter.most_common(1)[0]
        total = sum(counter.values())
        conf = _confidence_for(votes, total)
        raw = f"plz_vote:{votes}|{total}"

        conn.execute(
            "UPDATE listings_enriched SET canton_filled=?, canton_source=?, "
            "canton_confidence=?, canton_raw=? "
            "WHERE listing_id=? AND canton_source=?",
            (canton, PLZ_MAJORITY_VOTE, conf, raw, lid, UNKNOWN),
        )
        stats["stage2_filled"] += 1
        if conf <= 0.45:
            stats["stage2_filled_low_conf"] += 1
    conn.commit()


def run(db_path: Path, audit_path: Path) -> dict:
    conn = connect(db_path)
    try:
        stats = {
            "rows_in_scope": 0,
            "stage1_filled": 0,
            "stage1_bad_latlng": 0,
            "stage1_dropped_null_island": 0,
            "stage1_dropped_oob_ch": 0,
            "stage1_rg_not_ch": 0,
            "stage1_unmapped_admin1": 0,
            "stage2_filled": 0,
            "stage2_filled_low_conf": 0,
            "stage2_no_plz_or_unmapped": 0,
            "stage3_agreement": 0,
            "stage3_disagreement": 0,
            "residual_unknown": 0,
            "plz_map_size": 0,
            "plz_map_total_votes": 0,
        }

        rows = _collect_unknown_canton_rows(conn)
        stats["rows_in_scope"] = len(rows)

        plz_map = _build_plz_canton_map(conn)
        stats["plz_map_size"] = len(plz_map)
        stats["plz_map_total_votes"] = sum(sum(c.values()) for c in plz_map.values())

        if not rows:
            return stats

        disagreement_log: list[dict] = []

        # Stage 1 then Stage 2 (Stage 2 filters out rows Stage 1 already handled).
        stage1_map = _stage1_reverse_geocode(conn, rows, stats, disagreement_log)
        _stage2_plz_vote(conn, rows, plz_map, stage1_map, stats, disagreement_log)

        # Post-state count.
        stats["residual_unknown"] = conn.execute(
            "SELECT COUNT(*) FROM listings_enriched WHERE canton_source=?",
            (UNKNOWN,),
        ).fetchone()[0]

        # Write disagreement audit (even if empty — makes the artefact path stable).
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        audit_path.write_text(json.dumps({
            "stage3_disagreements": disagreement_log,
            "stats": stats,
        }, indent=2, ensure_ascii=False))

        return stats
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument(
        "--audit-path", type=Path,
        default=Path("enrichment/data/pass1d_disagreements.json"),
        help="Where to write the Stage 3 disagreement audit.",
    )
    args = parser.parse_args()
    if not args.db.exists():
        print(f"[ERROR] DB not found at {args.db}", file=sys.stderr)
        return 2

    stats = run(args.db, args.audit_path)
    print("Pass 1d complete:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    print(f"\nDisagreement audit written to: {args.audit_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
