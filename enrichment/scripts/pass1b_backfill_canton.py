"""Pass 1b backfill — fill `canton` from cached Nominatim responses.

Pass 1b (`pass1b_nominatim.py`) called the /reverse API for every pending
(postal_code OR street) row and cached the full JSON response, keyed by
rounded (lat, lng) @ 4 dp. Each response contains an `address.ISO3166-2-lvl4`
field like `"CH-ZH"` whenever the coordinate lands in Switzerland.

Pass 1a (`pass1_geocode.py`) fills `canton` from the offline
`reverse_geocoder` KD-tree. When that lookup returns a non-Swiss point
(border houses, imprecise listings), canton stays pending. For many of
those same rows, Nominatim DID resolve successfully with a Swiss ISO code
— but the pass-1b code only writes `postal_code` and `street`, so the
canton information sits unused in the cache.

This script:
  * walks every row with `canton_source = 'UNKNOWN-pending'` AND a non-UNKNOWN
    coordinate,
  * builds the same 4-dp cache key,
  * if the cached response has `country_code='ch'` AND a valid
    `ISO3166-2-lvl4` matching `CH-<2 letters>`, writes the canton via
    `write_field` (source=`rev_geo_nominatim`, confidence=0.9).

Zero new API calls. Idempotent (a second run is a no-op because the gate
refuses to overwrite rows that are no longer `UNKNOWN-pending`).

Usage:
    python -m enrichment.scripts.pass1b_backfill_canton --db /data/listings.db
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from enrichment.common.db import connect
from enrichment.common.provenance import write_field
from enrichment.common.sources import REV_GEO_NOMINATIM, UNKNOWN_PENDING

CACHE_PATH = Path(__file__).resolve().parents[1] / "data" / "cache" / "nominatim.json"
ROUND_DECIMALS = 4
CANTON_CONFIDENCE = 0.9   # Nominatim admin-boundary lookup is reliable for CH points


def _coord_key(lat: float, lng: float) -> str:
    return f"{round(lat, ROUND_DECIMALS)},{round(lng, ROUND_DECIMALS)}"


def _extract_canton_code(resp: dict) -> str | None:
    """Return the 2-letter canton code (e.g. 'ZH') or None if unusable."""
    addr = resp.get("address") or {}
    if (addr.get("country_code") or "").lower() != "ch":
        return None
    iso = addr.get("ISO3166-2-lvl4") or ""
    if not isinstance(iso, str) or not iso.startswith("CH-"):
        return None
    code = iso[3:].strip()
    if len(code) != 2 or not code.isalpha() or not code.isupper():
        return None
    return code


def run(db_path: Path) -> dict[str, int]:
    stats = {
        "pending_in": 0,
        "skipped_no_coords": 0,
        "cache_miss": 0,
        "non_ch_or_no_iso": 0,
        "canton_filled": 0,
    }

    if not CACHE_PATH.exists():
        print(
            f"[WARN] pass1b_backfill_canton: expected={CACHE_PATH} to exist "
            f"got=missing fallback=abort_run",
            flush=True,
        )
        return stats

    with CACHE_PATH.open() as f:
        cache = json.load(f)
    if not isinstance(cache, dict):
        print(
            f"[WARN] pass1b_backfill_canton: expected=dict got={type(cache).__name__} "
            f"fallback=abort_run",
            flush=True,
        )
        return stats

    conn = connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT listing_id, latitude_filled, longitude_filled
            FROM listings_enriched
            WHERE canton_source = ?;
            """,
            (UNKNOWN_PENDING,),
        ).fetchall()
        stats["pending_in"] = len(rows)

        for r in rows:
            lid = r["listing_id"]
            lat_s = r["latitude_filled"]
            lng_s = r["longitude_filled"]
            if not lat_s or not lng_s or lat_s == "UNKNOWN" or lng_s == "UNKNOWN":
                stats["skipped_no_coords"] += 1
                continue
            try:
                lat = float(lat_s)
                lng = float(lng_s)
            except (TypeError, ValueError):
                stats["skipped_no_coords"] += 1
                continue

            resp = cache.get(_coord_key(lat, lng))
            if resp is None:
                stats["cache_miss"] += 1
                continue

            code = _extract_canton_code(resp)
            if code is None:
                stats["non_ch_or_no_iso"] += 1
                continue

            # Gate: only write if still pending — makes the script idempotent.
            current = conn.execute(
                "SELECT canton_source FROM listings_enriched WHERE listing_id = ?;",
                (lid,),
            ).fetchone()
            if not current or current[0] != UNKNOWN_PENDING:
                continue

            write_field(
                conn,
                listing_id=lid,
                field="canton",
                filled=code,
                source=REV_GEO_NOMINATIM,
                confidence=CANTON_CONFIDENCE,
                raw=None,
            )
            stats["canton_filled"] += 1

        conn.commit()
        return stats
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    args = parser.parse_args()
    if not args.db.exists():
        print(f"[ERROR] DB not found at {args.db}", file=sys.stderr)
        return 2
    stats = run(args.db)
    print("Pass 1b backfill-canton complete:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
