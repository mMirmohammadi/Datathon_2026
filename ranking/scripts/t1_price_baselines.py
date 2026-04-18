"""T1.2 — Per-segment price baselines (median rent by canton × rooms and PLZ × rooms).

Why medians (not means): rents have extreme outliers in the raw corpus
(parking spots at 100 CHF, the Villa Rothschild at 50k CHF) and the null-fill
pipeline flags only the obvious ones. Medians are robust to the rest.

Bucket sizing: we require ≥ 5 same-bucket listings to emit a baseline.
Smaller buckets would be noisier than useful. Listings in a too-sparse bucket
leave `price_baseline_chf_* = NULL`; the ranker treats NULL as "no signal".

Per CLAUDE.md §5:
  * We never substitute a bucket's baseline with a parent bucket silently;
    the consumer gets NULL and must decide.
  * We log the number of listings with no baseline (per bucket type) so the
    data gap is visible.

Usage:
    python -m ranking.scripts.t1_price_baselines --db data/listings.db
"""
from __future__ import annotations

import argparse
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from ranking.common.db import connect
from ranking.schema import check_db_matches_registry

MIN_BUCKET_SIZE = 5
# Use the first two digits of the 4-digit Swiss PLZ. Post offices group
# physical delivery areas by the first two digits (e.g. 80xx = Zürich).
# Goes finer than canton, coarser than the full PLZ → good for ~25k corpus.
PLZ_PREFIX_LEN = 2


def _plz_prefix(plz: str | None) -> str | None:
    if plz is None:
        return None
    s = str(plz).strip()
    return s[:PLZ_PREFIX_LEN] if len(s) >= PLZ_PREFIX_LEN else None


def _rooms_bucket(rooms: float | None) -> float | None:
    """Snap rooms to the nearest 0.5 using half-up rounding.

    Swiss real-estate listings are overwhelmingly on 0.5-step counts (3, 3.5,
    4, 4.5). We use deterministic half-up instead of Python's banker's
    rounding so 3.25 → 3.5 (not 3.0) — matches how a human would bucket it.
    """
    if rooms is None:
        return None
    try:
        r = float(rooms)
    except (TypeError, ValueError):
        return None
    if r <= 0 or r > 15:
        return None
    # Half-up: add 0.5 to the doubled value, floor to int, halve.
    return int(r * 2 + 0.5) / 2.0


def _compute_baselines(rows: list[dict[str, Any]]) -> tuple[
    dict[tuple[str, float], tuple[float, int]],
    dict[tuple[str, float], tuple[float, int]],
    Counter,
]:
    """Return (canton_rooms_map, plz_rooms_map, bucket-size histogram)."""
    by_canton: dict[tuple[str, float], list[int]] = defaultdict(list)
    by_plz: dict[tuple[str, float], list[int]] = defaultdict(list)

    for r in rows:
        price = r["price"]
        if price is None or price <= 0:
            continue
        rooms_b = _rooms_bucket(r["rooms"])
        if rooms_b is None:
            continue
        canton = r["canton"]
        if canton:
            by_canton[(canton.upper()[:2], rooms_b)].append(int(price))
        prefix = _plz_prefix(r["postal_code"])
        if prefix:
            by_plz[(prefix, rooms_b)].append(int(price))

    canton_med: dict[tuple[str, float], tuple[float, int]] = {}
    for key, prices in by_canton.items():
        n = len(prices)
        if n >= MIN_BUCKET_SIZE:
            canton_med[key] = (round(float(median(prices)), 2), n)

    plz_med: dict[tuple[str, float], tuple[float, int]] = {}
    sizes = Counter()
    for key, prices in by_plz.items():
        n = len(prices)
        sizes[f"plz_rooms_bucket_size={n}"] += 1
        if n >= MIN_BUCKET_SIZE:
            plz_med[key] = (round(float(median(prices)), 2), n)
    return canton_med, plz_med, sizes


def run(db_path: Path) -> dict[str, Any]:
    t_start = time.monotonic()
    stats: dict[str, Any] = {"started_at": datetime.now(timezone.utc).isoformat()}

    with connect(db_path) as conn:
        check_db_matches_registry(conn)

        # Use the enriched city/canton/postal, not the raw listings columns —
        # that way reverse-geocoded SRED rows participate.
        rows = conn.execute(
            """
            SELECT l.listing_id,
                   l.price,
                   l.rooms,
                   le.canton_filled      AS canton,
                   le.postal_code_filled AS postal_code
            FROM listings l
            JOIN listings_enriched le USING(listing_id)
            WHERE le.offer_type_source != 'DROPPED_bad_data'
              AND le.price_source      != 'DROPPED_bad_data'
              AND l.price IS NOT NULL
            """
        ).fetchall()
        rows_list = [dict(r) for r in rows]
        print(
            f"[INFO] t1_price_baselines: candidate rows for baseline computation = {len(rows_list):,}",
            flush=True,
        )
        if not rows_list:
            raise RuntimeError("No candidate rows — is the DB populated and enrichment run?")

        canton_med, plz_med, bucket_hist = _compute_baselines(rows_list)
        stats["canton_rooms_buckets"] = len(canton_med)
        stats["plz_rooms_buckets"]    = len(plz_med)
        print(
            f"[INFO] t1_price_baselines: canton×rooms buckets with ≥{MIN_BUCKET_SIZE} listings = {len(canton_med):,}",
            flush=True,
        )
        print(
            f"[INFO] t1_price_baselines: plz×rooms buckets    with ≥{MIN_BUCKET_SIZE} listings = {len(plz_med):,}",
            flush=True,
        )

        # Apply the baselines back to every row; commit in a single transaction
        # with per-row UPDATE (ranking signals table has no concurrent writer).
        now_iso = datetime.now(timezone.utc).isoformat()
        conn.execute("BEGIN;")
        n_updated = 0
        n_cr_filled = 0
        n_pr_filled = 0
        for r in rows_list:
            price = r["price"]
            if price is None or price <= 0:
                continue
            rooms_b = _rooms_bucket(r["rooms"])
            if rooms_b is None:
                continue
            canton = (r["canton"] or "").upper()[:2] or None
            plz = _plz_prefix(r["postal_code"])

            cr = canton_med.get((canton, rooms_b)) if canton else None
            pr = plz_med.get((plz, rooms_b)) if plz else None

            cr_base, cr_n = (cr if cr is not None else (None, None))
            pr_base, pr_n = (pr if pr is not None else (None, None))

            cr_delta = None if cr_base in (None, 0) else round((price - cr_base) / cr_base, 4)
            pr_delta = None if pr_base in (None, 0) else round((price - pr_base) / pr_base, 4)

            conn.execute(
                """
                UPDATE listings_ranking_signals SET
                    price_baseline_chf_canton_rooms = ?,
                    price_baseline_chf_plz_rooms    = ?,
                    price_delta_pct_canton_rooms    = ?,
                    price_delta_pct_plz_rooms       = ?,
                    price_baseline_n_canton_rooms   = ?,
                    price_baseline_n_plz_rooms      = ?,
                    last_updated_utc                = ?
                WHERE listing_id = ?;
                """,
                (
                    cr_base, pr_base,
                    cr_delta, pr_delta,
                    cr_n, pr_n,
                    now_iso,
                    r["listing_id"],
                ),
            )
            n_updated += 1
            if cr_base is not None: n_cr_filled += 1
            if pr_base is not None: n_pr_filled += 1
        conn.commit()

        stats["rows_touched"]            = n_updated
        stats["rows_with_canton_delta"]  = n_cr_filled
        stats["rows_with_plz_delta"]     = n_pr_filled
        stats["coverage_pct_canton"]     = round(100 * n_cr_filled / max(len(rows_list), 1), 1)
        stats["coverage_pct_plz"]        = round(100 * n_pr_filled / max(len(rows_list), 1), 1)

        # Loud coverage report — CLAUDE.md §5: if a large fraction of rows get
        # no baseline, that's visible, not silent.
        n_missing_canton = len(rows_list) - n_cr_filled
        n_missing_plz    = len(rows_list) - n_pr_filled
        if n_missing_canton:
            print(
                f"[WARN] t1_price_baselines: expected=canton baseline for all candidates, "
                f"got={n_missing_canton:,} listings with no canton×rooms baseline "
                f"(bucket size < {MIN_BUCKET_SIZE} or canton missing), fallback=NULL",
                flush=True,
            )
        if n_missing_plz:
            print(
                f"[WARN] t1_price_baselines: expected=plz baseline for all candidates, "
                f"got={n_missing_plz:,} listings with no plz×rooms baseline "
                f"(bucket size < {MIN_BUCKET_SIZE} or PLZ missing), fallback=NULL",
                flush=True,
            )

    elapsed = time.monotonic() - t_start
    stats["elapsed_s"] = round(elapsed, 3)
    print(
        f"[INFO] t1_price_baselines: DONE rows_touched={n_updated:,} "
        f"canton_coverage={stats['coverage_pct_canton']}% "
        f"plz_coverage={stats['coverage_pct_plz']}% "
        f"elapsed_s={elapsed:.2f}",
        flush=True,
    )
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    args = parser.parse_args()
    if not args.db.exists():
        print(f"[ERROR] t1_price_baselines: db not found at {args.db}", file=sys.stderr)
        return 2
    run(args.db)
    return 0


if __name__ == "__main__":
    sys.exit(main())
