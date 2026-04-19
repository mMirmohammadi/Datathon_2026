"""T1 signal hardening — idempotent ALTER + backfill for two quality signals.

Adds two columns to `listings_ranking_signals`:

  * `nearest_stop_lines_log` (REAL)  — ln(1 + nearest_stop_lines_count).
    Fixes the Cornavin (41,083 lines) / Bel-Air (30,947) / Bern Hauptbahnhof
    (22,101) over-dominance that happens when `nearest_stop_lines_count` is
    percentile-normalised in the blend. Log-transform compresses these
    mega-hubs onto a scale where a typical city stop (p50 ≈ 213) is around
    5.36, Cornavin is ~10.62, and a quiet stop with 1 line is 0.69.

  * `price_plausibility` (TEXT)      — 'normal' | 'suspect'.
    'suspect' = |price_delta_pct_canton_rooms| > 3.0 (listing is 300%+ away
    from its canton × rooms bucket median). These are almost certainly
    mis-categorised multi-unit buildings listed with rooms=1, e.g.:
      listing 5248   ZH × 1 room, price=23,200  CHF  (baseline 1,225)  → +1,794%
      listing 220343 VD × 3 rooms, price=31,020 CHF  (baseline 1,787)  → +1,636%
    If the ranker percentile-normalises `price_delta` across the candidate
    pool, these 74 outliers either dominate the "cheap/expensive" ends or
    compress the range for everyone else. Flagging them lets the ranker
    drop them from that normalisation AND demote them.

Both columns are registered in `ranking/schema.py` so the registry-drift
guard (`check_db_matches_registry`) stays green.

Usage:
    python -m ranking.scripts.t1_signal_hardening --db data/listings.db

Idempotent: re-runs are no-ops on the ALTER (uses IF NOT EXISTS via a
PRAGMA check) and recompute the values from scratch.

Per CLAUDE.md §5: every fallback path emits `[WARN] context: expected=X,
got=Y, fallback=Z`. NULL source → NULL derived value (no fabrication).
"""
from __future__ import annotations

import argparse
import math
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from ranking.common.db import connect
from ranking.schema import check_db_matches_registry

# Threshold for the price plausibility flag. 3.0 = 300% from bucket median.
# Chosen empirically from the histogram at >1σ above the p99 of real Swiss
# rent variation within a canton×rooms bucket. See the distribution summary
# in _context/LAYER2_STATE.md §3.
PRICE_DELTA_SUSPECT_THRESHOLD = 3.0


def _ensure_column(conn: sqlite3.Connection, col: str, decl: str) -> bool:
    """ALTER TABLE ADD COLUMN iff it's not already there. Return True if added."""
    existing = {r[1] for r in conn.execute(
        "PRAGMA table_info(listings_ranking_signals)"
    ).fetchall()}
    if col in existing:
        return False
    conn.execute(f"ALTER TABLE listings_ranking_signals ADD COLUMN {col} {decl}")
    conn.commit()
    return True


def _log1p_backfill(conn: sqlite3.Connection) -> dict:
    rows = conn.execute("""
        SELECT listing_id, nearest_stop_lines_count
          FROM listings_ranking_signals
    """).fetchall()
    n_total = len(rows)
    n_null_source = 0
    n_bad = 0
    updates: list[tuple[float | None, str]] = []
    for listing_id, lines in rows:
        if lines is None:
            n_null_source += 1
            updates.append((None, listing_id))
            continue
        if lines < 0:
            # Shouldn't happen — GTFS counts can't be negative. Guard anyway.
            print(f"[WARN] t1_signal_hardening.log_backfill: "
                  f"expected=lines>=0, got={lines} for listing_id={listing_id}, "
                  "fallback=NULL", flush=True)
            n_bad += 1
            updates.append((None, listing_id))
            continue
        updates.append((math.log1p(lines), listing_id))

    conn.executemany(
        "UPDATE listings_ranking_signals SET nearest_stop_lines_log = ? "
        "WHERE listing_id = ?",
        updates,
    )
    conn.commit()
    return {
        "n_total": n_total,
        "n_null_source": n_null_source,
        "n_bad": n_bad,
        "n_filled": n_total - n_null_source - n_bad,
    }


def _plausibility_backfill(conn: sqlite3.Connection) -> dict:
    """Write 'normal' / 'suspect' / NULL based on price_delta_pct_canton_rooms."""
    # Single SQL UPDATE: each branch handled; NULL stays NULL.
    conn.execute(f"""
        UPDATE listings_ranking_signals
           SET price_plausibility = CASE
                 WHEN price_delta_pct_canton_rooms IS NULL THEN NULL
                 WHEN ABS(price_delta_pct_canton_rooms) > {PRICE_DELTA_SUSPECT_THRESHOLD}
                      THEN 'suspect'
                 ELSE 'normal'
               END
    """)
    conn.commit()
    counts = dict(conn.execute("""
        SELECT COALESCE(price_plausibility, '__NULL__'), COUNT(*)
          FROM listings_ranking_signals
         GROUP BY price_plausibility
    """).fetchall())
    return counts


def _stamp_updated(conn: sqlite3.Connection) -> int:
    """Touch last_updated_utc for every row we potentially changed."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    cur = conn.execute(
        "UPDATE listings_ranking_signals SET last_updated_utc = ?", (now,)
    )
    conn.commit()
    return cur.rowcount


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", required=True, type=Path,
                        help="Path to SQLite DB (data/listings.db)")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"[ERROR] DB not found: {args.db}", file=sys.stderr)
        sys.exit(2)

    with connect(args.db) as conn:
        added_log   = _ensure_column(conn, "nearest_stop_lines_log", "REAL")
        added_flag  = _ensure_column(conn, "price_plausibility",     "TEXT")
        check_db_matches_registry(conn)  # will raise on any registry drift

        log_stats = _log1p_backfill(conn)
        flag_counts = _plausibility_backfill(conn)
        n_touched = _stamp_updated(conn)

    print(f"t1_signal_hardening ok  db={args.db}")
    print(f"  nearest_stop_lines_log: added={added_log}  "
          f"filled={log_stats['n_filled']:,}  "
          f"null_source={log_stats['n_null_source']:,}  "
          f"bad={log_stats['n_bad']:,}")
    print(f"  price_plausibility:     added={added_flag}  "
          f"counts={flag_counts}  "
          f"threshold={PRICE_DELTA_SUSPECT_THRESHOLD}")
    print(f"  last_updated_utc stamped on {n_touched:,} rows")


if __name__ == "__main__":
    main()
