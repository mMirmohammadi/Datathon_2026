"""T1.1 — Create or validate the `listings_ranking_signals` side-table.

Idempotent: re-running is a no-op when the schema matches the registry.
Emits a [WARN] per CLAUDE.md §5 on any drift so we never silently diverge.

Usage:
    python -m ranking.scripts.t1_create_table --db data/listings.db
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from ranking.common.db import connect, table_exists
from ranking.schema import SIGNALS, INDEX_SQL, create_table_sql, signal_names


def run(db_path: Path, *, force_recreate: bool = False) -> dict:
    t0 = time.monotonic()
    stats = {
        "existed": False,
        "created": False,
        "recreated": False,
        "columns_total": len(SIGNALS),
        "rows_after": 0,
    }
    with connect(db_path) as conn:
        existed = table_exists(conn, "listings_ranking_signals")
        stats["existed"] = existed

        if existed and force_recreate:
            conn.execute("DROP TABLE IF EXISTS listings_ranking_signals;")
            conn.commit()
            print(
                "[WARN] t1_create_table: expected=table, got=dropped "
                "(forced --recreate) — any existing signal data is gone",
                flush=True,
            )
            existed = False
            stats["recreated"] = True

        if not existed:
            conn.executescript(create_table_sql())
            for idx_sql in INDEX_SQL:
                conn.execute(idx_sql)
            conn.commit()
            stats["created"] = True

        # Seed one row per listing so downstream UPDATE (vs UPSERT) is cheap
        # and never has to worry about missing PK. Uses listing_ids from the
        # harness-owned `listings` table. Safe to re-run: INSERT OR IGNORE.
        n_before = conn.execute(
            "SELECT COUNT(*) FROM listings_ranking_signals;"
        ).fetchone()[0]
        conn.execute(
            """
            INSERT OR IGNORE INTO listings_ranking_signals (listing_id)
            SELECT listing_id FROM listings;
            """
        )
        conn.commit()
        n_after = conn.execute(
            "SELECT COUNT(*) FROM listings_ranking_signals;"
        ).fetchone()[0]
        stats["rows_before"] = n_before
        stats["rows_after"] = n_after
        stats["rows_seeded"] = n_after - n_before

        # Drift check — fail loud if PRAGMA columns disagree with SIGNALS
        rows = conn.execute(
            "PRAGMA table_info(listings_ranking_signals);"
        ).fetchall()
        db_cols = [r[1] for r in rows]
        registry = ["listing_id", *signal_names()]
        if set(db_cols) != set(registry):
            extra = sorted(set(db_cols) - set(registry))
            missing = sorted(set(registry) - set(db_cols))
            raise RuntimeError(
                "Schema drift detected:\n"
                f"  extra in DB:     {extra}\n"
                f"  missing from DB: {missing}\n"
                "Fix: edit ranking/schema.py then re-run with --recreate "
                "(WARNING: drops data in this table)."
            )

    elapsed = time.monotonic() - t0
    stats["elapsed_s"] = round(elapsed, 3)
    print(
        f"[INFO] t1_create_table: existed={stats['existed']} "
        f"created={stats['created']} recreated={stats['recreated']} "
        f"columns={stats['columns_total']} rows={stats['rows_after']} "
        f"seeded={stats['rows_seeded']} elapsed_s={elapsed:.3f}",
        flush=True,
    )
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True, help="path to listings.db")
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="DROP + CREATE (destructive — emits [WARN] and wipes all signals).",
    )
    args = parser.parse_args()
    if not args.db.exists():
        print(f"[ERROR] t1_create_table: db not found at {args.db}", file=sys.stderr)
        return 2
    try:
        run(args.db, force_recreate=args.recreate)
    except RuntimeError as exc:
        print(f"[ERROR] t1_create_table: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
