"""One-shot migration — add 16 columns for the 4 pass-2b fields.

Called BEFORE pass 2b can run. Idempotent.

Without this, the existing 25,546-row `listings_enriched` table has no
columns for bathroom_count / bathroom_shared / has_cellar / kitchen_shared,
so pass 3's schema-drift guard would raise `RuntimeError("FIELDS declares
fields whose _filled columns are missing …")`.

SQLite constraint (auditor R8.1): `ALTER TABLE … ADD COLUMN … NOT NULL`
requires a DEFAULT. Migrations here supply:
  * `{field}_filled TEXT NOT NULL DEFAULT 'UNKNOWN'`
  * `{field}_source TEXT NOT NULL DEFAULT 'UNKNOWN-pending'`
  * `{field}_confidence REAL NOT NULL DEFAULT 0.0`
  * `{field}_raw TEXT` (nullable — no default needed)

With DEFAULTs set, the 25,546 existing rows immediately comply with pass 3's
invariant. No separate UPDATE is required.

CLAUDE.md §5: emits `[WARN]` on any fallback path.

Usage:
    python -m enrichment.scripts.migrate_pass2b_columns --db data/listings.db
    python -m enrichment.scripts.migrate_pass2b_columns --db data/listings.db --dry-run
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

from enrichment.common.db import connect
from enrichment.schema import FIELDS


PASS2B_FIELD_NAMES: tuple[str, ...] = (
    "bathroom_count",
    "bathroom_shared",
    "has_cellar",
    "kitchen_shared",
)


def _existing_columns(conn: sqlite3.Connection) -> set[str]:
    return {r[1] for r in conn.execute("PRAGMA table_info(listings_enriched)").fetchall()}


def _alter_statements(missing: set[str]) -> list[str]:
    """Build `ALTER TABLE ADD COLUMN` statements for missing cols with DEFAULTs."""
    out: list[str] = []
    for name in PASS2B_FIELD_NAMES:
        col_filled     = f"{name}_filled"
        col_source     = f"{name}_source"
        col_confidence = f"{name}_confidence"
        col_raw        = f"{name}_raw"
        if col_filled in missing:
            out.append(
                f"ALTER TABLE listings_enriched ADD COLUMN {col_filled} "
                f"TEXT NOT NULL DEFAULT 'UNKNOWN'"
            )
        if col_source in missing:
            out.append(
                f"ALTER TABLE listings_enriched ADD COLUMN {col_source} "
                f"TEXT NOT NULL DEFAULT 'UNKNOWN-pending'"
            )
        if col_confidence in missing:
            out.append(
                f"ALTER TABLE listings_enriched ADD COLUMN {col_confidence} "
                f"REAL NOT NULL DEFAULT 0.0"
            )
        if col_raw in missing:
            out.append(
                f"ALTER TABLE listings_enriched ADD COLUMN {col_raw} TEXT"
            )
    return out


def _verify_registry_alignment() -> None:
    """Make sure the 4 pass-2b fields are registered with extraction_only."""
    by_name = {f.name: f for f in FIELDS}
    for name in PASS2B_FIELD_NAMES:
        if name not in by_name:
            raise RuntimeError(
                f"FIELDS registry missing {name!r}. Add to enrichment/schema.py "
                f"before running this migration."
            )
        f = by_name[name]
        if f.origin != "extraction_only":
            raise RuntimeError(
                f"FIELDS[{name}].origin expected 'extraction_only', got {f.origin!r}"
            )
    return None


def run(db_path: Path, dry_run: bool) -> dict:
    _verify_registry_alignment()

    conn = connect(db_path)
    try:
        existing = _existing_columns(conn)
        expected = {
            f"{name}_{suffix}"
            for name in PASS2B_FIELD_NAMES
            for suffix in ("filled", "source", "confidence", "raw")
        }
        missing = expected - existing

        stats = {
            "expected_cols": len(expected),
            "already_present": len(expected & existing),
            "to_add": len(missing),
            "altered": 0,
            "dry_run": dry_run,
        }

        if not missing:
            print("migrate_pass2b_columns: all 16 columns already present — no-op.")
            return stats

        statements = _alter_statements(missing)
        for stmt in statements:
            if dry_run:
                print(f"  [dry-run] {stmt}")
                continue
            try:
                conn.execute(stmt)
            except sqlite3.DatabaseError as exc:
                print(f"[WARN] migrate_pass2b_columns: expected=ALTER ok, "
                      f"got={type(exc).__name__}: {exc}, stmt={stmt!r}, "
                      f"fallback=abort", flush=True)
                raise
            stats["altered"] += 1

        if not dry_run:
            conn.commit()

        # Post-state check: all 16 cols now present.
        after = _existing_columns(conn)
        still_missing = expected - after
        if still_missing and not dry_run:
            raise RuntimeError(
                f"Post-migration drift: still missing {sorted(still_missing)}"
            )

        # Post-state: every row has UNKNOWN-pending on all 4 _source cols.
        if not dry_run:
            for name in PASS2B_FIELD_NAMES:
                n_pending = conn.execute(
                    f"SELECT COUNT(*) FROM listings_enriched "
                    f"WHERE {name}_source='UNKNOWN-pending'"
                ).fetchone()[0]
                n_total = conn.execute(
                    "SELECT COUNT(*) FROM listings_enriched"
                ).fetchone()[0]
                if n_pending != n_total:
                    print(f"[WARN] migrate_pass2b_columns: expected={n_total} rows "
                          f"with {name}_source='UNKNOWN-pending', got={n_pending}, "
                          f"fallback=continue", flush=True)
                stats[f"{name}_pending_count"] = n_pending

        return stats
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print ALTER statements without executing.")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"[ERROR] DB not found at {args.db}", file=sys.stderr)
        return 2

    stats = run(args.db, args.dry_run)
    print("Migration complete:" if not args.dry_run else "Migration dry-run:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
