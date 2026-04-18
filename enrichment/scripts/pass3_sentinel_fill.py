"""Pass 3 — convert every remaining `UNKNOWN-pending` source to explicit `UNKNOWN`.

This is the final pass. After it runs, no row may have `_source='UNKNOWN-pending'`
in any column covered by the FIELDS registry, and every `_filled` is non-null.

Refusal guards (both must pass before any writes):
  1. Every `<name>_filled` column present in listings_enriched must have a
     corresponding entry in `enrichment.schema.FIELDS`. A drift (new column
     not in the registry) makes this pass `raise`, not silently sentinel-fill.
  2. Every field in `FIELDS` must have the four columns present in the DB.
     A missing column (from a stale schema) also `raise`s.

Usage:
    python -m enrichment.scripts.pass3_sentinel_fill --db /data/listings.db
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from enrichment.common.db import connect, enriched_column_names, table_exists
from enrichment.common.provenance import UNKNOWN_VALUE
from enrichment.common.sources import UNKNOWN, UNKNOWN_PENDING
from enrichment.schema import FIELDS


def _check_registry_alignment(conn) -> None:
    """Raise if the DB schema and FIELDS registry disagree."""
    if not table_exists(conn, "listings_enriched"):
        raise RuntimeError(
            "listings_enriched does not exist. Run pass 0 first."
        )

    actual_cols = set(enriched_column_names(conn))
    actual_filled = {c for c in actual_cols if c.endswith("_filled")}
    expected_filled = {f"{f.name}_filled" for f in FIELDS}

    drift_in_db = actual_filled - expected_filled
    missing_in_db = expected_filled - actual_filled

    if drift_in_db:
        raise RuntimeError(
            "Schema drift: listings_enriched has _filled columns not in "
            f"enrichment.schema.FIELDS: {sorted(drift_in_db)}. "
            "Add them to FIELDS explicitly before running pass 3."
        )
    if missing_in_db:
        raise RuntimeError(
            "Schema drift: FIELDS declares fields whose _filled columns are "
            f"missing from listings_enriched: {sorted(missing_in_db)}. "
            "Re-run pass 0 to rebuild the table."
        )

    # For every registered field, the full 4-column quadruple must exist.
    for f in FIELDS:
        required = {f"{f.name}_filled", f"{f.name}_source",
                    f"{f.name}_confidence", f"{f.name}_raw"}
        missing = required - actual_cols
        if missing:
            raise RuntimeError(
                f"Field {f.name!r} in FIELDS is missing DB columns: {sorted(missing)}"
            )


def run(db_path: Path) -> dict[str, int]:
    conn = connect(db_path)
    try:
        _check_registry_alignment(conn)

        stats: dict[str, int] = {"fields_touched": 0, "total_rows_updated": 0}
        per_field: dict[str, int] = {}

        for f in FIELDS:
            # Count how many rows are still pending for this field.
            pending_n = conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched WHERE {f.name}_source = ?;",
                (UNKNOWN_PENDING,),
            ).fetchone()[0]

            if pending_n == 0:
                continue

            # Sentinel-fill: set _filled (if still 'UNKNOWN' placeholder from pass 0,
            # it stays; any other value set by pass 1/2 also stays — we only change
            # source + confidence so provenance is honest). Since pass 0 set the
            # placeholder to UNKNOWN_VALUE and later passes only write when pending,
            # every pending row already has _filled = UNKNOWN_VALUE. We update
            # defensively to guarantee the literal.
            conn.execute(
                f"""UPDATE listings_enriched
                    SET {f.name}_filled = ?,
                        {f.name}_source = ?,
                        {f.name}_confidence = 0.0
                    WHERE {f.name}_source = ?;""",
                (UNKNOWN_VALUE, UNKNOWN, UNKNOWN_PENDING),
            )
            per_field[f.name] = pending_n
            stats["fields_touched"] += 1
            stats["total_rows_updated"] += pending_n

        conn.commit()

        # Final verification: no UNKNOWN-pending must remain. CLAUDE.md §5 compliance:
        # if this somehow fails (concurrent writer, bug), fail loud rather than silent.
        for f in FIELDS:
            leftover = conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched WHERE {f.name}_source = ?;",
                (UNKNOWN_PENDING,),
            ).fetchone()[0]
            if leftover != 0:
                raise RuntimeError(
                    f"Pass 3 post-condition violated: {leftover} rows still have "
                    f"{f.name}_source='{UNKNOWN_PENDING}' after UPDATE."
                )

        stats["per_field"] = per_field  # type: ignore[assignment]
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
    print("Pass 3 complete:")
    print(f"  fields_touched: {stats['fields_touched']}")
    print(f"  total_rows_updated: {stats['total_rows_updated']}")
    if stats.get("per_field"):
        print("  per_field:")
        for name, n in sorted(stats["per_field"].items(), key=lambda kv: -kv[1]):
            print(f"    {name}: {n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
