"""Pass 0 — create `listings_enriched` and backfill 'original' values.

Invariants established here:
  * Every listing in `listings` has exactly one row in `listings_enriched`.
  * Every `*_filled` column is non-null (uses literal 'UNKNOWN' when pending).
  * `*_source`='original' for values that were non-null at ingest; otherwise
    `*_source`='UNKNOWN-pending' (pass 1/2 overwrite; pass 3 flips to 'UNKNOWN').

Usage:
    python -m enrichment.scripts.pass0_create_table --db /data/listings.db
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from enrichment.common.db import connect, enriched_column_names, table_exists
from enrichment.common.provenance import UNKNOWN_VALUE, coerce_to_filled
from enrichment.common.sources import ORIGINAL, UNKNOWN_PENDING
from enrichment.schema import FIELDS, INDEX_SQL, EnrichedField, create_table_sql


def validate_against_listings_schema(conn) -> None:
    """Fail loudly if FIELDS references listings columns that don't exist."""
    if not table_exists(conn, "listings"):
        raise RuntimeError("listings table missing — run app.harness.bootstrap first.")
    actual = {r[1] for r in conn.execute("PRAGMA table_info(listings);").fetchall()}
    missing: list[str] = []
    for f in FIELDS:
        if f.origin == "listings_column":
            assert f.listings_column is not None  # enforced by validate_fields() at import
            if f.listings_column not in actual:
                missing.append(f"FIELDS[{f.name}] -> listings.{f.listings_column} not in schema")
    if missing:
        raise RuntimeError("Schema drift:\n  " + "\n  ".join(missing))


def _parse_raw_json(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError as e:
        # CLAUDE.md §5: announce the fallback, don't swallow
        print(f"[WARN] 00_create_enriched_table: expected=json_dict, got=invalid_json "
              f"(err={e!s}), fallback=empty_dict", flush=True)
        return {}


def _get_field_value(listing_row, raw_json: dict, field: EnrichedField):
    if field.origin == "listings_column":
        return listing_row[field.listings_column]
    # raw_json
    return raw_json.get(field.raw_json_key)


def _build_insert_sql(col_order: list[str]) -> str:
    placeholders = ",".join("?" * len(col_order))
    cols = ",".join(col_order)
    return f"INSERT OR IGNORE INTO listings_enriched ({cols}) VALUES ({placeholders});"


def _column_order() -> list[str]:
    """Order must match the order of values we generate per row."""
    cols = ["listing_id", "enriched_at"]
    for f in FIELDS:
        cols.extend([f"{f.name}_filled", f"{f.name}_source",
                     f"{f.name}_confidence", f"{f.name}_raw"])
    return cols


def _build_row(listing_row, raw_json: dict, enriched_at: str) -> tuple:
    values: list = [listing_row["listing_id"], enriched_at]
    for f in FIELDS:
        raw_val = _get_field_value(listing_row, raw_json, f)
        filled = coerce_to_filled(raw_val)
        if filled is None:
            values.extend([UNKNOWN_VALUE, UNKNOWN_PENDING, 0.0, None])
        else:
            values.extend([filled, ORIGINAL, 1.0, None])
    return tuple(values)


def run(db_path: Path, *, batch_size: int = 1000) -> dict[str, int]:
    conn = connect(db_path)
    try:
        validate_against_listings_schema(conn)

        # Create table + indexes (idempotent).
        conn.executescript(create_table_sql())
        for stmt in INDEX_SQL:
            conn.execute(stmt)
        conn.commit()

        # Sanity: every field in FIELDS has 4 columns in the table.
        actual_cols = set(enriched_column_names(conn))
        expected_cols = {"listing_id", "enriched_at"} | {
            f"{f.name}_{suffix}" for f in FIELDS
            for suffix in ("filled", "source", "confidence", "raw")
        }
        missing = expected_cols - actual_cols
        if missing:
            raise RuntimeError(f"listings_enriched missing columns: {sorted(missing)}")

        before = conn.execute("SELECT COUNT(*) FROM listings_enriched;").fetchone()[0]

        col_order = _column_order()
        sql = _build_insert_sql(col_order)
        enriched_at = datetime.now(timezone.utc).isoformat()

        total_listings = conn.execute("SELECT COUNT(*) FROM listings;").fetchone()[0]
        cursor = conn.execute("SELECT * FROM listings;")

        batch: list[tuple] = []
        for row in cursor:
            raw = _parse_raw_json(row["raw_json"] if "raw_json" in row.keys() else None)
            batch.append(_build_row(row, raw, enriched_at))
            if len(batch) >= batch_size:
                conn.executemany(sql, batch)
                batch.clear()
        if batch:
            conn.executemany(sql, batch)
        conn.commit()

        after = conn.execute("SELECT COUNT(*) FROM listings_enriched;").fetchone()[0]
        inserted = after - before

        return {
            "listings_total": total_listings,
            "enriched_before": before,
            "enriched_after": after,
            "inserted": inserted,
            "skipped_existing": total_listings - inserted,
        }
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True, help="Path to listings.db")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"[ERROR] DB not found at {args.db}", file=sys.stderr)
        return 2

    stats = run(args.db)
    print("Pass 0 complete:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
