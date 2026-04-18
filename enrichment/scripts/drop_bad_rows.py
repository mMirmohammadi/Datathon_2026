"""Mark bogus field values with `DROPPED_bad_data` source per REPORT.md.

Three classes of known-bad values (all empirically identified in analysis/REPORT.md):

  1. price ∈ [1, 199] CHF  → "parking/garage/commercial leaking into residential
     channel" (REPORT §6 L117: 1,331 rows). Mark price as dropped.
  2. price > 50,000 CHF     → "clearly bogus" (REPORT §11 L244: 14 rows > 50k,
     4 > 100k, plus the sentinel 1,111,111).
  3. rooms = 0             → 959 rows in struct_noi, "all parking/garage/
     commercial" (REPORT §6 L123). Mark rooms as dropped.

Conservative: we only mark the known-bogus FIELD, not the whole row. Other
fields on the same listing are unaffected — some of those rows may still have
valid descriptions or images the ranker can use.

Usage:
    python -m enrichment.scripts.drop_bad_rows --db /data/listings.db
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from enrichment.common.db import connect
from enrichment.common.provenance import UNKNOWN_VALUE, write_field
from enrichment.common.sources import DROPPED_BAD_DATA


def _listing_ids_price_too_low(conn) -> list[tuple[str, str]]:
    """price between 1 and 199 CHF inclusive (price=0 is treated as 'not provided').

    Returns (listing_id, original_price_str) so we can preserve the pre-drop value
    in `_raw` for audit.
    """
    rows = conn.execute("""
        SELECT listing_id, price FROM listings
        WHERE price IS NOT NULL AND price > 0 AND price < 200;
    """).fetchall()
    return [(r["listing_id"], str(r["price"])) for r in rows]


def _listing_ids_price_too_high(conn) -> list[tuple[str, str]]:
    rows = conn.execute("""
        SELECT listing_id, price FROM listings
        WHERE price IS NOT NULL AND price > 50000;
    """).fetchall()
    return [(r["listing_id"], str(r["price"])) for r in rows]


def _listing_ids_rooms_zero(conn) -> list[tuple[str, str, str | None]]:
    """Returns (listing_id, rooms_str, price_str_or_none) for audit."""
    rows = conn.execute("""
        SELECT listing_id, rooms, price FROM listings
        WHERE rooms = 0;
    """).fetchall()
    return [
        (r["listing_id"], str(r["rooms"]), None if r["price"] is None else str(r["price"]))
        for r in rows
    ]


def _drop_field(conn, listing_id: str, field: str, reason: str, original_value: str | None = None) -> None:
    """Drop a field with audit: `raw = reason` or `reason:original_was=<value>`."""
    raw = reason if original_value is None else f"{reason}:original_was={original_value}"
    write_field(
        conn,
        listing_id=listing_id,
        field=field,
        filled=UNKNOWN_VALUE,
        source=DROPPED_BAD_DATA,
        confidence=0.0,
        raw=raw,
    )


def run(db_path: Path) -> dict[str, int]:
    conn = connect(db_path)
    try:
        stats = {
            "price_too_low": 0,
            "price_too_high": 0,
            "rooms_zero": 0,
            "total_field_drops": 0,
        }

        for listing_id, price_str in _listing_ids_price_too_low(conn):
            _drop_field(conn, listing_id, "price", "price_below_200_chf", price_str)
            stats["price_too_low"] += 1

        for listing_id, price_str in _listing_ids_price_too_high(conn):
            _drop_field(conn, listing_id, "price", "price_above_50000_chf", price_str)
            stats["price_too_high"] += 1

        for listing_id, rooms_str, price_str in _listing_ids_rooms_zero(conn):
            # Drop both rooms and price for "0-room" listings; they're usually
            # parking or garage spaces mislabeled as residential.
            _drop_field(conn, listing_id, "rooms", "rooms_zero_non_residential", rooms_str)
            _drop_field(conn, listing_id, "price", "rooms_zero_non_residential", price_str)
            stats["rooms_zero"] += 1

        stats["total_field_drops"] = (
            stats["price_too_low"] + stats["price_too_high"] + 2 * stats["rooms_zero"]
        )
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
    print("drop_bad_rows complete:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
