"""Field-level write helpers — every update goes through write_field.

Invariants enforced here (cheap vs. paying the cost later):
- value is never the empty string (fail-loud)
- source is in VALID_SOURCES
- 0.0 <= confidence <= 1.0
"""
from __future__ import annotations

import sqlite3
from typing import Final

from enrichment.common.sources import VALID_SOURCES

UNKNOWN_VALUE: Final[str] = "UNKNOWN"


def coerce_to_filled(value: object) -> str | None:
    """Return the string form suitable for `{field}_filled`, or None if null-equivalent.

    None / NaN / "" / "NULL" / "nicht verfügbar" / "<missing area>" -> None.
    True/False -> "1"/"0" (consistent with integer feature flags in listings).
    """
    if value is None:
        return None
    if isinstance(value, bool):  # must precede int check
        return "1" if value else "0"
    if isinstance(value, float):
        import math
        if math.isnan(value):
            return None
        # drop trailing .0 for cleaner string; keep decimal if meaningful
        return str(int(value)) if value.is_integer() else str(value)
    if isinstance(value, int):
        return str(value)
    text = str(value).strip()
    if not text:
        return None
    if text.upper() == "NULL":
        return None
    # CSV sentinels we've seen during profiling (REPORT §6 L129)
    if text.lower() in {"nicht verfügbar", "<missing area>", "none"}:
        return None
    return text


def write_field(
    conn: sqlite3.Connection,
    *,
    listing_id: str,
    field: str,
    filled: str,
    source: str,
    confidence: float,
    raw: str | None = None,
) -> None:
    """Update the four provenance columns for one field on one row.

    Assumes the row already exists in listings_enriched (pass 0 inserts it).
    """
    if not filled:
        raise ValueError(f"write_field: empty filled value for {listing_id=} {field=}")
    if source not in VALID_SOURCES:
        raise ValueError(f"write_field: unknown source {source!r} for {field=}")
    if not (0.0 <= confidence <= 1.0):
        raise ValueError(f"write_field: confidence {confidence} out of [0,1] for {field=}")

    conn.execute(
        f"""UPDATE listings_enriched
            SET {field}_filled = ?,
                {field}_source = ?,
                {field}_confidence = ?,
                {field}_raw = ?
            WHERE listing_id = ?;""",
        (filled, source, float(confidence), raw, listing_id),
    )
