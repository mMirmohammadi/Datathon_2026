"""Unit tests for common.provenance.coerce_to_filled + write_field guards."""
from __future__ import annotations

import sqlite3

import pytest

from enrichment.common.provenance import UNKNOWN_VALUE, coerce_to_filled, write_field
from enrichment.common.sources import ORIGINAL, UNKNOWN_PENDING, VALID_SOURCES


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, None),
        ("", None),
        ("   ", None),
        ("NULL", None),
        ("null", None),
        ("nicht verfügbar", None),
        ("<missing area>", None),
        ("none", None),
        ("Zurich", "Zurich"),
        ("Zürich", "Zürich"),
        ("  Zürich  ", "Zürich"),
        (0, "0"),
        (1, "1"),
        (3.5, "3.5"),
        (3.0, "3"),
        (True, "1"),
        (False, "0"),
        (2500, "2500"),
    ],
)
def test_coerce_to_filled(value, expected):
    assert coerce_to_filled(value) == expected


def test_coerce_to_filled_nan_is_none():
    import math
    assert coerce_to_filled(math.nan) is None


def test_write_field_happy_path():
    conn = sqlite3.connect(":memory:")
    conn.execute("""CREATE TABLE listings_enriched (
        listing_id TEXT PRIMARY KEY,
        enriched_at TEXT NOT NULL,
        city_filled TEXT NOT NULL, city_source TEXT NOT NULL,
        city_confidence REAL NOT NULL, city_raw TEXT
    );""")
    conn.execute(
        "INSERT INTO listings_enriched VALUES (?,?,?,?,?,?);",
        ("L1", "2026-04-18T00:00:00+00:00", UNKNOWN_VALUE, UNKNOWN_PENDING, 0.0, None),
    )
    write_field(conn, listing_id="L1", field="city",
                filled="Zürich", source=ORIGINAL, confidence=1.0, raw=None)
    row = conn.execute("SELECT * FROM listings_enriched WHERE listing_id='L1';").fetchone()
    assert row[2] == "Zürich"
    assert row[3] == ORIGINAL
    assert row[4] == 1.0


def test_write_field_rejects_empty_filled():
    conn = sqlite3.connect(":memory:")
    with pytest.raises(ValueError, match="empty filled"):
        write_field(conn, listing_id="L1", field="city",
                    filled="", source=ORIGINAL, confidence=1.0)


def test_write_field_rejects_unknown_source():
    conn = sqlite3.connect(":memory:")
    with pytest.raises(ValueError, match="unknown source"):
        write_field(conn, listing_id="L1", field="city",
                    filled="Zurich", source="made_up", confidence=1.0)


def test_write_field_rejects_bad_confidence():
    conn = sqlite3.connect(":memory:")
    with pytest.raises(ValueError, match="confidence"):
        write_field(conn, listing_id="L1", field="city",
                    filled="Zurich", source=ORIGINAL, confidence=1.5)
    with pytest.raises(ValueError, match="confidence"):
        write_field(conn, listing_id="L1", field="city",
                    filled="Zurich", source=ORIGINAL, confidence=-0.1)


def test_unknown_pending_is_in_valid_sources_but_not_final():
    from enrichment.common.sources import FINAL_SOURCES
    assert UNKNOWN_PENDING in VALID_SOURCES
    assert UNKNOWN_PENDING not in FINAL_SOURCES
