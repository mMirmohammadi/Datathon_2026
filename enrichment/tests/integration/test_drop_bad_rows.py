"""Integration tests for drop_bad_rows — runs pass 0 then drop_bad_rows, verifies."""
from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest

from enrichment.common.sources import DROPPED_BAD_DATA


def _connect(db_path: Path) -> sqlite3.Connection:
    c = sqlite3.connect(str(db_path))
    c.row_factory = sqlite3.Row
    return c


@pytest.fixture
def enriched_db_after_drops(tmp_path, base_db) -> Path:
    from enrichment.scripts.drop_bad_rows import run as drop_run
    from enrichment.scripts.pass0_create_table import run as pass0_run

    dst = tmp_path / "listings.db"
    shutil.copy(str(base_db), str(dst))
    pass0_run(dst)
    drop_run(dst)
    return dst


def test_price_below_200_is_dropped(enriched_db_after_drops: Path):
    """Every listing with raw price ∈ [1, 199] must have price_source='DROPPED_bad_data'.

    The specific reason string may be overwritten by a subsequent rule (e.g.
    rooms=0 writes 'rooms_zero_non_residential' on top), so the assertion
    checks the source, not the reason. Every such row must be DROPPED regardless.
    """
    with _connect(enriched_db_after_drops) as conn:
        n_expected = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE price > 0 AND price < 200;"
        ).fetchone()[0]
        n_actual = conn.execute("""
            SELECT COUNT(*) FROM listings_enriched le
            JOIN listings l USING(listing_id)
            WHERE l.price > 0 AND l.price < 200
              AND le.price_source = ?;
        """, (DROPPED_BAD_DATA,)).fetchone()[0]
    assert n_actual == n_expected, (
        f"price<200 drops: expected {n_expected} rows with price_source=DROPPED, got {n_actual}"
    )


def test_price_above_50k_is_dropped(enriched_db_after_drops: Path):
    """Every listing with price > 50k is dropped. Source check tolerates reason overlap."""
    with _connect(enriched_db_after_drops) as conn:
        n_expected = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE price > 50000;"
        ).fetchone()[0]
        n_actual = conn.execute("""
            SELECT COUNT(*) FROM listings_enriched le
            JOIN listings l USING(listing_id)
            WHERE l.price > 50000 AND le.price_source = ?;
        """, (DROPPED_BAD_DATA,)).fetchone()[0]
    assert n_actual == n_expected


def test_rooms_zero_drops_both_rooms_and_price(enriched_db_after_drops: Path):
    """rooms=0 rows must have rooms_source=DROPPED. The reason string now embeds
    the pre-drop value, so we use LIKE to match the prefix."""
    with _connect(enriched_db_after_drops) as conn:
        n_expected = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE rooms = 0;"
        ).fetchone()[0]
        n_rooms_dropped = conn.execute("""
            SELECT COUNT(*) FROM listings_enriched
            WHERE rooms_source = ? AND rooms_raw LIKE 'rooms_zero_non_residential%';
        """, (DROPPED_BAD_DATA,)).fetchone()[0]
        n_price_dropped = conn.execute("""
            SELECT COUNT(*) FROM listings_enriched
            WHERE price_source = ? AND price_raw LIKE 'rooms_zero_non_residential%';
        """, (DROPPED_BAD_DATA,)).fetchone()[0]
    assert n_rooms_dropped == n_expected
    # price may be tagged with a different reason (price<200 overlap); here we
    # only require that the rooms_zero-reason price drops land on every rooms=0 row
    # that doesn't have a competing price rule. Since rooms_zero runs LAST and
    # always writes price with the rooms_zero reason, this count equals n_expected.
    assert n_price_dropped == n_expected


def test_drops_do_not_touch_other_fields(enriched_db_after_drops: Path):
    """Dropping price/rooms must not disturb city/canton/feature_* sources."""
    with _connect(enriched_db_after_drops) as conn:
        # Sample a dropped row and check other fields are unchanged
        dropped = conn.execute("""
            SELECT listing_id FROM listings_enriched
            WHERE price_source = ?
            LIMIT 1;
        """, (DROPPED_BAD_DATA,)).fetchone()
        if dropped is None:
            pytest.skip("no dropped rows in fixture")
        lid = dropped[0]
        row = conn.execute("""
            SELECT city_source, canton_source, feature_balcony_source
            FROM listings_enriched WHERE listing_id=?;
        """, (lid,)).fetchone()
    # None of these should be DROPPED_bad_data — only price/rooms get dropped.
    assert row["city_source"] != DROPPED_BAD_DATA
    assert row["feature_balcony_source"] != DROPPED_BAD_DATA


def test_valid_residential_prices_are_not_dropped(enriched_db_after_drops: Path):
    """Rent in [200, 50000] must keep price_source='original'."""
    with _connect(enriched_db_after_drops) as conn:
        n_valid = conn.execute("""
            SELECT COUNT(*) FROM listings_enriched le
            JOIN listings l USING(listing_id)
            WHERE l.price BETWEEN 200 AND 50000
              AND l.rooms > 0
              AND le.price_source != 'original';
        """).fetchone()[0]
    assert n_valid == 0, f"{n_valid} valid residential rows had price_source != 'original'"


def test_drop_bad_rows_idempotent(enriched_db_after_drops: Path):
    from enrichment.scripts.drop_bad_rows import run

    with _connect(enriched_db_after_drops) as conn:
        before = conn.execute("""
            SELECT COUNT(*) FROM listings_enriched WHERE price_source = ?
            OR rooms_source = ?;
        """, (DROPPED_BAD_DATA, DROPPED_BAD_DATA)).fetchone()[0]
    run(enriched_db_after_drops)
    with _connect(enriched_db_after_drops) as conn:
        after = conn.execute("""
            SELECT COUNT(*) FROM listings_enriched WHERE price_source = ?
            OR rooms_source = ?;
        """, (DROPPED_BAD_DATA, DROPPED_BAD_DATA)).fetchone()[0]
    assert before == after
