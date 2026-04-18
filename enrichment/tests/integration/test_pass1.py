"""Integration tests for pass 1 — runs against the full live DB.

Tests pass 0 → pass 1 in sequence and asserts that:
  * SRED rows (11,105) get city + canton from reverse_geocoder
  * Existing 'original' values are NOT overwritten
  * Canton codes are from the valid 26-entry set
  * Pass 1 is idempotent
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from enrichment.common.cantons import ADMIN1_TO_CANTON_CODE
from enrichment.common.sources import (
    DROPPED_BAD_DATA,
    ORIGINAL,
    REV_GEO_OFFLINE,
    UNKNOWN_PENDING,
)


def _connect(db_path: Path) -> sqlite3.Connection:
    c = sqlite3.connect(str(db_path))
    c.row_factory = sqlite3.Row
    return c


@pytest.fixture
def enriched_db_pass1(enriched_db_pass0: Path) -> Path:
    """pass 0 + pass 1a applied."""
    from enrichment.scripts.pass1_geocode import run
    run(enriched_db_pass0)
    return enriched_db_pass0


def test_pass1_fills_sred_canton(enriched_db_pass1: Path):
    """After pass 1, SRED canton_source should mostly be 'rev_geo_offline'."""
    with _connect(enriched_db_pass1) as conn:
        n_sred = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE scrape_source='SRED';"
        ).fetchone()[0]
        n_filled = conn.execute(
            """SELECT COUNT(*) FROM listings_enriched le
               JOIN listings l USING(listing_id)
               WHERE l.scrape_source='SRED' AND le.canton_source=?;""",
            (REV_GEO_OFFLINE,),
        ).fetchone()[0]
        n_still_pending = conn.execute(
            """SELECT COUNT(*) FROM listings_enriched le
               JOIN listings l USING(listing_id)
               WHERE l.scrape_source='SRED' AND le.canton_source=?;""",
            (UNKNOWN_PENDING,),
        ).fetchone()[0]
        n_dropped = conn.execute(
            """SELECT COUNT(*) FROM listings_enriched le
               JOIN listings l USING(listing_id)
               WHERE l.scrape_source='SRED' AND le.canton_source=?;""",
            (DROPPED_BAD_DATA,),
        ).fetchone()[0]
    # Filled + (still pending) + dropped should equal n_sred.
    # Still-pending allowed only if SRED has a lat/lng outside CH or admin1 unmapped.
    assert n_filled + n_still_pending + n_dropped == n_sred
    # Expect ≥ 95% filled (SRED is all-Swiss by construction).
    fill_rate = n_filled / n_sred
    assert fill_rate >= 0.95, f"SRED canton fill rate {fill_rate:.1%} < 95%"


def test_pass1_fills_sred_city(enriched_db_pass1: Path):
    with _connect(enriched_db_pass1) as conn:
        n_sred = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE scrape_source='SRED';"
        ).fetchone()[0]
        n_filled = conn.execute(
            """SELECT COUNT(*) FROM listings_enriched le
               JOIN listings l USING(listing_id)
               WHERE l.scrape_source='SRED' AND le.city_source=?;""",
            (REV_GEO_OFFLINE,),
        ).fetchone()[0]
    assert n_filled / n_sred >= 0.95


def test_pass1_does_not_overwrite_original_values(enriched_db_pass1: Path):
    """Every non-SRED row had city_source='original' after pass 0; that must survive pass 1."""
    with _connect(enriched_db_pass1) as conn:
        n_non_sred = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE scrape_source != 'SRED';"
        ).fetchone()[0]
        n_original_after = conn.execute(
            """SELECT COUNT(*) FROM listings_enriched le
               JOIN listings l USING(listing_id)
               WHERE l.scrape_source != 'SRED' AND le.city_source=?;""",
            (ORIGINAL,),
        ).fetchone()[0]
    # ≥95% of non-SRED rows must still be 'original' (same bar as pass 0 invariant).
    assert n_original_after / n_non_sred >= 0.95


def test_all_filled_canton_codes_are_in_valid_set(enriched_db_pass1: Path):
    valid_codes = set(ADMIN1_TO_CANTON_CODE.values())
    with _connect(enriched_db_pass1) as conn:
        rows = conn.execute(
            """SELECT DISTINCT canton_filled FROM listings_enriched
               WHERE canton_source=?;""",
            (REV_GEO_OFFLINE,),
        ).fetchall()
    bad = [r[0] for r in rows if r[0] not in valid_codes]
    assert not bad, f"Pass 1 emitted invalid canton codes: {bad}"


def test_all_filled_city_names_non_empty(enriched_db_pass1: Path):
    with _connect(enriched_db_pass1) as conn:
        empties = conn.execute(
            """SELECT COUNT(*) FROM listings_enriched
               WHERE city_source=? AND (city_filled IS NULL OR city_filled='');""",
            (REV_GEO_OFFLINE,),
        ).fetchone()[0]
    assert empties == 0


def test_canton_raw_snippet_is_the_admin1_string(enriched_db_pass1: Path):
    """For auditability, canton_raw should equal the admin1 string rg returned."""
    with _connect(enriched_db_pass1) as conn:
        row = conn.execute(
            """SELECT canton_raw, canton_filled FROM listings_enriched
               WHERE canton_source=? LIMIT 1;""",
            (REV_GEO_OFFLINE,),
        ).fetchone()
    assert row is not None
    assert row["canton_raw"] in ADMIN1_TO_CANTON_CODE
    assert ADMIN1_TO_CANTON_CODE[row["canton_raw"]] == row["canton_filled"]


def test_pass1_idempotent(enriched_db_pass1: Path):
    """Running pass 1 a second time changes nothing."""
    from enrichment.scripts.pass1_geocode import run

    with _connect(enriched_db_pass1) as conn:
        before_canton_counts = dict(conn.execute(
            "SELECT canton_source, COUNT(*) FROM listings_enriched GROUP BY canton_source;"
        ).fetchall())
        before_sample = conn.execute(
            """SELECT listing_id, canton_filled, canton_source, canton_confidence
               FROM listings_enriched WHERE canton_source=?
               ORDER BY listing_id LIMIT 10;""",
            (REV_GEO_OFFLINE,),
        ).fetchall()

    run(enriched_db_pass1)

    with _connect(enriched_db_pass1) as conn:
        after_canton_counts = dict(conn.execute(
            "SELECT canton_source, COUNT(*) FROM listings_enriched GROUP BY canton_source;"
        ).fetchall())
        after_sample = conn.execute(
            """SELECT listing_id, canton_filled, canton_source, canton_confidence
               FROM listings_enriched WHERE canton_source=?
               ORDER BY listing_id LIMIT 10;""",
            (REV_GEO_OFFLINE,),
        ).fetchall()

    assert before_canton_counts == after_canton_counts
    assert [tuple(r) for r in before_sample] == [tuple(r) for r in after_sample]


def test_zero_null_invariant_still_holds(enriched_db_pass1: Path):
    """After pass 1, the no-NULL _filled invariant from pass 0 must still hold."""
    from enrichment.schema import FIELDS
    with _connect(enriched_db_pass1) as conn:
        failures = []
        for f in FIELDS:
            n = conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched WHERE {f.name}_filled IS NULL;"
            ).fetchone()[0]
            if n:
                failures.append((f.name, n))
    assert not failures, f"pass 1 introduced NULLs in: {failures}"


def test_confidence_values_match_module_constants(enriched_db_pass1: Path):
    from enrichment.scripts.pass1_geocode import CANTON_CONFIDENCE, CITY_CONFIDENCE
    with _connect(enriched_db_pass1) as conn:
        bad_canton = conn.execute(
            """SELECT COUNT(*) FROM listings_enriched
               WHERE canton_source=? AND canton_confidence != ?;""",
            (REV_GEO_OFFLINE, CANTON_CONFIDENCE),
        ).fetchone()[0]
        bad_city = conn.execute(
            """SELECT COUNT(*) FROM listings_enriched
               WHERE city_source=? AND city_confidence != ?;""",
            (REV_GEO_OFFLINE, CITY_CONFIDENCE),
        ).fetchone()[0]
    assert bad_canton == 0
    assert bad_city == 0
