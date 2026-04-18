"""Integration tests for pass 0 — runs against the full 22,819-row DB.

These tests require the real raw_data/ (4 CSVs) to be present at repo root.
They are slow (~30s DB bootstrap) but are the key correctness gate.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from enrichment.common.sources import ORIGINAL, UNKNOWN_PENDING
from enrichment.schema import FIELDS


def _connect(db_path: Path) -> sqlite3.Connection:
    c = sqlite3.connect(str(db_path))
    c.row_factory = sqlite3.Row
    return c


def test_listings_enriched_table_exists(enriched_db_pass0: Path):
    with _connect(enriched_db_pass0) as conn:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='listings_enriched';"
        ).fetchone()
    assert row is not None


def test_row_count_matches_listings(enriched_db_pass0: Path):
    with _connect(enriched_db_pass0) as conn:
        n_listings = conn.execute("SELECT COUNT(*) FROM listings;").fetchone()[0]
        n_enriched = conn.execute("SELECT COUNT(*) FROM listings_enriched;").fetchone()[0]
    assert n_enriched == n_listings, f"coverage gap: {n_listings - n_enriched} listings missing enrichment rows"


def test_every_listing_id_has_exactly_one_enriched_row(enriched_db_pass0: Path):
    with _connect(enriched_db_pass0) as conn:
        orphans = conn.execute("""
            SELECT COUNT(*) FROM listings l
            LEFT JOIN listings_enriched le USING(listing_id)
            WHERE le.listing_id IS NULL;
        """).fetchone()[0]
        dupes = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT listing_id, COUNT(*) c FROM listings_enriched GROUP BY listing_id HAVING c > 1
            );
        """).fetchone()[0]
    assert orphans == 0, f"{orphans} listings missing an enriched row"
    assert dupes == 0, f"{dupes} duplicate listing_ids in listings_enriched"


def test_zero_null_invariant_after_pass0(enriched_db_pass0: Path):
    """Every *_filled column must be NON-NULL after pass 0."""
    with _connect(enriched_db_pass0) as conn:
        failures = []
        for f in FIELDS:
            n = conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched WHERE {f.name}_filled IS NULL;"
            ).fetchone()[0]
            if n > 0:
                failures.append((f.name, n))
    assert not failures, f"null _filled values after pass 0: {failures}"


def test_source_values_are_from_known_set(enriched_db_pass0: Path):
    from enrichment.common.sources import VALID_SOURCES
    with _connect(enriched_db_pass0) as conn:
        for f in FIELDS:
            rows = conn.execute(
                f"SELECT DISTINCT {f.name}_source FROM listings_enriched;"
            ).fetchall()
            for r in rows:
                assert r[0] in VALID_SOURCES, f"unknown source {r[0]!r} in {f.name}_source"


def test_confidence_bounds(enriched_db_pass0: Path):
    with _connect(enriched_db_pass0) as conn:
        for f in FIELDS:
            oob = conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched "
                f"WHERE {f.name}_confidence < 0.0 OR {f.name}_confidence > 1.0;"
            ).fetchone()[0]
            assert oob == 0, f"{f.name} has {oob} out-of-bounds confidence values"


def test_original_confidence_is_1_pending_is_0(enriched_db_pass0: Path):
    with _connect(enriched_db_pass0) as conn:
        for f in FIELDS:
            wrong_original = conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched "
                f"WHERE {f.name}_source='{ORIGINAL}' AND {f.name}_confidence != 1.0;"
            ).fetchone()[0]
            wrong_pending = conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched "
                f"WHERE {f.name}_source='{UNKNOWN_PENDING}' AND {f.name}_confidence != 0.0;"
            ).fetchone()[0]
            assert wrong_original == 0, f"{f.name}: {wrong_original} rows with 'original' source but confidence != 1.0"
            assert wrong_pending == 0, f"{f.name}: {wrong_pending} rows with '{UNKNOWN_PENDING}' source but confidence != 0.0"


@pytest.mark.parametrize(
    "field_name",
    ["city", "canton", "postal_code", "street"],
)
def test_sred_addresses_all_pending_after_pass0(enriched_db_pass0: Path, field_name: str):
    """REPORT §3 L43: SRED ships 100% null city/canton/postal/street.

    After pass 0 (no reverse-geocode yet), every SRED row's {field}_source must be
    UNKNOWN-pending (pass 1 will fill them).
    """
    with _connect(enriched_db_pass0) as conn:
        n_sred = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE scrape_source='SRED';"
        ).fetchone()[0]
        n_pending = conn.execute(f"""
            SELECT COUNT(*) FROM listings_enriched le
            JOIN listings l USING(listing_id)
            WHERE l.scrape_source='SRED' AND le.{field_name}_source=?;
        """, (UNKNOWN_PENDING,)).fetchone()[0]
    assert n_pending == n_sred, (
        f"SRED {field_name}: expected all {n_sred} rows pending after pass 0, "
        f"got {n_pending}. Drift from REPORT §3 L43 — check listing_row_parser."
    )


def test_non_sred_city_mostly_original(enriched_db_pass0: Path):
    """COMPARIS + ROBINREAL ship structured addresses, so ≥ 95% of their rows should
    have city_source='original'. A regression in listing_row_parser (or a schema
    change in a CSV) would push this rate down.
    """
    with _connect(enriched_db_pass0) as conn:
        n_non_sred = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE scrape_source != 'SRED';"
        ).fetchone()[0]
        n_pending = conn.execute(f"""
            SELECT COUNT(*) FROM listings_enriched le
            JOIN listings l USING(listing_id)
            WHERE l.scrape_source != 'SRED' AND le.city_source=?;
        """, (UNKNOWN_PENDING,)).fetchone()[0]
    assert n_non_sred > 0, "no non-SRED rows in fixture — unexpected"
    pct_pending = n_pending / n_non_sred
    assert pct_pending <= 0.05, (
        f"non-SRED city pending rate {pct_pending:.1%} exceeds 5% — possible parser regression"
    )


def test_sred_features_all_pending_after_pass0(enriched_db_pass0: Path):
    """REPORT §7 L145: SRED ships no orig_data → all 12 feature flags 100% null.
    Pass 2 will derive them from description text; after pass 0 they must be pending.
    """
    feature_names = [f.name for f in FIELDS if f.name.startswith("feature_")]
    assert len(feature_names) == 12, f"expected 12 feature flags, got {len(feature_names)}"
    with _connect(enriched_db_pass0) as conn:
        n_sred = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE scrape_source='SRED';"
        ).fetchone()[0]
        for feat in feature_names:
            n_pending = conn.execute(f"""
                SELECT COUNT(*) FROM listings_enriched le
                JOIN listings l USING(listing_id)
                WHERE l.scrape_source='SRED' AND le.{feat}_source=?;
            """, (UNKNOWN_PENDING,)).fetchone()[0]
            assert n_pending == n_sred, (
                f"SRED {feat}: expected all {n_sred} pending after pass 0, got {n_pending}"
            )


def test_original_counts_are_complementary(enriched_db_pass0: Path):
    """For every field, original + pending = total rows (no other sources at pass 0)."""
    with _connect(enriched_db_pass0) as conn:
        total = conn.execute("SELECT COUNT(*) FROM listings_enriched;").fetchone()[0]
        for f in FIELDS:
            orig = conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched WHERE {f.name}_source='{ORIGINAL}';"
            ).fetchone()[0]
            pending = conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched WHERE {f.name}_source='{UNKNOWN_PENDING}';"
            ).fetchone()[0]
            assert orig + pending == total, (
                f"{f.name}: original({orig}) + pending({pending}) != total({total}). "
                "Pass 0 should emit only these two sources."
            )


def test_idempotent_rerun_does_not_corrupt(enriched_db_pass0: Path):
    """Running pass 0 again must not duplicate rows or change values."""
    from enrichment.scripts.pass0_create_table import run

    with _connect(enriched_db_pass0) as conn:
        before = conn.execute("SELECT COUNT(*) FROM listings_enriched;").fetchone()[0]
        sample_before = conn.execute(
            "SELECT city_filled, city_source FROM listings_enriched ORDER BY listing_id LIMIT 5;"
        ).fetchall()

    run(enriched_db_pass0)

    with _connect(enriched_db_pass0) as conn:
        after = conn.execute("SELECT COUNT(*) FROM listings_enriched;").fetchone()[0]
        sample_after = conn.execute(
            "SELECT city_filled, city_source FROM listings_enriched ORDER BY listing_id LIMIT 5;"
        ).fetchall()

    assert before == after, f"rerun changed row count: {before} -> {after}"
    assert [tuple(r) for r in sample_before] == [tuple(r) for r in sample_after], \
        "rerun mutated existing rows (should be INSERT OR IGNORE)"
