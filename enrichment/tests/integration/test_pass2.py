"""Integration tests for pass 2 — regex extraction from description text.

Expects pass 0 + pass 1 to have run first (the enriched_db_pass2 fixture
chains them automatically).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from enrichment.common.sources import UNKNOWN_PENDING
from enrichment.schema import FIELDS


def _connect(db_path: Path) -> sqlite3.Connection:
    c = sqlite3.connect(str(db_path))
    c.row_factory = sqlite3.Row
    return c


@pytest.fixture(scope="module")
def enriched_db_pass2(tmp_path_factory, base_db) -> Path:
    """base_db + pass 0 + pass 1 + pass 2. Module-scoped for speed (pass 2 is
    the heaviest pass and its output is read-only in tests)."""
    import sqlite3
    from enrichment.scripts.pass0_create_table import run as pass0_run
    from enrichment.scripts.pass1_geocode import run as pass1_run
    from enrichment.scripts.pass2_text_extract import run as pass2_run

    # Work on a COPY of base_db so we don't pollute the other tests' state.
    import shutil
    dst = tmp_path_factory.mktemp("enriched_pass2") / "listings.db"
    shutil.copy(str(base_db), str(dst))

    pass0_run(dst)
    pass1_run(dst)
    pass2_run(dst)
    return dst


VALID_TEXT_REGEX_SOURCES = frozenset({
    "text_regex_de", "text_regex_fr", "text_regex_it", "text_regex_en",
})


def test_zero_null_invariant_still_holds(enriched_db_pass2: Path):
    """After 3 passes, no _filled column has SQL NULLs."""
    with _connect(enriched_db_pass2) as conn:
        failures = []
        for f in FIELDS:
            n = conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched WHERE {f.name}_filled IS NULL;"
            ).fetchone()[0]
            if n:
                failures.append((f.name, n))
    assert not failures, f"NULLs appeared in pass 2: {failures}"


def test_pass2_fills_sred_features(enriched_db_pass2: Path):
    """SRED has no structured feature flags → pass 2 is the only way to fill them.
    At least one feature must have meaningful coverage on SRED rows (>1%)."""
    with _connect(enriched_db_pass2) as conn:
        sred_rows = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE scrape_source='SRED';"
        ).fetchone()[0]

        coverage = {}
        for feat in ("balcony", "elevator", "parking", "garage", "fireplace"):
            n_filled = conn.execute(f"""
                SELECT COUNT(*) FROM listings_enriched le
                JOIN listings l USING(listing_id)
                WHERE l.scrape_source='SRED'
                  AND le.feature_{feat}_source LIKE 'text_regex_%';
            """).fetchone()[0]
            coverage[feat] = n_filled / sred_rows if sred_rows else 0

    # Swiss real-estate descriptions mention balcony VERY often. If we hit <1%
    # something is wrong with the patterns or the language detection.
    assert coverage["balcony"] > 0.10, f"SRED balcony coverage only {coverage['balcony']:.1%}"
    # At least 3 of the 5 main features should have >5% coverage on SRED.
    above_5pct = sum(1 for v in coverage.values() if v > 0.05)
    assert above_5pct >= 3, f"Only {above_5pct}/5 features have >5% SRED coverage: {coverage}"


def test_pass2_does_not_overwrite_original(enriched_db_pass2: Path):
    """If a row had a non-NULL feature_* in `listings`, its listings_enriched
    source must stay 'original'. Pass 2 only writes when source='UNKNOWN-pending'.
    """
    with _connect(enriched_db_pass2) as conn:
        for feat in ("balcony", "elevator", "parking", "garage", "fireplace",
                     "child_friendly", "pets_allowed", "temporary", "new_build",
                     "wheelchair_accessible", "private_laundry", "minergie_certified"):
            violations = conn.execute(f"""
                SELECT COUNT(*) FROM listings_enriched le
                JOIN listings l USING(listing_id)
                WHERE le.feature_{feat}_source LIKE 'text_regex_%'
                  AND l.feature_{feat} IS NOT NULL;
            """).fetchone()[0]
            assert violations == 0, (
                f"feature_{feat}: {violations} rows had non-NULL structured flag but "
                f"pass 2 wrote a text_regex_* source. Pass 2 must respect UNKNOWN-pending gate."
            )


def test_pass2_sources_in_valid_set(enriched_db_pass2: Path):
    """All feature_*, year_built, agency_* sources written by pass 2 must be text_regex_{lang}."""
    with _connect(enriched_db_pass2) as conn:
        for col in ("feature_balcony_source", "feature_elevator_source",
                    "year_built_source", "agency_phone_source", "agency_email_source"):
            rows = conn.execute(f"""
                SELECT DISTINCT {col} FROM listings_enriched
                WHERE {col} LIKE 'text_regex_%';
            """).fetchall()
            for r in rows:
                assert r[0] in VALID_TEXT_REGEX_SOURCES, f"Unexpected text_regex source: {r[0]}"


def test_year_built_values_are_plausible(enriched_db_pass2: Path):
    with _connect(enriched_db_pass2) as conn:
        rows = conn.execute("""
            SELECT year_built_filled FROM listings_enriched
            WHERE year_built_source LIKE 'text_regex_%';
        """).fetchall()
    for r in rows:
        try:
            y = int(r[0])
        except ValueError:
            raise AssertionError(f"year_built not a year: {r[0]!r}")
        assert 1800 <= y <= 2030, f"implausible year_built: {y}"


def test_agency_phone_normalized_to_e164(enriched_db_pass2: Path):
    with _connect(enriched_db_pass2) as conn:
        rows = conn.execute("""
            SELECT agency_phone_filled FROM listings_enriched
            WHERE agency_phone_source LIKE 'text_regex_%'
            LIMIT 20;
        """).fetchall()
    for r in rows:
        v = r[0]
        assert v.startswith("+41 "), f"phone not E.164: {v!r}"


def test_agency_email_lowercased(enriched_db_pass2: Path):
    with _connect(enriched_db_pass2) as conn:
        bad = conn.execute("""
            SELECT COUNT(*) FROM listings_enriched
            WHERE agency_email_source LIKE 'text_regex_%'
            AND agency_email_filled != LOWER(agency_email_filled);
        """).fetchone()[0]
    assert bad == 0


def test_confidence_in_0_1(enriched_db_pass2: Path):
    with _connect(enriched_db_pass2) as conn:
        for col in ("feature_balcony_confidence", "year_built_confidence",
                    "agency_phone_confidence", "agency_email_confidence"):
            oob = conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched WHERE {col} < 0.0 OR {col} > 1.0;"
            ).fetchone()[0]
            assert oob == 0, f"{col}: {oob} out-of-bounds values"


def test_idempotent_rerun(enriched_db_pass2: Path):
    from enrichment.scripts.pass2_text_extract import run

    with _connect(enriched_db_pass2) as conn:
        before = conn.execute("""
            SELECT COUNT(*) FROM listings_enriched
            WHERE feature_balcony_source LIKE 'text_regex_%';
        """).fetchone()[0]

    run(enriched_db_pass2)

    with _connect(enriched_db_pass2) as conn:
        after = conn.execute("""
            SELECT COUNT(*) FROM listings_enriched
            WHERE feature_balcony_source LIKE 'text_regex_%';
        """).fetchone()[0]

    # Pass 2 re-run: already-filled rows now have source='text_regex_*' (not pending),
    # so the gate blocks them. Count should stay identical.
    assert before == after
