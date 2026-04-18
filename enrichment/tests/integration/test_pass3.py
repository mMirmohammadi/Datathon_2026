"""Integration tests for pass 3 — sentinel fill (UNKNOWN-pending → UNKNOWN).

Runs pass 0 → pass 1 → pass 2 → pass 3 and asserts the final-state contract:
  * No SQL NULL in any _filled column.
  * No `UNKNOWN-pending` left in any _source column.
  * Every registered field is either filled (real value) or explicitly UNKNOWN.
"""
from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest

from enrichment.common.sources import FINAL_SOURCES, UNKNOWN, UNKNOWN_PENDING
from enrichment.schema import FIELDS


def _connect(db_path: Path) -> sqlite3.Connection:
    c = sqlite3.connect(str(db_path))
    c.row_factory = sqlite3.Row
    return c


@pytest.fixture(scope="module")
def enriched_db_final(tmp_path_factory, base_db) -> Path:
    """base_db + pass 0 + pass 1 + pass 2 + pass 3."""
    from enrichment.scripts.pass0_create_table import run as pass0_run
    from enrichment.scripts.pass1_geocode import run as pass1_run
    from enrichment.scripts.pass2_text_extract import run as pass2_run
    from enrichment.scripts.pass3_sentinel_fill import run as pass3_run

    dst = tmp_path_factory.mktemp("enriched_final") / "listings.db"
    shutil.copy(str(base_db), str(dst))

    pass0_run(dst)
    pass1_run(dst)
    pass2_run(dst)
    pass3_run(dst)
    return dst


def test_no_unknown_pending_remaining(enriched_db_final: Path):
    with _connect(enriched_db_final) as conn:
        leftovers = []
        for f in FIELDS:
            n = conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched WHERE {f.name}_source = ?;",
                (UNKNOWN_PENDING,),
            ).fetchone()[0]
            if n:
                leftovers.append((f.name, n))
    assert not leftovers, f"pass 3 left UNKNOWN-pending in: {leftovers}"


def test_every_filled_is_non_null(enriched_db_final: Path):
    with _connect(enriched_db_final) as conn:
        failures = []
        for f in FIELDS:
            n = conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched WHERE {f.name}_filled IS NULL;"
            ).fetchone()[0]
            if n:
                failures.append((f.name, n))
    assert not failures, f"NULLs in _filled after pass 3: {failures}"


def test_every_source_is_final(enriched_db_final: Path):
    with _connect(enriched_db_final) as conn:
        for f in FIELDS:
            bad = conn.execute(
                f"SELECT DISTINCT {f.name}_source FROM listings_enriched "
                f"WHERE {f.name}_source NOT IN ({','.join('?' * len(FINAL_SOURCES))});",
                tuple(FINAL_SOURCES),
            ).fetchall()
            assert not bad, f"{f.name} has post-pass-3 source not in FINAL_SOURCES: {[r[0] for r in bad]}"


def test_sentinel_rows_have_confidence_zero(enriched_db_final: Path):
    with _connect(enriched_db_final) as conn:
        for f in FIELDS:
            bad = conn.execute(
                f"""SELECT COUNT(*) FROM listings_enriched
                    WHERE {f.name}_source = ?
                      AND {f.name}_confidence != 0.0;""",
                (UNKNOWN,),
            ).fetchone()[0]
            assert bad == 0, f"{f.name}: {bad} UNKNOWN rows have non-zero confidence"


def test_sentinel_rows_have_unknown_filled_literal(enriched_db_final: Path):
    with _connect(enriched_db_final) as conn:
        for f in FIELDS:
            bad = conn.execute(
                f"""SELECT COUNT(*) FROM listings_enriched
                    WHERE {f.name}_source = ?
                      AND {f.name}_filled != 'UNKNOWN';""",
                (UNKNOWN,),
            ).fetchone()[0]
            assert bad == 0, f"{f.name}: {bad} UNKNOWN-source rows have non-UNKNOWN _filled"


def test_pass3_idempotent(enriched_db_final: Path):
    from enrichment.scripts.pass3_sentinel_fill import run

    with _connect(enriched_db_final) as conn:
        before = conn.execute(
            "SELECT city_source, canton_source, status_source, year_built_source "
            "FROM listings_enriched ORDER BY listing_id LIMIT 50;"
        ).fetchall()
    run(enriched_db_final)
    with _connect(enriched_db_final) as conn:
        after = conn.execute(
            "SELECT city_source, canton_source, status_source, year_built_source "
            "FROM listings_enriched ORDER BY listing_id LIMIT 50;"
        ).fetchall()
    assert [tuple(r) for r in before] == [tuple(r) for r in after]


def test_pass3_rejects_schema_drift(enriched_db_final: Path, tmp_path: Path):
    """If listings_enriched has a _filled column not in FIELDS, pass 3 must raise."""
    from enrichment.scripts.pass3_sentinel_fill import run

    dst = tmp_path / "drifted.db"
    shutil.copy(str(enriched_db_final), str(dst))
    with sqlite3.connect(str(dst)) as conn:
        conn.execute("ALTER TABLE listings_enriched ADD COLUMN rogue_field_filled TEXT;")
        conn.execute("ALTER TABLE listings_enriched ADD COLUMN rogue_field_source TEXT;")
        conn.execute("ALTER TABLE listings_enriched ADD COLUMN rogue_field_confidence REAL;")
        conn.execute("ALTER TABLE listings_enriched ADD COLUMN rogue_field_raw TEXT;")
        conn.commit()

    with pytest.raises(RuntimeError, match="Schema drift"):
        run(dst)
