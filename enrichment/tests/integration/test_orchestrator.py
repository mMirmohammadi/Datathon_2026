"""Integration tests for the `enrich_all` orchestrator (end-to-end minus pass 1b HTTP)."""
from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest

from enrichment.common.sources import FINAL_SOURCES, UNKNOWN_PENDING
from enrichment.schema import FIELDS


def _connect(db_path: Path) -> sqlite3.Connection:
    c = sqlite3.connect(str(db_path))
    c.row_factory = sqlite3.Row
    return c


@pytest.fixture(scope="module")
def fully_enriched_db(tmp_path_factory, base_db) -> Path:
    """Full pipeline with --skip-1b (avoids live Nominatim calls in CI)."""
    from enrichment.scripts.enrich_all import run

    dst = tmp_path_factory.mktemp("enrich_all") / "listings.db"
    shutil.copy(str(base_db), str(dst))
    run(dst, skip_pass1b=True)
    return dst


def test_pipeline_completes(fully_enriched_db: Path):
    """If enrich_all.run returns normally, the assert_no_nulls post-condition passed."""
    assert fully_enriched_db.exists()


def test_every_filled_is_non_null(fully_enriched_db: Path):
    with _connect(fully_enriched_db) as conn:
        failures = []
        for f in FIELDS:
            n = conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched WHERE {f.name}_filled IS NULL;"
            ).fetchone()[0]
            if n:
                failures.append((f.name, n))
    assert not failures, f"NULLs after full pipeline: {failures}"


def test_no_unknown_pending_remains(fully_enriched_db: Path):
    with _connect(fully_enriched_db) as conn:
        pending = []
        for f in FIELDS:
            n = conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched WHERE {f.name}_source = ?;",
                (UNKNOWN_PENDING,),
            ).fetchone()[0]
            if n:
                pending.append((f.name, n))
    assert not pending, f"UNKNOWN-pending after pass 3: {pending}"


def test_every_source_is_final(fully_enriched_db: Path):
    with _connect(fully_enriched_db) as conn:
        for f in FIELDS:
            rows = conn.execute(
                f"SELECT DISTINCT {f.name}_source FROM listings_enriched;"
            ).fetchall()
            for r in rows:
                assert r[0] in FINAL_SOURCES, f"{f.name}: non-final source {r[0]!r}"


def test_rerun_does_not_change_counts(fully_enriched_db: Path):
    """Running enrich_all twice must not change the row counts or the source distribution."""
    from enrichment.scripts.enrich_all import run

    with _connect(fully_enriched_db) as conn:
        before = {}
        for f in FIELDS:
            rows = conn.execute(
                f"SELECT {f.name}_source, COUNT(*) FROM listings_enriched GROUP BY 1;"
            ).fetchall()
            before[f.name] = dict(rows)
        before_rowcount = conn.execute("SELECT COUNT(*) FROM listings_enriched;").fetchone()[0]

    run(fully_enriched_db, skip_pass1b=True)

    with _connect(fully_enriched_db) as conn:
        after = {}
        for f in FIELDS:
            rows = conn.execute(
                f"SELECT {f.name}_source, COUNT(*) FROM listings_enriched GROUP BY 1;"
            ).fetchall()
            after[f.name] = dict(rows)
        after_rowcount = conn.execute("SELECT COUNT(*) FROM listings_enriched;").fetchone()[0]

    assert before_rowcount == after_rowcount
    for f in FIELDS:
        assert before[f.name] == after[f.name], f"{f.name} source distribution changed on rerun"


def test_returns_stats_dict(base_db, tmp_path):
    from enrichment.scripts.enrich_all import run

    dst = tmp_path / "listings.db"
    shutil.copy(str(base_db), str(dst))
    result = run(dst, skip_pass1b=True)
    assert "pass0" in result
    assert "pass1a" in result
    assert "pass2" in result
    assert "pass3" in result
    assert "drop_bad_rows" in result
    assert "total_seconds" in result
    assert isinstance(result["total_seconds"], float)
