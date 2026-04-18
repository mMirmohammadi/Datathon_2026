"""Shared pytest fixtures for enrichment tests.

Mirrors the `build_database(tmp_path)` idiom from tests/test_hard_filters.py
so enrichment tests run against a fresh SQLite built from the repo's raw_data/.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.harness.bootstrap import bootstrap_database

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def base_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Bootstrap `listings` from the real CSVs in raw_data/ into a session-scoped temp SQLite.

    Session-scoped to avoid rebuilding the 25k-row DB for every integration test.
    Individual tests MUST NOT mutate `listings`; they can freely read it and mutate
    `listings_enriched` (which is torn down per test via the `enriched_db_pass0` fixture).
    """
    db_path = tmp_path_factory.mktemp("enrichment_db") / "listings.db"
    bootstrap_database(db_path=db_path, raw_data_dir=REPO_ROOT / "raw_data")
    return db_path


@pytest.fixture
def enriched_db_pass0(base_db: Path) -> Path:
    """Per-test: drop any prior listings_enriched, re-run pass 0.

    Keeps tests isolated (each gets a fresh listings_enriched) while reusing the
    session-scoped listings table.
    """
    import sqlite3

    from enrichment.scripts.pass0_create_table import run

    with sqlite3.connect(str(base_db)) as conn:
        conn.execute("DROP TABLE IF EXISTS listings_enriched;")
        conn.commit()
    run(base_db)
    return base_db
