"""End-to-end smoke: install + migrate the real teammate bundle from the repo.

Runs against the actual ``datathon2026_dataset/listings.db.gz`` if present,
drops the DB to a tmp path so we do not disturb the developer's local cache.
No torch required.
"""
from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest

pytestmark = pytest.mark.smoke


REPO_ROOT = Path(__file__).resolve().parents[2]
BUNDLE_DIR = REPO_ROOT / "datathon2026_dataset"
LANDMARKS = REPO_ROOT / "data" / "ranking" / "landmarks.json"


@pytest.fixture(scope="module")
def installed_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    if not (BUNDLE_DIR / "listings.db.gz").exists():
        pytest.skip(f"bundle not available at {BUNDLE_DIR}")
    if not LANDMARKS.exists():
        pytest.skip(f"landmarks.json not available at {LANDMARKS}")

    work = tmp_path_factory.mktemp("dataset")
    target = work / "listings.db"
    bundle_copy = work / "datathon2026_dataset"
    bundle_copy.mkdir()
    shutil.copy2(BUNDLE_DIR / "listings.db.gz", bundle_copy / "listings.db.gz")

    from scripts.install_dataset import ensure_installed
    from scripts.migrate_db_to_app_schema import migrate

    ensure_installed(db_path=target, bundle_dir=bundle_copy, ranking_dir=work / "ranking")
    report = migrate(target, LANDMARKS)

    # Install should have decompressed the 417 MB DB.
    assert target.exists()
    assert target.stat().st_size > 100 * 1024 * 1024
    return target


def test_three_source_tables_have_the_expected_row_count(installed_db: Path) -> None:
    with sqlite3.connect(installed_db) as conn:
        for table in ("listings", "listings_enriched", "listings_ranking_signals"):
            n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert n == 25546, f"{table} has {n} rows, expected 25546"


def test_migration_added_5_listings_columns(installed_db: Path) -> None:
    with sqlite3.connect(installed_db) as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(listings)")}
    for col in (
        "year_built", "object_category_raw", "house_number", "city_slug", "floor"
    ):
        assert col in cols


def test_city_slug_coverage(installed_db: Path) -> None:
    with sqlite3.connect(installed_db) as conn:
        populated = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE city_slug IS NOT NULL"
        ).fetchone()[0]
    assert populated >= 25400  # 99%+ coverage; allows for DROPPED_bad_data


def test_object_category_is_english_after_migration(installed_db: Path) -> None:
    with sqlite3.connect(installed_db) as conn:
        cats = {r[0] for r in conn.execute(
            "SELECT DISTINCT object_category FROM listings "
            "WHERE object_category IS NOT NULL"
        )}
    assert "apartment" in cats
    # No lingering German terms.
    assert not any("Wohnung" in c for c in cats)


def test_fts_match_on_common_german_term(installed_db: Path) -> None:
    with sqlite3.connect(installed_db) as conn:
        hits = conn.execute(
            "SELECT COUNT(*) FROM listings_fts WHERE listings_fts MATCH 'Wohnung'"
        ).fetchone()[0]
    assert hits >= 4000


def test_landmark_distance_columns_populated(installed_db: Path) -> None:
    with sqlite3.connect(installed_db) as conn:
        # 30 columns and each populated on the 23,909 listings with lat/lon.
        cols = {r[1] for r in conn.execute(
            "PRAGMA table_info(listings_ranking_signals)"
        )}
        landmark_cols = [c for c in cols if c.startswith("dist_landmark_")]
        assert len(landmark_cols) >= 20  # allow for lake/oldtown dup-keys
        # Spot-check ETH and Zurich HB (teammate gazetteer uses hb_<city> keys):
        for c in ("dist_landmark_eth_zentrum_m", "dist_landmark_hb_zurich_m"):
            n = conn.execute(
                f"SELECT COUNT(*) FROM listings_ranking_signals WHERE {c} IS NOT NULL"
            ).fetchone()[0]
            assert n >= 20000, f"{c}: only {n} populated"


def test_data_quality_columns_populated(installed_db: Path) -> None:
    with sqlite3.connect(installed_db) as conn:
        clamped = conn.execute(
            "SELECT COUNT(*) FROM listings_ranking_signals "
            "WHERE nearest_stop_lines_count_clamped IS NOT NULL"
        ).fetchone()[0]
        suspect = conn.execute(
            "SELECT COUNT(*) FROM listings_ranking_signals "
            "WHERE price_plausibility = 'suspect'"
        ).fetchone()[0]
    assert clamped >= 20000
    # Per the STATUS doc we expect ~25 suspects; allow for drift.
    assert 0 < suspect < 200
