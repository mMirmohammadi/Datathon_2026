from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path

from app.db import get_connection
from app.harness.csv_import import create_indexes, create_schema
from app.harness.enriched_import import import_enriched_csv
from app.harness.sred_transform import ensure_sred_normalized_csv


logger = logging.getLogger(__name__)

ENRICHED_CSV_RELATIVE = Path("sample_data_enriched") / "sample_enriched_500.csv"


def _bundle_install_allowed() -> bool:
    """Tests set ``LISTINGS_SKIP_BUNDLE_INSTALL=1`` so each tmp DB does NOT
    decompress the 417 MB teammate bundle just to run a unit test.
    """
    return os.environ.get("LISTINGS_SKIP_BUNDLE_INSTALL", "0") != "1"


def bootstrap_database(*, db_path: Path, raw_data_dir: Path) -> None:
    """Three-step startup:

    1. If ``db_path`` already exists but the schema lacks the app-owned
       columns, run the in-place migration (teammate bundle path).
    2. If ``db_path`` is missing but the teammate bundle is on disk, install
       it (gunzip + copy ranking artefacts) and migrate.
    3. Otherwise fall back to the legacy 500-row CSV importer for smoke tests
       and fresh clones without the bundle.
    """
    ensure_sred_normalized_csv(raw_data_dir)

    if db_path.exists():
        if not _schema_matches(db_path):
            if _migration_is_possible(db_path):
                logger.info(
                    "\033[36mListings DB at %s is missing app-owned columns; "
                    "running scripts.migrate_db_to_app_schema.migrate() in place.\033[0m",
                    db_path,
                )
                _run_migration(db_path)
            else:
                logger.error(
                    "\033[31mListings DB schema mismatch at %s AND the migration "
                    "source tables (listings_enriched, listings_ranking_signals) "
                    "are absent. Preserving DB untouched; remove it and rerun to "
                    "rebuild from the bundle or the legacy CSV.\033[0m",
                    db_path,
                )
        return

    # DB missing. Try the teammate bundle first; fall back to legacy CSV.
    if _bundle_install_allowed():
        try:
            from scripts.install_dataset import ensure_installed

            ensure_installed(db_path=db_path)
            _run_migration(db_path)
            return
        except FileNotFoundError as exc:
            logger.warning(
                "\033[33m[WARN] using_legacy_500_row_fallback: expected=teammate "
                "bundle at datathon2026_dataset/, got=%s, fallback=CSV import of "
                "500 enriched rows from %s\033[0m",
                exc,
                raw_data_dir / ENRICHED_CSV_RELATIVE,
            )

    enriched_csv_path = raw_data_dir / ENRICHED_CSV_RELATIVE
    if not enriched_csv_path.exists():
        raise FileNotFoundError(
            f"Neither the teammate DB bundle nor the legacy CSV was found. "
            f"Expected one of: data/listings.db, "
            f"datathon2026_dataset/listings.db.gz, {enriched_csv_path}."
        )

    with get_connection(db_path) as connection:
        create_schema(connection)
        import_enriched_csv(connection, enriched_csv_path)
        create_indexes(connection)
        connection.execute("INSERT INTO listings_fts(listings_fts) VALUES('rebuild')")
        connection.commit()


def _migration_is_possible(db_path: Path) -> bool:
    """True iff the two source tables the migration reads from exist."""
    try:
        with get_connection(db_path) as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ('listings_enriched', 'listings_ranking_signals')"
            ).fetchall()
            return len({r[0] for r in rows}) == 2
    except sqlite3.Error:
        return False


def _run_migration(db_path: Path) -> None:
    from scripts.migrate_db_to_app_schema import migrate

    try:
        report = migrate(db_path)
        logger.info("migrated DB at %s: %s", db_path, report)
    except (sqlite3.Error, RuntimeError) as exc:
        logger.error(
            "\033[31m[ERROR] migrate_db_to_app_schema failed: %s\033[0m",
            exc,
        )
        raise


def _schema_matches(db_path: Path) -> bool:
    required_columns = {
        "latitude",
        "longitude",
        "features_json",
        "platform_id",
        "scrape_source",
        "street",
        "house_number",
        "city_slug",
        "floor",
        "year_built",
        "object_category_raw",
        "object_type",
        "feature_wheelchair_accessible",
        "feature_private_laundry",
        "feature_minergie_certified",
    }

    with get_connection(db_path) as connection:
        table = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'listings'"
        ).fetchone()
        if table is None:
            return False

        columns = {
            column[1]
            for column in connection.execute("PRAGMA table_info(listings)").fetchall()
        }

    return required_columns <= columns
