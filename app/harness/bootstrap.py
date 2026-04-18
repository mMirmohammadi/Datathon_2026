from __future__ import annotations

import logging
from pathlib import Path

from app.db import get_connection
from app.harness.csv_import import create_indexes, create_schema
from app.harness.enriched_import import import_enriched_csv
from app.harness.sred_transform import ensure_sred_normalized_csv


logger = logging.getLogger(__name__)

ENRICHED_CSV_RELATIVE = Path("sample_data_enriched") / "sample_enriched_500.csv"


def bootstrap_database(*, db_path: Path, raw_data_dir: Path) -> None:
    ensure_sred_normalized_csv(raw_data_dir)

    if db_path.exists():
        if not _schema_matches(db_path):
            logger.error(
                "\033[31mListings DB schema mismatch at %s. The harness will not overwrite the existing database. "
                "Remove or migrate it manually if you need the newer schema.\033[0m",
                db_path,
            )
            return
        return

    enriched_csv_path = raw_data_dir / ENRICHED_CSV_RELATIVE
    if not enriched_csv_path.exists():
        raise FileNotFoundError(f"Enriched CSV not found: {enriched_csv_path}")

    with get_connection(db_path) as connection:
        create_schema(connection)
        import_enriched_csv(connection, enriched_csv_path)
        create_indexes(connection)
        connection.execute("INSERT INTO listings_fts(listings_fts) VALUES('rebuild')")
        connection.commit()


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
