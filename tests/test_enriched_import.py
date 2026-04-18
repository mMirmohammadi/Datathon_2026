from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from app.db import get_connection
from app.harness.csv_import import create_indexes, create_schema
from app.harness.enriched_import import import_enriched_csv


ENRICHED_CSV = (
    Path(__file__).resolve().parents[1]
    / "raw_data"
    / "sample_data_enriched"
    / "sample_enriched_500.csv"
)


@pytest.fixture(scope="module")
def loaded_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    db_path = tmp_path_factory.mktemp("enriched") / "listings.db"
    with get_connection(db_path) as conn:
        create_schema(conn)
        count = import_enriched_csv(conn, ENRICHED_CSV)
        assert count == 500
        create_indexes(conn)
    return db_path


def _fetch(db_path: Path, listing_id: str) -> sqlite3.Row:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM listings WHERE listing_id = ?", (listing_id,)
        ).fetchone()
    assert row is not None, f"listing_id {listing_id} missing"
    return row


def test_row_count_matches_csv(loaded_db: Path) -> None:
    with sqlite3.connect(loaded_db) as conn:
        total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    assert total == 500


def test_city_is_slugged_to_english_lowercase(loaded_db: Path) -> None:
    with sqlite3.connect(loaded_db) as conn:
        cities = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT city FROM listings WHERE city IS NOT NULL"
            ).fetchall()
        }
    assert "zurich" in cities
    assert "geneva" in cities
    assert "basel" in cities
    # No accented or capitalised raw forms should leak through.
    for c in cities:
        assert c == c.lower()
        assert c.isascii()


def test_city_and_city_slug_agree(loaded_db: Path) -> None:
    with sqlite3.connect(loaded_db) as conn:
        mismatch = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE city IS NOT NULL AND city != city_slug"
        ).fetchone()[0]
    assert mismatch == 0


def test_offer_type_is_mono_rent(loaded_db: Path) -> None:
    with sqlite3.connect(loaded_db) as conn:
        offer_types = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT offer_type FROM listings"
            ).fetchall()
        }
    assert offer_types == {"RENT"}


def test_object_category_is_english_canonical(loaded_db: Path) -> None:
    with sqlite3.connect(loaded_db) as conn:
        categories = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT object_category FROM listings WHERE object_category IS NOT NULL"
            ).fetchall()
        }
    # Sample has 15 German categories; all should map to English.
    assert "apartment" in categories
    # No German residue.
    assert not any("Wohnung" in c for c in categories)
    assert not any("Haus" in c for c in categories)


def test_postal_code_stored_as_integer(loaded_db: Path) -> None:
    with sqlite3.connect(loaded_db) as conn:
        row = conn.execute(
            "SELECT typeof(postal_code) FROM listings WHERE postal_code IS NOT NULL LIMIT 1"
        ).fetchone()
    assert row[0] == "integer"


def test_available_from_is_iso_text(loaded_db: Path) -> None:
    with sqlite3.connect(loaded_db) as conn:
        dates = [
            row[0]
            for row in conn.execute(
                "SELECT available_from FROM listings WHERE available_from IS NOT NULL LIMIT 20"
            ).fetchall()
        ]
    assert dates, "no dates imported"
    for d in dates:
        assert len(d) == 10 and d[4] == "-" and d[7] == "-"


def test_first_row_is_fully_normalized(loaded_db: Path) -> None:
    # listing_id=10286 in row 1: Grenchen, SO, 2540, Bettlachstrasse 43, Wohnung.
    row = _fetch(loaded_db, "10286")
    assert row["city"] == "grenchen"
    assert row["city_slug"] == "grenchen"
    assert row["canton"] == "SO"
    assert row["postal_code"] == 2540
    assert row["street"] == "bettlachstrasse"
    assert row["house_number"] == "43"
    assert row["object_category"] == "apartment"
    assert row["price"] == 1950
    assert row["rooms"] == 3.5
    assert row["area"] == 89
    assert row["floor"] == 3
    assert row["year_built"] == 2022
    assert row["available_from"] == "2026-02-03"
    assert row["feature_balcony"] == 1
    assert row["feature_elevator"] == 1
    assert row["offer_type"] == "RENT"


def test_second_row_geneva_with_duplex_becomes_maisonette(loaded_db: Path) -> None:
    # listing_id=1167: Genève, GE, 1205, Rue des Pavillons 5Bis 4, Maisonette.
    row = _fetch(loaded_db, "1167")
    assert row["city"] == "geneva"
    assert row["canton"] == "GE"
    assert row["object_category"] == "maisonette"
    # Complex house_number preserves both tokens.
    assert row["street"] == "rue des pavillons"
    assert row["house_number"] == "5Bis 4"


def test_features_json_reflects_set_flags(loaded_db: Path) -> None:
    row = _fetch(loaded_db, "10286")
    features = json.loads(row["features_json"])
    assert "balcony" in features
    assert "elevator" in features
    assert "parking" in features
    assert "fireplace" not in features


def test_description_is_html_stripped(loaded_db: Path) -> None:
    # listing_id=1167 has HTML <p> tags in description_head in the raw CSV.
    row = _fetch(loaded_db, "1167")
    description = row["description"]
    assert description is not None
    assert "<p>" not in description
    assert "<br" not in description
