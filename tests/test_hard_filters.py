from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core.hard_filters import HardFilterParams, search_listings
from app.db import get_connection
from app.harness.csv_import import create_indexes, create_schema


# Fixture rows. Columns align with the INSERT below.
# Default sort is listing_id ASC, so IDs are alphabetical on purpose.
_ROWS: list[dict[str, object]] = [
    {
        "listing_id": "L1",
        "title": "A",
        "city": "zurich", "city_slug": "zurich",
        "postal_code": 8001, "canton": "ZH",
        "price": 2000, "rooms": 2.0, "area": 55, "floor": 1, "year_built": 2010,
        "available_from": "2026-03-01",
        "latitude": 47.37, "longitude": 8.54,
        "offer_type": "RENT", "object_category": "apartment",
        "house_number": "10",
        "balcony": 1, "elevator": 0, "parking": 0,
        "features": ["balcony"],
    },
    {
        "listing_id": "L2",
        "title": "B",
        "city": "zurich", "city_slug": "zurich",
        "postal_code": 8002, "canton": "ZH",
        "price": 3000, "rooms": 3.5, "area": 80, "floor": 3, "year_built": 2020,
        "available_from": "2026-05-01",
        "latitude": 47.38, "longitude": 8.55,
        "offer_type": "RENT", "object_category": "apartment",
        "house_number": "12a",
        "balcony": 1, "elevator": 1, "parking": 0,
        "features": ["balcony", "elevator"],
    },
    {
        "listing_id": "L3",
        "title": "C",
        "city": "winterthur", "city_slug": "winterthur",
        "postal_code": 8400, "canton": "ZH",
        "price": 1500, "rooms": 2.5, "area": 60, "floor": 0, "year_built": 1995,
        "available_from": "2026-01-15",
        "latitude": 47.50, "longitude": 8.72,
        "offer_type": "RENT", "object_category": "apartment",
        "house_number": "1",
        "balcony": 0, "elevator": 0, "parking": 1,
        "features": ["parking"],
    },
    {
        "listing_id": "L4",
        "title": "D",
        "city": "winterthur", "city_slug": "winterthur",
        "postal_code": 8400, "canton": "ZH",
        "price": 2800, "rooms": 4.5, "area": 120, "floor": 2, "year_built": 2025,
        "available_from": "2026-07-01",
        "latitude": 47.51, "longitude": 8.73,
        "offer_type": "RENT", "object_category": "house",
        "house_number": "5",
        "balcony": 1, "elevator": 0, "parking": 1,
        "features": ["balcony", "parking"],
    },
    {
        "listing_id": "L5",
        "title": "E",
        "city": "geneva", "city_slug": "geneva",
        "postal_code": 1201, "canton": "GE",
        "price": 2500, "rooms": 3.0, "area": 70, "floor": 4, "year_built": 2015,
        "available_from": "2026-04-01",
        "latitude": 46.20, "longitude": 6.14,
        "offer_type": "RENT", "object_category": "apartment",
        "house_number": "7",
        "balcony": 0, "elevator": 1, "parking": 0,
        "features": ["elevator"],
    },
    {
        "listing_id": "L6",
        "title": "F",
        "city": "geneva", "city_slug": "geneva",
        "postal_code": 1202, "canton": "GE",
        "price": 5000, "rooms": 5.0, "area": 180, "floor": 0, "year_built": 2000,
        "available_from": "2026-02-01",
        "latitude": 46.21, "longitude": 6.15,
        "offer_type": "RENT", "object_category": "house",
        "house_number": "9",
        "balcony": 1, "elevator": 1, "parking": 1,
        "features": ["balcony", "elevator", "parking"],
    },
    {
        "listing_id": "L7",
        "title": "G",
        "city": "bern", "city_slug": "bern",
        "postal_code": 3000, "canton": "BE",
        "price": None, "rooms": None, "area": None, "floor": None, "year_built": None,
        "available_from": None,
        "latitude": None, "longitude": None,
        "offer_type": "RENT", "object_category": "apartment",
        "house_number": None,
        "balcony": 0, "elevator": 0, "parking": 0,
        "features": [],
    },
    {
        "listing_id": "L8",
        "title": "H",
        "city": "basel", "city_slug": "basel",
        "postal_code": 4000, "canton": "BS",
        "price": 2200, "rooms": 2.5, "area": 50, "floor": -1, "year_built": 1980,
        "available_from": "2026-06-01",
        "latitude": 47.56, "longitude": 7.59,
        "offer_type": "RENT", "object_category": "studio",
        "house_number": "3",
        "balcony": 1, "elevator": 0, "parking": 0,
        "features": ["balcony"],
    },
]


def _insert(connection, row: dict[str, object]) -> None:
    connection.execute(
        """
        INSERT INTO listings (
            listing_id, title, city, city_slug, postal_code, canton,
            price, rooms, area, floor, year_built, available_from,
            latitude, longitude, house_number,
            feature_balcony, feature_elevator, feature_parking,
            features_json, offer_type, object_category, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["listing_id"], row["title"], row["city"], row["city_slug"],
            row["postal_code"], row["canton"],
            row["price"], row["rooms"], row["area"], row["floor"],
            row["year_built"], row["available_from"],
            row["latitude"], row["longitude"], row["house_number"],
            row["balcony"], row["elevator"], row["parking"],
            json.dumps(row["features"]),
            row["offer_type"], row["object_category"],
            "{}",
        ),
    )


@pytest.fixture(scope="module")
def fixture_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    db_path = tmp_path_factory.mktemp("hf") / "listings.db"
    with get_connection(db_path) as connection:
        create_schema(connection)
        for row in _ROWS:
            _insert(connection, row)
        connection.commit()
        create_indexes(connection)
    return db_path


def _ids(rows: list[dict]) -> list[str]:
    return [row["listing_id"] for row in rows]


# ---------- base ----------

def test_empty_filters_returns_all_rows_sorted_by_listing_id(fixture_db: Path) -> None:
    rows = search_listings(fixture_db, HardFilterParams(limit=100))
    assert _ids(rows) == ["L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8"]


# ---------- city (slug + alias) ----------

def test_city_english_input_matches_stored_slug(fixture_db: Path) -> None:
    rows = search_listings(fixture_db, HardFilterParams(city=["Zurich"], limit=100))
    assert _ids(rows) == ["L1", "L2"]


def test_city_umlaut_input_also_matches(fixture_db: Path) -> None:
    rows = search_listings(fixture_db, HardFilterParams(city=["Zürich"], limit=100))
    assert _ids(rows) == ["L1", "L2"]


def test_city_french_geneva_matches_english_slug(fixture_db: Path) -> None:
    rows = search_listings(fixture_db, HardFilterParams(city=["Genève"], limit=100))
    assert _ids(rows) == ["L5", "L6"]


def test_city_multiple_values(fixture_db: Path) -> None:
    rows = search_listings(fixture_db, HardFilterParams(city=["zurich", "geneva"], limit=100))
    assert _ids(rows) == ["L1", "L2", "L5", "L6"]


def test_city_no_match_returns_empty(fixture_db: Path) -> None:
    rows = search_listings(fixture_db, HardFilterParams(city=["Nonexistent"], limit=100))
    assert rows == []


def test_city_empty_list_is_noop(fixture_db: Path) -> None:
    rows = search_listings(fixture_db, HardFilterParams(city=[], limit=100))
    assert len(rows) == 8


# ---------- postal_code ----------

def test_postal_code_single(fixture_db: Path) -> None:
    rows = search_listings(fixture_db, HardFilterParams(postal_code=["8400"], limit=100))
    assert _ids(rows) == ["L3", "L4"]


def test_postal_code_multiple(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db, HardFilterParams(postal_code=["8001", "8002"], limit=100)
    )
    assert _ids(rows) == ["L1", "L2"]


# ---------- canton ----------

def test_canton_is_case_insensitive(fixture_db: Path) -> None:
    rows = search_listings(fixture_db, HardFilterParams(canton="zh", limit=100))
    assert _ids(rows) == ["L1", "L2", "L3", "L4"]


# ---------- price ----------

def test_price_range_inclusive_boundaries(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db, HardFilterParams(min_price=2000, max_price=3000, limit=100)
    )
    assert _ids(rows) == ["L1", "L2", "L4", "L5", "L8"]


def test_price_impossible_range_returns_empty(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db, HardFilterParams(min_price=5000, max_price=1000, limit=100)
    )
    assert rows == []


# ---------- rooms ----------

def test_rooms_range_inclusive_float_boundaries(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db, HardFilterParams(min_rooms=2.5, max_rooms=4.5, limit=100)
    )
    assert _ids(rows) == ["L2", "L3", "L4", "L5", "L8"]


# ---------- area ----------

def test_min_area(fixture_db: Path) -> None:
    rows = search_listings(fixture_db, HardFilterParams(min_area=80, limit=100))
    assert _ids(rows) == ["L2", "L4", "L6"]


def test_max_area(fixture_db: Path) -> None:
    rows = search_listings(fixture_db, HardFilterParams(max_area=60, limit=100))
    assert _ids(rows) == ["L1", "L3", "L8"]


def test_area_range(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db, HardFilterParams(min_area=60, max_area=80, limit=100)
    )
    assert _ids(rows) == ["L2", "L3", "L5"]


# ---------- floor ----------

def test_min_floor_excludes_ground_floor_and_basement(fixture_db: Path) -> None:
    rows = search_listings(fixture_db, HardFilterParams(min_floor=1, limit=100))
    assert _ids(rows) == ["L1", "L2", "L4", "L5"]


def test_max_floor(fixture_db: Path) -> None:
    rows = search_listings(fixture_db, HardFilterParams(max_floor=0, limit=100))
    assert _ids(rows) == ["L3", "L6", "L8"]


def test_floor_basement_negative_value(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db, HardFilterParams(min_floor=-1, max_floor=-1, limit=100)
    )
    assert _ids(rows) == ["L8"]


# ---------- year_built ----------

def test_min_year_built_for_modern_only(fixture_db: Path) -> None:
    rows = search_listings(fixture_db, HardFilterParams(min_year_built=2015, limit=100))
    assert _ids(rows) == ["L2", "L4", "L5"]


def test_max_year_built(fixture_db: Path) -> None:
    rows = search_listings(fixture_db, HardFilterParams(max_year_built=2000, limit=100))
    assert _ids(rows) == ["L3", "L6", "L8"]


# ---------- available_from_after ----------

def test_available_from_after_iso_string_comparison(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db, HardFilterParams(available_from_after="2026-04-01", limit=100)
    )
    assert _ids(rows) == ["L2", "L4", "L5", "L8"]


# ---------- object_category ----------

def test_object_category_apartment(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db, HardFilterParams(object_category=["apartment"], limit=100)
    )
    assert _ids(rows) == ["L1", "L2", "L3", "L5", "L7"]


def test_object_category_multiple(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db,
        HardFilterParams(object_category=["apartment", "house"], limit=100),
    )
    assert _ids(rows) == ["L1", "L2", "L3", "L4", "L5", "L6", "L7"]


# ---------- features required ----------

def test_features_single_balcony(fixture_db: Path) -> None:
    rows = search_listings(fixture_db, HardFilterParams(features=["balcony"], limit=100))
    assert _ids(rows) == ["L1", "L2", "L4", "L6", "L8"]


def test_features_multiple_require_all(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db, HardFilterParams(features=["balcony", "elevator"], limit=100)
    )
    assert _ids(rows) == ["L2", "L6"]


def test_features_unknown_key_is_silently_ignored(fixture_db: Path) -> None:
    # Unknown keys are dropped without a warning today; revisit if we adopt
    # CLAUDE.md §5 logging at this layer.
    rows = search_listings(
        fixture_db, HardFilterParams(features=["unknown_feature"], limit=100)
    )
    assert len(rows) == 8


# ---------- features excluded ----------

def test_features_excluded_drops_rows_with_that_feature(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db, HardFilterParams(features_excluded=["balcony"], limit=100)
    )
    # Only rows with feature_balcony = 0 (strict). All fixture rows have it set.
    assert _ids(rows) == ["L3", "L5", "L7"]


def test_features_excluded_is_strict_on_null_rows(fixture_db: Path) -> None:
    # Insert a row with NULL feature_elevator and confirm it is dropped by
    # `features_excluded=["elevator"]` (we treat NULL as "cannot guarantee absent").
    with get_connection(fixture_db) as conn:
        conn.execute(
            "INSERT INTO listings (listing_id, title, city, city_slug, features_json, raw_json, feature_elevator) "
            "VALUES ('LX', 'Unknown', 'bern', 'bern', '[]', '{}', NULL)"
        )
        conn.commit()
    try:
        rows = search_listings(
            fixture_db, HardFilterParams(features_excluded=["elevator"], limit=100)
        )
        assert "LX" not in _ids(rows)
    finally:
        with get_connection(fixture_db) as conn:
            conn.execute("DELETE FROM listings WHERE listing_id = 'LX'")
            conn.commit()


def test_features_required_and_excluded_combined(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db,
        HardFilterParams(
            features=["balcony"], features_excluded=["elevator"], limit=100
        ),
    )
    assert _ids(rows) == ["L1", "L4", "L8"]


# ---------- geo / radius ----------

def test_radius_filter_excludes_null_coords_and_sorts_by_distance(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db,
        HardFilterParams(latitude=47.37, longitude=8.54, radius_km=5.0, limit=100),
    )
    assert _ids(rows) == ["L1", "L2"]


def test_radius_filter_zero_km_returns_empty(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db,
        HardFilterParams(latitude=0.0, longitude=0.0, radius_km=0.0, limit=100),
    )
    assert rows == []


# ---------- pagination ----------

def test_pagination_limit_and_offset(fixture_db: Path) -> None:
    rows = search_listings(fixture_db, HardFilterParams(limit=1, offset=2))
    assert _ids(rows) == ["L3"]


# ---------- sort ----------

def test_sort_by_price_asc_nulls_last(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db, HardFilterParams(sort_by="price_asc", limit=100)
    )
    assert _ids(rows) == ["L3", "L1", "L8", "L5", "L4", "L2", "L6", "L7"]


def test_sort_by_price_desc_nulls_last(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db, HardFilterParams(sort_by="price_desc", limit=100)
    )
    assert _ids(rows) == ["L6", "L2", "L4", "L5", "L8", "L1", "L3", "L7"]


def test_sort_by_rooms_asc(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db, HardFilterParams(sort_by="rooms_asc", limit=100)
    )
    assert _ids(rows) == ["L1", "L3", "L8", "L5", "L2", "L4", "L6", "L7"]


def test_sort_by_rooms_desc(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db, HardFilterParams(sort_by="rooms_desc", limit=100)
    )
    assert _ids(rows) == ["L6", "L4", "L2", "L5", "L3", "L8", "L1", "L7"]


# ---------- combined ----------

def test_combined_city_price_features_sorted(fixture_db: Path) -> None:
    rows = search_listings(
        fixture_db,
        HardFilterParams(
            city=["zurich", "winterthur"],
            max_price=2800,
            features=["balcony"],
            sort_by="price_asc",
            limit=100,
        ),
    )
    assert _ids(rows) == ["L1", "L4"]
