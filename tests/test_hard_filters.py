import sqlite3
from pathlib import Path

from app.core.hard_filters import HardFilterParams, search_listings
from app.harness.bootstrap import bootstrap_database


def build_database(tmp_path: Path) -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    raw_data_dir = repo_root / "raw_data"
    db_path = tmp_path / "listings.db"
    bootstrap_database(db_path=db_path, raw_data_dir=raw_data_dir)
    return db_path


def test_hard_filter_by_city_returns_matching_rows(tmp_path: Path) -> None:
    db_path = build_database(tmp_path)

    rows = search_listings(
        db_path,
        HardFilterParams(city=["Winterthur"], limit=5),
    )

    assert rows
    assert all((row.get("city") or "").lower() == "winterthur" for row in rows)


def test_hard_filter_by_price_and_rooms_returns_matching_rows(tmp_path: Path) -> None:
    db_path = build_database(tmp_path)

    rows = search_listings(
        db_path,
        HardFilterParams(min_price=1000, max_price=3000, min_rooms=2.0, max_rooms=4.5),
    )

    assert rows
    for row in rows[:25]:
        price = row.get("price")
        rooms = row.get("rooms")
        assert price is not None and 1000 <= price <= 3000
        assert rooms is not None and 2.0 <= rooms <= 4.5


def test_hard_filter_pagination_limits_results(tmp_path: Path) -> None:
    db_path = build_database(tmp_path)

    rows = search_listings(db_path, HardFilterParams(limit=3, offset=0))

    assert len(rows) <= 3

    with sqlite3.connect(db_path) as connection:
        total = connection.execute("SELECT COUNT(*) FROM listings").fetchone()[0]

    assert total >= len(rows)


def test_hard_filter_by_features_returns_feature_matches(tmp_path: Path) -> None:
    db_path = build_database(tmp_path)

    rows = search_listings(
        db_path,
        HardFilterParams(features=["child_friendly"], limit=20),
    )

    assert rows
    assert all("child_friendly" in (row.get("features") or []) for row in rows)


def test_hard_filter_by_coordinates_and_radius_returns_nearby_rows(tmp_path: Path) -> None:
    db_path = build_database(tmp_path)

    seed_rows = search_listings(db_path, HardFilterParams(city=["Winterthur"], limit=50))
    assert seed_rows
    seed = next(
        row
        for row in seed_rows
        if row.get("latitude") is not None and row.get("longitude") is not None
    )

    rows = search_listings(
        db_path,
        HardFilterParams(
            latitude=seed["latitude"],
            longitude=seed["longitude"],
            radius_km=1.0,
            limit=20,
        ),
    )

    assert rows
    assert any(row["listing_id"] == seed["listing_id"] for row in rows)
