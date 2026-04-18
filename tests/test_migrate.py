from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from scripts.migrate_db_to_app_schema import migrate


# ------------------------------------------------------------------
# Fixture: a tiny DB shaped like the teammate bundle (3 source tables).
# ------------------------------------------------------------------


def _make_mini_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        # Minimal `listings` schema mirroring the teammate bundle's 41-col raw
        # table. We only exercise the columns the migration reads / writes.
        conn.execute(
            """
            CREATE TABLE listings (
                listing_id TEXT PRIMARY KEY,
                platform_id TEXT,
                scrape_source TEXT,
                title TEXT NOT NULL,
                description TEXT,
                street TEXT,
                city TEXT,
                postal_code TEXT,
                canton TEXT,
                price INTEGER,
                rooms REAL,
                area REAL,
                available_from TEXT,
                latitude REAL,
                longitude REAL,
                object_category TEXT,
                object_type TEXT,
                offer_type TEXT,
                features_json TEXT NOT NULL DEFAULT '[]',
                images_json TEXT,
                raw_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE listings_enriched (
                listing_id TEXT PRIMARY KEY,
                city_filled TEXT,     city_source TEXT,
                canton_filled TEXT,   canton_source TEXT,
                year_built_filled TEXT, year_built_source TEXT,
                floor_filled TEXT,      floor_source TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE listings_ranking_signals (
                listing_id TEXT PRIMARY KEY,
                price_delta_pct_canton_rooms REAL,
                price_delta_pct_plz_rooms REAL,
                dist_nearest_stop_m REAL,
                nearest_stop_lines_count INTEGER,
                dist_motorway_m REAL,
                dist_primary_road_m REAL
            )
            """
        )
        rows_listings = [
            # ID, platform_id, source, title, description, street, city (native),
            # postal, canton, price, rooms, area, avail, lat, lon,
            # obj_cat (German), obj_type, offer_type, features_json, images_json, raw_json
            (
                "L1", "L1", "COMPARIS", "Mietwohnung Zürich", "Helle Wohnung",
                "Bahnhofstrasse 10", "Zurich", "8001", "ZH", 2500, 3.5, 80,
                "2026-06-01", 47.377, 8.541, "Wohnung", None, "RENT",
                "[]", None, "{}",
            ),
            (
                "L2", "L2", "COMPARIS", "Dachwohnung Lausanne", "Nice attic",
                "Av. Test", "Lausanne", "1003", "VD", 3000, 4.5, 100,
                "2026-07-01", 46.520, 6.633, "Dachwohnung", None, "RENT",
                "[]", None, "{}",
            ),
            (
                "L3", "L3", "SRED", "Bright house", "Sunny spot",
                "Some Rd 5", None, None, None, 1800, 3.0, 75,
                None, 46.800, 8.000, "Haus", None, "RENT",
                "[]", None, "{}",
            ),
        ]
        conn.executemany(
            "INSERT INTO listings VALUES (" + ",".join(["?"] * 21) + ")",
            rows_listings,
        )
        rows_enriched = [
            ("L1", "Zurich",    "original",       "ZH", "original", "2015", "original", "2", "original"),
            ("L2", "Lausanne",  "original",       "VD", "original", "UNKNOWN", "UNKNOWN", "3", "original"),
            ("L3", "Altdorf",   "rev_geo_offline", "UR", "rev_geo_offline", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"),
        ]
        conn.executemany(
            "INSERT INTO listings_enriched VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows_enriched,
        )
        rows_signals = [
            ("L1", -0.07,  None,  205.0, 5,     409.0, 300.0),
            ("L2",  1.90,  None,  500.0, 41083, 800.0, 500.0),  # lines_count outlier
            ("L3",  3.20,  None,  None,  None,  None,  None),   # plausibility: suspect
        ]
        conn.executemany(
            "INSERT INTO listings_ranking_signals VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows_signals,
        )
        conn.commit()
    finally:
        conn.close()


def _write_landmarks(path: Path) -> Path:
    path.write_text(
        json.dumps([
            {"key": "hb_zurich", "kind": "transit", "lat": 47.378, "lon": 8.540, "aliases": []},
            {"key": "hb_lausanne", "kind": "transit", "lat": 46.517, "lon": 6.629, "aliases": []},
            {"key": "eth_zentrum", "kind": "university", "lat": 47.376, "lon": 8.548, "aliases": ["ETH"]},
        ]),
        encoding="utf-8",
    )
    return path


@pytest.fixture
def mini_db(tmp_path: Path) -> tuple[Path, Path]:
    db = tmp_path / "mini.db"
    _make_mini_db(db)
    landmarks = _write_landmarks(tmp_path / "landmarks.json")
    return db, landmarks


# ---------- migration behaviour ----------


def test_migration_adds_5_listings_columns(mini_db) -> None:
    db, lm = mini_db
    migrate(db, lm)
    with sqlite3.connect(db) as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(listings)")}
    for col in ("year_built", "object_category_raw", "house_number", "city_slug", "floor"):
        assert col in cols


def test_city_slug_populated_from_enriched(mini_db) -> None:
    db, lm = mini_db
    migrate(db, lm)
    with sqlite3.connect(db) as conn:
        rows = dict(conn.execute("SELECT listing_id, city_slug FROM listings").fetchall())
    assert rows["L1"] == "zurich"
    assert rows["L2"] == "lausanne"
    assert rows["L3"] == "altdorf"


def test_object_category_translated_and_raw_preserved(mini_db) -> None:
    db, lm = mini_db
    migrate(db, lm)
    with sqlite3.connect(db) as conn:
        rows = dict(
            (r[0], (r[1], r[2]))
            for r in conn.execute(
                "SELECT listing_id, object_category, object_category_raw FROM listings"
            )
        )
    assert rows["L1"] == ("apartment", "Wohnung")
    assert rows["L2"] == ("attic_apartment", "Dachwohnung")
    assert rows["L3"] == ("house", "Haus")


def test_unknown_sentinel_not_coerced_to_zero(mini_db) -> None:
    db, lm = mini_db
    migrate(db, lm)
    with sqlite3.connect(db) as conn:
        rows = dict(
            (r[0], (r[1], r[2]))
            for r in conn.execute(
                "SELECT listing_id, year_built, floor FROM listings"
            )
        )
    assert rows["L1"] == (2015, 2)
    # L2 year_built is UNKNOWN -> stays NULL (not 0).
    assert rows["L2"] == (None, 3)
    # L3 all numeric _filled are UNKNOWN -> both NULL.
    assert rows["L3"] == (None, None)


def test_house_number_extracted(mini_db) -> None:
    db, lm = mini_db
    migrate(db, lm)
    with sqlite3.connect(db) as conn:
        rows = dict(conn.execute(
            "SELECT listing_id, house_number FROM listings"
        ).fetchall())
    assert rows["L1"] == "10"
    assert rows["L3"] == "5"


def test_landmark_distance_columns_added_and_populated(mini_db) -> None:
    db, lm = mini_db
    migrate(db, lm)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        cols = {r[1] for r in conn.execute("PRAGMA table_info(listings_ranking_signals)")}
        for key in ("hb_zurich", "hb_lausanne", "eth_zentrum"):
            assert f"dist_landmark_{key}_m" in cols
        row = conn.execute(
            "SELECT dist_landmark_hb_zurich_m, dist_landmark_eth_zentrum_m "
            "FROM listings_ranking_signals WHERE listing_id = 'L1'"
        ).fetchone()
    # L1 is at (47.377, 8.541), Zurich HB at (47.378, 8.540) -> well under 200m.
    assert 0 < row["dist_landmark_hb_zurich_m"] < 200
    # ETH Zentrum at (47.376, 8.548) -> between 500 m and 1.5 km.
    assert 300 < row["dist_landmark_eth_zentrum_m"] < 2000


def test_lines_count_clamped_to_100(mini_db) -> None:
    db, lm = mini_db
    migrate(db, lm)
    with sqlite3.connect(db) as conn:
        rows = dict(conn.execute(
            "SELECT listing_id, nearest_stop_lines_count_clamped "
            "FROM listings_ranking_signals"
        ).fetchall())
    assert rows["L1"] == 5
    assert rows["L2"] == 100  # was 41083, clamped.
    assert rows["L3"] is None


def test_price_plausibility_flag(mini_db) -> None:
    db, lm = mini_db
    migrate(db, lm)
    with sqlite3.connect(db) as conn:
        rows = dict(conn.execute(
            "SELECT listing_id, price_plausibility "
            "FROM listings_ranking_signals"
        ).fetchall())
    # L1 delta -0.07 -> normal. L2 +1.90 -> normal (<3). L3 +3.20 -> suspect.
    assert rows["L1"] == "normal"
    assert rows["L2"] == "normal"
    assert rows["L3"] == "suspect"


def test_commute_proxy_populated_when_inputs_present(mini_db) -> None:
    db, lm = mini_db
    migrate(db, lm)
    with sqlite3.connect(db) as conn:
        rows = {
            r[0]: (r[1], r[2])
            for r in conn.execute(
                "SELECT listing_id, commute_proxy_zurich_min, commute_proxy_lausanne_min "
                "FROM listings_ranking_signals"
            )
        }
    # L1 has dist_nearest_stop 205 m and is ~140 m from Zurich HB;
    # proxy = 205/80 + 140/1000 ≈ 2.7 minutes.
    assert 0 < rows["L1"][0] < 5
    # L3 has NULL dist_nearest_stop_m -> proxy NULL.
    assert rows["L3"][0] is None


def test_fts_index_rebuilt(mini_db) -> None:
    db, lm = mini_db
    migrate(db, lm)
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name='listings_fts'"
        ).fetchone() is not None
        count = conn.execute(
            "SELECT COUNT(*) FROM listings_fts WHERE listings_fts MATCH 'Wohnung'"
        ).fetchone()[0]
    assert count >= 1


def test_migration_is_idempotent(mini_db) -> None:
    db, lm = mini_db
    migrate(db, lm)
    report = migrate(db, lm)
    # Second run should not add any new columns (already present).
    assert report["listings"]["columns_added"] == 0
    assert report["ranking_signals"]["landmark_cols_added"] == 0
    assert report["ranking_signals"]["quality_cols_added"] == 0
    assert report["ranking_signals"]["commute_cols_added"] == 0


def test_migration_raises_when_source_tables_missing(tmp_path: Path) -> None:
    db = tmp_path / "empty.db"
    # Only `listings` — the other two are absent.
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE listings (listing_id TEXT PRIMARY KEY, title TEXT NOT NULL, "
        "features_json TEXT NOT NULL DEFAULT '[]', raw_json TEXT NOT NULL DEFAULT '{}')"
    )
    conn.commit()
    conn.close()
    with pytest.raises(RuntimeError, match="missing"):
        migrate(db, tmp_path / "landmarks.json")
