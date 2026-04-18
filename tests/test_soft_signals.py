from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from app.core import landmarks
from app.core.soft_signals import build_soft_rankings
from app.models.schemas import SoftPreferences


# ---------------- fixture DB ----------------

def _build_signals_db(path: Path) -> None:
    """Minimal sqlite with a ``listings_ranking_signals`` table shaped like
    the live schema post-migration. Only the columns our soft-signal kernels
    read are materialised, to keep the fixture tight and testable.
    """
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE listings_ranking_signals (
                listing_id TEXT PRIMARY KEY,
                price_delta_pct_canton_rooms REAL,
                price_delta_pct_plz_rooms REAL,
                price_plausibility TEXT,
                dist_nearest_stop_m REAL,
                nearest_stop_lines_count_clamped INTEGER,
                dist_motorway_m REAL,
                dist_primary_road_m REAL,
                poi_school_1km INTEGER,
                poi_supermarket_300m INTEGER,
                poi_park_500m INTEGER,
                poi_playground_500m INTEGER,
                poi_kindergarten_500m INTEGER,
                commute_proxy_zurich_min REAL,
                dist_landmark_eth_zentrum_m REAL
            )
            """
        )
        # Row shapes chosen so each kernel has an obvious winner:
        #   L1 — cheap (delta -0.2), noisy, few schools, far from ETH
        #   L2 — expensive (delta +0.5) but quiet and transit-dense
        #   L3 — median-priced, suspect plausibility, school-dense
        #   L4 — family heaven, close to ETH
        #   L5 — NULLs everywhere (should be omitted from every ranking)
        rows = [
            ("L1", -0.2, -0.1, "normal", 50.0, 20, 100.0, 80.0, 1, 2, 0, 0, 0, 25.0, 4000.0),
            ("L2", 0.5,  0.4, "normal", 60.0, 60, 1200.0, 900.0, 2, 1, 1, 1, 1, 18.0, 1500.0),
            ("L3", 0.0, None, "suspect", 120.0, 30, 600.0, 400.0, 10, 4, 0, 0, 0, 22.0, 2500.0),
            ("L4", 0.05, 0.05, "normal", 200.0, 12, 400.0, 200.0, 3, 0, 3, 5, 5, 30.0, 300.0),
            ("L5", None, None, None, None, None, None, None, None, None, None, None, None, None, None),
        ]
        conn.executemany(
            """
            INSERT INTO listings_ranking_signals VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def signals_db(tmp_path: Path) -> Path:
    db = tmp_path / "signals.db"
    _build_signals_db(db)
    return db


@pytest.fixture
def candidates() -> list[dict]:
    return [{"listing_id": f"L{i}"} for i in range(1, 6)]


@pytest.fixture
def landmarks_for_tests(tmp_path: Path) -> None:
    """Populate landmarks cache with ETH Zentrum only."""
    path = tmp_path / "landmarks.json"
    path.write_text(json.dumps([
        {"key": "eth_zentrum", "kind": "university", "lat": 47.3765,
         "lon": 8.5482, "aliases": ["ETH", "ETHZ"]},
    ]), encoding="utf-8")
    landmarks._STATE.update({"by_slug": None, "by_key": None, "path": None})
    landmarks.load(path)
    yield
    landmarks._STATE.update({"by_slug": None, "by_key": None, "path": None})


# ---------------- tests ----------------


def test_returns_empty_when_soft_prefs_none(signals_db, candidates) -> None:
    assert build_soft_rankings(candidates, None, signals_db) == []


def test_returns_empty_for_empty_candidates(signals_db) -> None:
    soft = SoftPreferences(quiet=True)
    assert build_soft_rankings([], soft, signals_db) == []


def test_cheap_ranks_lowest_delta_first_and_drops_suspect(signals_db, candidates) -> None:
    soft = SoftPreferences(price_sentiment="cheap")
    rankings = build_soft_rankings(candidates, soft, signals_db)
    assert len(rankings) == 1
    cheap = rankings[0]
    # L1 delta=-0.2 must lead; L3 (suspect) and L5 (NULL) are excluded.
    assert cheap[0] == "L1"
    assert "L3" not in cheap
    assert "L5" not in cheap


def test_premium_ranks_highest_delta_first(signals_db, candidates) -> None:
    soft = SoftPreferences(price_sentiment="premium")
    rankings = build_soft_rankings(candidates, soft, signals_db)
    assert rankings[0][0] == "L2"  # +0.5 delta wins


def test_quiet_prefers_further_from_noise(signals_db, candidates) -> None:
    soft = SoftPreferences(quiet=True)
    rankings = build_soft_rankings(candidates, soft, signals_db)
    assert len(rankings) == 1
    # L2: 1200+900=2100; L3: 600+400=1000; L4: 400+200=600; L1: 100+80=180.
    assert rankings[0] == ["L2", "L3", "L4", "L1"]


def test_near_public_transport_composite(signals_db, candidates) -> None:
    soft = SoftPreferences(near_public_transport=True)
    rankings = build_soft_rankings(candidates, soft, signals_db)
    # L2 is the clear winner: 60 lines clamped, low distance 60m.
    assert rankings[0][0] == "L2"
    assert "L5" not in rankings[0]


def test_commute_target_reads_dedicated_column(signals_db, candidates) -> None:
    soft = SoftPreferences(commute_target="zurich_hb")
    rankings = build_soft_rankings(candidates, soft, signals_db)
    # Smaller commute_proxy_zurich_min is better: L2 (18) < L3 (22) < L1 (25) < L4 (30)
    assert rankings[0] == ["L2", "L3", "L1", "L4"]


def test_near_schools_ranks_by_poi_count(signals_db, candidates) -> None:
    soft = SoftPreferences(near_schools=True)
    rankings = build_soft_rankings(candidates, soft, signals_db)
    assert rankings[0][0] == "L3"  # 10 schools


def test_family_friendly_is_composite(signals_db, candidates) -> None:
    soft = SoftPreferences(family_friendly=True)
    rankings = build_soft_rankings(candidates, soft, signals_db)
    # L4: playground 5 + kindergarten 5 = 10. L2: 1+1=2. Others: 0 or NULL.
    assert rankings[0][0] == "L4"


def test_near_landmark_uses_resolved_column(
    signals_db, candidates, landmarks_for_tests
) -> None:
    soft = SoftPreferences(near_landmark=["ETH"])
    rankings = build_soft_rankings(candidates, soft, signals_db)
    assert len(rankings) == 1
    # Closest to ETH: L4 (300m) < L2 (1500m) < L3 (2500m) < L1 (4000m); L5 NULL skipped.
    assert rankings[0] == ["L4", "L2", "L3", "L1"]


def test_unresolvable_landmark_warns_and_skips(
    signals_db, candidates, landmarks_for_tests, capsys
) -> None:
    soft = SoftPreferences(near_landmark=["DefinitelyNotALandmark"])
    rankings = build_soft_rankings(candidates, soft, signals_db)
    assert rankings == []
    out = capsys.readouterr().out
    assert "[WARN] soft_signals.build_soft_rankings" in out
    assert "DefinitelyNotALandmark" in out


def test_all_prefs_activate_expected_number_of_rankings(
    signals_db, candidates, landmarks_for_tests
) -> None:
    soft = SoftPreferences(
        price_sentiment="cheap",
        quiet=True,
        near_public_transport=True,
        near_schools=True,
        near_supermarket=True,
        near_park=True,
        family_friendly=True,
        commute_target="zurich_hb",
        near_landmark=["ETH", "ETHZ"],  # two entries; both resolve to same landmark
    )
    rankings = build_soft_rankings(candidates, soft, signals_db)
    # 8 scalar kernels + 2 landmarks = 10 rankings.
    assert len(rankings) == 10


def test_null_signals_omitted_from_their_ranking(signals_db, candidates) -> None:
    soft = SoftPreferences(near_supermarket=True)
    rankings = build_soft_rankings(candidates, soft, signals_db)
    # L5 has NULL poi_supermarket_300m: must be absent.
    assert "L5" not in rankings[0]


def test_missing_signals_table_returns_empty(tmp_path: Path, candidates) -> None:
    empty_db = tmp_path / "empty.db"
    sqlite3.connect(empty_db).close()  # exists but has no tables
    soft = SoftPreferences(quiet=True)
    # No WARN crash - just an empty list.
    assert build_soft_rankings(candidates, soft, empty_db) == []
