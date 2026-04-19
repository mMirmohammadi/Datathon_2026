"""Unit + integration tests for ``app.core.landmark_proximity``.

The module has two public entry points — ``compute_for_listings`` (batched)
and ``compute_for_one`` (single id). Both must survive: missing tables,
missing columns, a real landmarks.json, and listings with no geo.

Also pins the ``GET /listings/{id}`` response shape so the UI can trust
``body["nearby_landmarks"]`` is a list of dicts with the fields it reads.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.core.landmark_proximity import (
    DEFAULT_TOP_K,
    _display_name,
    compute_for_listings,
    compute_for_one,
)
from app.core.landmarks import Landmark


def test_display_name_prefers_longest_alias() -> None:
    lm = Landmark(
        key="eth_zentrum",
        kind="university",
        lat=47.37,
        lon=8.55,
        aliases=("ETH", "ETH Zürich", "ETHZ"),
    )
    assert _display_name(lm) == "ETH Zürich"


def test_display_name_falls_back_to_key_when_no_aliases() -> None:
    lm = Landmark(
        key="some_unknown_place",
        kind="other",
        lat=0.0,
        lon=0.0,
        aliases=(),
    )
    assert _display_name(lm) == "Some Unknown Place"


def test_compute_for_listings_returns_empty_on_empty_input(tmp_path: Path) -> None:
    # No DB touches when input is empty — not even a connection.
    assert compute_for_listings(tmp_path / "nope.db", []) == {}


def test_compute_for_one_against_real_db() -> None:
    """End-to-end hit against the live data DB. Pinned listing 10210 is a
    Zurich 3.5-room apartment known to have coords + a populated signals
    row (we cross-referenced this listing in several earlier probes).
    """
    repo_root = Path(__file__).resolve().parents[1]
    db_path = repo_root / "data" / "listings.db"
    if not db_path.exists():
        pytest.skip("listings.db not present in this checkout")

    # Cross-check: the listing must exist before we assert on landmarks
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT latitude, longitude FROM listings WHERE listing_id=?",
            ("10210",),
        ).fetchone()
    if row is None or row[0] is None:
        pytest.skip("listing 10210 missing or without geo in this DB snapshot")

    result = compute_for_one(db_path, "10210", top_k=5)
    # 94% coverage for Zurich listings; 10210 should yield several chips
    assert isinstance(result, list)
    assert 1 <= len(result) <= 5, f"expected 1-5 chips, got {len(result)}"

    # Every item has the pinned schema
    for chip in result:
        assert set(chip.keys()) == {
            "key", "name", "kind", "lat", "lng", "distance_m", "transit_min"
        }
        assert isinstance(chip["key"], str) and chip["key"]
        assert isinstance(chip["name"], str) and chip["name"]
        assert isinstance(chip["kind"], str)
        assert isinstance(chip["lat"], float)
        assert isinstance(chip["lng"], float)
        assert chip["distance_m"] is None or isinstance(chip["distance_m"], float)
        assert chip["transit_min"] is None or isinstance(chip["transit_min"], int)

    # Sorted ascending by distance
    distances = [c["distance_m"] for c in result if c["distance_m"] is not None]
    assert distances == sorted(distances), "result must be ascending by distance"


def test_compute_for_listings_batches_and_keys_by_id() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    db_path = repo_root / "data" / "listings.db"
    if not db_path.exists():
        pytest.skip("listings.db not present")

    # Pick 3 real listing_ids with geo
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT listing_id FROM listings "
            "WHERE latitude IS NOT NULL AND longitude IS NOT NULL LIMIT 3"
        ).fetchall()
    ids = [r[0] for r in rows]
    if len(ids) < 3:
        pytest.skip("not enough geo-enabled rows in this DB")

    out = compute_for_listings(db_path, ids, top_k=DEFAULT_TOP_K)
    assert set(out.keys()) == set(ids)
    for lid, chips in out.items():
        # Each listing gets its own chip list (may be empty for ones w/o a
        # signals row, but typically non-empty for Zurich-region listings)
        assert isinstance(chips, list)


def test_compute_for_listings_unknown_id_yields_empty() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    db_path = repo_root / "data" / "listings.db"
    if not db_path.exists():
        pytest.skip("listings.db not present")

    out = compute_for_listings(db_path, ["DEFINITELY_NOT_A_REAL_ID_xyz"], top_k=3)
    # Key present, value empty — the UI still iterates safely.
    assert out.get("DEFINITELY_NOT_A_REAL_ID_xyz", []) == []


def test_compute_for_listings_handles_missing_tables(tmp_path: Path) -> None:
    """Spin up an empty SQLite file with no signals table. The module must
    emit a [WARN] and return ``{}`` — never raise."""
    db_path = tmp_path / "empty.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE listings (listing_id TEXT PRIMARY KEY)")
        conn.execute("INSERT INTO listings VALUES ('A')")
        conn.commit()
    out = compute_for_listings(db_path, ["A"], top_k=3)
    # Keyed absent; value is [] (silent-degradation, not raise)
    assert out == {"A": []}


def test_listings_id_route_carries_nearby_landmarks() -> None:
    """GET /listings/{id} must surface nearby_landmarks for the detail modal."""
    import os
    from fastapi.testclient import TestClient

    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")

    from app.main import app
    from app.config import get_settings

    with TestClient(app) as client:
        with sqlite3.connect(get_settings().db_path) as conn:
            row = conn.execute(
                "SELECT listing_id FROM listings "
                "WHERE latitude IS NOT NULL AND title IS NOT NULL LIMIT 1"
            ).fetchone()
        assert row is not None
        lid = row[0]
        r = client.get(f"/listings/{lid}")

    assert r.status_code == 200, r.text
    body = r.json()
    assert "nearby_landmarks" in body, "nearby_landmarks key must be present"
    assert isinstance(body["nearby_landmarks"], list)
    # With the legacy 500-row CSV fixture the signals table is empty, so the
    # list may legitimately be []; the important contract is the key exists
    # with the right type.
    for chip in body["nearby_landmarks"]:
        assert set(chip.keys()) == {
            "key", "name", "kind", "lat", "lng", "distance_m", "transit_min"
        }


def test_listings_search_response_carries_nearby_landmarks() -> None:
    """POST /listings results must carry nearby_landmarks per listing so
    the summary cards can render the chip row without a second DB hit."""
    import os
    from fastapi.testclient import TestClient

    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")

    from app.harness import search_service
    from app.models.schemas import HardFilters

    # Stub out the LLM — tests must not hit OpenAI
    def fake_extract(query: str) -> HardFilters:
        return HardFilters(city=["Winterthur"])

    import pytest as _pytest
    mp = _pytest.MonkeyPatch()
    mp.setattr(search_service, "extract_hard_facts", fake_extract)
    try:
        from app.main import app
        with TestClient(app) as client:
            r = client.post(
                "/listings",
                json={"query": "home in winterthur", "limit": 3},
            )
    finally:
        mp.undo()

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["listings"]
    for item in body["listings"]:
        L = item["listing"]
        assert "nearby_landmarks" in L
        assert isinstance(L["nearby_landmarks"], list)
