from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.models.schemas import HardFilters


def test_post_listings_pipeline_filters_by_extracted_city(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")
    os.environ["LISTINGS_DB_PATH"] = str(tmp_path / "listings.db")

    from app.harness import search_service

    captured: dict[str, str] = {}

    def fake_extract(query: str) -> HardFilters:
        captured["query"] = query
        return HardFilters(city=["Winterthur"])

    monkeypatch.setattr(search_service, "extract_hard_facts", fake_extract)

    from app.main import app

    with TestClient(app) as client:
        response = client.post(
            "/listings",
            json={"query": "flat in winterthur", "limit": 5},
        )

    assert response.status_code == 200
    body = response.json()
    assert captured["query"] == "flat in winterthur"
    assert body["listings"]
    assert len(body["listings"]) <= 5
    assert all(
        (item["listing"].get("city") or "").lower() == "winterthur"
        for item in body["listings"]
    )


def test_post_listings_pipeline_bm25_keywords_rank_matching_rows_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")
    os.environ["LISTINGS_DB_PATH"] = str(tmp_path / "listings.db")

    from app.harness import search_service

    def fake_extract(query: str) -> HardFilters:
        return HardFilters(city=["zurich"], bm25_keywords=["Balkon"])

    monkeypatch.setattr(search_service, "extract_hard_facts", fake_extract)

    from app.main import app

    with TestClient(app) as client:
        response = client.post(
            "/listings",
            json={"query": "Zurich with Balkon", "limit": 10},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["listings"]
    # At least one listing must have a non-zero (text-match) score; that listing
    # must be ranked before any listing with score == 0.
    scores = [item["score"] for item in body["listings"]]
    assert any(s > 0 for s in scores)
    first_zero = next((i for i, s in enumerate(scores) if s == 0.0), len(scores))
    last_nonzero = max(
        (i for i, s in enumerate(scores) if s > 0), default=-1
    )
    assert last_nonzero < first_zero


def test_post_listings_pipeline_hybrid_rrf_uses_visual_and_bm25(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Hybrid path: env=1, monkeypatched SigLIP, assert rrf_score surfaces in
    the API response and that listings with a visual match appear first."""
    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")
    os.environ["LISTINGS_DB_PATH"] = str(tmp_path / "listings.db")
    monkeypatch.setenv("LISTINGS_VISUAL_ENABLED", "1")

    from app.core import visual_search
    from app.harness import search_service

    def fake_extract(query: str) -> HardFilters:
        return HardFilters(city=["zurich"], limit=10)

    monkeypatch.setattr(search_service, "extract_hard_facts", fake_extract)

    # Avoid the real SigLIP load: make load_visual_index a no-op (also patch
    # the already-imported binding on app.main).
    noop_load = lambda *a, **kw: None  # noqa: E731
    monkeypatch.setattr(visual_search, "load_visual_index", noop_load)

    from app import main as app_main

    monkeypatch.setattr(app_main, "load_visual_index", noop_load)

    # Provide deterministic visual scores keyed by listing_id. Only the FIRST
    # listing returned by the gate gets a positive visual score; every other
    # one is omitted.
    captured_first_id: dict[str, str] = {}

    def fake_score_candidates(query_text: str, candidates):
        if not candidates:
            return {}
        first_id = str(candidates[0]["listing_id"])
        captured_first_id["id"] = first_id
        return {first_id: 0.99}

    monkeypatch.setattr(visual_search, "score_candidates", fake_score_candidates)
    monkeypatch.setattr(visual_search, "is_loaded", lambda: True)
    # search_service imported these by name at import time; patch the binding there too.
    monkeypatch.setattr(search_service, "visual_score_candidates", fake_score_candidates)
    monkeypatch.setattr(search_service, "visual_is_loaded", lambda: True)

    from app.main import app

    with TestClient(app) as client:
        response = client.post(
            "/listings", json={"query": "bright zurich", "limit": 5}
        )

    assert response.status_code == 200
    body = response.json()
    assert body["listings"]
    top = body["listings"][0]
    # The listing we gave a visual score to must win the rank.
    assert top["listing_id"] == captured_first_id["id"]
    # Score is an RRF float (positive, small: ~1/(60+1) + 1/(60+1) ≈ 0.033).
    assert top["score"] > 0
    assert top["score"] < 0.1
    assert "visual match" in top["reason"]
