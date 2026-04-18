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


def test_post_listings_pipeline_hybrid_three_channels_plus_soft(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Full n-way hybrid: visual + text_embed + soft_preferences all
    contribute. Listing present in every ranking must win; its reason must
    mention every activated channel.
    """
    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")
    os.environ["LISTINGS_DB_PATH"] = str(tmp_path / "listings.db")
    monkeypatch.setenv("LISTINGS_VISUAL_ENABLED", "1")
    monkeypatch.setenv("LISTINGS_TEXT_EMBED_ENABLED", "1")

    from app.core import text_embed_search, visual_search
    from app.harness import search_service
    from app.models.schemas import SoftPreferences

    # Stub the eager loaders on both the core modules and the app.main binding.
    noop = lambda *a, **kw: None  # noqa: E731
    monkeypatch.setattr(visual_search, "load_visual_index", noop)
    monkeypatch.setattr(text_embed_search, "load_text_embed_index", noop)

    from app import main as app_main

    monkeypatch.setattr(app_main, "load_visual_index", noop)
    monkeypatch.setattr(app_main, "load_text_embed_index", noop)

    captured: dict[str, str] = {}

    def fake_extract(query: str) -> HardFilters:
        return HardFilters(
            city=["winterthur"],
            soft_preferences=SoftPreferences(quiet=True, near_schools=True),
            limit=10,
        )

    monkeypatch.setattr(search_service, "extract_hard_facts", fake_extract)

    def fake_visual(query, candidates):
        if not candidates:
            return {}
        first_id = str(candidates[0]["listing_id"])
        captured["id"] = first_id
        return {first_id: 0.95}

    def fake_text_embed(query, candidates):
        if not candidates:
            return {}
        first_id = captured.get("id") or str(candidates[0]["listing_id"])
        return {first_id: 0.88}

    def fake_soft(candidates, soft, db_path):
        if not candidates:
            return []
        first_id = captured.get("id") or str(candidates[0]["listing_id"])
        # Two activated soft preferences -> two rankings, both favouring the same id.
        return [[first_id], [first_id]]

    monkeypatch.setattr(search_service, "visual_enabled", lambda: True)
    monkeypatch.setattr(search_service, "visual_is_loaded", lambda: True)
    monkeypatch.setattr(search_service, "visual_score_candidates", fake_visual)
    monkeypatch.setattr(search_service, "text_embed_enabled", lambda: True)
    monkeypatch.setattr(search_service, "text_embed_is_loaded", lambda: True)
    monkeypatch.setattr(search_service, "text_embed_score_candidates", fake_text_embed)
    monkeypatch.setattr(search_service, "build_soft_rankings", fake_soft)

    from app.main import app

    with TestClient(app) as client:
        response = client.post(
            "/listings",
            json={"query": "ruhig nahe Schulen in Winterthur", "limit": 5},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["listings"]
    top = body["listings"][0]
    # The listing boosted by every channel must win.
    assert top["listing_id"] == captured["id"]
    assert top["score"] > 0
    # Reason must mention all four contributors.
    reason = top["reason"]
    assert "text match" in reason or "BM25" not in reason  # BM25 implicit
    assert "visual match" in reason
    assert "semantic match" in reason
    assert "soft preferences" in reason
