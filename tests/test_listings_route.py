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


def test_post_listings_response_carries_demo_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pin the meta + per-listing `breakdown` fields the demo UI reads.

    The /demo page renders the extracted query plan, activated soft
    preferences, pipeline state, and a per-listing score breakdown. Any
    rename or removal of these fields silently breaks that UI, so this
    test pins the exact shape.
    """
    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")
    os.environ["LISTINGS_DB_PATH"] = str(tmp_path / "listings.db")

    from app.harness import search_service
    from app.models.schemas import SoftPreferences

    def fake_extract(query: str) -> HardFilters:
        return HardFilters(
            city=["zurich"],
            max_price=3000,
            features=["balcony"],
            bm25_keywords=["ruhig"],
            soft_preferences=SoftPreferences(quiet=True, near_public_transport=True),
        )

    monkeypatch.setattr(search_service, "extract_hard_facts", fake_extract)

    from app.main import app

    with TestClient(app) as client:
        response = client.post(
            "/listings",
            json={"query": "zurich ruhig balkon", "limit": 3},
        )

    assert response.status_code == 200
    body = response.json()

    meta = body["meta"]
    assert meta["query"] == "zurich ruhig balkon"
    # query_plan must be the LLM hard-filter JSON (with soft_preferences inline)
    qp = meta["query_plan"]
    assert qp["city"] == ["zurich"]
    assert qp["max_price"] == 3000
    assert qp["features"] == ["balcony"]
    assert qp["bm25_keywords"] == ["ruhig"]
    assert qp["soft_preferences"]["quiet"] is True
    assert qp["soft_preferences"]["near_public_transport"] is True
    # pipeline object required keys
    pipeline = meta["pipeline"]
    assert set(pipeline) >= {"bm25", "visual", "text_embed", "soft_rankings", "rrf_k"}
    assert pipeline["bm25"] is True
    assert pipeline["rrf_k"] == 60
    assert isinstance(pipeline["soft_rankings"], int)
    # pool/returned visibility
    assert isinstance(meta["candidate_pool_size"], int)
    assert isinstance(meta["returned"], int)

    # Every returned listing must carry a breakdown with the pinned keys.
    # Memory-related keys were added when user-system personalization shipped;
    # they're always present and default to (0, None) for anonymous callers.
    assert body["listings"]
    for item in body["listings"]:
        bd = item["breakdown"]
        assert bd is not None
        # Tier 3a extended this with 4 per-channel memory fields.
        assert set(bd) == {
            "rrf_score",
            "bm25_score",
            "visual_score",
            "text_embed_score",
            "soft_signals_activated",
            "memory_rankings_activated",
            "memory_score",
            "memory_semantic",
            "memory_visual",
            "memory_feature",
            "memory_price",
        }
        # With no keyword match, bm25_score is None, not a huge sentinel.
        assert bd["bm25_score"] is None or bd["bm25_score"] > 0
        assert bd["soft_signals_activated"] == pipeline["soft_rankings"]
        # Anonymous callers never trigger the memory channel.
        assert bd["memory_rankings_activated"] == 0
        assert bd["memory_score"] is None
        # All four per-channel memory fields are None for anonymous callers.
        assert bd["memory_semantic"] is None
        assert bd["memory_visual"] is None
        assert bd["memory_feature"] is None
        assert bd["memory_price"] is None

    # Every returned listing must also carry match_detail, shaped for the UI.
    for item in body["listings"]:
        md = item["match_detail"]
        assert md is not None
        assert set(md) == {
            "hard_checks",
            "matched_keywords",
            "unmatched_keywords",
            "soft_facts",
        }
        # One hard_check per requested hard constraint. Our fake_extract set
        # city + max_price + features=[balcony] → expect 3 check rows.
        labels = [h["label"] for h in md["hard_checks"]]
        assert "city" in labels
        assert "price" in labels
        assert any(lbl.startswith("feature: balcony") for lbl in labels)
        # Keyword classification must be disjoint and cover the requested set.
        all_kw = set(md["matched_keywords"]) | set(md["unmatched_keywords"])
        assert all_kw == {"ruhig"}
        assert not (set(md["matched_keywords"]) & set(md["unmatched_keywords"]))
        # Activated soft prefs were {quiet, near_public_transport} → expect
        # at most two soft_facts (with real data may be 2; with no signal
        # row we get [] and the UI renders "no data"); never more.
        assert len(md["soft_facts"]) <= 2
        for fact in md["soft_facts"]:
            assert set(fact) == {"axis", "label", "value", "interpretation"}
            assert fact["interpretation"] in {"good", "ok", "poor", "unknown"}


def test_demo_page_served(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The /demo page and its CSS/JS assets are reachable."""
    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")
    os.environ["LISTINGS_DB_PATH"] = str(tmp_path / "listings.db")

    from app.main import app

    with TestClient(app) as client:
        r = client.get("/demo")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]
        assert "Listings Search" in r.text
        r = client.get("/demo-assets/demo.css")
        assert r.status_code == 200
        r = client.get("/demo-assets/demo.js")
        assert r.status_code == 200


# ---------- GET /listings/{id} --------------------------------------------


def test_get_listing_returns_full_data(tmp_path, monkeypatch) -> None:
    """Clicking a saved card calls ``GET /listings/{id}``; the handler
    must return the full ``ListingData`` shape the detail modal renders."""
    import os
    import sqlite3
    from pathlib import Path
    from fastapi.testclient import TestClient

    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")

    from app.main import app
    from app.config import get_settings

    with TestClient(app) as client:
        with sqlite3.connect(get_settings().db_path) as db:
            row = db.execute(
                "SELECT listing_id FROM listings "
                "WHERE title IS NOT NULL AND price IS NOT NULL LIMIT 1"
            ).fetchone()
        assert row is not None
        lid = row[0]
        r = client.get(f"/listings/{lid}")
    assert r.status_code == 200, r.text
    body = r.json()
    # Pinned shape for the detail modal.
    assert body["id"] == lid
    assert "title" in body and body["title"]
    assert "price_chf" in body
    assert "rooms" in body
    assert "image_urls" in body and isinstance(body["image_urls"], (list, type(None)))
    assert "features" in body and isinstance(body["features"], list)


def test_get_listing_404(tmp_path) -> None:
    from fastapi.testclient import TestClient
    from app.main import app

    with TestClient(app) as client:
        r = client.get("/listings/nonexistent_xyz_1234")
    assert r.status_code == 404
