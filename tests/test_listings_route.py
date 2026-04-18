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
