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
