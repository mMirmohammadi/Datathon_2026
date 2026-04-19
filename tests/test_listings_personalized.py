"""Integration: POST /listings respects personalize flag + user session.

Anonymous behaviour must be identical to the pre-memory pipeline; a warm
user's ranking changes reflecting their saved listings; hard filters are
still strictly enforced.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.models.schemas import HardFilters


@pytest.fixture()
def client_with_fake_llm(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """TestClient with the LLM extractor monkeypatched to a fixed plan.

    Using a deterministic hard-filter set means the candidate pool is the same
    for every call in this test, so any ordering difference is attributable to
    the memory channel (and nothing else).
    """
    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")

    def fake_extract(query: str) -> HardFilters:
        return HardFilters(city=["zurich"])

    from app.harness import search_service
    monkeypatch.setattr(search_service, "extract_hard_facts", fake_extract)

    from app.main import app
    with TestClient(app) as c:
        yield c


def _save(client: TestClient, listing_id: str, csrf_token: str) -> None:
    r = client.post(
        "/me/interactions",
        json={"listing_id": listing_id, "kind": "save"},
        headers={"X-CSRF-Token": csrf_token},
    )
    assert r.status_code == 201, r.text


def _real_ids(limit: int = 10) -> list[str]:
    from app.config import get_settings
    with sqlite3.connect(get_settings().db_path) as db:
        rows = db.execute(
            "SELECT listing_id FROM listings WHERE city_slug='zurich' LIMIT ?",
            (limit,),
        ).fetchall()
    return [r[0] for r in rows]


def test_anonymous_response_has_no_memory_channel(client_with_fake_llm: TestClient) -> None:
    r = client_with_fake_llm.post("/listings", json={"query": "Zuerich", "limit": 5})
    assert r.status_code == 200
    meta = r.json()["meta"]
    assert meta["pipeline"]["memory"] is False
    assert meta["pipeline"]["memory_rankings"] == 0


def test_anonymous_personalize_flag_is_ignored(client_with_fake_llm: TestClient) -> None:
    """Anonymous caller cannot trigger personalization even with personalize=true."""
    r = client_with_fake_llm.post(
        "/listings", json={"query": "Zuerich", "limit": 5, "personalize": True}
    )
    assert r.json()["meta"]["pipeline"]["memory"] is False


def test_warm_user_triggers_memory_channel(
    client_with_fake_llm: TestClient,
) -> None:
    """A user with enough positives gets the memory channel in the pipeline."""
    c = client_with_fake_llm
    r = c.post(
        "/auth/register",
        json={"username": "warmuser", "email": "w@x.co", "password": "hunter222"},
    )
    assert r.status_code == 201
    tok = c.get("/auth/csrf").json()["csrf_token"]
    ids = _real_ids(limit=4)
    for lid in ids[:3]:
        _save(c, lid, tok)

    r = c.post(
        "/listings", json={"query": "Zuerich", "limit": 5, "personalize": True}
    )
    meta = r.json()["meta"]
    assert meta["pipeline"]["memory"] is True
    assert meta["pipeline"]["memory_rankings"] >= 1


def test_opt_out_disables_memory_for_authenticated_user(
    client_with_fake_llm: TestClient,
) -> None:
    c = client_with_fake_llm
    c.post(
        "/auth/register",
        json={"username": "optuser", "email": "o@x.co", "password": "hunter222"},
    )
    tok = c.get("/auth/csrf").json()["csrf_token"]
    ids = _real_ids(limit=4)
    for lid in ids[:3]:
        _save(c, lid, tok)
    r = c.post(
        "/listings", json={"query": "Zuerich", "limit": 5, "personalize": False}
    )
    assert r.json()["meta"]["pipeline"]["memory"] is False


def test_warm_user_hard_filter_still_strict(
    client_with_fake_llm: TestClient,
) -> None:
    """Every listing returned to a personalized user must still satisfy
    the LLM-extracted hard filters (city=zurich in this fixture)."""
    c = client_with_fake_llm
    c.post(
        "/auth/register",
        json={"username": "strict", "email": "s@x.co", "password": "hunter222"},
    )
    tok = c.get("/auth/csrf").json()["csrf_token"]
    ids = _real_ids(limit=4)
    for lid in ids[:3]:
        _save(c, lid, tok)

    r = c.post(
        "/listings", json={"query": "Zuerich", "limit": 10, "personalize": True}
    )
    assert r.status_code == 200
    for item in r.json()["listings"]:
        # The fake extractor sets city=['zurich']; all results must match.
        city_slug = item["listing"].get("city") or ""
        assert city_slug.lower().startswith("zürich") or city_slug.lower().startswith("zurich"), item
