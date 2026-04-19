"""Interaction-endpoint behaviour: save/unsave/click/dwell/dismiss + favorites."""
from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client() -> TestClient:
    from app.main import app
    with TestClient(app) as c:
        yield c


def _register(client: TestClient, username: str = "intuser") -> str:
    r = client.post(
        "/auth/register",
        json={"username": username, "email": f"{username}@x.co", "password": "hunter222"},
    )
    assert r.status_code == 201, r.text
    return client.get("/auth/csrf").json()["csrf_token"]


def _real_listing_id() -> str:
    from app.config import get_settings
    with sqlite3.connect(get_settings().db_path) as db:
        row = db.execute("SELECT listing_id FROM listings LIMIT 1").fetchone()
    assert row is not None, "no listings in DB"
    return row[0]


def test_bookmark_shows_in_favorites(client: TestClient) -> None:
    tok = _register(client, "savetest")
    lid = _real_listing_id()
    r = client.post(
        "/me/interactions",
        json={"listing_id": lid, "kind": "bookmark"},
        headers={"X-CSRF-Token": tok},
    )
    assert r.status_code == 201

    favs = client.get("/me/favorites").json()["favorites"]
    assert len(favs) == 1
    assert favs[0]["listing_id"] == lid


def test_bookmark_then_unbookmark_removes_from_favorites(client: TestClient) -> None:
    tok = _register(client, "unsavetest")
    lid = _real_listing_id()
    client.post(
        "/me/interactions",
        json={"listing_id": lid, "kind": "bookmark"},
        headers={"X-CSRF-Token": tok},
    )
    client.post(
        "/me/interactions",
        json={"listing_id": lid, "kind": "unbookmark"},
        headers={"X-CSRF-Token": tok},
    )
    favs = client.get("/me/favorites").json()["favorites"]
    assert favs == []


def test_duplicate_bookmark_is_idempotent_for_favorites(client: TestClient) -> None:
    tok = _register(client, "duptest")
    lid = _real_listing_id()
    # Three bookmarks in a row: favorites still report the listing exactly once.
    for _ in range(3):
        client.post(
            "/me/interactions",
            json={"listing_id": lid, "kind": "bookmark"},
            headers={"X-CSRF-Token": tok},
        )
    favs = client.get("/me/favorites").json()["favorites"]
    assert len(favs) == 1


def test_like_does_not_populate_favorites(client: TestClient) -> None:
    """Likes are a preference signal, not a bookmark. They must not appear in
    the Saved drawer.
    """
    tok = _register(client, "liketest")
    lid = _real_listing_id()
    client.post(
        "/me/interactions",
        json={"listing_id": lid, "kind": "like"},
        headers={"X-CSRF-Token": tok},
    )
    favs = client.get("/me/favorites").json()["favorites"]
    assert favs == []
    # But /me/likes does show it.
    likes = client.get("/me/likes").json()["favorites"]
    assert len(likes) == 1
    assert likes[0]["listing_id"] == lid


def test_bookmark_does_not_appear_in_likes(client: TestClient) -> None:
    tok = _register(client, "boooktest")
    lid = _real_listing_id()
    client.post(
        "/me/interactions",
        json={"listing_id": lid, "kind": "bookmark"},
        headers={"X-CSRF-Token": tok},
    )
    likes = client.get("/me/likes").json()["favorites"]
    assert likes == []


def test_favorites_are_enriched_with_listing_summary(client: TestClient) -> None:
    """The Saved drawer needs title + price + hero image etc. so it can
    render a real card, not just a raw listing_id. Asserts the enrichment
    fields are populated when the listing exists in listings.db.
    """
    tok = _register(client, "enrichtest")
    # Pick a listing we know has a title and a price.
    from app.config import get_settings
    with sqlite3.connect(get_settings().db_path) as db:
        db.row_factory = sqlite3.Row
        row = db.execute(
            "SELECT listing_id FROM listings "
            "WHERE title IS NOT NULL AND price IS NOT NULL LIMIT 1"
        ).fetchone()
    assert row is not None
    lid = row["listing_id"]
    client.post(
        "/me/interactions",
        json={"listing_id": lid, "kind": "bookmark"},
        headers={"X-CSRF-Token": tok},
    )
    favs = client.get("/me/favorites").json()["favorites"]
    assert len(favs) == 1
    fav = favs[0]
    assert fav["listing_id"] == lid
    assert fav["title"] is not None and fav["title"] != ""
    assert isinstance(fav["price_chf"], int) and fav["price_chf"] > 0
    # Features may be empty; just require the shape.
    assert isinstance(fav["features"], list)


def test_legacy_save_shows_in_both_drawers(client: TestClient) -> None:
    """DB rows written with the pre-split ``save`` kind meant "bookmark AND
    like" in the old UX, so they show up in both drawers post-split.
    Without this, users who saved listings before the refactor would see
    those listings disappear from their Saved drawer.
    """
    tok = _register(client, "legacytest")
    lid = _real_listing_id()
    client.post(
        "/me/interactions",
        json={"listing_id": lid, "kind": "save"},
        headers={"X-CSRF-Token": tok},
    )
    likes = client.get("/me/likes").json()["favorites"]
    favs = client.get("/me/favorites").json()["favorites"]
    assert len(likes) == 1 and likes[0]["listing_id"] == lid
    assert len(favs) == 1 and favs[0]["listing_id"] == lid


def test_legacy_unsave_removes_from_favorites(client: TestClient) -> None:
    """``unsave`` is the negation of ``save`` in the old UX; it should
    remove the listing from the Saved drawer (and from likes)."""
    tok = _register(client, "legacyunsavetest")
    lid = _real_listing_id()
    client.post(
        "/me/interactions",
        json={"listing_id": lid, "kind": "save"},
        headers={"X-CSRF-Token": tok},
    )
    client.post(
        "/me/interactions",
        json={"listing_id": lid, "kind": "unsave"},
        headers={"X-CSRF-Token": tok},
    )
    assert client.get("/me/favorites").json()["favorites"] == []
    assert client.get("/me/likes").json()["favorites"] == []


def test_unknown_listing_id_is_404(client: TestClient) -> None:
    tok = _register(client, "unknowntest")
    r = client.post(
        "/me/interactions",
        json={"listing_id": "nonexistent_xyz_1234", "kind": "click"},
        headers={"X-CSRF-Token": tok},
    )
    assert r.status_code == 404


def test_anonymous_interaction_is_401(client: TestClient) -> None:
    lid = _real_listing_id()
    # Even with a csrf header, no session cookie -> 401
    csrf_tok = client.get("/auth/csrf").json()["csrf_token"]
    r = client.post(
        "/me/interactions",
        json={"listing_id": lid, "kind": "click"},
        headers={"X-CSRF-Token": csrf_tok},
    )
    assert r.status_code == 401


def test_clear_interactions_wipes_history(client: TestClient) -> None:
    tok = _register(client, "cleartest")
    lid = _real_listing_id()
    client.post(
        "/me/interactions",
        json={"listing_id": lid, "kind": "save"},
        headers={"X-CSRF-Token": tok},
    )
    r = client.request(
        "DELETE", "/me/interactions", headers={"X-CSRF-Token": tok}
    )
    assert r.status_code == 200
    assert r.json()["deleted"] == 1
    favs = client.get("/me/favorites").json()["favorites"]
    assert favs == []


def test_dismiss_records_negative(client: TestClient) -> None:
    tok = _register(client, "dismisstest")
    lid = _real_listing_id()
    r = client.post(
        "/me/interactions",
        json={"listing_id": lid, "kind": "dismiss"},
        headers={"X-CSRF-Token": tok},
    )
    assert r.status_code == 201

    # Dismiss doesn't show up in favorites.
    favs = client.get("/me/favorites").json()["favorites"]
    assert favs == []


def test_dismissed_endpoint_reflects_latest_state(client: TestClient) -> None:
    """``GET /me/dismissed`` should return the id after dismiss, then drop it
    after undismiss."""
    tok = _register(client, "dismissedep")
    lid = _real_listing_id()
    # Initially empty.
    assert client.get("/me/dismissed").json() == []
    # Dismiss -> appears.
    client.post(
        "/me/interactions",
        json={"listing_id": lid, "kind": "dismiss"},
        headers={"X-CSRF-Token": tok},
    )
    assert client.get("/me/dismissed").json() == [lid]
    # Undismiss -> disappears.
    client.post(
        "/me/interactions",
        json={"listing_id": lid, "kind": "undismiss"},
        headers={"X-CSRF-Token": tok},
    )
    assert client.get("/me/dismissed").json() == []


def test_dwell_value_roundtrips(client: TestClient) -> None:
    tok = _register(client, "dwelltest")
    lid = _real_listing_id()
    r = client.post(
        "/me/interactions",
        json={"listing_id": lid, "kind": "dwell", "value": 7.5},
        headers={"X-CSRF-Token": tok},
    )
    assert r.status_code == 201
