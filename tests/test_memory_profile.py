"""Profile builder: cold-start gate, weight math, price stats, feature taste."""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path

import pytest

from app.auth.db import bootstrap_users_db, connect
from app.memory import profile as profile_mod
from app.memory.profile import COLD_START_MIN_POSITIVES, build_profile


def _seed_users_db(tmp_path: Path) -> Path:
    users_db = tmp_path / "users.db"
    bootstrap_users_db(users_db)
    with connect(users_db) as conn:
        conn.execute(
            "INSERT INTO users (username, email, password_hash, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("tester", "t@x.co", "hash", "2026-01-01T00:00:00+00:00"),
        )
        conn.commit()
    return users_db


def _seed_listings_db(
    tmp_path: Path, rows: list[tuple[str, int, dict]]
) -> Path:
    listings_db = tmp_path / "listings.db"
    with sqlite3.connect(listings_db) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE listings (
                listing_id TEXT PRIMARY KEY,
                price INTEGER,
                features_json TEXT
            )
            """
        )
        for lid, price, feats in rows:
            conn.execute(
                "INSERT INTO listings (listing_id, price, features_json) VALUES (?, ?, ?)",
                (lid, price, json.dumps(list(feats))),
            )
        conn.commit()
    return listings_db


def _insert_interaction(
    users_db: Path,
    *,
    user_id: int,
    listing_id: str,
    kind: str,
    value: float | None = None,
    created_at: str | None = None,
) -> None:
    created_at = created_at or dt.datetime.now(dt.timezone.utc).isoformat()
    with connect(users_db) as conn:
        conn.execute(
            "INSERT INTO user_interactions "
            "(user_id, listing_id, kind, value, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (user_id, listing_id, kind, value, created_at),
        )
        conn.commit()


def test_empty_history_is_cold_start(tmp_path: Path) -> None:
    users_db = _seed_users_db(tmp_path)
    listings_db = _seed_listings_db(tmp_path, [])
    profile = build_profile(
        user_id=1, users_db_path=users_db, listings_db_path=listings_db
    )
    assert profile.is_cold_start is True
    assert profile.positive_count == 0
    assert profile.weights_by_id == {}


def test_cold_start_threshold(tmp_path: Path) -> None:
    users_db = _seed_users_db(tmp_path)
    listings_db = _seed_listings_db(
        tmp_path, [(f"l{i}", 2000, {"balcony"}) for i in range(5)]
    )
    # Only 2 saves -> still cold start
    for i in range(COLD_START_MIN_POSITIVES - 1):
        _insert_interaction(
            users_db, user_id=1, listing_id=f"l{i}", kind="save"
        )
    profile = build_profile(
        user_id=1, users_db_path=users_db, listings_db_path=listings_db
    )
    assert profile.is_cold_start is True


def test_enough_positives_exits_cold_start(tmp_path: Path) -> None:
    users_db = _seed_users_db(tmp_path)
    listings_db = _seed_listings_db(
        tmp_path, [(f"l{i}", 2000, {"balcony"}) for i in range(5)]
    )
    for i in range(COLD_START_MIN_POSITIVES):
        _insert_interaction(
            users_db, user_id=1, listing_id=f"l{i}", kind="save"
        )
    profile = build_profile(
        user_id=1, users_db_path=users_db, listings_db_path=listings_db
    )
    assert profile.is_cold_start is False
    assert profile.positive_count == COLD_START_MIN_POSITIVES


def test_save_then_unsave_nets_to_zero(tmp_path: Path) -> None:
    users_db = _seed_users_db(tmp_path)
    listings_db = _seed_listings_db(tmp_path, [("l1", 2000, set())])
    _insert_interaction(users_db, user_id=1, listing_id="l1", kind="save")
    _insert_interaction(users_db, user_id=1, listing_id="l1", kind="unsave")
    profile = build_profile(
        user_id=1, users_db_path=users_db, listings_db_path=listings_db
    )
    assert profile.weights_by_id.get("l1", 0.0) == 0.0


def test_dismiss_is_negative_weight(tmp_path: Path) -> None:
    users_db = _seed_users_db(tmp_path)
    listings_db = _seed_listings_db(tmp_path, [("l1", 2000, set())])
    _insert_interaction(users_db, user_id=1, listing_id="l1", kind="dismiss")
    profile = build_profile(
        user_id=1, users_db_path=users_db, listings_db_path=listings_db
    )
    assert "l1" in profile.dismissed_ids
    assert profile.weights_by_id["l1"] < 0.0


def test_undismiss_reverses_dismiss_cleanly(tmp_path: Path) -> None:
    """A regretted dismissal must leave no footprint: no negative weight,
    and the listing must be out of the dismissed set so future rankings
    don't demote it.
    """
    import time
    users_db = _seed_users_db(tmp_path)
    listings_db = _seed_listings_db(tmp_path, [("l1", 2000, set())])
    _insert_interaction(users_db, user_id=1, listing_id="l1", kind="dismiss")
    # Small time gap so created_at orders correctly (ISO strings compare lex).
    time.sleep(0.01)
    _insert_interaction(users_db, user_id=1, listing_id="l1", kind="undismiss")
    profile = build_profile(
        user_id=1, users_db_path=users_db, listings_db_path=listings_db
    )
    assert "l1" not in profile.dismissed_ids
    assert profile.weights_by_id.get("l1", 0.0) == 0.0


def test_feature_taste_reflects_saved_features(tmp_path: Path) -> None:
    users_db = _seed_users_db(tmp_path)
    listings_db = _seed_listings_db(
        tmp_path,
        [
            ("l1", 2000, {"balcony", "elevator"}),
            ("l2", 2200, {"balcony", "minergie_certified"}),
            ("l3", 2100, {"balcony", "parking"}),
        ],
    )
    for lid in ("l1", "l2", "l3"):
        _insert_interaction(users_db, user_id=1, listing_id=lid, kind="save")
    profile = build_profile(
        user_id=1, users_db_path=users_db, listings_db_path=listings_db
    )
    assert profile.is_cold_start is False
    # Balcony is present in all 3 saves -> strongly positive
    assert profile.feature_taste["balcony"] > 0.5
    # Fireplace is in zero saves -> negative (saves had "no fireplace")
    assert profile.feature_taste["fireplace"] < 0


def test_price_stats_from_saves(tmp_path: Path) -> None:
    users_db = _seed_users_db(tmp_path)
    listings_db = _seed_listings_db(
        tmp_path,
        [("l1", 2000, set()), ("l2", 2200, set()), ("l3", 2100, set())],
    )
    for lid in ("l1", "l2", "l3"):
        _insert_interaction(users_db, user_id=1, listing_id=lid, kind="save")
    profile = build_profile(
        user_id=1, users_db_path=users_db, listings_db_path=listings_db
    )
    import math
    assert profile.price_mu is not None
    assert profile.price_sigma is not None
    # mu should sit near log(2100) (the middle save)
    assert abs(profile.price_mu - math.log(2100)) < 0.15


def test_like_contributes_same_as_save(tmp_path: Path) -> None:
    """``like`` is the canonical positive kind; ``save`` is a legacy alias.
    Both should land on the exact same weight.
    """
    users_db = _seed_users_db(tmp_path)
    listings_db = _seed_listings_db(
        tmp_path, [("a", 2000, set()), ("b", 2000, set())]
    )
    _insert_interaction(users_db, user_id=1, listing_id="a", kind="like")
    _insert_interaction(users_db, user_id=1, listing_id="b", kind="save")
    profile = build_profile(
        user_id=1, users_db_path=users_db, listings_db_path=listings_db
    )
    assert profile.weights_by_id["a"] == profile.weights_by_id["b"]
    assert profile.weights_by_id["a"] > 0


def test_bookmark_weighted_higher_than_like(tmp_path: Path) -> None:
    """Bookmarks are a stronger positive signal than likes: the user didn't
    just approve the style, they decided to keep the listing around.
    """
    users_db = _seed_users_db(tmp_path)
    listings_db = _seed_listings_db(
        tmp_path,
        [("b", 2000, {"balcony"}), ("l", 2000, {"balcony"})],
    )
    _insert_interaction(users_db, user_id=1, listing_id="b", kind="bookmark")
    _insert_interaction(users_db, user_id=1, listing_id="l", kind="like")
    profile = build_profile(
        user_id=1, users_db_path=users_db, listings_db_path=listings_db
    )
    w_bookmark = profile.weights_by_id["b"]
    w_like = profile.weights_by_id["l"]
    assert w_bookmark > w_like > 0


def test_bookmark_then_unbookmark_nets_to_zero(tmp_path: Path) -> None:
    """Undoing a bookmark fully reverses its contribution to the profile."""
    users_db = _seed_users_db(tmp_path)
    listings_db = _seed_listings_db(tmp_path, [("x", 2000, set())])
    _insert_interaction(users_db, user_id=1, listing_id="x", kind="bookmark")
    _insert_interaction(users_db, user_id=1, listing_id="x", kind="unbookmark")
    profile = build_profile(
        user_id=1, users_db_path=users_db, listings_db_path=listings_db
    )
    assert profile.weights_by_id.get("x", 0.0) == 0.0


def test_events_older_than_window_are_ignored(tmp_path: Path) -> None:
    users_db = _seed_users_db(tmp_path)
    listings_db = _seed_listings_db(
        tmp_path, [(f"l{i}", 2000, {"balcony"}) for i in range(5)]
    )
    ancient = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=365)).isoformat()
    for i in range(5):
        _insert_interaction(
            users_db,
            user_id=1,
            listing_id=f"l{i}",
            kind="save",
            created_at=ancient,
        )
    profile = build_profile(
        user_id=1, users_db_path=users_db, listings_db_path=listings_db
    )
    assert profile.is_cold_start is True
    assert profile.weights_by_id == {}
