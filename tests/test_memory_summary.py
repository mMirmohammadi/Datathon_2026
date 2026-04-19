"""Profile-summary helpers: feature splitting, price band, activity counts."""
from __future__ import annotations

import datetime as dt
import json
import math
import sqlite3
from pathlib import Path

import pytest

from app.auth.db import bootstrap_users_db, connect as connect_users_db
from app.memory.profile import UserProfile, build_profile
from app.memory.summary import (
    FEATURE_LABELS,
    FEATURE_TASTE_MIN_ABS,
    MAX_TAGS_PER_SIDE,
    derive_price_range,
    fetch_interaction_stats,
    split_feature_taste,
    summarize_profile,
)


def _seed_users_db(tmp_path: Path) -> Path:
    users_db = tmp_path / "users.db"
    bootstrap_users_db(users_db)
    with connect_users_db(users_db) as conn:
        conn.execute(
            "INSERT INTO users (username, email, password_hash, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("t", "t@example.com", "hash", "2026-01-01T00:00:00+00:00"),
        )
        conn.commit()
    return users_db


def _seed_listings_db(
    tmp_path: Path, rows: list[tuple[str, int, set]]
) -> Path:
    listings_db = tmp_path / "listings.db"
    with sqlite3.connect(listings_db) as conn:
        conn.execute(
            "CREATE TABLE listings "
            "(listing_id TEXT PRIMARY KEY, price INTEGER, features_json TEXT)"
        )
        for lid, price, feats in rows:
            conn.execute(
                "INSERT INTO listings VALUES (?, ?, ?)",
                (lid, price, json.dumps(list(feats))),
            )
        conn.commit()
    return listings_db


def _insert(users_db: Path, user_id: int, lid: str, kind: str, value=None) -> None:
    with connect_users_db(users_db) as conn:
        conn.execute(
            "INSERT INTO user_interactions "
            "(user_id, listing_id, kind, value, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, lid, kind, value, dt.datetime.now(dt.timezone.utc).isoformat()),
        )
        conn.commit()


# ---------- split_feature_taste ---------------------------------------------


def test_split_feature_taste_bands() -> None:
    """Only features with |weight| >= 0.5 (≈75% signal agreement) show up,
    sorted by absolute strength, capped at MAX_TAGS_PER_SIDE.

    Features with weaker but non-zero signal (0.2, 0.4, etc.) are
    deliberately dropped: we don't surface "the system thinks you avoid X"
    from a single coincidental observation.
    """
    profile = UserProfile(
        user_id=1,
        positive_count=5,
        feature_taste={
            "balcony": 0.9,              # above threshold → liked
            "elevator": 0.5,             # exactly threshold → liked
            "minergie_certified": 0.3,   # below threshold → filtered
            "parking": 0.05,             # below → filtered
            "fireplace": -0.4,           # |w|<0.5 → filtered (not confident enough)
            "temporary": -0.8,           # above threshold → avoided
            "child_friendly": -0.02,     # below → filtered
            "garage": 0.0,
            "pets_allowed": 0.0,
            "new_build": 0.0,
            "wheelchair_accessible": 0.0,
            "private_laundry": 0.0,
        },
    )
    liked, avoided = split_feature_taste(profile)
    assert [x["key"] for x in liked] == ["balcony", "elevator"]
    assert [x["key"] for x in avoided] == ["temporary"]
    # Sanity: every below-threshold key dropped.
    all_keys = {x["key"] for x in liked + avoided}
    for drop in ("parking", "child_friendly", "minergie_certified", "fireplace"):
        assert drop not in all_keys
    # Labels come from the explicit map where available.
    assert liked[0]["label"] == FEATURE_LABELS["balcony"]


def test_split_feature_taste_caps_at_six_per_side() -> None:
    """Even a pathologically opinionated user sees at most 6 chips per side."""
    taste = {k: 0.5 for k in FEATURE_LABELS}
    profile = UserProfile(
        user_id=1, positive_count=3, feature_taste=taste
    )
    liked, avoided = split_feature_taste(profile)
    assert len(liked) == MAX_TAGS_PER_SIDE
    assert avoided == []


def test_split_feature_taste_empty_profile() -> None:
    profile = UserProfile(user_id=1)
    liked, avoided = split_feature_taste(profile)
    assert liked == []
    assert avoided == []


def test_feature_taste_threshold_requires_strong_signal() -> None:
    """The confidence threshold (currently 0.5) gates what shows up as a tag.

    The exact value is a policy choice, not a contract - but we pin the
    boundary behaviour so future tweaks stay intentional.
    """
    # Just below threshold → filtered.
    profile = UserProfile(
        user_id=1,
        positive_count=5,
        feature_taste={"balcony": FEATURE_TASTE_MIN_ABS - 0.001},
    )
    liked, _ = split_feature_taste(profile)
    assert liked == []
    # At threshold → included.
    profile = UserProfile(
        user_id=1,
        positive_count=5,
        feature_taste={"balcony": FEATURE_TASTE_MIN_ABS},
    )
    liked, _ = split_feature_taste(profile)
    assert len(liked) == 1
    # A "2 out of 3 positives have it" style signal (~0.33) must not surface.
    profile = UserProfile(
        user_id=1,
        positive_count=3,
        feature_taste={"balcony": 0.33},
    )
    liked, _ = split_feature_taste(profile)
    assert liked == [], (
        "two-out-of-three is coincidence, not preference; must stay hidden"
    )


# ---------- derive_price_range ----------------------------------------------


def test_price_range_exponentiates_log_mu_sigma() -> None:
    profile = UserProfile(
        user_id=1,
        positive_count=3,
        price_mu=math.log(2500.0),
        price_sigma=0.2,
    )
    band = derive_price_range(profile)
    assert band is not None
    assert abs(band["mid_chf"] - 2500) <= 1
    # ±1σ band: low < mid < high
    assert band["low_chf"] < band["mid_chf"] < band["high_chf"]


def test_price_range_none_when_no_stats() -> None:
    profile = UserProfile(user_id=1, positive_count=3)
    assert derive_price_range(profile) is None


# ---------- fetch_interaction_stats -----------------------------------------


def test_fetch_stats_counts_active_listings(tmp_path: Path) -> None:
    users_db = _seed_users_db(tmp_path)
    # like on l1, dismiss on l2, bookmark on l3
    _insert(users_db, 1, "l1", "like")
    _insert(users_db, 1, "l2", "dismiss")
    _insert(users_db, 1, "l3", "bookmark")
    stats = fetch_interaction_stats(users_db, user_id=1)
    assert stats == {"likes": 1, "bookmarks": 1, "dismissals": 1}


def test_fetch_stats_netting(tmp_path: Path) -> None:
    """Unlike / unbookmark / undismiss correctly remove from counts."""
    import time
    users_db = _seed_users_db(tmp_path)
    # Like then unlike → 0 likes.
    _insert(users_db, 1, "a", "like")
    time.sleep(0.01)
    _insert(users_db, 1, "a", "unlike")
    # Bookmark only → 1 bookmark.
    _insert(users_db, 1, "b", "bookmark")
    # Dismiss then undismiss → 0 dismissals.
    _insert(users_db, 1, "c", "dismiss")
    time.sleep(0.01)
    _insert(users_db, 1, "c", "undismiss")
    stats = fetch_interaction_stats(users_db, user_id=1)
    assert stats == {"likes": 0, "bookmarks": 1, "dismissals": 0}


def test_fetch_stats_legacy_save_counts_in_likes_and_bookmarks(tmp_path: Path) -> None:
    """A pre-split ``save`` contributes to both drawers. Matches the
    drawer-endpoint semantics.
    """
    users_db = _seed_users_db(tmp_path)
    _insert(users_db, 1, "x", "save")
    stats = fetch_interaction_stats(users_db, user_id=1)
    assert stats["likes"] == 1
    assert stats["bookmarks"] == 1


# ---------- summarize_profile (full shape) ----------------------------------


def test_summarize_profile_cold_start(tmp_path: Path) -> None:
    users_db = _seed_users_db(tmp_path)
    listings_db = _seed_listings_db(tmp_path, [("l1", 2000, {"balcony"})])
    # One like → below the 3-positive cold-start threshold.
    _insert(users_db, 1, "l1", "like")
    profile = build_profile(
        user_id=1, users_db_path=users_db, listings_db_path=listings_db
    )
    summary = summarize_profile(profile, users_db)
    assert summary["is_cold_start"] is True
    # Cold-start skips the feature vector and the price band.
    assert summary["liked_features"] == []
    assert summary["avoided_features"] == []
    assert summary["price_range_chf"] is None
    # But activity counts are always returned.
    assert summary["stats"]["likes"] == 1


def test_summarize_profile_warm_shape(tmp_path: Path) -> None:
    users_db = _seed_users_db(tmp_path)
    listings_db = _seed_listings_db(
        tmp_path,
        [
            ("l1", 2200, {"balcony", "elevator"}),
            ("l2", 2400, {"balcony", "minergie_certified"}),
            ("l3", 2300, {"balcony"}),
        ],
    )
    for lid in ("l1", "l2", "l3"):
        _insert(users_db, 1, lid, "like")
    profile = build_profile(
        user_id=1, users_db_path=users_db, listings_db_path=listings_db
    )
    summary = summarize_profile(profile, users_db)
    assert summary["is_cold_start"] is False
    # Balcony is in every like -> strongest liked feature.
    assert summary["liked_features"][0]["key"] == "balcony"
    # Price band is populated.
    assert summary["price_range_chf"] is not None
    assert summary["stats"]["likes"] == 3
