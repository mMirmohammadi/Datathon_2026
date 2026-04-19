"""Human-readable projection of the memory profile for the UI.

The ranker consumes ``UserProfile`` directly; the UI gets a trimmed-down
``dict`` via :func:`summarize_profile` that drops the heavy vectors
(``weights_by_id``, listing id lists) and surfaces:

* The 12 feature-taste coordinates, split into "liked" and "avoided"
  subsets and filtered to absolute-weight >= 0.15 so near-zero noise
  doesn't show up as a tag.
* The log-price mean/std converted to a CHF low/mid/high band.
* Simple activity counts (likes / bookmarks / dismissals) computed per
  listing using the same "latest event wins" rule as the drawer
  endpoints, so the numbers match what the user sees in the UI.
* A ``is_cold_start`` flag mirroring ``UserProfile.is_cold_start``.

Pure read-only; never mutates anything.
"""
from __future__ import annotations

import math
import sqlite3
from pathlib import Path

from app.auth.db import connect as connect_users_db
from app.core.hard_filters import FEATURE_COLUMN_MAP
from app.memory.profile import UserProfile


# User-facing labels for the 12 feature flags. Keyed on the same slugs
# ``FEATURE_COLUMN_MAP`` uses so the profile's ``feature_taste`` dict maps
# 1:1. Unlisted keys fall back to a title-cased version of the slug.
FEATURE_LABELS: dict[str, str] = {
    "balcony": "Balcony",
    "elevator": "Elevator",
    "parking": "Parking",
    "garage": "Garage",
    "fireplace": "Fireplace",
    "child_friendly": "Child-friendly",
    "pets_allowed": "Pets allowed",
    "temporary": "Temporary lease",
    "new_build": "New build",
    "wheelchair_accessible": "Wheelchair accessible",
    "private_laundry": "Private laundry",
    "minergie_certified": "Minergie-certified",
}

# Only tags with |weight| >= this value are surfaced in the modal.
#
# Given the normalisation used in ``profile.py`` (Σ w × (±1) / Σ |w|), a
# coordinate of 0.5 means at least ~75% of the weighted signal agrees:
#   * 3 positives with feat present in 3/3: weight = +1.0  → shown
#   * 3 positives with feat present in 2/3: weight ≈ +0.33 → hidden (noise)
#   * 5 positives with feat present in 4/5: weight ≈ +0.6  → shown
#   * 5 positives with feat present in 3/5: weight ≈ +0.2  → hidden (noise)
#
# Tuned on the conservative side because "the system thinks you avoid X"
# is a claim we don't want to make from a single coincidence.
FEATURE_TASTE_MIN_ABS = 0.5

# Cap the number of tags we show per side so the modal stays skimmable.
MAX_TAGS_PER_SIDE = 6


def _label_for(key: str) -> str:
    """Return the user-facing label for a feature key.

    Sanity: every key in ``FEATURE_COLUMN_MAP`` should have a matching entry
    in ``FEATURE_LABELS``; if one slips through (e.g. a teammate adds a new
    feature without updating this file) we fall back to a title-cased
    slug so the UI stays functional.
    """
    if key in FEATURE_LABELS:
        return FEATURE_LABELS[key]
    return key.replace("_", " ").title()


def split_feature_taste(
    profile: UserProfile,
) -> tuple[list[dict], list[dict]]:
    """Return ``(liked, avoided)`` lists of ``{key, label, weight}`` dicts.

    ``liked`` is sorted by descending weight (strongest positive first);
    ``avoided`` by ascending weight (strongest negative first). Each side
    is capped at :data:`MAX_TAGS_PER_SIDE`.
    """
    items = [
        (k, float(v))
        for k, v in profile.feature_taste.items()
        if abs(v) >= FEATURE_TASTE_MIN_ABS
    ]
    liked_src = sorted((kv for kv in items if kv[1] > 0), key=lambda kv: -kv[1])
    avoided_src = sorted((kv for kv in items if kv[1] < 0), key=lambda kv: kv[1])

    def fmt(kv: tuple[str, float]) -> dict:
        k, v = kv
        return {"key": k, "label": _label_for(k), "weight": round(v, 3)}

    liked = [fmt(kv) for kv in liked_src[:MAX_TAGS_PER_SIDE]]
    avoided = [fmt(kv) for kv in avoided_src[:MAX_TAGS_PER_SIDE]]
    return liked, avoided


def derive_price_range(profile: UserProfile) -> dict | None:
    """Turn ``(log_price_mu, sigma)`` into an approximate ±1σ CHF band.

    Returns ``None`` when the profile has no price stats (cold-start or no
    positives with a usable price). The band is **indicative**, not a hard
    filter - ``low <= mid <= high`` always holds; the ranker itself uses
    the raw ``(mu, sigma)`` pair, not this rounded trio.
    """
    if profile.price_mu is None or profile.price_sigma is None:
        return None
    sigma = max(profile.price_sigma, 0.05)
    mid = math.exp(profile.price_mu)
    low = math.exp(profile.price_mu - sigma)
    high = math.exp(profile.price_mu + sigma)
    return {
        "low_chf": int(round(low)),
        "mid_chf": int(round(mid)),
        "high_chf": int(round(high)),
    }


def _count_active(
    conn: sqlite3.Connection,
    user_id: int,
    positive_kinds: tuple[str, ...],
    negative_kinds: tuple[str, ...],
) -> int:
    """How many distinct listings are currently in the "positive" state?

    "Currently" = latest event for each ``(user_id, listing_id)`` pair among
    ``positive_kinds ∪ negative_kinds`` is in ``positive_kinds``. Mirrors
    the drawer-endpoint semantics so the counts match what the user sees
    when they open the corresponding list.
    """
    all_kinds = positive_kinds + negative_kinds
    kinds_placeholders = ",".join("?" for _ in all_kinds)
    pos_placeholders = ",".join("?" for _ in positive_kinds)
    sql = f"""
    WITH last_kind AS (
        SELECT listing_id, kind,
               ROW_NUMBER() OVER (
                   PARTITION BY listing_id
                   ORDER BY created_at DESC, id DESC
               ) AS rn
        FROM user_interactions
        WHERE user_id = ? AND kind IN ({kinds_placeholders})
    )
    SELECT COUNT(*) FROM last_kind WHERE rn = 1 AND kind IN ({pos_placeholders})
    """
    row = conn.execute(
        sql, (user_id, *all_kinds, *positive_kinds)
    ).fetchone()
    return int(row[0]) if row is not None else 0


def fetch_interaction_stats(
    users_db_path: Path, user_id: int
) -> dict[str, int]:
    """Human-facing activity counts. All three counts mirror drawer state:

    * ``likes``      = cardinality of ``/me/likes`` response
    * ``bookmarks``  = cardinality of ``/me/favorites`` response
    * ``dismissals`` = length of ``/me/dismissed`` response
    """
    with connect_users_db(users_db_path) as conn:
        likes = _count_active(
            conn, user_id,
            positive_kinds=("like", "save"),
            negative_kinds=("unlike", "unsave"),
        )
        bookmarks = _count_active(
            conn, user_id,
            positive_kinds=("bookmark", "save"),
            negative_kinds=("unbookmark", "unsave"),
        )
        dismissals = _count_active(
            conn, user_id,
            positive_kinds=("dismiss",),
            negative_kinds=("undismiss",),
        )
    return {
        "likes": likes,
        "bookmarks": bookmarks,
        "dismissals": dismissals,
    }


def summarize_profile(
    profile: UserProfile,
    users_db_path: Path,
) -> dict:
    """Assemble the full ``UserProfileSummary`` payload for the UI."""
    liked, avoided = split_feature_taste(profile)
    return {
        "is_cold_start": profile.is_cold_start,
        "positive_count": profile.positive_count,
        "liked_features": liked,
        "avoided_features": avoided,
        "price_range_chf": derive_price_range(profile),
        "stats": fetch_interaction_stats(users_db_path, profile.user_id),
    }
