"""Build a personalization profile from a user's interaction history.

Reduces the raw event stream in ``user_interactions`` into:

* ``weights_by_id``  - net signed weight per ``listing_id`` (save=+3, dwell>=5s=+2,
  click=+1, unsave=-1, dismiss=-2), ignoring anything older than 180 days.
* ``positive_ids``, ``negative_ids``  - the listing ids with positive / negative
  net weight. Lists and not sets so numpy fancy-indexing stays stable.
* ``feature_taste`` - per-feature signed preference in [-1, 1].
* ``price_mu``, ``price_sigma``  - mean and std of log(price) over positives.
* ``dismissed_ids``  - listings the user explicitly dismissed; used for demotion.
* ``positive_count`` - cold-start gate. Memory rankings are skipped when < 3.

The profile is read-only and cheap to rebuild per request (one SELECT + a
small numpy reduction). We deliberately don't cache across requests so a
save the user just made is visible on the next search without a cache bust.
"""
from __future__ import annotations

import datetime as dt
import json
import math
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

from app.auth.db import connect as connect_users_db
from app.core.hard_filters import FEATURE_COLUMN_MAP
from app.db import get_connection as connect_listings_db


HISTORY_WINDOW_DAYS = 180
COLD_START_MIN_POSITIVES = 3
DWELL_POSITIVE_THRESHOLD_S = 5.0

EVENT_WEIGHTS = {
    # Strongest positive: "I want to come back to THIS listing". Deliberate,
    # listing-specific action, so it outweighs a generic "like".
    "bookmark": 5.0,
    "unbookmark": -5.0,
    # Moderate positive: "I like this style". Feeds the same centroids / taste
    # vectors as a bookmark, just with less pull.
    "like": 3.0,
    "unlike": -3.0,
    # Legacy alias for ``like``; pre-split DB rows keep the same weight they
    # had before the split so existing personalization is preserved.
    "save": 3.0,
    "unsave": -3.0,
    "click": 1.0,
    "dismiss": -2.0,
    # "dwell" is handled specially in _accumulate_weights (value-aware).
}


@dataclass(slots=True)
class UserProfile:
    user_id: int
    positive_ids: list[str] = field(default_factory=list)
    negative_ids: list[str] = field(default_factory=list)
    weights_by_id: dict[str, float] = field(default_factory=dict)
    feature_taste: dict[str, float] = field(default_factory=dict)
    price_mu: float | None = None
    price_sigma: float | None = None
    dismissed_ids: set[str] = field(default_factory=set)
    positive_count: int = 0

    @property
    def is_cold_start(self) -> bool:
        return self.positive_count < COLD_START_MIN_POSITIVES


def _cutoff_iso(now: dt.datetime) -> str:
    return (now - dt.timedelta(days=HISTORY_WINDOW_DAYS)).isoformat()


def _accumulate_weights(
    rows: list[sqlite3.Row],
) -> tuple[dict[str, float], set[str]]:
    """Fold (kind, value) events into ``{listing_id: net_weight}`` plus a
    separate set of every listing currently dismissed.

    Rows are consumed in chronological order (the SELECT orders by
    ``created_at ASC``), so ``dismiss`` followed by ``undismiss`` cleanly
    flips the listing in and then back out of the dismissed set. Weights
    net exactly to zero for that pair, so a regretted dismissal leaves no
    footprint on the ranker.
    """
    weights: dict[str, float] = {}
    dismissed: set[str] = set()
    for row in rows:
        lid = row["listing_id"]
        kind = row["kind"]
        value = row["value"]
        if kind == "dismiss":
            dismissed.add(lid)
            weights[lid] = weights.get(lid, 0.0) + EVENT_WEIGHTS["dismiss"]
            continue
        if kind == "undismiss":
            dismissed.discard(lid)
            # Exact inverse so dismiss+undismiss nets to 0.
            weights[lid] = weights.get(lid, 0.0) - EVENT_WEIGHTS["dismiss"]
            continue
        if kind == "dwell":
            if value is not None and float(value) >= DWELL_POSITIVE_THRESHOLD_S:
                weights[lid] = weights.get(lid, 0.0) + 2.0
            continue
        delta = EVENT_WEIGHTS.get(kind)
        if delta is None:
            # Unknown kind - surfaced by the pydantic validator in practice,
            # but belt-and-braces here because the DB stores raw strings.
            continue
        weights[lid] = weights.get(lid, 0.0) + delta
    return weights, dismissed


def _feature_set_for(row: sqlite3.Row | None) -> set[str] | None:
    """Parse a listings row's ``features_json`` into a set of feature keys.

    Returns ``None`` when there's no listing - we skip unknown listings
    rather than treating "missing" as "every feature absent" (which would
    bias taste negatively).
    """
    if row is None:
        return None
    raw = row["features_json"] or "[]"
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = []
    if not isinstance(parsed, list):
        parsed = []
    return {str(x) for x in parsed if x}


def _fetch_listing_rows(
    listings_db_path: Path, listing_ids: list[str]
) -> dict[str, sqlite3.Row]:
    if not listing_ids:
        return {}
    out: dict[str, sqlite3.Row] = {}
    with connect_listings_db(listings_db_path) as conn:
        CHUNK = 800
        for i in range(0, len(listing_ids), CHUNK):
            chunk = listing_ids[i : i + CHUNK]
            placeholders = ", ".join("?" for _ in chunk)
            sql = (
                f"SELECT listing_id, price, features_json "
                f"FROM listings WHERE listing_id IN ({placeholders})"
            )
            for row in conn.execute(sql, chunk):
                out[row["listing_id"]] = row
    return out


def _compute_feature_taste(
    weights: dict[str, float],
    listing_rows: dict[str, sqlite3.Row],
) -> dict[str, float]:
    """Per-feature signed preference in [-1, 1].

    For each (feature, listing) pair, adds ``weight * (+1 if present else -1)``
    to the feature's running sum. Normalizes by the total |weight| so results
    are comparable across users with very different activity volumes.
    """
    feature_keys = list(FEATURE_COLUMN_MAP)
    accum = {k: 0.0 for k in feature_keys}
    abs_total = 0.0
    for lid, w in weights.items():
        if w == 0.0:
            continue
        row = listing_rows.get(lid)
        feats = _feature_set_for(row)
        if feats is None:
            continue
        abs_total += abs(w)
        for k in feature_keys:
            accum[k] += w * (1.0 if k in feats else -1.0)
    if abs_total <= 0.0:
        return {k: 0.0 for k in feature_keys}
    return {k: v / abs_total for k, v in accum.items()}


def _compute_price_stats(
    weights: dict[str, float],
    listing_rows: dict[str, sqlite3.Row],
) -> tuple[float | None, float | None]:
    """(mean, std) of log(price) across positively-weighted listings.

    Any non-positive weight, missing price, or non-positive price is excluded.
    Returns (None, None) when fewer than 2 usable points exist (can't estimate sigma).
    """
    logs: list[float] = []
    for lid, w in weights.items():
        if w <= 0.0:
            continue
        row = listing_rows.get(lid)
        if row is None:
            continue
        price = row["price"]
        if price is None:
            continue
        try:
            p = float(price)
        except (TypeError, ValueError):
            continue
        if p <= 0.0:
            continue
        logs.append(math.log(p))
    if len(logs) < 2:
        if not logs:
            return None, None
        # Single positive - we can use its log-price as mu and a gentle
        # default sigma so the ranker still has a target.
        return logs[0], 0.3
    n = len(logs)
    mu = sum(logs) / n
    var = sum((x - mu) ** 2 for x in logs) / (n - 1)
    sigma = math.sqrt(var)
    return mu, max(sigma, 0.05)


def build_profile(
    *,
    user_id: int,
    users_db_path: Path,
    listings_db_path: Path,
    now: dt.datetime | None = None,
) -> UserProfile:
    """Assemble a read-only :class:`UserProfile` for ``user_id``."""
    now = now or dt.datetime.now(dt.timezone.utc)
    cutoff = _cutoff_iso(now)

    with connect_users_db(users_db_path) as conn:
        rows = conn.execute(
            """
            SELECT listing_id, kind, value, created_at
            FROM user_interactions
            WHERE user_id = ? AND created_at >= ?
            ORDER BY created_at ASC, id ASC
            """,
            (user_id, cutoff),
        ).fetchall()

    if not rows:
        print(
            f"[WARN] memory.profile: user {user_id} has no interactions within "
            f"{HISTORY_WINDOW_DAYS}d, fallback=skip personalization",
            flush=True,
        )
        return UserProfile(user_id=user_id)

    weights, dismissed = _accumulate_weights(rows)
    positive_ids = [lid for lid, w in weights.items() if w > 0.0]
    negative_ids = [lid for lid, w in weights.items() if w < 0.0]
    positive_count = len(positive_ids)

    if positive_count < COLD_START_MIN_POSITIVES:
        print(
            f"[WARN] memory.profile: user {user_id} has {positive_count} positives "
            f"(< {COLD_START_MIN_POSITIVES}), fallback=skip personalization",
            flush=True,
        )
        # Still return the weights / dismissals so dismissal-demotion can fire
        # on cold-start - negative feedback is useful immediately.
        return UserProfile(
            user_id=user_id,
            positive_ids=positive_ids,
            negative_ids=negative_ids,
            weights_by_id=weights,
            dismissed_ids=dismissed,
            positive_count=positive_count,
        )

    listing_rows = _fetch_listing_rows(
        listings_db_path, list(weights.keys())
    )
    feature_taste = _compute_feature_taste(weights, listing_rows)
    price_mu, price_sigma = _compute_price_stats(weights, listing_rows)

    return UserProfile(
        user_id=user_id,
        positive_ids=positive_ids,
        negative_ids=negative_ids,
        weights_by_id=weights,
        feature_taste=feature_taste,
        price_mu=price_mu,
        price_sigma=price_sigma,
        dismissed_ids=dismissed,
        positive_count=positive_count,
    )
