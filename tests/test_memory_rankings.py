"""build_memory_rankings: output shape, cold-start, dismissal demotion."""
from __future__ import annotations

import numpy as np

from app.memory.profile import UserProfile
from app.memory.rankings import build_memory_rankings


def _candidates(specs: list[tuple[str, int, set]]) -> list[dict]:
    """Build candidate dicts in the shape ``_rerank_hybrid`` passes around."""
    return [
        {
            "listing_id": lid,
            "price": price,
            "features": list(feats),
            "scrape_source": "COMPARIS",
            "platform_id": lid,
        }
        for lid, price, feats in specs
    ]


def test_empty_candidates_returns_empty_rankings() -> None:
    profile = UserProfile(user_id=1)
    rankings, signals = build_memory_rankings(
        candidates=[], profile=profile, text_state=None, visual_state=None
    )
    assert rankings == []
    assert signals.semantic == {}


def test_cold_start_skips_positive_channels() -> None:
    """Cold-start users still get the dismissal channel if they've dismissed
    anything, but never the positive-taste channels."""
    cands = _candidates(
        [("a", 2000, {"balcony"}), ("b", 2500, {"elevator"})]
    )
    profile = UserProfile(user_id=1, positive_count=0)
    rankings, signals = build_memory_rankings(
        candidates=cands, profile=profile, text_state=None, visual_state=None
    )
    # No positives and no dismissals -> no rankings at all.
    assert rankings == []


def test_cold_start_with_dismissals_sinks_them() -> None:
    cands = _candidates(
        [("a", 2000, set()), ("b", 2500, set()), ("c", 2100, set())]
    )
    profile = UserProfile(
        user_id=1,
        positive_count=0,
        dismissed_ids={"b"},
    )
    rankings, _ = build_memory_rankings(
        candidates=cands, profile=profile, text_state=None, visual_state=None
    )
    assert rankings == [["a", "c", "b"]]


def test_feature_taste_boosts_similar_listings() -> None:
    # User who loves balconies + elevators
    profile = UserProfile(
        user_id=1,
        positive_ids=["p1", "p2", "p3"],
        positive_count=3,
        weights_by_id={"p1": 3.0, "p2": 3.0, "p3": 3.0},
        feature_taste={
            "balcony": 0.9, "elevator": 0.9, "parking": 0.0, "garage": 0.0,
            "fireplace": -0.3, "child_friendly": 0.0, "pets_allowed": 0.0,
            "temporary": -0.5, "new_build": 0.0,
            "wheelchair_accessible": 0.0, "private_laundry": 0.0,
            "minergie_certified": 0.3,
        },
        price_mu=None,
        price_sigma=None,
    )
    cands = _candidates(
        [
            ("candA", 2500, {"balcony", "elevator"}),
            ("candB", 2500, {"parking"}),
            ("candC", 2500, {"fireplace", "temporary"}),
        ]
    )
    rankings, _ = build_memory_rankings(
        candidates=cands, profile=profile, text_state=None, visual_state=None
    )
    # Feature ranking should put candA first, candC last
    feat_ranking = next(r for r in rankings if r)
    assert feat_ranking[0] == "candA"
    assert feat_ranking[-1] == "candC"


def test_price_taste_clusters_around_saved_mean() -> None:
    import math
    profile = UserProfile(
        user_id=1,
        positive_ids=["p1"],
        positive_count=3,
        weights_by_id={"p1": 3.0},
        feature_taste={},
        price_mu=math.log(2500),
        price_sigma=0.1,
    )
    cands = _candidates(
        [("near", 2500, set()), ("cheap", 1000, set()), ("pricey", 6000, set())]
    )
    rankings, signals = build_memory_rankings(
        candidates=cands, profile=profile, text_state=None, visual_state=None
    )
    # Only the price channel is possible here; feature taste is all-zero.
    assert rankings  # at least one ranking present
    price_ranking = rankings[0]
    assert price_ranking[0] == "near"
    assert price_ranking[-1] in ("cheap", "pricey")
    # "near" should have the highest (least-negative) price score.
    assert signals.price["near"] > signals.price["cheap"]
    assert signals.price["near"] > signals.price["pricey"]


def test_semantic_channel_uses_text_state() -> None:
    """Given a toy 3-listing text matrix where two positives share a direction,
    the channel should rank a candidate close to that direction first.
    """
    # 4 listings total: p1, p2 (positives pointing +x), c1 (candidate near +x),
    # c2 (candidate perpendicular).
    ids = ["p1", "p2", "c1", "c2"]
    matrix = np.array(
        [
            [1.0, 0.0],  # p1
            [0.95, 0.05],  # p2
            [0.8, 0.1],  # c1 (near the +x direction)
            [0.0, 1.0],  # c2 (perpendicular)
        ],
        dtype=np.float32,
    )
    text_state = {"matrix": matrix, "ids": ids}

    profile = UserProfile(
        user_id=1,
        positive_ids=["p1", "p2"],
        positive_count=3,  # pretend cold-start passed
        weights_by_id={"p1": 3.0, "p2": 3.0},
        feature_taste={},
    )
    cands = [
        {"listing_id": "c1", "features": []},
        {"listing_id": "c2", "features": []},
    ]
    rankings, signals = build_memory_rankings(
        candidates=cands,
        profile=profile,
        text_state=text_state,
        visual_state=None,
    )
    assert rankings and rankings[0] == ["c1", "c2"]
    assert signals.semantic["c1"] > signals.semantic["c2"]


def test_memory_signals_composite_is_none_when_no_channel_fired() -> None:
    """When every per-channel score dict is empty, the composite returns None."""
    from app.memory.rankings import MemorySignals
    sig = MemorySignals(semantic={}, visual={}, feature={}, price={})
    assert sig.composite("anything") is None


def test_rankings_only_contain_candidate_ids() -> None:
    """Memory never introduces listings outside the candidate pool."""
    profile = UserProfile(
        user_id=1,
        positive_ids=["p1", "p2", "p3"],
        positive_count=3,
        weights_by_id={"p1": 3.0, "p2": 3.0, "p3": 3.0},
        feature_taste={
            k: 0.5 for k in
            ("balcony", "elevator", "parking", "garage", "fireplace",
             "child_friendly", "pets_allowed", "temporary", "new_build",
             "wheelchair_accessible", "private_laundry", "minergie_certified")
        },
    )
    cands = _candidates(
        [("only1", 2500, {"balcony"}), ("only2", 2500, {"elevator"})]
    )
    rankings, _ = build_memory_rankings(
        candidates=cands, profile=profile, text_state=None, visual_state=None
    )
    for r in rankings:
        assert set(r) <= {"only1", "only2"}
