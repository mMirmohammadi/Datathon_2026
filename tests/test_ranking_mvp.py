"""Tests for ranking.py — percentile correctness, signal contribution, reason
rendering. No Claude calls; all data is synthetic."""
from __future__ import annotations

import pytest

from app.participant.ranking import (
    _feature_match_raw,
    _freshness_raw,
    _percentile_higher_is_better,
    _price_fit_scores,
    rank_listings,
)


def _cand(
    *, id_: str = "L1", rooms=3.0, price=2500, city="Zürich",
    features=None, title="3-Zimmer-Wohnung", desc="", bm25_score=-10.0,
    available_from=None,
):
    return {
        "listing_id": id_,
        "title": title,
        "description": desc,
        "city": city,
        "rooms": rooms,
        "price": price,
        "features": list(features or []),
        "bm25_score": bm25_score,
        "available_from": available_from,
    }


def test_empty_candidates_returns_empty():
    assert rank_listings([], {}) == []


def test_rank_produces_bounded_scores():
    cs = [_cand(id_="A"), _cand(id_="B"), _cand(id_="C")]
    out = rank_listings(cs, {"keywords": [], "negatives": [], "rewrites": []})
    assert len(out) == 3
    for r in out:
        # Score ≤ sum of positive weights; ≥ −negative weight.
        assert -0.1 <= r.score <= 1.01


def test_rank_higher_bm25_ranks_higher():
    """Lower (more-negative) BM25 should produce higher blended score, all else equal."""
    cs = [
        _cand(id_="low_bm25", bm25_score=-30.0),
        _cand(id_="mid_bm25", bm25_score=-20.0),
        _cand(id_="high_bm25", bm25_score=-10.0),
    ]
    out = rank_listings(cs, {"keywords": [], "negatives": [], "rewrites": []})
    ordered = [r.listing_id for r in out]
    assert ordered == ["low_bm25", "mid_bm25", "high_bm25"]


def test_rank_negative_hit_demotes():
    """A listing with a negative-keyword hit should rank lower than one without, all else equal."""
    cs = [
        _cand(id_="clean", title="3 rooms", desc="nice place"),
        _cand(id_="hit", title="3 rooms basement apartment", desc="basement unit"),
    ]
    out = rank_listings(cs, {"negatives": ["basement"], "keywords": [], "rewrites": []})
    # Both have same bm25; the non-hit should rank first
    assert out[0].listing_id == "clean"
    assert out[1].listing_id == "hit"
    # The hit-candidate's reason mentions the penalty
    assert "negated keyword" in out[1].reason or "⚠" in out[1].reason


def test_rank_feature_match_contributes():
    """A candidate matching required features should outrank one that doesn't."""
    cs = [
        _cand(id_="no_feat", features=[]),
        _cand(id_="has_feat", features=["balcony"]),
    ]
    soft = {
        "soft_features": [{"name": "balcony", "required": True}],
        "keywords": [], "negatives": [], "rewrites": [],
    }
    out = rank_listings(cs, soft)
    assert out[0].listing_id == "has_feat"


def test_reason_mentions_hard_filters():
    cs = [_cand(id_="A", rooms=3, city="Zürich", price=2500, features=["balcony"])]
    out = rank_listings(cs, {"keywords": [], "negatives": [], "rewrites": []})
    assert "3 rooms" in out[0].reason
    assert "Zürich" in out[0].reason
    assert "2500" in out[0].reason
    assert "balcony" in out[0].reason


def test_percentile_handles_ties():
    pcts = _percentile_higher_is_better([5.0, 5.0, 5.0])
    assert pcts == [0.5, 0.5, 0.5]  # all tied → midpoint, then /(N-1) = 0.5


def test_percentile_none_is_neutral():
    pcts = _percentile_higher_is_better([None, 1.0, 10.0])
    assert pcts[0] == 0.5
    assert pcts[1] < pcts[2]


def test_percentile_single_non_none_is_top():
    pcts = _percentile_higher_is_better([None, 1.0, None, None])
    assert pcts[1] == 1.0
    assert pcts[0] == pcts[2] == pcts[3] == 0.5


def test_feature_match_raw_no_features_is_neutral():
    assert _feature_match_raw(_cand(features=["balcony"]), []) == 0.5


def test_feature_match_raw_required_full_credit():
    cand = _cand(features=["balcony"])
    soft = [{"name": "balcony", "required": True}]
    assert _feature_match_raw(cand, soft) == 1.0


def test_feature_match_raw_preferred_half_credit():
    cand = _cand(features=["balcony"])
    soft = [
        {"name": "balcony", "required": False},
        {"name": "elevator", "required": False},
    ]
    # Both preferred (0.5 each); cand has only balcony → 0.5 / 1.0 = 0.5
    assert _feature_match_raw(cand, soft) == 0.5


def test_price_fit_no_sentiment_neutral():
    out = _price_fit_scores([1000.0, 2000.0, 3000.0], None)
    assert out == [0.5, 0.5, 0.5]


def test_price_fit_cheap_favors_low():
    out = _price_fit_scores([1000.0, 2000.0, 3000.0], "cheap")
    # cheap → target=p25=1500; 1000 is closer to 1500 than 3000 is
    assert out[0] > out[2]


def test_freshness_immediately_is_full():
    assert _freshness_raw("sofort") == 1.0
    assert _freshness_raw("immediately") == 1.0


def test_freshness_invalid_returns_none():
    assert _freshness_raw("not a date") is None
    assert _freshness_raw(None) is None
    assert _freshness_raw("") is None
