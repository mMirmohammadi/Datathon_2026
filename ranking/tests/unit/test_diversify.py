"""Unit tests for Pareto + MMR diversification."""
from __future__ import annotations

import pytest

from ranking.runtime.diversify import MMRDimension, mmr, pareto_frontier


# ---------- Pareto --------------------------------------------------------


def _listing(id_, **kw):
    return {"id": id_, **kw}


def test_pareto_single_item_identity():
    items = [_listing("a", price=1000, noise=50)]
    assert pareto_frontier(items, minimise=["price", "noise"]) == items


def test_pareto_strict_dominance_removed():
    items = [
        _listing("a", price=1000, noise=50),
        _listing("b", price=2000, noise=80),   # dominated by a
        _listing("c", price=1500, noise=40),
    ]
    front = pareto_frontier(items, minimise=["price", "noise"])
    ids = {x["id"] for x in front}
    assert "a" in ids and "c" in ids
    assert "b" not in ids


def test_pareto_tradeoff_both_kept():
    items = [
        _listing("a", price=1000, noise=90),   # cheapest but noisy
        _listing("b", price=2000, noise=10),   # quiet but expensive
    ]
    front = pareto_frontier(items, minimise=["price", "noise"])
    assert len(front) == 2


def test_pareto_null_treated_as_worst():
    items = [
        _listing("real",    price=1500, noise=30),
        _listing("missing", price=1500, noise=None),  # unknown noise → dominated
    ]
    front = pareto_frontier(items, minimise=["price", "noise"])
    assert "real" in {x["id"] for x in front}


def test_pareto_maximise_axis():
    """Higher quality is better — the item with best quality at same price is kept."""
    items = [
        _listing("a", price=1000, quality=0.8),
        _listing("b", price=1000, quality=0.6),  # dominated
    ]
    front = pareto_frontier(items, minimise=["price"], maximise=["quality"])
    assert {x["id"] for x in front} == {"a"}


def test_pareto_empty_input():
    assert pareto_frontier([], minimise=["price"]) == []


def test_pareto_no_axes_returns_all():
    items = [_listing("a"), _listing("b")]
    assert len(pareto_frontier(items, minimise=[], maximise=[])) == 2


# ---------- MMR -----------------------------------------------------------


def _with_rel(id_, rel, city, price_band):
    return {"id": id_, "relevance": rel, "city": city, "price_band": price_band}


def _dims():
    return [
        MMRDimension(name="city",      extractor=lambda x: x["city"]),
        MMRDimension(name="price_band", extractor=lambda x: x["price_band"]),
    ]


def test_mmr_pure_relevance_lambda_1():
    items = [_with_rel("a", 0.9, "Zurich", 1), _with_rel("b", 0.7, "Bern", 2), _with_rel("c", 0.6, "Zurich", 1)]
    picked = mmr(items, k=2, relevance_key="relevance", dims=_dims(), lambda_=1.0)
    assert [p["id"] for p in picked] == ["a", "b"]  # pure rank by relevance


def test_mmr_diversity_prefers_different_city():
    """With moderate lambda, MMR should prefer a differently-positioned runner-up
    over a near-duplicate of the first pick, even if the runner-up has slightly
    lower relevance."""
    items = [
        _with_rel("a",  0.9, "Zurich", 1),   # first pick (highest relevance)
        _with_rel("b",  0.85, "Zurich", 1),  # near-duplicate of a on both dims
        _with_rel("c",  0.80, "Bern",   2),  # different city & band
    ]
    picked = mmr(items, k=2, relevance_key="relevance", dims=_dims(), lambda_=0.4)
    ids = [p["id"] for p in picked]
    assert ids[0] == "a"
    assert ids[1] == "c", f"expected MMR to surface 'c' (diverse), got {ids}"


def test_mmr_k_larger_than_items():
    items = [_with_rel("a", 0.9, "Z", 1), _with_rel("b", 0.5, "B", 2)]
    picked = mmr(items, k=10, relevance_key="relevance", dims=_dims())
    assert len(picked) == 2


def test_mmr_rejects_invalid_lambda():
    items = [_with_rel("a", 0.9, "Z", 1)]
    with pytest.raises(ValueError):
        mmr(items, k=1, relevance_key="relevance", dims=_dims(), lambda_=1.5)
    with pytest.raises(ValueError):
        mmr(items, k=1, relevance_key="relevance", dims=_dims(), lambda_=-0.1)


def test_mmr_empty_list_returns_empty():
    assert mmr([], k=5, relevance_key="relevance", dims=_dims()) == []


def test_mmr_numeric_bucketing():
    """With a 500-CHF bucket: 2100 and 2180 round to the same bucket → similar."""
    items = [
        {"id": "a", "rel": 0.9, "price": 2100},
        {"id": "b", "rel": 0.8, "price": 2180},    # same bucket as a (size=500)
        {"id": "c", "rel": 0.75, "price": 2900},   # different bucket
    ]
    dims = [MMRDimension(name="price_bucket", extractor=lambda x: x["price"], numeric_bucket_size=500)]
    picked = mmr(items, k=2, relevance_key="rel", dims=dims, lambda_=0.4)
    ids = [p["id"] for p in picked]
    assert ids[0] == "a"
    # 'c' is in a different bucket → should win over 'b' (same bucket)
    assert ids[1] == "c", f"expected 'c' (different price bucket), got {ids}"
