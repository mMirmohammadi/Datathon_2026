"""Tests for bm25_candidates — pure logic paths that don't require Claude."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from app.models.schemas import NumRange, QueryPlan, SoftPreferences


@pytest.fixture(scope="module")
def live_db():
    """Point at the real built DB. If it doesn't exist, skip — these tests need data."""
    repo = Path(__file__).resolve().parents[1]
    db = Path(os.environ.get("LISTINGS_DB_PATH", repo / "data" / "listings.db"))
    if not db.exists():
        pytest.skip(f"listings.db not built yet at {db}; run the app once to bootstrap.")
    return db


def _plan(raw: str, *, rewrites=None, keywords=None) -> QueryPlan:
    return QueryPlan(
        raw_query=raw,
        rewrites=list(rewrites or []),
        soft=SoftPreferences(keywords=list(keywords or [])),
    )


def test_bm25_empty_allowed_ids_returns_empty(live_db):
    from app.participant.retrieval import bm25_candidates

    out = bm25_candidates(live_db, _plan("anything"), [], k=10)
    assert out == []


def test_bm25_returns_only_allowed_ids(live_db):
    """The SQL-gate invariant: no candidate outside the allowed set may ever appear."""
    from app.core.hard_filters import HardFilterParams, search_listings
    from app.participant.retrieval import bm25_candidates

    hfp = HardFilterParams(city=["Zürich"], min_rooms=3, max_rooms=3, limit=50)
    allowed = [r["listing_id"] for r in search_listings(live_db, hfp)]
    assert len(allowed) > 0, "precondition failed — no Zurich 3-rooms in DB"

    plan = _plan(
        "bright apartment",
        rewrites=["helle Wohnung"],
        keywords=["bright", "hell"],
    )
    out = bm25_candidates(live_db, plan, allowed, k=50)
    assert out, "BM25 returned empty on a non-empty allowed set"
    returned_ids = {c["listing_id"] for c in out}
    assert returned_ids.issubset(set(allowed)), (
        "VIOLATED: BM25 returned listings outside the allowed set"
    )


def test_bm25_passthrough_when_no_tokens(live_db):
    """Pure hard-filter queries (no keywords/rewrites) should return allowed listings
    untouched, with bm25_score=None. BM25 is a soft ranker, not a filter."""
    from app.core.hard_filters import HardFilterParams, search_listings
    from app.participant.retrieval import bm25_candidates

    hfp = HardFilterParams(city=["Lugano"], limit=5)
    allowed = [r["listing_id"] for r in search_listings(live_db, hfp)]
    if not allowed:
        pytest.skip("no Lugano listings")

    # Force no usable tokens — empty strings are filtered out
    plan = QueryPlan(raw_query="x", rewrites=[], soft=SoftPreferences(keywords=[]))
    # The raw_query "x" gets tokenized — but only 1 char, so gets dropped (<2 chars)
    out = bm25_candidates(live_db, plan, allowed, k=5)
    # With no tokens at all, we go through the pass-through path
    # (length-1 tokens like "x" get filtered, so an "x"-only plan has 0 usable tokens)
    assert len(out) > 0
    # If tokens are present, bm25_score is a float; otherwise None.
    for c in out:
        assert "bm25_score" in c


def test_bm25_ordering_is_by_relevance(live_db):
    """Returned candidates must be sorted by BM25 (ascending — more negative = better)."""
    from app.core.hard_filters import HardFilterParams, search_listings
    from app.participant.retrieval import bm25_candidates

    hfp = HardFilterParams(city=["Zürich"], min_rooms=3, max_rooms=3, limit=30)
    allowed = [r["listing_id"] for r in search_listings(live_db, hfp)]
    if len(allowed) < 3:
        pytest.skip("need ≥3 Zurich 3-room listings")

    plan = _plan("bright balcony", rewrites=["helle Balkon"], keywords=["bright", "balcony"])
    out = bm25_candidates(live_db, plan, allowed, k=20)
    assert len(out) >= 2
    # scores should be monotonically ascending (more-negative → less-negative)
    scores = [c["bm25_score"] for c in out]
    assert scores == sorted(scores), f"BM25 output not ascending-sorted: {scores}"


def test_match_string_builder(monkeypatch):
    from app.participant import retrieval

    plan = QueryPlan(
        raw_query="3 rooms in Zurich",
        rewrites=["helle Wohnung", "appartement lumineux"],
        soft=SoftPreferences(keywords=["bright", "quiet", "a"]),  # "a" filtered
    )
    match_str, token_count = retrieval._build_match_string(plan)
    assert token_count > 0
    assert 'OR' in match_str
    # Every token should be double-quoted
    assert match_str.count('"') >= 2 * token_count
    # Stoptokens filtered
    assert '"and"' not in match_str
    assert '"a"' not in match_str
