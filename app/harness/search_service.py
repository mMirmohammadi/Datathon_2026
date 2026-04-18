from __future__ import annotations

from pathlib import Path
from typing import Any

from app.core.hard_filters import HardFilterParams, search_listings
from app.core.soft_signals import build_soft_rankings
from app.core.text_embed_search import (
    is_loaded as text_embed_is_loaded,
    score_candidates as text_embed_score_candidates,
    text_embed_enabled,
)
from app.core.visual_search import (
    fuse_rankings,
    is_loaded as visual_is_loaded,
    score_candidates as visual_score_candidates,
    visual_enabled,
)
from app.models.schemas import HardFilters, ListingsResponse, SoftPreferences
from app.participant.hard_fact_extraction import extract_hard_facts
from app.participant.ranking import rank_listings
from app.participant.soft_fact_extraction import extract_soft_facts
from app.participant.soft_filtering import filter_soft_facts


HYBRID_POOL = 300


def filter_hard_facts(db_path: Path, hard_facts: HardFilters) -> list[dict[str, Any]]:
    return search_listings(db_path, to_hard_filter_params(hard_facts))


def _rerank_hybrid(
    candidates: list[dict[str, Any]],
    query: str,
    soft: SoftPreferences | None,
    db_path: Path,
) -> list[dict[str, Any]]:
    """Collect BM25 + visual + text_embed + per-soft rankings and fuse via RRF.

    Always mutates each candidate dict with ``rrf_score``; adds
    ``visual_score`` / ``text_embed_score`` / ``soft_signals_activated`` when
    the respective channels contributed. Sorts candidates by descending
    ``rrf_score`` in place.

    BM25 is always present (comes in via the input order). Visual and
    text-embedding channels are each skipped when their env flag is off or
    their index has not been loaded.
    """
    if not candidates:
        return candidates

    listing_ids = [str(c["listing_id"]) for c in candidates]
    rankings: list[list[str]] = [listing_ids]  # BM25 channel (input order)

    if visual_enabled() and visual_is_loaded():
        visual_scores = visual_score_candidates(query, candidates)
        rankings.append(
            sorted(visual_scores.keys(), key=lambda lid: -visual_scores[lid])
        )
    else:
        visual_scores = {}

    if text_embed_enabled() and text_embed_is_loaded():
        text_embed_scores = text_embed_score_candidates(query, candidates)
        rankings.append(
            sorted(text_embed_scores.keys(), key=lambda lid: -text_embed_scores[lid])
        )
    else:
        text_embed_scores = {}

    soft_rankings = build_soft_rankings(candidates, soft, db_path)
    rankings.extend(soft_rankings)

    fused = fuse_rankings(rankings)

    for candidate in candidates:
        listing_id = str(candidate["listing_id"])
        candidate["visual_score"] = visual_scores.get(listing_id)
        candidate["text_embed_score"] = text_embed_scores.get(listing_id)
        candidate["rrf_score"] = fused.get(listing_id, 0.0)
        candidate["soft_signals_activated"] = len(soft_rankings)

    candidates.sort(key=lambda c: -c["rrf_score"])
    return candidates


def query_from_text(
    *,
    db_path: Path,
    query: str,
    limit: int,
    offset: int,
) -> ListingsResponse:
    hard_facts = extract_hard_facts(query)
    hard_facts.limit = max(limit, HYBRID_POOL)
    hard_facts.offset = 0
    soft_facts = extract_soft_facts(query)
    candidates = filter_hard_facts(db_path, hard_facts)
    candidates = _rerank_hybrid(
        candidates, query, hard_facts.soft_preferences, db_path
    )
    candidates = candidates[offset : offset + limit]
    candidates = filter_soft_facts(candidates, soft_facts)
    return ListingsResponse(
        listings=rank_listings(candidates, soft_facts),
        meta={},
    )


def query_from_filters(
    *,
    db_path: Path,
    hard_facts: HardFilters | None,
) -> ListingsResponse:
    structured_hard_facts = hard_facts or HardFilters()
    soft_facts = extract_soft_facts("")
    candidates = filter_hard_facts(db_path, structured_hard_facts)
    candidates = filter_soft_facts(candidates, soft_facts)
    return ListingsResponse(
        listings=rank_listings(candidates, soft_facts),
        meta={},
    )


def to_hard_filter_params(hard_facts: HardFilters) -> HardFilterParams:
    return HardFilterParams(
        city=hard_facts.city,
        postal_code=hard_facts.postal_code,
        canton=hard_facts.canton,
        min_price=hard_facts.min_price,
        max_price=hard_facts.max_price,
        min_rooms=hard_facts.min_rooms,
        max_rooms=hard_facts.max_rooms,
        min_area=hard_facts.min_area,
        max_area=hard_facts.max_area,
        min_floor=hard_facts.min_floor,
        max_floor=hard_facts.max_floor,
        min_year_built=hard_facts.min_year_built,
        max_year_built=hard_facts.max_year_built,
        available_from_after=hard_facts.available_from_after,
        latitude=hard_facts.latitude,
        longitude=hard_facts.longitude,
        radius_km=hard_facts.radius_km,
        features=hard_facts.features,
        features_excluded=hard_facts.features_excluded,
        object_category=hard_facts.object_category,
        bm25_keywords=hard_facts.bm25_keywords,
        limit=hard_facts.limit,
        offset=hard_facts.offset,
        sort_by=hard_facts.sort_by,
    )
