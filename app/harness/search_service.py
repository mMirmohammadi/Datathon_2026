from __future__ import annotations

from pathlib import Path
from typing import Any

from app.core.hard_filters import HardFilterParams, search_listings
from app.core.match_explain import build_match_detail
from app.core.soft_signals import _load_signal_rows, build_soft_rankings
from app.core.text_embed_search import (
    _STATE as _TEXT_EMBED_STATE,
    is_loaded as text_embed_is_loaded,
    score_candidates as text_embed_score_candidates,
    text_embed_enabled,
)
from app.core.visual_search import (
    _STATE as _VISUAL_STATE,
    SCRAPE_SOURCE_TO_IMAGE_SOURCE,
    fuse_rankings,
    is_loaded as visual_is_loaded,
    score_candidates as visual_score_candidates,
    visual_enabled,
)
from app.memory.profile import UserProfile, build_profile
from app.memory.rankings import MemorySignals, build_memory_rankings
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
    *,
    user_id: int | None = None,
    personalize: bool = False,
    users_db_path: Path | None = None,
) -> list[dict[str, Any]]:
    """Collect BM25 + visual + text_embed + soft + (opt) memory rankings and
    fuse them via RRF.

    Always mutates each candidate dict with ``rrf_score``; adds
    ``visual_score`` / ``text_embed_score`` / ``soft_signals_activated`` when
    the respective channels contributed. Sorts candidates by descending
    ``rrf_score`` in place.

    BM25 is always present (comes in via the input order). Visual and
    text-embedding channels are each skipped when their env flag is off or
    their index has not been loaded.

    When ``user_id`` is set AND ``personalize`` is True, :mod:`app.memory`
    builds up to five additional rankings from the user's interaction
    history. Memory is purely additive - nothing existing is replaced.
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

    memory_rankings_count = 0
    memory_signals: MemorySignals | None = None
    profile: UserProfile | None = None
    if personalize and user_id is not None and users_db_path is not None:
        profile = build_profile(
            user_id=user_id,
            users_db_path=users_db_path,
            listings_db_path=db_path,
        )
        mem_rankings, memory_signals = build_memory_rankings(
            candidates=candidates,
            profile=profile,
            text_state=_TEXT_EMBED_STATE if text_embed_is_loaded() else None,
            visual_state=_VISUAL_STATE if visual_is_loaded() else None,
            scrape_to_image_source=SCRAPE_SOURCE_TO_IMAGE_SOURCE,
        )
        rankings.extend(mem_rankings)
        memory_rankings_count = len(mem_rankings)

    fused = fuse_rankings(rankings)

    for candidate in candidates:
        listing_id = str(candidate["listing_id"])
        candidate["visual_score"] = visual_scores.get(listing_id)
        candidate["text_embed_score"] = text_embed_scores.get(listing_id)
        candidate["rrf_score"] = fused.get(listing_id, 0.0)
        candidate["soft_signals_activated"] = len(soft_rankings)
        candidate["memory_rankings_activated"] = memory_rankings_count
        if memory_signals is not None:
            candidate["memory_score"] = memory_signals.composite(listing_id)
        else:
            candidate["memory_score"] = None

    candidates.sort(key=lambda c: -c["rrf_score"])

    # Personalized path: hard-drop anything the user explicitly dismissed so
    # it can't surface in the top-K. The dismissal-demotion channel in
    # ``memory/rankings.py`` is just one of ~15 rankings in RRF, so a strongly
    # positive listing that the user hated could still leak into the top;
    # this filter ensures it can't. Dismissed listings remain reachable via
    # the anonymous path or with ``personalize=False`` so the Undo button
    # on the dimmed card still works.
    if profile is not None and profile.dismissed_ids:
        before = len(candidates)
        candidates = [
            c for c in candidates
            if str(c["listing_id"]) not in profile.dismissed_ids
        ]
        dropped = before - len(candidates)
        if dropped > 0:
            print(
                f"[INFO] _rerank_hybrid: dropped {dropped} dismissed listing(s) "
                f"from personalized results for user_id={user_id}",
                flush=True,
            )
    return candidates


def _pipeline_snapshot(
    candidates: list[dict[str, Any]],
    soft: SoftPreferences | None,
) -> dict[str, Any]:
    """Describe which ranking channels actually ran this turn.

    Visual and text-embed flags mirror the env gate + load-state checks that
    ``_rerank_hybrid`` uses so the UI reports the real pipeline, not the
    aspirational one. ``soft_rankings`` counts how many per-preference
    rankings joined the RRF fusion (each activated soft key with at least one
    non-NULL candidate value contributes one ranking).
    """
    soft_count = 0
    memory_count = 0
    if candidates:
        # Every candidate carries the same counts (set by _rerank_hybrid).
        soft_count = int(candidates[0].get("soft_signals_activated") or 0)
        memory_count = int(candidates[0].get("memory_rankings_activated") or 0)
    return {
        "bm25": True,  # always in the fusion (input order channel)
        "visual": bool(visual_enabled() and visual_is_loaded()),
        "text_embed": bool(text_embed_enabled() and text_embed_is_loaded()),
        "soft_rankings": soft_count,
        "memory": memory_count > 0,
        "memory_rankings": memory_count,
        "rrf_k": 60,
    }


def query_from_text(
    *,
    db_path: Path,
    query: str,
    limit: int,
    offset: int,
    user_id: int | None = None,
    personalize: bool = False,
    users_db_path: Path | None = None,
) -> ListingsResponse:
    hard_facts = extract_hard_facts(query)
    hard_facts.limit = max(limit, HYBRID_POOL)
    hard_facts.offset = 0
    soft_facts = extract_soft_facts(query)
    candidates = filter_hard_facts(db_path, hard_facts)
    pool_size = len(candidates)
    candidates = _rerank_hybrid(
        candidates,
        query,
        hard_facts.soft_preferences,
        db_path,
        user_id=user_id,
        personalize=personalize,
        users_db_path=users_db_path,
    )
    pipeline = _pipeline_snapshot(candidates, hard_facts.soft_preferences)
    candidates = candidates[offset : offset + limit]
    candidates = filter_soft_facts(candidates, soft_facts)
    _attach_match_details(candidates, hard_facts, db_path)
    return ListingsResponse(
        listings=rank_listings(candidates, soft_facts),
        meta={
            "query": query,
            "query_plan": hard_facts.model_dump(),
            "pipeline": pipeline,
            "candidate_pool_size": pool_size,
            "returned": min(limit, len(candidates)),
        },
    )


def _attach_match_details(
    candidates: list[dict[str, Any]],
    hard: HardFilters,
    db_path: Path,
) -> None:
    """Attach a ``_match_detail`` MatchDetail object to each top-K candidate.

    One DB read (batched) fetches the signal rows for the visible top-K; this
    is the same query shape the soft-ranker uses, so we don't add a new kind
    of DB pressure. Listings without a signals row still get hard-check and
    keyword data — only the soft-fact panel will be empty for them.
    """
    if not candidates:
        return
    listing_ids = [str(c["listing_id"]) for c in candidates]
    try:
        rows = _load_signal_rows(db_path, listing_ids)
    except Exception as exc:
        print(
            f"[WARN] _attach_match_details: expected=signal rows for top-K, "
            f"got={type(exc).__name__}: {exc}, fallback=hard+keyword only",
            flush=True,
        )
        rows = {}
    for candidate in candidates:
        lid = str(candidate["listing_id"])
        candidate["_match_detail"] = build_match_detail(
            listing=candidate,
            hard=hard,
            signal_row=rows.get(lid),
        )


def query_from_filters(
    *,
    db_path: Path,
    hard_facts: HardFilters | None,
) -> ListingsResponse:
    structured_hard_facts = hard_facts or HardFilters()
    soft_facts = extract_soft_facts("")
    candidates = filter_hard_facts(db_path, structured_hard_facts)
    pool_size = len(candidates)
    candidates = filter_soft_facts(candidates, soft_facts)
    return ListingsResponse(
        listings=rank_listings(candidates, soft_facts),
        meta={
            "query": None,
            "query_plan": structured_hard_facts.model_dump(),
            "pipeline": {
                "bm25": False,
                "visual": False,
                "text_embed": False,
                "soft_rankings": 0,
                "rrf_k": 60,
            },
            "candidate_pool_size": pool_size,
            "returned": len(candidates),
        },
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
