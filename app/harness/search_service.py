from __future__ import annotations

from pathlib import Path
from typing import Any, TYPE_CHECKING

from app.core.dinov2_search import (
    _STATE as _DINOV2_STATE,
    dinov2_enabled,
    is_loaded as dinov2_is_loaded,
    score_candidates_for_image,
)
from app.core.hard_filters import HardFilterParams, search_listings
from app.core.match_explain import build_match_detail
from app.core.soft_signals import (
    _load_commute_rows,
    _load_signal_rows,
    build_soft_rankings,
)
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

if TYPE_CHECKING:
    from PIL.Image import Image as PILImage
from app.memory.profile import UserProfile, build_profile
from app.memory.rankings import MemorySignals, build_memory_rankings
from app.models.schemas import HardFilters, ListingsResponse, SoftPreferences
from app.participant.hard_fact_extraction import extract_hard_facts
from app.participant.ranking import rank_listings
from app.participant.soft_fact_extraction import extract_soft_facts
from app.participant.soft_filtering import filter_soft_facts


HYBRID_POOL = 300


def _reorder_image_urls_inplace(candidate: dict[str, Any], best_image_id: str) -> None:
    """Reorder ``candidate['image_urls']`` so the photo matching
    ``best_image_id`` sits at index 0. Mutates in place. No-op when the id
    doesn't match any URL (e.g. SRED montage image_ids that don't map 1:1
    to S3 paths).

    The DINOv2 ``image_id`` is shaped ``<source>/<platform_id>/<stem>``
    where the stem matches the last path segment of the S3 URL, minus the
    file extension. Substring matching on the stem handles the ``.jpeg /
    .png / .webp / .JPEG`` variants without an extension-specific matcher.
    """
    urls = list(candidate.get("image_urls") or [])
    if not urls:
        return
    stem = best_image_id.rsplit("/", 1)[-1]
    if not stem:
        return
    target_i = -1
    for i, url in enumerate(urls):
        if stem in str(url):
            target_i = i
            break
    if target_i <= 0:
        return
    match = urls.pop(target_i)
    urls.insert(0, match)
    candidate["image_urls"] = urls
    candidate["hero_image_url"] = match


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
    image_pil: "PILImage | None" = None,
) -> tuple[list[dict[str, Any]], int]:
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
        return candidates, 0

    listing_ids = [str(c["listing_id"]) for c in candidates]
    # BM25 channel = input order. Skip it for image-only queries (empty text)
    # because then "input order" degenerates to natural DB order, which would
    # otherwise dominate the RRF fusion and drown the real DINOv2 look-alikes.
    rankings: list[list[str]] = []
    if query.strip():
        rankings.append(listing_ids)

    # SigLIP visual (text→image) and Arctic semantic (text→text) both need a
    # non-empty query to produce a meaningful ranking. On an image-only query
    # (empty text), their encoders return a near-neutral vector and every
    # listing scores roughly the same, wasting an RRF slot. Skip both.
    has_text = bool(query.strip())
    if has_text and visual_enabled() and visual_is_loaded():
        visual_scores = visual_score_candidates(query, candidates)
        rankings.append(
            sorted(visual_scores.keys(), key=lambda lid: -visual_scores[lid])
        )
    else:
        visual_scores = {}

    if has_text and text_embed_enabled() and text_embed_is_loaded():
        text_embed_scores = text_embed_score_candidates(query, candidates)
        rankings.append(
            sorted(text_embed_scores.keys(), key=lambda lid: -text_embed_scores[lid])
        )
    else:
        text_embed_scores = {}

    # DINOv2 image-query channel. Only fires when the caller supplied a photo
    # AND the DINOv2 store is loaded. The ranking is per-candidate max cosine
    # vs the uploaded image embedding (same aggregation as the per-listing
    # /similar endpoint). RRF-fused with every other ranking so text + image
    # combine without one dominating. We also capture which specific image of
    # each candidate scored highest, so downstream layers can surface that
    # photo first on the card (UX: show the photo that actually matched).
    dinov2_image_scores: dict[str, float] = {}
    dinov2_best_image_ids: dict[str, str] = {}
    if image_pil is not None and dinov2_enabled() and dinov2_is_loaded():
        try:
            dinov2_image_scores, dinov2_best_image_ids = score_candidates_for_image(
                image_pil, candidates, return_best_image_ids=True
            )
            if dinov2_image_scores:
                rankings.append(
                    sorted(
                        dinov2_image_scores.keys(),
                        key=lambda lid: -dinov2_image_scores[lid],
                    )
                )
        except Exception as exc:
            print(
                f"[WARN] _rerank_hybrid: expected=DINOv2 image ranking, "
                f"got={type(exc).__name__}: {exc}, fallback=skip image channel",
                flush=True,
            )

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
        candidate["dinov2_image_score"] = dinov2_image_scores.get(listing_id)
        best_img_id = dinov2_best_image_ids.get(listing_id)
        if best_img_id:
            # Reorder this candidate's image_urls so the one that matched
            # best is first. _to_listing_data reads image_urls as-is, so
            # this must happen before rank_listings builds the response.
            _reorder_image_urls_inplace(candidate, best_img_id)
        candidate["rrf_score"] = fused.get(listing_id, 0.0)
        candidate["soft_signals_activated"] = len(soft_rankings)
        candidate["memory_rankings_activated"] = memory_rankings_count
        if memory_signals is not None:
            candidate["memory_score"] = memory_signals.composite(listing_id)
            # Tier 3a: per-channel scores so the UI can show WHY a listing is
            # personalized (semantic description taste vs visual photo taste
            # vs feature checklist vs price habit).
            candidate["memory_semantic"] = memory_signals.semantic.get(listing_id)
            candidate["memory_visual"] = memory_signals.visual.get(listing_id)
            candidate["memory_feature"] = memory_signals.feature.get(listing_id)
            candidate["memory_price"] = memory_signals.price.get(listing_id)
        else:
            candidate["memory_score"] = None
            candidate["memory_semantic"] = None
            candidate["memory_visual"] = None
            candidate["memory_feature"] = None
            candidate["memory_price"] = None

    candidates.sort(key=lambda c: -c["rrf_score"])

    # Personalized path: hard-drop anything the user explicitly dismissed so
    # it can't surface in the top-K. The dismissal-demotion channel in
    # ``memory/rankings.py`` is just one of ~15 rankings in RRF, so a strongly
    # positive listing that the user hated could still leak into the top;
    # this filter ensures it can't. Dismissed listings remain reachable via
    # the anonymous path or with ``personalize=False`` so the Undo button
    # on the dimmed card still works.
    dismissed_dropped = 0
    if profile is not None and profile.dismissed_ids:
        before = len(candidates)
        candidates = [
            c for c in candidates
            if str(c["listing_id"]) not in profile.dismissed_ids
        ]
        dismissed_dropped = before - len(candidates)
        if dismissed_dropped > 0:
            print(
                f"[INFO] _rerank_hybrid: dropped {dismissed_dropped} "
                f"dismissed listing(s) from personalized results for "
                f"user_id={user_id}",
                flush=True,
            )
    return candidates, dismissed_dropped


def _pipeline_snapshot(
    candidates: list[dict[str, Any]],
    soft: SoftPreferences | None,
    *,
    image_query: bool = False,
    has_text: bool = True,
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
        # BM25 input-order + text-based channels require a non-empty text
        # query to contribute a meaningful ranking.
        "bm25": has_text,
        "visual": bool(has_text and visual_enabled() and visual_is_loaded()),
        "text_embed": bool(
            has_text and text_embed_enabled() and text_embed_is_loaded()
        ),
        "dinov2_image": bool(
            image_query and dinov2_enabled() and dinov2_is_loaded()
        ),
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
    image_pil: "PILImage | None" = None,
) -> ListingsResponse:
    """Main query path. ``image_pil`` is optional — when present, a DINOv2
    image-similarity channel is added to the RRF fusion alongside BM25,
    Arctic semantic, SigLIP visual, soft preferences and memory, so a text
    query and an uploaded photo jointly drive the ranking.

    Pure image queries (empty ``query``) still work — BM25 returns the full
    candidate pool in natural order, Arctic contributes a weak ranking, and
    the DINOv2 channel dominates via RRF. Pure text queries leave
    ``image_pil=None`` and this function behaves exactly as before.
    """
    hard_facts = extract_hard_facts(query)
    # Image queries can match any listing in the 15,291-listing DINOv2 pool;
    # without a text query there's no lexical/semantic signal to narrow the
    # candidate set, so we need to widen the pool or the top DINOv2 hits
    # will be outside it and get RRF-zero by omission. 25k covers the full
    # corpus and one extra matmul at ranking time is cheap.
    hard_facts.limit = (
        25546 if image_pil is not None else max(limit, HYBRID_POOL)
    )
    hard_facts.offset = 0
    soft_facts = extract_soft_facts(query)
    candidates = filter_hard_facts(db_path, hard_facts)
    pool_size = len(candidates)
    candidates, dismissed_dropped = _rerank_hybrid(
        candidates,
        query,
        hard_facts.soft_preferences,
        db_path,
        user_id=user_id,
        personalize=personalize,
        users_db_path=users_db_path,
        image_pil=image_pil,
    )
    pipeline = _pipeline_snapshot(
        candidates,
        hard_facts.soft_preferences,
        image_query=image_pil is not None,
        has_text=bool(query.strip()),
    )
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
            # Tier 5a: number of listings suppressed because the authenticated
            # user dismissed them (or a very similar one) earlier. Drives
            # the "N listings hidden" toast in the demo UI.
            "hidden_dismissed": dismissed_dropped,
            "has_image_query": image_pil is not None,
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
    # Tier 2: load r5py real-commute rows once per request for the top-K so
    # both commute_target and near_landmark MatchFacts can surface real
    # transit minutes rather than Haversine-derived proxies.
    soft = hard.soft_preferences
    needs_commute = bool(
        soft and (soft.commute_target or soft.near_landmark)
    )
    try:
        commute_rows = (
            _load_commute_rows(db_path, listing_ids) if needs_commute else {}
        )
    except Exception as exc:
        print(
            f"[WARN] _attach_match_details: expected=listing_commute_times rows, "
            f"got={type(exc).__name__}: {exc}, fallback=commute_proxy wide columns",
            flush=True,
        )
        commute_rows = {}
    for candidate in candidates:
        lid = str(candidate["listing_id"])
        candidate["_match_detail"] = build_match_detail(
            listing=candidate,
            hard=hard,
            signal_row=rows.get(lid),
            commute_rows=commute_rows,
        )


def default_feed(
    *,
    db_path: Path,
    users_db_path: Path | None,
    limit: int,
    user_id: int | None = None,
) -> ListingsResponse:
    """Homepage feed shown before the user types a query.

    Returns natural-order listings, re-ranked by the authenticated user's
    memory profile when available. Skips the LLM hard-fact call and the
    BM25/visual/text-embed channels — there is no query to drive them.
    Cold-start users and anonymous callers get the natural pool order.

    Every fallback logs a ``[WARN]`` per CLAUDE.md §5 so a dark memory
    pipeline (missing profile, stale state) is visible instead of silently
    degrading to the unpersonalized path.
    """
    empty_hard = HardFilters()
    pool_hard = HardFilters(limit=max(limit * 20, HYBRID_POOL))
    candidates = filter_hard_facts(db_path, pool_hard)
    pool_size = len(candidates)

    personalized = False
    mem_ranking_count = 0
    if candidates and user_id is not None and users_db_path is not None:
        profile: UserProfile | None = None
        try:
            profile = build_profile(
                user_id=user_id,
                users_db_path=users_db_path,
                listings_db_path=db_path,
            )
        except Exception as exc:
            print(
                f"[WARN] default_feed.build_profile: expected=profile for "
                f"user {user_id}, got={type(exc).__name__}: {exc}, "
                f"fallback=natural order",
                flush=True,
            )

        if profile is not None and not profile.is_cold_start:
            try:
                mem_rankings, _sig = build_memory_rankings(
                    candidates=candidates,
                    profile=profile,
                    text_state=_TEXT_EMBED_STATE if text_embed_is_loaded() else None,
                    visual_state=_VISUAL_STATE if visual_is_loaded() else None,
                    scrape_to_image_source=SCRAPE_SOURCE_TO_IMAGE_SOURCE,
                )
            except Exception as exc:
                print(
                    f"[WARN] default_feed.memory_rankings: user {user_id}, "
                    f"got={type(exc).__name__}: {exc}, fallback=natural order",
                    flush=True,
                )
                mem_rankings = []

            if mem_rankings:
                natural = [str(c["listing_id"]) for c in candidates]
                fused = fuse_rankings([natural] + mem_rankings)
                for cand in candidates:
                    cand["rrf_score"] = fused.get(str(cand["listing_id"]), 0.0)
                    cand["memory_rankings_activated"] = len(mem_rankings)
                candidates.sort(key=lambda c: -c.get("rrf_score", 0.0))
                mem_ranking_count = len(mem_rankings)
                personalized = True

    top = candidates[:limit]
    _attach_match_details(top, empty_hard, db_path)

    return ListingsResponse(
        listings=rank_listings(top, {"raw_query": ""}),
        meta={
            "query": None,
            "query_plan": empty_hard.model_dump(),
            "pipeline": {
                "bm25": False,
                "visual": False,
                "text_embed": False,
                "soft_rankings": 0,
                "memory": personalized,
                "memory_rankings": mem_ranking_count,
                "rrf_k": 60,
            },
            "candidate_pool_size": pool_size,
            "returned": len(top),
            "default_feed": True,
            "personalized": personalized,
            "has_image_query": False,
        },
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
        min_bathrooms=hard_facts.min_bathrooms,
        max_bathrooms=hard_facts.max_bathrooms,
        bathroom_shared=hard_facts.bathroom_shared,
        has_cellar=hard_facts.has_cellar,
        kitchen_shared=hard_facts.kitchen_shared,
        bm25_keywords=hard_facts.bm25_keywords,
        limit=hard_facts.limit,
        offset=hard_facts.offset,
        sort_by=hard_facts.sort_by,
    )
