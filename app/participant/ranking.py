from __future__ import annotations

import json
from typing import Any

from app.models.schemas import (
    ListingData,
    MatchDetail,
    RankedListingResult,
    RankingBreakdown,
)


_BM25_NO_MATCH_THRESHOLD = 1e8


def rank_listings(
    candidates: list[dict[str, Any]],
    soft_facts: dict[str, Any],
) -> list[RankedListingResult]:
    results: list[RankedListingResult] = []
    for candidate in candidates:
        rrf = candidate.get("rrf_score")
        bm25 = candidate.get("bm25_score")
        if rrf is not None and rrf > 0.0:
            score = float(rrf)
            reason = _hybrid_reason(candidate)
        elif bm25 is not None and bm25 < _BM25_NO_MATCH_THRESHOLD:
            # SQLite FTS5 bm25() returns a negative relevance score where more
            # negative = better. Flip sign so the API contract is higher=better.
            score = float(-bm25)
            reason = "Matched hard filters; ranked by text relevance."
        else:
            score = 0.0
            reason = "Matched hard filters; no text or visual match."
        detail = candidate.get("_match_detail")
        match_detail = detail if isinstance(detail, MatchDetail) else None
        results.append(
            RankedListingResult(
                listing_id=str(candidate["listing_id"]),
                score=score,
                reason=reason,
                listing=_to_listing_data(candidate),
                breakdown=_to_breakdown(candidate),
                match_detail=match_detail,
            )
        )
    return results


def _to_breakdown(candidate: dict[str, Any]) -> RankingBreakdown:
    """Flatten the per-channel scores the search service stored on `candidate`.

    BM25 is flipped to positive-higher-is-better to match the API contract; a
    sentinel score (``>= _BM25_NO_MATCH_THRESHOLD``) means the FTS5 query
    didn't match this listing and is reported as ``None`` rather than a huge
    misleading number.
    """
    bm25_raw = candidate.get("bm25_score")
    bm25_out: float | None = None
    if bm25_raw is not None and bm25_raw < _BM25_NO_MATCH_THRESHOLD:
        bm25_out = float(-bm25_raw)

    visual = candidate.get("visual_score")
    text_embed = candidate.get("text_embed_score")
    dinov2_image = candidate.get("dinov2_image_score")
    soft_count = candidate.get("soft_signals_activated")
    memory_count = candidate.get("memory_rankings_activated")
    memory_score = candidate.get("memory_score")
    mem_sem = candidate.get("memory_semantic")
    mem_vis = candidate.get("memory_visual")
    mem_feat = candidate.get("memory_feature")
    mem_price = candidate.get("memory_price")
    rrf = candidate.get("rrf_score")

    return RankingBreakdown(
        rrf_score=float(rrf) if rrf is not None else None,
        bm25_score=bm25_out,
        visual_score=float(visual) if visual is not None else None,
        text_embed_score=float(text_embed) if text_embed is not None else None,
        dinov2_image_score=float(dinov2_image) if dinov2_image is not None else None,
        soft_signals_activated=int(soft_count) if isinstance(soft_count, int) else 0,
        memory_rankings_activated=int(memory_count) if isinstance(memory_count, int) else 0,
        memory_score=float(memory_score) if memory_score is not None else None,
        memory_semantic=float(mem_sem) if mem_sem is not None else None,
        memory_visual=float(mem_vis) if mem_vis is not None else None,
        memory_feature=float(mem_feat) if mem_feat is not None else None,
        memory_price=float(mem_price) if mem_price is not None else None,
    )


def _hybrid_reason(candidate: dict[str, Any]) -> str:
    """Human-readable reason string for a hybrid-ranked listing.

    Reads the per-channel scores the search service attached to the candidate
    dict and emits one clause per channel that contributed. Soft-signal
    activations are reported as a single summary count because the individual
    ranking identities are internal to `soft_signals.build_soft_rankings`.
    """
    parts = ["Matched hard filters"]
    bm25 = candidate.get("bm25_score")
    if bm25 is not None and bm25 < _BM25_NO_MATCH_THRESHOLD:
        parts.append("text match")
    visual = candidate.get("visual_score")
    if visual is not None and visual > 0:
        parts.append(f"visual match ({float(visual):.2f})")
    text_embed = candidate.get("text_embed_score")
    if text_embed is not None and text_embed > 0:
        parts.append(f"semantic match ({float(text_embed):.2f})")
    soft_count = candidate.get("soft_signals_activated")
    if isinstance(soft_count, int) and soft_count > 0:
        parts.append(
            "soft preferences" if soft_count == 1
            else f"{soft_count} soft preferences"
        )
    memory_count = candidate.get("memory_rankings_activated")
    if isinstance(memory_count, int) and memory_count > 0:
        memory_score = candidate.get("memory_score")
        if memory_score is not None:
            parts.append(
                f"personalized ({memory_count} memory signals, "
                f"score {float(memory_score):.2f})"
            )
        else:
            parts.append(f"personalized ({memory_count} memory signals)")
    if len(parts) == 1:
        parts.append("hybrid rank")
    return "; ".join(parts) + "."


def _to_listing_data(candidate: dict[str, Any]) -> ListingData:
    return ListingData(
        id=str(candidate["listing_id"]),
        title=candidate["title"],
        description=candidate.get("description"),
        street=candidate.get("street"),
        city=candidate.get("city"),
        postal_code=_coerce_str(candidate.get("postal_code")),
        canton=candidate.get("canton"),
        latitude=candidate.get("latitude"),
        longitude=candidate.get("longitude"),
        price_chf=candidate.get("price"),
        rooms=candidate.get("rooms"),
        living_area_sqm=_coerce_int(candidate.get("area")),
        available_from=candidate.get("available_from"),
        image_urls=_coerce_image_urls(candidate.get("image_urls")),
        hero_image_url=candidate.get("hero_image_url"),
        original_listing_url=candidate.get("original_url"),
        features=candidate.get("features") or [],
        offer_type=candidate.get("offer_type"),
        object_category=candidate.get("object_category"),
        object_type=candidate.get("object_type"),
    )


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None


def _coerce_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _coerce_image_urls(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return [value]
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    return None
