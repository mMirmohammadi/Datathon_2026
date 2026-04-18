from __future__ import annotations

import json
from typing import Any

from app.models.schemas import ListingData, RankedListingResult


_BM25_NO_MATCH_THRESHOLD = 1e8


def rank_listings(
    candidates: list[dict[str, Any]],
    soft_facts: dict[str, Any],
) -> list[RankedListingResult]:
    results: list[RankedListingResult] = []
    for candidate in candidates:
        bm25 = candidate.get("bm25_score")
        if bm25 is None or bm25 >= _BM25_NO_MATCH_THRESHOLD:
            score = 0.0
            reason = "Matched hard filters; no text match."
        else:
            # SQLite FTS5 bm25() returns a negative relevance score where more
            # negative = better. Flip sign so the API contract is higher=better.
            score = float(-bm25)
            reason = "Matched hard filters; ranked by text relevance."
        results.append(
            RankedListingResult(
                listing_id=str(candidate["listing_id"]),
                score=score,
                reason=reason,
                listing=_to_listing_data(candidate),
            )
        )
    return results


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
