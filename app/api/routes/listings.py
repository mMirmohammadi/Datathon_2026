from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.deps import get_current_user
from app.config import get_settings
from app.core.dinov2_search import (
    dinov2_enabled,
    find_similar_listings,
    is_loaded as dinov2_is_loaded,
)
from app.core.hard_filters import _parse_row
from app.db import get_connection
from app.harness.search_service import query_from_filters, query_from_text
from app.models.schemas import (
    HealthResponse,
    ListingData,
    ListingsQueryRequest,
    ListingsResponse,
    ListingsSearchRequest,
    SimilarListing,
    SimilarListingsResponse,
)
from app.participant.ranking import _to_listing_data

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok")


@router.post("/listings", response_model=ListingsResponse)
def listings(
    request: ListingsQueryRequest,
    user: dict[str, Any] | None = Depends(get_current_user(required=False)),
) -> ListingsResponse:
    settings = get_settings()
    user_id = int(user["id"]) if user is not None else None
    # Anonymous callers never trigger personalization regardless of the flag.
    personalize = bool(request.personalize and user_id is not None)
    return query_from_text(
        db_path=settings.db_path,
        query=request.query,
        limit=request.limit,
        offset=request.offset,
        user_id=user_id,
        personalize=personalize,
        users_db_path=settings.users_db_path,
    )


@router.get("/listings/{listing_id}", response_model=ListingData)
def get_listing(listing_id: str) -> ListingData:
    """Return the full ``ListingData`` for one listing by id.

    Used by the Saved-listings drawer to render a detail view on click.
    Reuses ``_parse_row`` + ``_to_listing_data`` so the shape is identical
    to what the search endpoint returns on the hot path.
    """
    settings = get_settings()
    with get_connection(settings.db_path) as conn:
        row = conn.execute(
            "SELECT * FROM listings WHERE listing_id = ?",
            (listing_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"listing {listing_id!r} not found",
        )
    return _to_listing_data(_parse_row(dict(row)))


@router.post("/listings/search/filter", response_model=ListingsResponse)
def listings_search(request: ListingsSearchRequest) -> ListingsResponse:
    settings = get_settings()
    return query_from_filters(
        db_path=settings.db_path,
        hard_facts=request.hard_filters,
    )


@router.get(
    "/listings/{listing_id}/similar",
    response_model=SimilarListingsResponse,
)
def similar_listings(listing_id: str, k: int = 10) -> SimilarListingsResponse:
    """DINOv2 reverse-image search — find listings that LOOK LIKE this one.

    Runs the query listing's mean image embedding against the 70,548-row
    DINOv2 main index (ViT-L/14 + GeM pooling, 1024-d L2-normalized).
    Returns the top-K similar listings by max cosine, with their full
    :class:`ListingData` for UI rendering.

    Disabled behaviours:
      * 404 when the query listing doesn't exist in the listings table.
      * 503 when the DINOv2 channel is off or not loaded.
      * Empty result list when the listing has no images in the store.
    """
    k_clamped = max(1, min(int(k), 50))
    settings = get_settings()

    # Verify the query listing exists first so callers get a clean 404, not
    # an opaque "no similar results" when the id is wrong.
    with get_connection(settings.db_path) as conn:
        hit = conn.execute(
            "SELECT 1 FROM listings WHERE listing_id = ? LIMIT 1",
            (listing_id,),
        ).fetchone()
    if hit is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"listing {listing_id!r} not found",
        )

    if not dinov2_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "DINOv2 reverse-image channel is disabled on this server "
                "(set LISTINGS_DINOV2_ENABLED=1 and restart)."
            ),
        )
    if not dinov2_is_loaded():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "DINOv2 index is not loaded. Check the server startup logs "
                "for [WARN] dinov2_*."
            ),
        )

    # The DINOv2 store is keyed on image_id "<source>/<platform_id>/<idx-hash>",
    # so find_similar_listings operates on platform_id-strings. We need BOTH
    # sides of the translation:
    #   1. Resolve the query listing's platform_id before calling the lookup.
    #   2. Translate each returned platform_id back to a listing_id and enrich
    #      with the full ListingData.
    # For ROBINREAL, platform_id == listing_id (both are MongoDB ObjectIds);
    # for COMPARIS they differ. Doing the platform_id round-trip handles both.
    with get_connection(settings.db_path) as conn:
        pid_row = conn.execute(
            "SELECT platform_id FROM listings WHERE listing_id = ?",
            (listing_id,),
        ).fetchone()
    query_platform_id = pid_row["platform_id"] if pid_row is not None else listing_id

    similar = find_similar_listings(query_platform_id, k=k_clamped)
    if not similar:
        return SimilarListingsResponse(
            query_listing_id=listing_id,
            results=[],
            meta={
                "model": "dinov2_vitl14_reg",
                "note": (
                    "Query listing has no images in the DINOv2 store "
                    "(likely dropped during triage)."
                ),
            },
        )

    # Enrich with listing rows so the UI can render cards directly.
    similar_pids = [pid for pid, _ in similar]
    placeholders = ", ".join("?" for _ in similar_pids)
    with get_connection(settings.db_path) as conn:
        rows = conn.execute(
            f"SELECT * FROM listings WHERE platform_id IN ({placeholders})",
            similar_pids,
        ).fetchall()
    by_pid: dict[str, tuple[str, Any]] = {}
    for row in rows:
        parsed = _parse_row(dict(row))
        by_pid[str(row["platform_id"])] = (
            str(row["listing_id"]),
            _to_listing_data(parsed),
        )

    results: list[SimilarListing] = []
    for pid, cosine in similar:
        resolved = by_pid.get(str(pid))
        if resolved is None:
            continue
        real_listing_id, ld = resolved
        results.append(SimilarListing(
            listing_id=real_listing_id,
            cosine=float(cosine),
            listing=ld,
        ))

    return SimilarListingsResponse(
        query_listing_id=listing_id,
        results=results,
        meta={
            "model": "dinov2_vitl14_reg",
            "embed_dim": 1024,
            "aggregation": "max cosine per candidate listing",
            "k_requested": k_clamped,
            "k_returned": len(results),
        },
    )
