from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.deps import get_current_user
from app.config import get_settings
from app.core.hard_filters import _parse_row
from app.db import get_connection
from app.harness.search_service import query_from_filters, query_from_text
from app.models.schemas import (
    HealthResponse,
    ListingData,
    ListingsQueryRequest,
    ListingsResponse,
    ListingsSearchRequest,
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
