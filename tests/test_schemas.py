from pydantic import ValidationError

from app.models.schemas import (
    HardFilters,
    ListingData,
    ListingsResponse,
    RankedListingResult,
    ListingsQueryRequest,
    ListingsSearchRequest,
)


def test_query_request_requires_query() -> None:
    request = ListingsQueryRequest(query="bright apartment in zurich")

    assert request.query == "bright apartment in zurich"
    assert request.limit == 25
    assert request.offset == 0


def test_structured_search_request_allows_explicit_filters() -> None:
    request = ListingsSearchRequest(
        hard_filters=HardFilters(
            city=["Zurich"],
            min_price=1000,
            max_rooms=4.5,
            features=["balcony", "elevator"],
            latitude=47.0,
            longitude=8.0,
            radius_km=5.0,
        ),
    )

    assert request.hard_filters is not None
    assert request.hard_filters.city == ["Zurich"]
    assert request.hard_filters.min_price == 1000
    assert request.hard_filters.max_rooms == 4.5
    assert request.hard_filters.features == ["balcony", "elevator"]
    assert request.hard_filters.latitude == 47.0
    assert request.hard_filters.longitude == 8.0
    assert request.hard_filters.radius_km == 5.0


def test_ranked_listing_result_shape() -> None:
    result = RankedListingResult(
        listing_id="123",
        score=1.0,
        reason="Matched hard filters; soft ranking stub.",
        listing=ListingData(
            id="123",
            title="Test listing",
            city="Zurich",
            price_chf=2500,
            rooms=3.0,
            latitude=47.37,
            longitude=8.54,
        ),
    )

    assert result.listing_id == "123"
    assert result.score == 1.0
    assert isinstance(result.reason, str)
    assert result.listing.id == "123"


def test_listings_response_shape() -> None:
    response = ListingsResponse(
        listings=[
            RankedListingResult(
                listing_id="123",
                score=1.0,
                reason="Matched hard filters; soft ranking stub.",
                listing=ListingData(id="123", title="Test listing"),
            )
        ],
        meta={"source": "stub"},
    )

    assert len(response.listings) == 1
    assert response.listings[0].listing_id == "123"
    assert response.meta["source"] == "stub"


def test_limit_must_be_positive() -> None:
    try:
        ListingsQueryRequest(query="test", limit=0)
    except ValidationError as exc:
        assert "limit" in str(exc)
    else:
        raise AssertionError("expected validation error")
