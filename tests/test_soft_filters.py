from app.participant.ranking import rank_listings
from app.participant.soft_fact_extraction import extract_soft_facts
from app.participant.soft_filtering import filter_soft_facts


def test_extract_soft_facts_returns_stub_structure() -> None:
    result = extract_soft_facts("bright flat near transport")

    assert isinstance(result, dict)


def test_filter_soft_facts_returns_candidate_subset() -> None:
    candidates = [{"listing_id": "1"}, {"listing_id": "2"}]

    filtered = filter_soft_facts(candidates, {"raw_query": "quiet"})

    assert isinstance(filtered, list)
    assert {item["listing_id"] for item in filtered} <= {"1", "2"}


def test_rank_listings_returns_ranked_shape() -> None:
    ranked = rank_listings(
        candidates=[
            {
                "listing_id": "abc",
                "title": "Example",
                "city": "Zurich",
                "price": 2500,
                "rooms": 3.0,
                "latitude": 47.37,
                "longitude": 8.54,
                "street": "Main 1",
                "postal_code": "8000",
                "canton": "ZH",
                "area": 75.0,
                "available_from": "2026-06-01",
                "image_urls": ["https://example.com/1.jpg"],
                "hero_image_url": "https://example.com/1.jpg",
                "original_url": "https://example.com/listing",
                "features": ["balcony", "elevator"],
                "offer_type": "RENT",
                "object_category": "Wohnung",
                "object_type": "Apartment",
            }
        ],
        soft_facts={"raw_query": "bright"},
    )

    assert len(ranked) == 1
    assert ranked[0].listing_id == "abc"
    assert isinstance(ranked[0].score, float)
    assert isinstance(ranked[0].reason, str)
    assert ranked[0].listing.id == "abc"
    assert ranked[0].listing.title == "Example"
    assert ranked[0].listing.city == "Zurich"
    assert ranked[0].listing.image_urls
