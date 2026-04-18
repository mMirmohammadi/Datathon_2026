import os
from pathlib import Path

from fastapi.testclient import TestClient


def test_health_endpoint(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")
    os.environ["LISTINGS_DB_PATH"] = str(tmp_path / "listings.db")

    from app.main import app

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_post_listings_returns_ranked_results(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")
    os.environ["LISTINGS_DB_PATH"] = str(tmp_path / "listings.db")

    from app.main import app

    with TestClient(app) as client:
        response = client.post("/listings", json={"query": "3 room flat in winterthur"})

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, dict)
    assert "listings" in body
    assert "meta" in body
    assert isinstance(body["listings"], list)
    assert len(body["listings"]) <= 25
    assert body["listings"]
    assert {"listing_id", "score", "reason", "listing"} <= set(body["listings"][0].keys())
    assert {"id", "title"} <= set(body["listings"][0]["listing"].keys())
    assert isinstance(body["listings"][0]["score"], float)
    assert isinstance(body["listings"][0]["reason"], str)


def test_post_listings_search_filter_applies_explicit_hard_filters(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")
    os.environ["LISTINGS_DB_PATH"] = str(tmp_path / "listings.db")

    from app.main import app

    with TestClient(app) as client:
        response = client.post(
            "/listings/search/filter",
            json={
                "hard_filters": {"city": ["Winterthur"], "limit": 5},
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, dict)
    assert "listings" in body
    assert "meta" in body
    assert isinstance(body["listings"], list)
    assert len(body["listings"]) <= 5
    assert body["listings"]
    assert {"listing_id", "score", "reason", "listing"} <= set(body["listings"][0].keys())
    assert {"id", "title", "city"} <= set(body["listings"][0]["listing"].keys())
    assert all(
        (item["listing"].get("city") or "").lower() == "winterthur"
        for item in body["listings"]
    )


def test_raw_data_images_are_served_from_local_static_mount(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")
    os.environ["LISTINGS_DB_PATH"] = str(tmp_path / "listings.db")

    from app.main import app

    with TestClient(app) as client:
        response = client.get("/raw-data-images/4154142.jpeg")

    assert response.status_code == 200
    assert response.headers["content-type"] == "image/jpeg"
