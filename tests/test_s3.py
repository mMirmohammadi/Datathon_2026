import json
import os
import sqlite3
from pathlib import Path

from app.core.s3 import get_image_urls_by_listing_id
from app.harness.bootstrap import bootstrap_database


class FakeS3Client:
    def __init__(self) -> None:
        self.bucket: str | None = None
        self.prefix: str | None = None

    def list_objects_v2(self, *, Bucket: str, Prefix: str) -> dict:
        self.bucket = Bucket
        self.prefix = Prefix
        return {
            "Contents": [
                {"Key": f"{Prefix}image-1.jpg"},
                {"Key": f"{Prefix}image-2.webp"},
                {"Key": Prefix},
            ]
        }


class FailIfCalledS3Client:
    def list_objects_v2(self, *, Bucket: str, Prefix: str) -> dict:
        raise AssertionError(f"S3 should not be used for local SRED images: {Bucket=} {Prefix=}")


def test_get_image_urls_by_listing_id_uses_platform_id_prefix(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    raw_data_dir = repo_root / "raw_data"
    db_path = tmp_path / "listings.db"

    bootstrap_database(db_path=db_path, raw_data_dir=raw_data_dir)

    os.environ["LISTINGS_S3_BUCKET"] = "crawl-data-951752554117-eu-central-2-an"
    os.environ["LISTINGS_S3_REGION"] = "eu-central-2"
    os.environ["LISTINGS_S3_PREFIX"] = "prod"

    fake_client = FakeS3Client()
    monkeypatch.setattr("app.core.s3.boto3.client", lambda *args, **kwargs: fake_client)

    # Any COMPARIS listing from the enriched sample; importer sets platform_id=listing_id.
    urls = get_image_urls_by_listing_id(db_path=db_path, listing_id="10286")

    assert fake_client.bucket == "crawl-data-951752554117-eu-central-2-an"
    assert fake_client.prefix == "prod/comparis/images/platform_id=10286/"
    assert len(urls) == 2
    assert urls[0].endswith("image-1.jpg")
    assert urls[1].endswith("image-2.webp")
    assert all(
        "crawl-data-951752554117-eu-central-2-an.s3.eu-central-2.amazonaws.com" in url
        for url in urls
    )


def test_get_image_urls_by_listing_id_uses_local_montage_for_sred(
    tmp_path: Path,
    monkeypatch,
) -> None:
    # The enriched MVP sample does not contain SRED rows, so we insert a
    # minimal SRED fixture row directly to exercise the local-montage branch.
    repo_root = Path(__file__).resolve().parents[1]
    raw_data_dir = repo_root / "raw_data"
    db_path = tmp_path / "listings.db"

    bootstrap_database(db_path=db_path, raw_data_dir=raw_data_dir)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO listings (
                listing_id, platform_id, scrape_source, title,
                features_json, images_json, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "SRED-FAKE",
                "SRED-FAKE",
                "SRED",
                "Fake SRED listing",
                "[]",
                json.dumps({"images": ["/raw-data-images/sred-fake.jpeg"]}),
                "{}",
            ),
        )
        conn.commit()

    monkeypatch.setattr("app.core.s3.boto3.client", lambda *args, **kwargs: FailIfCalledS3Client())

    urls = get_image_urls_by_listing_id(db_path=db_path, listing_id="SRED-FAKE")

    assert len(urls) == 1
    assert urls[0].startswith("/raw-data-images/")
    assert urls[0].endswith(".jpeg")
