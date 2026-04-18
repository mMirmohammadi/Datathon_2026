from __future__ import annotations

from pathlib import Path
from urllib.parse import quote
import json

import boto3

from app.config import get_settings
from app.db import get_connection


IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp")


def get_image_urls_by_listing_id(*, db_path: Path, listing_id: str) -> list[str]:
    platform_id, scrape_source, images_json = _get_listing_storage_reference(
        db_path=db_path,
        listing_id=listing_id,
    )

    if platform_id is None or scrape_source is None:
        return []

    if scrape_source.upper() == "SRED":
        return _extract_image_urls(images_json)

    settings = get_settings()
    source_name = scrape_source.lower()
    prefix = f"{settings.s3_prefix}/{source_name}/images/platform_id={platform_id}/"

    client = boto3.client("s3", region_name=settings.s3_region)
    response = client.list_objects_v2(Bucket=settings.s3_bucket, Prefix=prefix)
    contents = response.get("Contents", [])

    urls: list[str] = []
    for item in sorted(contents, key=lambda entry: entry["Key"]):
        key = item["Key"]
        if key.endswith("/") or not key.lower().endswith(IMAGE_EXTENSIONS):
            continue
        encoded_key = quote(key, safe="/")
        urls.append(
            f"https://{settings.s3_bucket}.s3.{settings.s3_region}.amazonaws.com/{encoded_key}"
        )

    return urls


def _get_listing_storage_reference(
    *,
    db_path: Path,
    listing_id: str,
) -> tuple[str | None, str | None, str | None]:
    with get_connection(db_path) as connection:
        row = connection.execute(
            """
            SELECT platform_id, scrape_source, images_json
            FROM listings
            WHERE listing_id = ?
            """,
            [listing_id],
        ).fetchone()

    if row is None:
        raise LookupError(f"Listing {listing_id} not found.")

    return row["platform_id"], row["scrape_source"], row["images_json"]


def _extract_image_urls(images_json: str | None) -> list[str]:
    if not images_json:
        return []
    try:
        parsed = json.loads(images_json)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, dict):
        return []

    urls: list[str] = []
    for item in parsed.get("images", []) or []:
        if isinstance(item, dict) and item.get("url"):
            urls.append(str(item["url"]))
        elif isinstance(item, str) and item:
            urls.append(item)
    return urls
