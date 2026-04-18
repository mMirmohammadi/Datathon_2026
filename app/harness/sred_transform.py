from __future__ import annotations

import csv
import json
from pathlib import Path
import shutil


NORMALIZED_SRED_FILENAME = "sred_data.csv"
SRED_SOURCE_DIRNAME = "SRED_data(1)"
SRED_IMAGE_DIRNAME = "sred_images"
SRED_HEADERS = [
    "id",
    "platform_url",
    "platform_id",
    "title",
    "status",
    "time_of_creation",
    "last_scraped",
    "object_type_text",
    "price",
    "location_address",
    "partner_name",
    "remarks",
    "orig_data",
    "images",
    "price_type",
    "area",
    "object_category",
    "object_type",
    "offer_type",
    "number_of_rooms",
    "available_from",
    "object_description",
    "rent_net",
    "rent_extra",
    "distance_public_transport",
    "agency_name",
    "agency_phone",
    "agency_email",
    "floor",
    "year_built",
    "prop_balcony",
    "prop_elevator",
    "prop_parking",
    "prop_garage",
    "prop_fireplace",
    "prop_child_friendly",
    "geo_lat",
    "geo_lng",
    "scrape_source",
    "distance_shop",
    "distance_kindergarten",
    "distance_school_1",
    "distance_school_2",
    "animal_allowed",
    "object_street",
    "object_zip",
    "object_city",
    "object_state",
    "rent_gross",
    "maybe_temporary",
    "is_new_building",
    "supermarket_name",
]


def ensure_sred_normalized_csv(raw_data_dir: Path) -> Path | None:
    source_dir = raw_data_dir / SRED_SOURCE_DIRNAME
    if not source_dir.exists():
        return None

    metadata_dir = source_dir / "metadata"
    image_dir = _normalize_source_layout(raw_data_dir=raw_data_dir, source_dir=source_dir)
    output_path = raw_data_dir / NORMALIZED_SRED_FILENAME
    source_files = [
        metadata_dir / "train_data_with_text.csv",
        metadata_dir / "test_data_with_text.csv",
    ]

    for path in source_files:
        if not path.exists():
            raise FileNotFoundError(f"Missing SRED metadata file: {path}")

    rows: list[dict[str, str]] = []
    for split in ("train", "test"):
        rows.extend(_normalized_rows_for_split(source_dir=source_dir, image_dir=image_dir, split=split))

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SRED_HEADERS)
        writer.writeheader()
        writer.writerows(rows)

    return output_path


def _normalize_source_layout(*, raw_data_dir: Path, source_dir: Path) -> Path:
    metadata_dir = source_dir / "metadata"
    image_dir = raw_data_dir / SRED_IMAGE_DIRNAME
    processed_images_dir = source_dir / "processed_images"
    image_dir.mkdir(exist_ok=True)

    source_local_images_dir = source_dir / "images"
    if source_local_images_dir.exists():
        for source_path in source_local_images_dir.iterdir():
            if not source_path.is_file():
                continue
            target_path = image_dir / source_path.name
            if target_path.exists():
                source_path.unlink()
                continue
            shutil.move(str(source_path), str(target_path))
        shutil.rmtree(source_local_images_dir)

    if processed_images_dir.exists():
        for split in ("train", "test"):
            split_dir = processed_images_dir / split / "montage_organized"
            if not split_dir.exists():
                continue
            for source_path in split_dir.iterdir():
                if not source_path.is_file():
                    continue
                target_path = image_dir / source_path.name
                if target_path.exists():
                    raise FileExistsError(f"Duplicate SRED image filename while flattening: {target_path.name}")
                shutil.move(str(source_path), str(target_path))
        shutil.rmtree(processed_images_dir)

    for removable in (
        source_dir / ".DS_Store",
        metadata_dir / ".DS_Store",
        metadata_dir / "train_data.csv",
        metadata_dir / "test_data.csv",
    ):
        if removable.exists():
            removable.unlink()

    return image_dir


def _normalized_rows_for_split(*, source_dir: Path, image_dir: Path, split: str) -> list[dict[str, str]]:
    metadata_path = source_dir / "metadata" / f"{split}_data_with_text.csv"

    rows: list[dict[str, str]] = []
    with metadata_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            listing_id = _normalize_listing_id(row["listing_id"])
            image_path = _find_image_path(image_dir=image_dir, listing_id=listing_id)
            image_url = f"/raw-data-images/{image_path.name}" if image_path else ""
            description = row.get("ad_description", "")
            title = row.get("header", "").strip() or f"SRED listing {listing_id}"
            raw_payload = {
                "source": "sred",
                "split": split,
                "listing_id": listing_id,
                "image_url": image_url or None,
                "raw_row": row,
            }

            rows.append(
                {
                    "id": listing_id,
                    "platform_url": "",
                    "platform_id": listing_id,
                    "title": title,
                    "status": "",
                    "time_of_creation": "",
                    "last_scraped": "",
                    "object_type_text": "",
                    "price": row.get("price", ""),
                    "location_address": "{}",
                    "partner_name": "SRED",
                    "remarks": description,
                    "orig_data": json.dumps(raw_payload, ensure_ascii=False),
                    "images": json.dumps(
                        {
                            "images": (
                                [{"url": image_url, "filename": image_path.name}]
                                if image_path and image_url
                                else []
                            )
                        },
                        ensure_ascii=False,
                    ),
                    "price_type": "",
                    "area": row.get("living_space", ""),
                    "object_category": "",
                    "object_type": "",
                    "offer_type": "RENT",
                    "number_of_rooms": row.get("rooms", ""),
                    "available_from": "",
                    "object_description": description,
                    "rent_net": "",
                    "rent_extra": "",
                    "distance_public_transport": "",
                    "agency_name": "",
                    "agency_phone": "",
                    "agency_email": "",
                    "floor": "",
                    "year_built": "",
                    "prop_balcony": "",
                    "prop_elevator": "",
                    "prop_parking": "",
                    "prop_garage": "",
                    "prop_fireplace": "",
                    "prop_child_friendly": "",
                    "geo_lat": row.get("lat", ""),
                    "geo_lng": row.get("lon", ""),
                    "scrape_source": "SRED",
                    "distance_shop": "",
                    "distance_kindergarten": "",
                    "distance_school_1": "",
                    "distance_school_2": "",
                    "animal_allowed": "",
                    "object_street": "",
                    "object_zip": "",
                    "object_city": "",
                    "object_state": "",
                    "rent_gross": row.get("price", ""),
                    "maybe_temporary": "",
                    "is_new_building": "",
                    "supermarket_name": "",
                }
            )

    return rows


def _normalize_listing_id(value: str) -> str:
    cleaned = value.strip()
    return cleaned[:-2] if cleaned.endswith(".0") else cleaned


def _find_image_path(*, image_dir: Path, listing_id: str) -> Path | None:
    for suffix in (".jpeg", ".jpg", ".png", ".webp"):
        path = image_dir / f"{listing_id}{suffix}"
        if path.exists():
            return path
    return None
