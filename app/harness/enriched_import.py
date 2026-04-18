from __future__ import annotations

import csv
import json
import re
import sqlite3
from pathlib import Path
from typing import Any

from app.core.normalize import slug, split_street, translate_object_category


FEATURE_COLUMNS = [
    "feature_balcony",
    "feature_elevator",
    "feature_parking",
    "feature_garage",
    "feature_fireplace",
    "feature_child_friendly",
    "feature_pets_allowed",
    "feature_temporary",
    "feature_new_build",
    "feature_wheelchair_accessible",
    "feature_private_laundry",
    "feature_minergie_certified",
]

FEATURE_KEY_MAP = {column: column.removeprefix("feature_") for column in FEATURE_COLUMNS}

_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str | None) -> str | None:
    if not text:
        return None
    cleaned = _HTML_TAG_RE.sub(" ", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def _to_int(value: str) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        print(
            f"[WARN] enriched_import: expected=int, got={value!r}, fallback=None",
            flush=True,
        )
        return None


def _to_float(value: str) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        print(
            f"[WARN] enriched_import: expected=float, got={value!r}, fallback=None",
            flush=True,
        )
        return None


def _to_feature_int(value: str) -> int | None:
    if value is None or value == "":
        return None
    if value in ("0", "1"):
        return int(value)
    print(
        f"[WARN] enriched_import: expected=0|1, got={value!r}, fallback=None",
        flush=True,
    )
    return None


def _coerce_offer_type(value: str | None) -> str:
    if value is None or value == "" or value.upper() == "RENT":
        return "RENT"
    return value.upper()


def _build_features_json(feature_flags: dict[str, int | None]) -> str:
    keys = [
        FEATURE_KEY_MAP[col]
        for col, flag in feature_flags.items()
        if flag == 1
    ]
    return json.dumps(keys)


def _normalize_row(raw: dict[str, str]) -> dict[str, Any]:
    city_slug = slug(raw.get("city"))
    street_name, house_number = split_street(raw.get("street"))

    feature_flags = {col: _to_feature_int(raw.get(col, "")) for col in FEATURE_COLUMNS}

    listing_id = (raw.get("listing_id") or "").strip()
    return {
        "listing_id": listing_id,
        "platform_id": listing_id,
        "scrape_source": (raw.get("scrape_source") or None),
        "title": (raw.get("title") or "").strip() or "(untitled)",
        "description": _strip_html(raw.get("description_head")),
        "street": street_name,
        "house_number": house_number,
        "city": city_slug,
        "city_slug": city_slug,
        "postal_code": _to_int(raw.get("postal_code", "")),
        "canton": (raw.get("canton") or None),
        "price": _to_int(raw.get("price", "")),
        "rooms": _to_float(raw.get("rooms", "")),
        "area": _to_int(raw.get("area", "")),
        "floor": _to_int(raw.get("floor", "")),
        "year_built": _to_int(raw.get("year_built", "")),
        "available_from": (raw.get("available_from") or None),
        "latitude": _to_float(raw.get("latitude", "")),
        "longitude": _to_float(raw.get("longitude", "")),
        **feature_flags,
        "features_json": _build_features_json(feature_flags),
        "offer_type": _coerce_offer_type(raw.get("offer_type")),
        "object_category": translate_object_category(raw.get("object_category")),
        "object_category_raw": (raw.get("object_category") or None),
        "object_type": (raw.get("object_type") or None),
        "original_url": (raw.get("original_url") or None),
        "raw_json": json.dumps(raw, ensure_ascii=False),
    }


_INSERT_COLUMNS = [
    "listing_id", "platform_id", "scrape_source", "title", "description",
    "street", "house_number", "city", "city_slug", "postal_code", "canton",
    "price", "rooms", "area", "floor", "year_built", "available_from",
    "latitude", "longitude",
    *FEATURE_COLUMNS,
    "features_json", "offer_type", "object_category", "object_category_raw",
    "object_type", "original_url", "raw_json",
]


def import_enriched_csv(connection: sqlite3.Connection, csv_path: Path) -> int:
    """Load a normalized sample_enriched CSV into the listings table.

    Returns the number of rows inserted.
    """
    placeholders = ", ".join("?" for _ in _INSERT_COLUMNS)
    columns = ", ".join(_INSERT_COLUMNS)
    sql = f"INSERT INTO listings ({columns}) VALUES ({placeholders})"

    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        prepared = [
            tuple(_normalize_row(row)[col] for col in _INSERT_COLUMNS)
            for row in reader
        ]

    connection.executemany(sql, prepared)
    connection.commit()
    return len(prepared)
