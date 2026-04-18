from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.core.normalize import slug
from app.db import get_connection


@dataclass(slots=True)
class HardFilterParams:
    city: list[str] | None = None
    postal_code: list[str] | None = None
    canton: str | None = None
    min_price: int | None = None
    max_price: int | None = None
    min_rooms: float | None = None
    max_rooms: float | None = None
    min_area: int | None = None
    max_area: int | None = None
    min_floor: int | None = None
    max_floor: int | None = None
    min_year_built: int | None = None
    max_year_built: int | None = None
    available_from_after: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    radius_km: float | None = None
    features: list[str] | None = None
    features_excluded: list[str] | None = None
    object_category: list[str] | None = None
    bm25_keywords: list[str] | None = None
    limit: int = 20
    offset: int = 0
    sort_by: str | None = None


FEATURE_COLUMN_MAP = {
    "balcony": "feature_balcony",
    "elevator": "feature_elevator",
    "parking": "feature_parking",
    "garage": "feature_garage",
    "fireplace": "feature_fireplace",
    "child_friendly": "feature_child_friendly",
    "pets_allowed": "feature_pets_allowed",
    "temporary": "feature_temporary",
    "new_build": "feature_new_build",
    "wheelchair_accessible": "feature_wheelchair_accessible",
    "private_laundry": "feature_private_laundry",
    "minergie_certified": "feature_minergie_certified",
}


def _normalize_list(values: list[str] | None) -> list[str] | None:
    if not values:
        return None
    cleaned = [value.strip() for value in values if value and value.strip()]
    return cleaned or None


_FTS_NO_MATCH_SCORE = 1e9


def _build_fts_match(keywords: list[str] | None) -> str | None:
    if not keywords:
        return None
    cleaned: list[str] = []
    for keyword in keywords:
        if not keyword:
            continue
        stripped = keyword.replace('"', "").strip()
        if stripped:
            cleaned.append(stripped)
    if not cleaned:
        return None
    return " OR ".join(f'"{keyword}"' for keyword in cleaned)


def search_listings(db_path: Path, filters: HardFilterParams) -> list[dict[str, Any]]:
    where_clauses: list[str] = []
    params: list[Any] = []

    city = _normalize_list(filters.city)
    if city:
        slugs = [s for s in (slug(value) for value in city) if s]
        if slugs:
            placeholders = ", ".join("?" for _ in slugs)
            where_clauses.append(f"city_slug IN ({placeholders})")
            params.extend(slugs)

    postal_code = _normalize_list(filters.postal_code)
    if postal_code:
        placeholders = ", ".join("?" for _ in postal_code)
        where_clauses.append(f"postal_code IN ({placeholders})")
        params.extend(int(value) for value in postal_code)

    if filters.canton:
        where_clauses.append("UPPER(canton) = ?")
        params.append(filters.canton.upper())

    if filters.min_price is not None:
        where_clauses.append("price >= ?")
        params.append(filters.min_price)

    if filters.max_price is not None:
        where_clauses.append("price <= ?")
        params.append(filters.max_price)

    if filters.min_rooms is not None:
        where_clauses.append("rooms >= ?")
        params.append(filters.min_rooms)

    if filters.max_rooms is not None:
        where_clauses.append("rooms <= ?")
        params.append(filters.max_rooms)

    if filters.min_area is not None:
        where_clauses.append("area >= ?")
        params.append(filters.min_area)

    if filters.max_area is not None:
        where_clauses.append("area <= ?")
        params.append(filters.max_area)

    if filters.min_floor is not None:
        where_clauses.append("floor >= ?")
        params.append(filters.min_floor)

    if filters.max_floor is not None:
        where_clauses.append("floor <= ?")
        params.append(filters.max_floor)

    if filters.min_year_built is not None:
        where_clauses.append("year_built >= ?")
        params.append(filters.min_year_built)

    if filters.max_year_built is not None:
        where_clauses.append("year_built <= ?")
        params.append(filters.max_year_built)

    if filters.available_from_after:
        where_clauses.append("available_from >= ?")
        params.append(filters.available_from_after)

    object_category = _normalize_list(filters.object_category)
    if object_category:
        placeholders = ", ".join("?" for _ in object_category)
        where_clauses.append(f"object_category IN ({placeholders})")
        params.extend(object_category)

    features = _normalize_list(filters.features)
    if features:
        for feature_name in features:
            column_name = FEATURE_COLUMN_MAP.get(feature_name)
            if column_name:
                where_clauses.append(f"{column_name} = 1")

    features_excluded = _normalize_list(filters.features_excluded)
    if features_excluded:
        for feature_name in features_excluded:
            column_name = FEATURE_COLUMN_MAP.get(feature_name)
            if column_name:
                where_clauses.append(f"{column_name} = 0")

    fts_match = _build_fts_match(filters.bm25_keywords)

    select_cols = """
            listing_id,
            platform_id,
            scrape_source,
            title,
            description,
            street,
            house_number,
            city,
            city_slug,
            postal_code,
            canton,
            price,
            rooms,
            area,
            floor,
            year_built,
            available_from,
            latitude,
            longitude,
            distance_public_transport,
            distance_shop,
            distance_kindergarten,
            distance_school_1,
            distance_school_2,
            features_json,
            offer_type,
            object_category,
            object_type,
            original_url,
            images_json
    """

    if fts_match is not None:
        query = f"""
            SELECT {select_cols},
                COALESCE(fts.bm25_score, {_FTS_NO_MATCH_SCORE}) AS bm25_score
            FROM listings
            LEFT JOIN (
                SELECT rowid, bm25(listings_fts) AS bm25_score
                FROM listings_fts
                WHERE listings_fts MATCH ?
            ) fts ON fts.rowid = listings.rowid
        """
        params = [fts_match, *params]
    else:
        query = f"SELECT {select_cols} FROM listings"

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    if fts_match is not None:
        query += " ORDER BY bm25_score ASC, listing_id ASC"
    else:
        query += " ORDER BY " + _sort_clause(filters.sort_by)

    with get_connection(db_path) as connection:
        rows = connection.execute(query, params).fetchall()

    parsed_rows = [_parse_row(dict(row)) for row in rows]

    if (
        filters.latitude is not None
        and filters.longitude is not None
        and filters.radius_km is not None
    ):
        nearby_rows: list[tuple[float, dict[str, Any]]] = []
        for row in parsed_rows:
            if row.get("latitude") is None or row.get("longitude") is None:
                continue
            distance = _distance_km(
                filters.latitude,
                filters.longitude,
                row["latitude"],
                row["longitude"],
            )
            if distance <= filters.radius_km:
                nearby_rows.append((distance, row))

        nearby_rows.sort(key=lambda item: (item[0], item[1]["listing_id"]))
        parsed_rows = [row for _, row in nearby_rows]

    return parsed_rows[filters.offset : filters.offset + filters.limit]


def _parse_row(row: dict[str, Any]) -> dict[str, Any]:
    features_json = row.pop("features_json", "[]")
    images_json = row.pop("images_json", None)
    try:
        row["features"] = json.loads(features_json) if features_json else []
    except json.JSONDecodeError:
        row["features"] = []
    row["image_urls"] = _extract_image_urls(images_json)
    row["hero_image_url"] = row["image_urls"][0] if row["image_urls"] else None
    return row


def _extract_image_urls(images_json: Any) -> list[str]:
    if not images_json:
        return []
    try:
        parsed = json.loads(images_json) if isinstance(images_json, str) else images_json
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, dict):
        return []

    image_urls: list[str] = []
    for item in parsed.get("images", []) or []:
        if isinstance(item, dict) and item.get("url"):
            image_urls.append(str(item["url"]))
        elif isinstance(item, str) and item:
            image_urls.append(item)
    for item in parsed.get("image_paths", []) or []:
        if isinstance(item, str) and item:
            image_urls.append(item)
    return image_urls


def _distance_km(
    center_lat: float,
    center_lon: float,
    row_lat: float,
    row_lon: float,
) -> float:
    earth_radius_km = 6371.0
    delta_lat = math.radians(row_lat - center_lat)
    delta_lon = math.radians(row_lon - center_lon)
    start_lat = math.radians(center_lat)
    end_lat = math.radians(row_lat)

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(start_lat) * math.cos(end_lat) * math.sin(delta_lon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return earth_radius_km * c


def _sort_clause(sort_by: str | None) -> str:
    if sort_by == "price_asc":
        return "price ASC NULLS LAST, listing_id ASC"
    if sort_by == "price_desc":
        return "price DESC NULLS LAST, listing_id ASC"
    if sort_by == "rooms_asc":
        return "rooms ASC NULLS LAST, listing_id ASC"
    if sort_by == "rooms_desc":
        return "rooms DESC NULLS LAST, listing_id ASC"
    return "listing_id ASC"
