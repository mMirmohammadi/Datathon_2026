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
    # Pass 2b enriched fields (see enrichment/scripts/pass2b_bathroom_cellar_kitchen.py).
    # Integer + tri-state boolean: None means the user didn't constrain; 'UNKNOWN'
    # sentinel values in listings_enriched are excluded by positive constraints.
    min_bathrooms: int | None = None
    max_bathrooms: int | None = None
    bathroom_shared: bool | None = None
    has_cellar: bool | None = None
    kitchen_shared: bool | None = None
    # Precomputed landmark keys (resolved upstream from `soft_preferences.
    # near_landmark`). When set, the SQL applies a disjunctive proximity
    # filter: a listing must be within NEAR_LANDMARK_RADIUS_M of AT LEAST
    # ONE of these landmarks. Any unresolved alias is dropped by the caller
    # with a [WARN]; an all-empty list means "no landmark filter" — the
    # feature stays soft in the ranker.
    near_landmark_keys: list[str] | None = None
    bm25_keywords: list[str] | None = None
    limit: int = 20
    offset: int = 0
    sort_by: str | None = None


# Radius within which a listing is considered "near" a landmark, in metres.
# 3 km covers a generous ~30-min walk OR a couple of tram stops in a Swiss
# city — tight enough that "near ETH" excludes the bulk of the 25k corpus
# (714 rows near ETH OR HB Zurich vs 23,909 unfiltered), permissive enough
# that the user isn't surprised by a listing across the street failing.
NEAR_LANDMARK_RADIUS_M = 3000


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


# Object categories where a NULL/unknown label is a reasonable match for the
# user's intent. 47% of the corpus has `object_category IS NULL` at ingest
# (mostly unlabelled small-apartment scrapes), so strict equality on e.g.
# "furnished_apartment" wipes out thousands of semantically-compatible rows
# — that was the "Möbliertes 1.5-Zimmer Studio in Bern Altstadt" bug.
# But for distinctive categories (villa, commercial, parking, house,
# hobby_room, garage) NULL is NOT a safe assumption; a NULL row is much
# more likely to be an apartment than a villa, so letting NULL through on
# those queries pollutes the results.
#
# Membership test here is CaseSensitive and must match the slugs produced
# by app.core.normalize.translate_object_category (English canonical enum).
_APARTMENT_LIKE_CATEGORIES: frozenset[str] = frozenset({
    "apartment",
    "furnished_apartment",
    "studio",
    "attic_apartment",
    "maisonette",
    "loft",
    "penthouse",
    "terrace_apartment",
    "holiday_apartment",
    "duplex",
    "room",
    "shared_room",
})


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


def _build_where_and_params(
    filters: HardFilterParams,
) -> tuple[list[str], list[Any]]:
    """Return the WHERE clauses + parameter list for ``filters``.

    Shared by :func:`search_listings` (the ranker's candidate-pool query) and
    :func:`search_listing_coords` (the map-overlay query). Keeps both in lock-
    step — adding a new hard filter updates both endpoints with one edit.

    Assumes the caller will emit ``FROM listings l LEFT JOIN listings_enriched
    e`` — every reference is qualified with the ``l.`` or ``e.`` alias.
    """
    where_clauses: list[str] = []
    params: list[Any] = []

    city = _normalize_list(filters.city)
    if city:
        slugs = [s for s in (slug(value) for value in city) if s]
        if slugs:
            placeholders = ", ".join("?" for _ in slugs)
            where_clauses.append(f"l.city_slug IN ({placeholders})")
            params.extend(slugs)

    postal_code = _normalize_list(filters.postal_code)
    if postal_code:
        placeholders = ", ".join("?" for _ in postal_code)
        where_clauses.append(f"l.postal_code IN ({placeholders})")
        params.extend(int(value) for value in postal_code)

    if filters.canton:
        # `listings.canton` is only 41.8% populated (inherited from the raw CSV).
        # `listings_enriched.canton_filled` is 99.7% — backfilled by pass-1a
        # reverse_geocoder + pass-1b Nominatim + pass-1d PLZ majority vote +
        # pass-1e GPT-nano. Filtering on the enriched column unlocks the other
        # 58% of rows that otherwise silently vanish on any canton search. Both
        # the raw canton codes and the enriched _filled values are stored as
        # 2-letter uppercase ISO codes, so a direct equality (no UPPER()) works.
        # 'UNKNOWN' is the sentinel for the 0.3% unresolved — it never equals
        # a user-supplied canton code, so it's naturally excluded.
        where_clauses.append("e.canton_filled = ?")
        params.append(filters.canton.upper())

    if filters.min_price is not None:
        where_clauses.append("l.price >= ?")
        params.append(filters.min_price)

    if filters.max_price is not None:
        where_clauses.append("l.price <= ?")
        params.append(filters.max_price)

    if filters.min_rooms is not None:
        where_clauses.append("l.rooms >= ?")
        params.append(filters.min_rooms)

    if filters.max_rooms is not None:
        where_clauses.append("l.rooms <= ?")
        params.append(filters.max_rooms)

    if filters.min_area is not None:
        where_clauses.append("l.area >= ?")
        params.append(filters.min_area)

    if filters.max_area is not None:
        where_clauses.append("l.area <= ?")
        params.append(filters.max_area)

    if filters.min_floor is not None:
        where_clauses.append("l.floor >= ?")
        params.append(filters.min_floor)

    if filters.max_floor is not None:
        where_clauses.append("l.floor <= ?")
        params.append(filters.max_floor)

    if filters.min_year_built is not None:
        where_clauses.append("l.year_built >= ?")
        params.append(filters.min_year_built)

    if filters.max_year_built is not None:
        where_clauses.append("l.year_built <= ?")
        params.append(filters.max_year_built)

    if filters.available_from_after:
        where_clauses.append("l.available_from >= ?")
        params.append(filters.available_from_after)

    object_category = _normalize_list(filters.object_category)
    if object_category:
        placeholders = ", ".join("?" for _ in object_category)
        # NULL tolerance, but only when the user is asking for an apartment-
        # like category. 47% of listings have `object_category IS NULL` at
        # ingest, and empirically those unlabelled rows are overwhelmingly
        # small residential units — so strict equality on e.g.
        # `furnished_apartment` wipes out 12k listings that are semantically
        # compatible (the "Möbliertes 1.5-Zimmer Studio in Bern Altstadt"
        # bug). But for distinctive categories (villa, commercial, parking,
        # house, ...) NULL is NOT a safe bet — a NULL row is very unlikely
        # to be a villa, so letting NULL through pollutes those searches
        # (live test before this guard: "villa in Lugano" went from 1 to 46
        # results, 45 of which were NULL-category and probably not villas).
        #
        # Rule: allow NULL only when ALL requested categories are
        # apartment-like. Mixed lists (e.g. ["studio", "villa"]) default to
        # strict — the user is asking for a specific thing that villa-NULL
        # leakage would violate.
        if all(cat in _APARTMENT_LIKE_CATEGORIES for cat in object_category):
            where_clauses.append(
                f"(l.object_category IN ({placeholders}) OR l.object_category IS NULL)"
            )
        else:
            where_clauses.append(f"l.object_category IN ({placeholders})")
        params.extend(object_category)

    features = _normalize_list(filters.features)
    if features:
        for feature_name in features:
            column_name = FEATURE_COLUMN_MAP.get(feature_name)
            if column_name:
                where_clauses.append(f"l.{column_name} = 1")

    features_excluded = _normalize_list(filters.features_excluded)
    if features_excluded:
        for feature_name in features_excluded:
            column_name = FEATURE_COLUMN_MAP.get(feature_name)
            if column_name:
                where_clauses.append(f"l.{column_name} = 0")

    # Pass 2b filters. bathroom_count_filled is TEXT in listings_enriched (e.g.
    # '1', '2', 'UNKNOWN'); GLOB '[0-9]*' excludes UNKNOWN before CAST. The
    # boolean columns store literal 'true' / 'false' / 'UNKNOWN', so an exact
    # equality check naturally excludes UNKNOWN (positive-filter semantics:
    # UNKNOWN never satisfies a constraint).
    if filters.min_bathrooms is not None:
        where_clauses.append(
            "e.bathroom_count_filled GLOB '[0-9]*' "
            "AND CAST(e.bathroom_count_filled AS INTEGER) >= ?"
        )
        params.append(int(filters.min_bathrooms))

    if filters.max_bathrooms is not None:
        where_clauses.append(
            "e.bathroom_count_filled GLOB '[0-9]*' "
            "AND CAST(e.bathroom_count_filled AS INTEGER) <= ?"
        )
        params.append(int(filters.max_bathrooms))

    if filters.bathroom_shared is not None:
        where_clauses.append("e.bathroom_shared_filled = ?")
        params.append("true" if filters.bathroom_shared else "false")

    if filters.has_cellar is not None:
        where_clauses.append("e.has_cellar_filled = ?")
        params.append("true" if filters.has_cellar else "false")

    if filters.kitchen_shared is not None:
        where_clauses.append("e.kitchen_shared_filled = ?")
        params.append("true" if filters.kitchen_shared else "false")

    # Proximity filter: ANY-of semantics. We treat ``near_landmark`` as hard
    # on the map SQL (otherwise the user-facing UX of "I selected ETH + UZH
    # so please only show homes near them" is violated — reported as bug).
    # The columns live on ``listings_ranking_signals``; the caller opts into
    # the JOIN by asking :func:`_needs_signals_join`.
    if filters.near_landmark_keys:
        disjuncts: list[str] = []
        for key in filters.near_landmark_keys:
            if not key or not key.replace("_", "").isalnum():
                # Defensive: the resolver should never hand us something ugly,
                # but SQL-interpolating the key directly makes validation
                # non-negotiable. Anything that's not alnum+underscore is
                # dropped with a [WARN].
                print(
                    f"[WARN] _build_where_and_params: expected=landmark key "
                    f"in [a-z0-9_]+, got={key!r}, fallback=skip",
                    flush=True,
                )
                continue
            col = f"dist_landmark_{key}_m"
            disjuncts.append(f"(s.{col} IS NOT NULL AND s.{col} < ?)")
            params.append(int(NEAR_LANDMARK_RADIUS_M))
        if disjuncts:
            where_clauses.append("(" + " OR ".join(disjuncts) + ")")

    return where_clauses, params


def _needs_signals_join(filters: HardFilterParams) -> bool:
    """True when the WHERE builder references `s.<col>` on
    ``listings_ranking_signals``. Driven purely by whether any landmark
    proximity filter is active; extend if you add more signal-backed
    hard filters in the future.
    """
    return bool(filters.near_landmark_keys)


def search_listings(db_path: Path, filters: HardFilterParams) -> list[dict[str, Any]]:
    where_clauses, params = _build_where_and_params(filters)
    fts_match = _build_fts_match(filters.bm25_keywords)

    # Columns from `listings` are aliased through `l.`; the 4 pass-2b columns
    # come from the LEFT JOIN on `listings_enriched` (alias `e`). LEFT JOIN
    # (not INNER) so a listing without an enriched row still returns; the 4
    # pass-2b fields just come back as NULL in that path.
    select_cols = """
            l.listing_id,
            l.platform_id,
            l.scrape_source,
            l.title,
            l.description,
            l.street,
            l.house_number,
            l.city,
            l.city_slug,
            l.postal_code,
            l.canton,
            l.price,
            l.rooms,
            l.area,
            l.floor,
            l.year_built,
            l.available_from,
            l.latitude,
            l.longitude,
            l.distance_public_transport,
            l.distance_shop,
            l.distance_kindergarten,
            l.distance_school_1,
            l.distance_school_2,
            l.features_json,
            l.offer_type,
            l.object_category,
            l.object_type,
            l.original_url,
            l.images_json,
            e.bathroom_count_filled AS bathroom_count_raw,
            e.bathroom_shared_filled AS bathroom_shared_raw,
            e.has_cellar_filled AS has_cellar_raw,
            e.kitchen_shared_filled AS kitchen_shared_raw
    """

    # listings_ranking_signals is only joined when the WHERE builder actually
    # references `s.<col>` — avoids a wide join on unrelated queries.
    signals_join = (
        " LEFT JOIN listings_ranking_signals s ON s.listing_id = l.listing_id"
        if _needs_signals_join(filters)
        else ""
    )

    if fts_match is not None:
        query = f"""
            SELECT {select_cols},
                COALESCE(fts.bm25_score, {_FTS_NO_MATCH_SCORE}) AS bm25_score
            FROM listings l
            LEFT JOIN listings_enriched e ON e.listing_id = l.listing_id
            {signals_join}
            LEFT JOIN (
                SELECT rowid, bm25(listings_fts) AS bm25_score
                FROM listings_fts
                WHERE listings_fts MATCH ?
            ) fts ON fts.rowid = l.rowid
        """
        params = [fts_match, *params]
    else:
        query = f"""
            SELECT {select_cols}
            FROM listings l
            LEFT JOIN listings_enriched e ON e.listing_id = l.listing_id
            {signals_join}
        """

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    if fts_match is not None:
        query += " ORDER BY bm25_score ASC, l.listing_id ASC"
    else:
        query += " ORDER BY " + _sort_clause_qualified(filters.sort_by)

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


_BOOL_TRUE = frozenset({"true", "1", "yes"})
_BOOL_FALSE = frozenset({"false", "0", "no"})


def _coerce_bool_filled(raw: Any) -> bool | None:
    """Map a listings_enriched ``*_filled`` text cell to Python bool | None.

    UNKNOWN-sentinel rows (the 40-60% of listings the pass-2b extractor
    couldn't decide on) come back as ``None`` so the UI can render '—'
    instead of a misleading false claim. Unexpected values fall through to
    ``None`` with a [WARN] — never silently coerce garbage to False.
    """
    if raw is None:
        return None
    s = str(raw).strip().lower()
    if s in _BOOL_TRUE:
        return True
    if s in _BOOL_FALSE:
        return False
    if s in ("unknown", ""):
        return None
    print(
        f"[WARN] _coerce_bool_filled: expected=true|false|UNKNOWN, "
        f"got={raw!r}, fallback=None",
        flush=True,
    )
    return None


def _coerce_int_filled(raw: Any) -> int | None:
    """Map a listings_enriched ``bathroom_count_filled`` text cell to int | None."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.upper() == "UNKNOWN":
        return None
    try:
        return int(s)
    except ValueError:
        print(
            f"[WARN] _coerce_int_filled: expected=integer|UNKNOWN, "
            f"got={raw!r}, fallback=None",
            flush=True,
        )
        return None


def _parse_row(row: dict[str, Any]) -> dict[str, Any]:
    features_json = row.pop("features_json", "[]")
    images_json = row.pop("images_json", None)
    try:
        row["features"] = json.loads(features_json) if features_json else []
    except json.JSONDecodeError:
        row["features"] = []
    row["image_urls"] = _extract_image_urls(images_json)
    row["hero_image_url"] = row["image_urls"][0] if row["image_urls"] else None

    # Pass 2b enriched fields — pop the *_raw aliases from the JOIN and write
    # back typed Python scalars under the unprefixed name used throughout the
    # API. Absent columns (e.g. GET /listings/{id} pre-JOIN path) are tolerated.
    row["bathroom_count"] = _coerce_int_filled(row.pop("bathroom_count_raw", None))
    row["bathroom_shared"] = _coerce_bool_filled(row.pop("bathroom_shared_raw", None))
    row["has_cellar"] = _coerce_bool_filled(row.pop("has_cellar_raw", None))
    row["kitchen_shared"] = _coerce_bool_filled(row.pop("kitchen_shared_raw", None))
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


def _sort_clause_qualified(sort_by: str | None) -> str:
    """ORDER BY clause for the aliased ``FROM listings l`` + JOIN form."""
    if sort_by == "price_asc":
        return "l.price ASC NULLS LAST, l.listing_id ASC"
    if sort_by == "price_desc":
        return "l.price DESC NULLS LAST, l.listing_id ASC"
    if sort_by == "rooms_asc":
        return "l.rooms ASC NULLS LAST, l.listing_id ASC"
    if sort_by == "rooms_desc":
        return "l.rooms DESC NULLS LAST, l.listing_id ASC"
    return "l.listing_id ASC"


def search_listing_coords(
    db_path: Path, filters: HardFilterParams
) -> list[dict[str, Any]]:
    """Return `{listing_id, latitude, longitude, city, canton}` for every
    listing that matches the hard-filter set.

    Feeds the map-overlay panel. Unlike :func:`search_listings` this query
    has NO pagination and NO FTS join — the map wants to show every filter
    match so the user sees where the listings are geographically, not just
    the top-ranked few. Listings with NULL lat/lng are skipped in the Python
    projection (can't place them on a map).

    Tight tuple: the payload scales linearly with match count and we want
    it cheap on the wire (25k matches is ~2 MB; typical is ~40 KB).
    """
    where_clauses, params = _build_where_and_params(filters)

    signals_join = (
        " LEFT JOIN listings_ranking_signals s ON s.listing_id = l.listing_id"
        if _needs_signals_join(filters)
        else ""
    )
    query = f"""
        SELECT l.listing_id, l.latitude, l.longitude, l.city, l.canton,
               l.price, l.rooms, l.area, l.object_category
        FROM listings l
        LEFT JOIN listings_enriched e ON e.listing_id = l.listing_id
        {signals_join}
    """
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    # Deterministic order so the client can stable-sort pins if needed.
    query += " ORDER BY l.listing_id ASC"

    with get_connection(db_path) as connection:
        rows = connection.execute(query, params).fetchall()

    out: list[dict[str, Any]] = []
    for row in rows:
        d = dict(row)
        lat = d.get("latitude")
        lng = d.get("longitude")
        if lat is None or lng is None:
            continue  # unmappable; the card still appears in list results.
        area = d.get("area")
        out.append(
            {
                "listing_id": str(d["listing_id"]),
                "lat": float(lat),
                "lng": float(lng),
                "city": d.get("city"),
                "canton": d.get("canton"),
                "price_chf": d.get("price"),
                "rooms": d.get("rooms"),
                "living_area_sqm": int(area) if area is not None else None,
                "object_category": d.get("object_category"),
            }
        )
    return out
