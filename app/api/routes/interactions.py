"""User-interaction endpoints (the substrate for memory-based personalization).

* ``POST /me/interactions`` - record one save/unsave/click/dwell/dismiss event.
* ``GET  /me/favorites``    - list every currently-saved listing.
* ``DELETE /me/interactions`` - wipe the caller's history (also resets memory).

Interactions writes validate the target ``listing_id`` exists in
``data/listings.db`` so we don't grow garbage rows from typos / attacks.
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.deps import get_current_user, get_users_db_path, require_csrf
from app.auth.db import connect
from app.config import Settings, get_settings
from app.db import get_connection
from app.models.schemas import (
    FavoriteListing,
    FavoritesResponse,
    InteractionRequest,
)


router = APIRouter(prefix="/me", tags=["interactions"])


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _listing_exists(listings_db_path: Path, listing_id: str) -> bool:
    with get_connection(listings_db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM listings WHERE listing_id = ? LIMIT 1",
            (listing_id,),
        ).fetchone()
    return row is not None


def _first_image_url(images_json: Any) -> str | None:
    """Return the first renderable image URL from a listing's ``images_json``
    blob, or ``None`` when no usable URL exists.

    Accepts the SRED / Comparis / RobinReal shapes we see in practice:
    ``{"images": [{"url": "..."}, "..."]}`` or ``{"image_paths": [...]}``.
    """
    if not images_json:
        return None
    try:
        parsed = json.loads(images_json) if isinstance(images_json, str) else images_json
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None
    for item in parsed.get("images") or []:
        if isinstance(item, dict) and item.get("url"):
            return str(item["url"])
        if isinstance(item, str) and item:
            return item
    for item in parsed.get("image_paths") or []:
        if isinstance(item, str) and item:
            return item
    return None


def _listing_summaries(
    listings_db_path: Path, listing_ids: list[str]
) -> dict[str, dict[str, Any]]:
    """Return ``{listing_id: summary_dict}`` for each id that still exists.

    Chunked (SQLite default param limit is 999). We only pull the small set of
    columns the drawer card needs; the full ``ListingData`` shape is
    intentionally not reused here because it pulls in 20+ fields.
    """
    if not listing_ids:
        return {}
    out: dict[str, dict[str, Any]] = {}
    cols = (
        "listing_id, title, price, rooms, area, city, canton, "
        "object_category, features_json, images_json"
    )
    with get_connection(listings_db_path) as conn:
        CHUNK = 800
        for i in range(0, len(listing_ids), CHUNK):
            chunk = listing_ids[i : i + CHUNK]
            placeholders = ", ".join("?" for _ in chunk)
            sql = f"SELECT {cols} FROM listings WHERE listing_id IN ({placeholders})"
            for row in conn.execute(sql, chunk):
                features: list[str] = []
                try:
                    raw_feats = row["features_json"]
                    if raw_feats:
                        loaded = json.loads(raw_feats)
                        if isinstance(loaded, list):
                            features = [str(x) for x in loaded if x]
                except (json.JSONDecodeError, sqlite3.Error):
                    features = []
                out[row["listing_id"]] = {
                    "title": row["title"],
                    "price": row["price"],
                    "rooms": row["rooms"],
                    "area": row["area"],
                    "city": row["city"],
                    "canton": row["canton"],
                    "object_category": row["object_category"],
                    "hero_image_url": _first_image_url(row["images_json"]),
                    "features": features,
                }
    return out


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None


def _enrich_favorites(
    rows: list[sqlite3.Row],
    listings_db_path: Path,
) -> list[FavoriteListing]:
    """Merge ``(listing_id, created_at)`` rows with listing summaries."""
    listing_ids = [r["listing_id"] for r in rows]
    summaries = _listing_summaries(listings_db_path, listing_ids)
    out: list[FavoriteListing] = []
    for r in rows:
        lid = r["listing_id"]
        info = summaries.get(lid) or {}
        out.append(
            FavoriteListing(
                listing_id=lid,
                saved_at=r["created_at"],
                title=info.get("title"),
                price_chf=_coerce_int(info.get("price")),
                rooms=info.get("rooms"),
                area_sqm=_coerce_int(info.get("area")),
                city=info.get("city"),
                canton=info.get("canton"),
                object_category=info.get("object_category"),
                hero_image_url=info.get("hero_image_url"),
                features=info.get("features") or [],
            )
        )
    return out


@router.post("/interactions", status_code=status.HTTP_201_CREATED)
def record_interaction(
    payload: InteractionRequest,
    user: dict[str, Any] = Depends(get_current_user(required=True)),
    _csrf: None = Depends(require_csrf),
    users_db_path: Path = Depends(get_users_db_path),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    if not _listing_exists(settings.db_path, payload.listing_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"listing_id {payload.listing_id!r} not found",
        )
    now = _now_iso()
    with connect(users_db_path) as conn:
        conn.execute(
            "INSERT INTO user_interactions "
            "(user_id, listing_id, kind, value, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                int(user["id"]),
                payload.listing_id,
                payload.kind,
                payload.value,
                now,
            ),
        )
        conn.commit()
    return {"ok": True, "created_at": now}


@router.get("/favorites", response_model=FavoritesResponse)
def list_favorites(
    user: dict[str, Any] = Depends(get_current_user(required=True)),
    users_db_path: Path = Depends(get_users_db_path),
    settings: Settings = Depends(get_settings),
) -> FavoritesResponse:
    """Every listing the user has bookmarked (and not since unbookmarked).

    Bookmarks are a UX-only concept: they populate the "Saved listings"
    drawer. They do NOT influence ranking - that's the job of the separate
    ``like`` / ``unlike`` kinds, which feed the memory profile.

    Legacy ``save`` / ``unsave`` events (written by the pre-split Save
    button) are included here too: the old UX didn't distinguish between
    "bookmark" and "like", so a pre-split ``save`` counts as both a bookmark
    (this endpoint) and a like (``/me/likes``). That way users don't lose
    the list of listings they thought they'd saved.

    Each entry is enriched with a compact listing summary (title, price,
    rooms, city, hero image, features) so the client can render a card
    without a second round-trip.
    """
    sql = """
    WITH last_kind AS (
        SELECT listing_id,
               kind,
               created_at,
               ROW_NUMBER() OVER (
                   PARTITION BY listing_id
                   ORDER BY created_at DESC, id DESC
               ) AS rn
        FROM user_interactions
        WHERE user_id = ? AND kind IN ('bookmark', 'unbookmark', 'save', 'unsave')
    )
    SELECT listing_id, created_at
    FROM last_kind
    WHERE rn = 1 AND kind IN ('bookmark', 'save')
    ORDER BY created_at DESC
    """
    with connect(users_db_path) as conn:
        rows = conn.execute(sql, (int(user["id"]),)).fetchall()
    return FavoritesResponse(favorites=_enrich_favorites(rows, settings.db_path))


@router.get("/likes", response_model=FavoritesResponse)
def list_likes(
    user: dict[str, Any] = Depends(get_current_user(required=True)),
    users_db_path: Path = Depends(get_users_db_path),
    settings: Settings = Depends(get_settings),
) -> FavoritesResponse:
    """Every listing the user has liked (and not since unliked).

    Likes feed the memory profile and drive personalization. Returned in the
    same enriched shape as ``/me/favorites`` so the UI can reuse its rendering.
    """
    sql = """
    WITH last_kind AS (
        SELECT listing_id,
               kind,
               created_at,
               ROW_NUMBER() OVER (
                   PARTITION BY listing_id
                   ORDER BY created_at DESC, id DESC
               ) AS rn
        FROM user_interactions
        WHERE user_id = ? AND kind IN ('like', 'unlike', 'save', 'unsave')
    )
    SELECT listing_id, created_at
    FROM last_kind
    WHERE rn = 1 AND kind IN ('like', 'save')
    ORDER BY created_at DESC
    """
    with connect(users_db_path) as conn:
        rows = conn.execute(sql, (int(user["id"]),)).fetchall()
    return FavoritesResponse(favorites=_enrich_favorites(rows, settings.db_path))


@router.get("/dismissed", response_model=list[str])
def list_dismissed(
    user: dict[str, Any] = Depends(get_current_user(required=True)),
    users_db_path: Path = Depends(get_users_db_path),
) -> list[str]:
    """Listing ids the user has explicitly dismissed (and not since undone).

    Used by the UI on page load to hydrate the per-card "Not for me" button
    state so a reload doesn't make the card re-appear in its un-dismissed
    form. Computed from the latest ``dismiss``/``undismiss`` event per listing.
    """
    sql = """
    WITH last_kind AS (
        SELECT listing_id,
               kind,
               ROW_NUMBER() OVER (
                   PARTITION BY listing_id
                   ORDER BY created_at DESC, id DESC
               ) AS rn
        FROM user_interactions
        WHERE user_id = ? AND kind IN ('dismiss', 'undismiss')
    )
    SELECT listing_id FROM last_kind WHERE rn = 1 AND kind = 'dismiss'
    """
    with connect(users_db_path) as conn:
        rows = conn.execute(sql, (int(user["id"]),)).fetchall()
    return [r["listing_id"] for r in rows]


@router.delete("/interactions")
def clear_interactions(
    user: dict[str, Any] = Depends(get_current_user(required=True)),
    _csrf: None = Depends(require_csrf),
    users_db_path: Path = Depends(get_users_db_path),
) -> dict[str, Any]:
    with connect(users_db_path) as conn:
        cursor = conn.execute(
            "DELETE FROM user_interactions WHERE user_id = ?",
            (int(user["id"]),),
        )
        conn.commit()
    return {"ok": True, "deleted": cursor.rowcount}
