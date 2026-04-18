"""BM25 retrieval over the allowed set produced by the SQL hard-filter gate.

Design:
  - The SQL gate (`app.core.hard_filters.search_listings`) returns candidates
    that satisfy every hard constraint. This module scores / re-orders them
    by BM25F over `listings_fts` (title×3, description×1, street×0.5, city×0.5).
  - If the query plan has no usable BM25 tokens (pure hard-filter query), we
    pass the candidates through untouched with `bm25_score=None`.
  - Every failure path emits a `[WARN]` log per CLAUDE.md §5; there is no
    silent degradation.

FTS5 specifics:
  - `bm25()` returns NEGATIVE scores (smaller = more relevant). We preserve
    SQLite's convention and let the ranking layer normalize to percentile.
  - Tokens are double-quoted to treat each as a phrase and escape FTS operators.
  - `unicode61 remove_diacritics 2` folds accents both at index AND query time,
    so "Zürich" in the query matches "zurich" in the index.
"""
from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any

from app.db import get_connection
from app.models.schemas import QueryPlan

# BM25F field weights — title gets ~3× description per BM25F short-field convention
# (Robertson & Zaragoza). Street + city are thin fields, weighted 0.5.
BM25_WEIGHTS = (3.0, 1.0, 0.5, 0.5)  # title, description, street, city

# Token filter: drop < 2-char, very common tokens that BM25 IDF-downweights anyway
# but that clutter logs and burn compute.
_TOKEN_SPLIT_RE = re.compile(r"[^\w\-]+", re.UNICODE)
_STOPTOKENS = {
    # common multilingual filler that contributes no signal
    "a", "an", "and", "the", "or", "de", "la", "le", "les", "une", "un", "et",
    "di", "il", "lo", "gli", "e", "in", "per", "con", "der", "die", "das",
    "und", "mit", "im", "am", "at", "to", "of", "for",
}

# IN-clause chunk size — SQLite allows 999 bound parameters by default
_IN_CHUNK_SIZE = 900


def bm25_candidates(
    db_path: Path,
    plan: QueryPlan,
    allowed_ids: list[str],
    *,
    k: int = 100,
) -> list[dict[str, Any]]:
    """Return up to k candidates from allowed_ids ranked by BM25F.

    If no tokens can be extracted from the plan, returns allowed_ids as-is
    (truncated to k) with `bm25_score=None` — BM25 is a soft ranker, not a
    filter, so pure hard-filter queries should still return results.
    """
    if not allowed_ids:
        print(
            "[INFO] retrieval.bm25_candidates: empty allowed_ids, returning []",
            flush=True,
        )
        return []

    match_string, token_count = _build_match_string(plan)
    if not match_string:
        print(
            f"[INFO] retrieval.bm25_candidates: no usable tokens, "
            f"pass-through allowed={len(allowed_ids)} k={k}",
            flush=True,
        )
        return _fetch_by_ids(db_path, allowed_ids[:k], bm25_score=None)

    t0 = time.monotonic()
    # Build a fresh temp table per call instead of a giant IN clause —
    # covers allowed sets of any size uniformly, and is faster for >1k IDs.
    candidates: list[dict[str, Any]] = []
    with get_connection(db_path) as connection:
        try:
            connection.execute(
                "CREATE TEMP TABLE IF NOT EXISTS allowed_ids_tmp (listing_id TEXT PRIMARY KEY)"
            )
            connection.execute("DELETE FROM allowed_ids_tmp")
            for i in range(0, len(allowed_ids), _IN_CHUNK_SIZE):
                chunk = allowed_ids[i : i + _IN_CHUNK_SIZE]
                connection.executemany(
                    "INSERT OR IGNORE INTO allowed_ids_tmp(listing_id) VALUES (?)",
                    [(lid,) for lid in chunk],
                )
            sql = f"""
                SELECT
                    l.listing_id,
                    l.title,
                    l.description,
                    l.street,
                    l.city,
                    l.postal_code,
                    l.canton,
                    l.price,
                    l.rooms,
                    l.area,
                    l.available_from,
                    l.latitude,
                    l.longitude,
                    l.distance_public_transport,
                    l.distance_shop,
                    l.features_json,
                    l.offer_type,
                    l.object_category,
                    l.object_type,
                    l.original_url,
                    l.images_json,
                    bm25(listings_fts, {BM25_WEIGHTS[0]}, {BM25_WEIGHTS[1]},
                         {BM25_WEIGHTS[2]}, {BM25_WEIGHTS[3]}) AS bm25_score
                FROM listings_fts
                JOIN listings l ON l.rowid = listings_fts.rowid
                JOIN allowed_ids_tmp a ON a.listing_id = l.listing_id
                WHERE listings_fts MATCH ?
                ORDER BY bm25_score
                LIMIT ?
            """
            rows = connection.execute(sql, (match_string, int(k))).fetchall()
            for row in rows:
                candidates.append(_parse_row(dict(row)))
        except Exception as exc:
            print(
                f"[WARN] retrieval.bm25_candidates: expected=fts5_match_ok, "
                f"got={type(exc).__name__}, fallback=pass-through "
                f"match={match_string!r} allowed={len(allowed_ids)} exc={exc!r}",
                flush=True,
            )
            return _fetch_by_ids(db_path, allowed_ids[:k], bm25_score=None)

    elapsed = time.monotonic() - t0
    print(
        f"[INFO] retrieval.bm25_candidates: tokens={token_count} "
        f"allowed={len(allowed_ids)} returned={len(candidates)} "
        f"elapsed_s={elapsed:.3f}",
        flush=True,
    )
    return candidates


# --- helpers ----------------------------------------------------------------


def _build_match_string(plan: QueryPlan) -> tuple[str, int]:
    """Tokenize plan's rewrites + soft keywords into an OR-joined FTS5 MATCH.

    Returns (match_string, token_count). Empty string if no tokens.
    """
    sources: list[str] = []
    sources.extend(plan.rewrites)
    sources.extend(plan.soft.keywords)
    # If we have no rewrites or soft keywords (possible when Claude fell back to
    # regex and extracted only hard filters), fall back to the raw query so the
    # user's actual text still drives recall.
    if not any(s.strip() for s in sources):
        sources.append(plan.raw_query)

    tokens: list[str] = []
    seen: set[str] = set()
    for src in sources:
        for raw in _TOKEN_SPLIT_RE.split(src):
            tok = raw.strip().lower()
            if len(tok) < 2:
                continue
            if tok in _STOPTOKENS:
                continue
            if tok in seen:
                continue
            seen.add(tok)
            tokens.append(tok)
    # Cap to bound query size
    tokens = tokens[:64]
    if not tokens:
        return "", 0
    # Quote each token (FTS5 phrase syntax) to escape operators
    quoted = [f'"{_escape_fts_token(t)}"' for t in tokens]
    return " OR ".join(quoted), len(tokens)


def _escape_fts_token(token: str) -> str:
    """Escape embedded double-quotes (FTS5 string literal)."""
    return token.replace('"', '""')


def _fetch_by_ids(
    db_path: Path, ids: list[str], *, bm25_score: float | None
) -> list[dict[str, Any]]:
    """Fetch listings by id, preserving the order of `ids`. Attach bm25_score."""
    if not ids:
        return []
    out: list[dict[str, Any]] = []
    with get_connection(db_path) as connection:
        # Chunked to stay under the SQLite bound-param limit
        id_to_row: dict[str, dict[str, Any]] = {}
        for i in range(0, len(ids), _IN_CHUNK_SIZE):
            chunk = ids[i : i + _IN_CHUNK_SIZE]
            placeholders = ",".join(["?"] * len(chunk))
            rows = connection.execute(
                f"""
                SELECT
                    listing_id, title, description, street, city, postal_code, canton,
                    price, rooms, area, available_from, latitude, longitude,
                    distance_public_transport, distance_shop, features_json,
                    offer_type, object_category, object_type, original_url, images_json
                FROM listings
                WHERE listing_id IN ({placeholders})
                """,
                chunk,
            ).fetchall()
            for row in rows:
                d = _parse_row(dict(row))
                d["bm25_score"] = bm25_score
                id_to_row[d["listing_id"]] = d
    # Preserve input order
    for lid in ids:
        if lid in id_to_row:
            out.append(id_to_row[lid])
    return out


def _parse_row(row: dict[str, Any]) -> dict[str, Any]:
    """Same shape as hard_filters._parse_row — features + image_urls."""
    import json

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
    import json

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
