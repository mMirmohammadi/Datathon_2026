"""Compute the top-K closest landmarks per listing for the UI.

Every listing card in the demo shows a chip row of nearby landmarks; each
chip deep-links into Google Maps Directions from the apartment to that
landmark. This module does the proximity query + the name / kind / coords
lookup in one place so both the batch (search results) and single-listing
(``GET /listings/{id}``) paths produce identical output.

Data sources:
  * ``listings_ranking_signals.dist_landmark_<key>_m`` — Haversine distance
    in metres from each listing to each of the 45 curated landmarks in
    ``data/ranking/landmarks.json``. 94% listing coverage; primary sort key.
  * ``listing_commute_times`` — per-(listing, landmark) real r5py / GTFS
    peak-Tuesday-8-AM transit minutes. ~24% coverage; enriches each chip
    with "N min" when available, degrades silently otherwise.
  * :mod:`app.core.landmarks` — the gazetteer itself; provides the
    human-readable ``name`` (from aliases) + ``lat``/``lon`` to build the
    Google Maps directions URL on the frontend.

Never raises. On any missing table / column we emit one ``[WARN]`` and
return an empty list — the UI simply omits the chip row.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from app.core import landmarks as _landmarks
from app.core.landmarks import Landmark


# How many landmarks we surface per listing. Kept low because landmarks far
# beyond the 3rd closest are rarely useful — Zurich listings find Altstadt /
# HB / ETH / UZH / Zurichsee before the ranking runs out of "near" signal.
DEFAULT_TOP_K = 5


def _display_name(lm: Landmark) -> str:
    """Pick a human-friendly display name for the chip.

    Rule: first alias if present (aliases are the names the LLM extraction
    prompt uses, e.g. "ETH Zürich"), else derive from the key (underscores →
    spaces, title-case). Never returns an empty string.
    """
    if lm.aliases:
        # Prefer the richest alias — pick the longest as a cheap heuristic so
        # "ETH Zürich" beats the 3-letter "ETH".
        return max(lm.aliases, key=len)
    return lm.key.replace("_", " ").title()


def _to_nearby_dict(
    lm: Landmark,
    *,
    distance_m: float | None,
    transit_min: int | None,
) -> dict[str, Any]:
    return {
        "key": lm.key,
        "name": _display_name(lm),
        "kind": lm.kind,
        "lat": float(lm.lat),
        "lng": float(lm.lon),
        "distance_m": float(distance_m) if distance_m is not None else None,
        "transit_min": int(transit_min) if transit_min is not None else None,
    }


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_dist_rows(
    conn: sqlite3.Connection,
    listing_ids: list[str],
) -> dict[str, sqlite3.Row]:
    """``{listing_id: row}`` where row carries the 45 ``dist_landmark_*_m``
    columns. Missing listings (no geo, no signals row) are simply absent —
    caller treats absence as empty landmark list.

    Chunked at 800 ids/call to stay well under SQLite's 999-param limit.
    """
    if not listing_ids:
        return {}
    out: dict[str, sqlite3.Row] = {}
    CHUNK = 800
    for i in range(0, len(listing_ids), CHUNK):
        chunk = listing_ids[i : i + CHUNK]
        placeholders = ", ".join("?" for _ in chunk)
        try:
            for row in conn.execute(
                f"SELECT * FROM listings_ranking_signals "
                f"WHERE listing_id IN ({placeholders})",
                chunk,
            ):
                out[row["listing_id"]] = row
        except sqlite3.OperationalError as exc:
            print(
                f"[WARN] landmark_proximity._fetch_dist_rows: "
                f"expected=listings_ranking_signals table, "
                f"got={exc}, fallback=empty chip lists",
                flush=True,
            )
            return {}
    return out


def _fetch_commute_rows(
    conn: sqlite3.Connection,
    listing_ids: list[str],
) -> dict[tuple[str, str], int]:
    """``{(listing_id, landmark_key): travel_min}`` from the r5py matrix.

    Missing rows = the r5py run produced no valid path (either the listing
    has no coords, or the (origin, landmark) distance is > 40 km and the
    pre-filter dropped the pair, or the peak-Tuesday-8-AM snapshot had no
    feasible transit path).
    """
    if not listing_ids:
        return {}
    out: dict[tuple[str, str], int] = {}
    CHUNK = 800
    for i in range(0, len(listing_ids), CHUNK):
        chunk = listing_ids[i : i + CHUNK]
        placeholders = ", ".join("?" for _ in chunk)
        try:
            for row in conn.execute(
                f"SELECT listing_id, landmark_key, travel_min "
                f"FROM listing_commute_times WHERE listing_id IN ({placeholders})",
                chunk,
            ):
                travel = row["travel_min"]
                if travel is None:
                    continue
                out[(row["listing_id"], row["landmark_key"])] = int(travel)
        except sqlite3.OperationalError as exc:
            print(
                f"[WARN] landmark_proximity._fetch_commute_rows: "
                f"expected=listing_commute_times table, got={exc}, "
                f"fallback=Haversine distance only on chips",
                flush=True,
            )
            return {}
    return out


def _nearest_for_one(
    listing_id: str,
    dist_row: sqlite3.Row | None,
    commute_map: dict[tuple[str, str], int],
    all_landmarks: list[Landmark],
    top_k: int,
) -> list[dict[str, Any]]:
    """Pick top-K landmarks for a single listing.

    Rank: ascending Haversine distance from ``dist_landmark_<key>_m``
    (listings without that column don't rank at all).
    """
    if dist_row is None:
        return []
    ranked: list[tuple[float, Landmark]] = []
    for lm in all_landmarks:
        col = _landmarks.column_for(lm.key)
        try:
            dist = dist_row[col]
        except (IndexError, KeyError):
            continue
        if dist is None:
            continue
        ranked.append((float(dist), lm))
    ranked.sort(key=lambda p: p[0])
    top = ranked[:top_k]
    return [
        _to_nearby_dict(
            lm,
            distance_m=dist,
            transit_min=commute_map.get((listing_id, lm.key)),
        )
        for dist, lm in top
    ]


def compute_for_listings(
    db_path: Path,
    listing_ids: list[str],
    *,
    top_k: int = DEFAULT_TOP_K,
) -> dict[str, list[dict[str, Any]]]:
    """Batched top-K landmark lookup. Returns ``{listing_id: [chip dict, ...]}``.

    One DB connection, two batched queries (dist + commute). Listings with
    no signals row get ``[]`` (keyed-absent, caller may omit or fill with
    empty list — both acceptable). Safe to call with an empty input —
    returns ``{}`` with no DB work.
    """
    if not listing_ids:
        return {}
    try:
        all_lm = list(_landmarks.all_landmarks())
    except Exception as exc:
        print(
            f"[WARN] landmark_proximity.compute_for_listings: expected="
            f"gazetteer, got={type(exc).__name__}: {exc}, "
            f"fallback=empty chip lists",
            flush=True,
        )
        return {}
    if not all_lm:
        return {lid: [] for lid in listing_ids}

    try:
        conn = _connect(db_path)
    except sqlite3.Error as exc:
        print(
            f"[WARN] landmark_proximity.compute_for_listings: expected="
            f"readable DB at {db_path}, got={type(exc).__name__}: {exc}, "
            f"fallback=empty chip lists",
            flush=True,
        )
        return {}
    try:
        dist_rows = _fetch_dist_rows(conn, listing_ids)
        commute_map = _fetch_commute_rows(conn, listing_ids)
    finally:
        conn.close()

    return {
        lid: _nearest_for_one(
            lid, dist_rows.get(lid), commute_map, all_lm, top_k,
        )
        for lid in listing_ids
    }


def compute_for_one(
    db_path: Path,
    listing_id: str,
    *,
    top_k: int = DEFAULT_TOP_K,
) -> list[dict[str, Any]]:
    """Convenience wrapper around ``compute_for_listings`` for a single id."""
    return compute_for_listings(db_path, [listing_id], top_k=top_k).get(listing_id, [])
