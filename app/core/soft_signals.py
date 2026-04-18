"""Turn a ``SoftPreferences`` object into per-signal rankings for RRF fusion.

Each activated preference produces one ranking (best-first list of
``listing_id`` strings). Listings whose relevant signal value is ``NULL`` are
omitted from that specific ranking - missing signals must never tail-rank a
listing (their absence is treated as "no information", not "worst").

Reads ``listings_ranking_signals`` directly (not via
``ranking.runtime.signals_reader``) because the soft-signal channel uses the
migration-added columns that the ``SignalRow`` dataclass does not carry:
30 ``dist_landmark_<key>_m``, ``nearest_stop_lines_count_clamped``,
``price_plausibility`` and 8 ``commute_proxy_<city>_min``.
"""
from __future__ import annotations

import math
import sqlite3
from pathlib import Path
from typing import Any, Callable, Iterable

from app.core import landmarks
from app.models.schemas import SoftPreferences


SoftKey = str   # "cheap" / "quiet" / "near_schools" / "near_ETH" etc.


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _load_signal_rows(
    db_path: Path, listing_ids: list[str]
) -> dict[str, sqlite3.Row]:
    """Return ``{listing_id: Row}`` for every candidate with a signals row."""
    if not listing_ids:
        return {}
    out: dict[str, sqlite3.Row] = {}
    try:
        conn = _connect(db_path)
    except sqlite3.Error as exc:
        print(
            f"[WARN] soft_signals._load_signal_rows: expected=db connection, "
            f"got={type(exc).__name__}: {exc}, fallback=empty",
            flush=True,
        )
        return {}
    try:
        # SQLite's default parameter limit is 999; chunk to stay well under it.
        CHUNK = 800
        for i in range(0, len(listing_ids), CHUNK):
            chunk = listing_ids[i : i + CHUNK]
            placeholders = ", ".join("?" for _ in chunk)
            sql = (
                f"SELECT * FROM listings_ranking_signals "
                f"WHERE listing_id IN ({placeholders})"
            )
            try:
                for row in conn.execute(sql, chunk):
                    out[row["listing_id"]] = row
            except sqlite3.OperationalError as exc:
                print(
                    f"[WARN] soft_signals._load_signal_rows: "
                    f"expected=listings_ranking_signals table, "
                    f"got={exc}, fallback=empty rankings",
                    flush=True,
                )
                return {}
    finally:
        conn.close()
    return out


def _rank_by(
    listing_ids: list[str],
    rows: dict[str, sqlite3.Row],
    key: Callable[[sqlite3.Row], float | None],
    *,
    descending: bool,
    drop_listing: Callable[[sqlite3.Row], bool] | None = None,
) -> list[str]:
    """Build a best-first ranking. Listings with no row, NULL key value, or
    any ``drop_listing`` vote are excluded entirely from this ranking.
    """
    scored: list[tuple[float, str]] = []
    for listing_id in listing_ids:
        row = rows.get(listing_id)
        if row is None:
            continue
        if drop_listing is not None and drop_listing(row):
            continue
        value = key(row)
        if value is None:
            continue
        scored.append((float(value), listing_id))
    scored.sort(key=lambda pair: -pair[0] if descending else pair[0])
    return [lid for _, lid in scored]


def _sum_optional(*values: float | None) -> float | None:
    """Sum of non-None values, or None when all inputs are None."""
    collected = [v for v in values if v is not None]
    return float(sum(collected)) if collected else None


def _safe_row_get(row: sqlite3.Row, name: str) -> Any:
    """Like ``row[name]`` but returns ``None`` when the column does not exist."""
    try:
        return row[name]
    except (IndexError, KeyError):
        return None


def _cheap_key(row: sqlite3.Row) -> float | None:
    """Prefer the canton-rooms bucket delta, fall back to the PLZ-rooms one."""
    delta = _safe_row_get(row, "price_delta_pct_canton_rooms")
    if delta is None:
        delta = _safe_row_get(row, "price_delta_pct_plz_rooms")
    return float(delta) if delta is not None else None


def _is_suspect(row: sqlite3.Row) -> bool:
    return _safe_row_get(row, "price_plausibility") == "suspect"


def _near_pt_composite(row: sqlite3.Row) -> float | None:
    """Closer stop is better, frequent service better. Negative distance so
    that higher = better lets the caller use descending order.
    """
    dist = _safe_row_get(row, "dist_nearest_stop_m")
    lines = _safe_row_get(row, "nearest_stop_lines_count_clamped")
    if dist is None and lines is None:
        return None
    dist_score = -(float(dist) / 1000.0) if dist is not None else 0.0
    lines_score = math.log1p(float(lines)) if lines is not None else 0.0
    return dist_score + lines_score


def _composite_desc(
    row: sqlite3.Row, cols: Iterable[str]
) -> float | None:
    vals = [_safe_row_get(row, c) for c in cols]
    return _sum_optional(*vals)


def build_soft_rankings(
    candidates: list[dict[str, Any]],
    soft: SoftPreferences | None,
    db_path: Path,
) -> list[list[str]]:
    """Return one best-first ranking per activated soft preference.

    Never mutates ``candidates`` and never raises on missing tables / columns:
    silent-disable is forbidden, so every unusable branch emits a ``[WARN]``
    and is skipped.
    """
    if soft is None or not candidates:
        return []

    listing_ids = [str(c["listing_id"]) for c in candidates]
    rows = _load_signal_rows(db_path, listing_ids)
    if not rows:
        # No data at all; nothing to rank by.
        return []

    rankings: list[list[str]] = []

    # --- price_sentiment ---------------------------------------------------
    if soft.price_sentiment in ("cheap", "premium"):
        descending = soft.price_sentiment == "premium"
        rankings.append(_rank_by(
            listing_ids, rows,
            key=_cheap_key,
            descending=descending,
            drop_listing=_is_suspect,
        ))

    # --- quiet -------------------------------------------------------------
    if soft.quiet:
        rankings.append(_rank_by(
            listing_ids, rows,
            key=lambda r: _sum_optional(
                _safe_row_get(r, "dist_motorway_m"),
                _safe_row_get(r, "dist_primary_road_m"),
            ),
            descending=True,
        ))

    # --- transit + commute -------------------------------------------------
    if soft.near_public_transport:
        rankings.append(_rank_by(
            listing_ids, rows,
            key=_near_pt_composite,
            descending=True,
        ))

    if soft.commute_target:
        short = soft.commute_target.removesuffix("_hb")
        col = f"commute_proxy_{short}_min"
        rankings.append(_rank_by(
            listing_ids, rows,
            key=lambda r: _safe_row_get(r, col),
            descending=False,  # lower commute is better
        ))

    # --- POI preferences ---------------------------------------------------
    if soft.near_schools:
        rankings.append(_rank_by(
            listing_ids, rows,
            key=lambda r: _safe_row_get(r, "poi_school_1km"),
            descending=True,
        ))
    if soft.near_supermarket:
        rankings.append(_rank_by(
            listing_ids, rows,
            key=lambda r: _safe_row_get(r, "poi_supermarket_300m"),
            descending=True,
        ))
    if soft.near_park:
        rankings.append(_rank_by(
            listing_ids, rows,
            key=lambda r: _safe_row_get(r, "poi_park_500m"),
            descending=True,
        ))
    if soft.family_friendly:
        rankings.append(_rank_by(
            listing_ids, rows,
            key=lambda r: _composite_desc(
                r, ("poi_playground_500m", "poi_kindergarten_500m"),
            ),
            descending=True,
        ))

    # --- landmarks ---------------------------------------------------------
    for name in soft.near_landmark or []:
        lm = landmarks.resolve(name)
        if lm is None:
            print(
                f"[WARN] soft_signals.build_soft_rankings: "
                f"landmark {name!r} not resolved, "
                f"expected=key or alias in landmarks.json, "
                f"fallback=skip",
                flush=True,
            )
            continue
        col = landmarks.column_for(lm.key)
        rankings.append(_rank_by(
            listing_ids, rows,
            key=lambda r, col=col: _safe_row_get(r, col),
            descending=False,
        ))

    return rankings
