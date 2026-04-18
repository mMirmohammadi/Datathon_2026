"""Migrate the teammate-shipped ``data/listings.db`` into the app's schema.

The teammate bundle ships three tables (``listings`` raw / ``listings_enriched``
null-fill / ``listings_ranking_signals``) but the ``listings`` table lacks the
5 columns the harness bootstrap requires (``year_built``, ``object_category_raw``,
``house_number``, ``city_slug``, ``floor``) and ``listings_fts`` does not exist.

This migration is the single place that closes that gap. Also populates:
- 30 ``dist_landmark_<key>_m`` columns on ``listings_ranking_signals`` from
  ``data/ranking/landmarks.json`` + listing lat/lon via vectorised haversine.
- ``nearest_stop_lines_count_clamped`` capping outliers at 100.
- ``price_plausibility`` flagging abs(delta) > 3.
- 8 ``commute_proxy_<city>_min`` scalars (``<city>`` in zurich / bern / basel
  / geneve / lausanne / lucerne / winterthur / stgallen).

The migration is idempotent: every ALTER / INSERT is gated by an existence
check so a second run is a no-op. Writes to the live DB in place; no .bak
swap (SQLite ALTER TABLE ADD COLUMN is atomic and the fills are a single
transaction, so a crash leaves the DB in one of the two valid states).
"""
from __future__ import annotations

import json
import math
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np

from app.core.normalize import slug, split_street, translate_object_category


# ---- constants ----------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LANDMARKS = REPO_ROOT / "data" / "ranking" / "landmarks.json"

# Five listings columns the app bootstrap's _schema_matches() requires.
LISTINGS_NEW_COLS: list[tuple[str, str]] = [
    ("year_built", "INTEGER"),
    ("object_category_raw", "TEXT"),
    ("house_number", "TEXT"),
    ("city_slug", "TEXT"),
    ("floor", "INTEGER"),
]

# Main-station commute proxies. Maps the teammate gazetteer's landmark key
# (``hb_<city>``, the column the migration populates) to the short city name
# used in the ``commute_proxy_<city>_min`` column and in the
# :class:`SoftPreferences.commute_target` enum suffix ``<city>_hb``.
HB_KEYS: dict[str, str] = {
    "hb_zurich": "zurich",
    "hb_bern": "bern",
    "hb_basel": "basel",
    "hb_geneve": "geneve",
    "hb_lausanne": "lausanne",
    "hb_lugano": "lugano",
    "hb_winterthur": "winterthur",
    "hb_st_gallen": "st_gallen",
}

# Data-quality extras added to listings_ranking_signals.
QUALITY_COLS: list[tuple[str, str]] = [
    ("nearest_stop_lines_count_clamped", "INTEGER"),
    ("price_plausibility", "TEXT"),
]


# ---- helpers ------------------------------------------------------------


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _add_col_if_missing(conn: sqlite3.Connection, table: str, name: str, sql_type: str) -> bool:
    if name in _columns(conn, table):
        return False
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {sql_type}")
    return True


def _safe_int(value: Any) -> int | None:
    """Coerce ``listings_enriched._filled`` strings to int; preserve NULL for
    the literal ``"UNKNOWN"`` sentinel so CAST does not silently turn it into 0.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.upper() == "UNKNOWN":
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _haversine_km_vec(
    lat1: np.ndarray, lon1: np.ndarray, lat2: float, lon2: float
) -> np.ndarray:
    r = 6371.0088
    phi1 = np.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlam = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2.0) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlam / 2.0) ** 2
    return 2.0 * r * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))


def _load_landmarks(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        print(
            f"[WARN] migrate: landmarks not found at {path}, "
            f"expected=ranking/landmarks.json, fallback=skip landmark columns",
            flush=True,
        )
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise RuntimeError(f"landmarks file has unexpected shape: {type(data)}")
    return data


# ---- migration steps ----------------------------------------------------


def _validate_source_tables(conn: sqlite3.Connection) -> dict[str, int]:
    counts: dict[str, int] = {}
    for t in ("listings", "listings_enriched", "listings_ranking_signals"):
        if not _table_exists(conn, t):
            raise RuntimeError(
                f"migrate: required table {t!r} missing; cannot migrate. "
                "Install the bundle first via scripts/install_dataset.py."
            )
        counts[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    return counts


def _migrate_listings_columns(conn: sqlite3.Connection) -> dict[str, int]:
    added = []
    for name, sql_type in LISTINGS_NEW_COLS:
        if _add_col_if_missing(conn, "listings", name, sql_type):
            added.append(name)

    # Populate from listings_enriched for every listing.
    rows = list(conn.execute(
        """
        SELECT l.listing_id,
               l.street,
               l.object_category,
               e.year_built_filled, e.year_built_source,
               e.floor_filled,      e.floor_source,
               e.city_filled,       e.city_source
          FROM listings l
          JOIN listings_enriched e USING (listing_id)
        """
    ))

    updates: list[tuple] = []
    for r in rows:
        (listing_id, street_raw, object_category_raw,
         year_built_filled, year_built_source,
         floor_filled, floor_source,
         city_filled, city_source) = r

        year_built = (
            _safe_int(year_built_filled)
            if (year_built_source or "") != "UNKNOWN" else None
        )
        floor = (
            _safe_int(floor_filled)
            if (floor_source or "") != "UNKNOWN" else None
        )
        _street_stem, house_number = split_street(street_raw)
        # city_slug: prefer the enriched native form, but skip UNKNOWN sentinels
        source_city = None if (city_source or "") == "UNKNOWN" else city_filled
        city_slug = slug(source_city) if source_city else None

        updates.append((
            year_built, object_category_raw, house_number, city_slug, floor,
            listing_id,
        ))

    conn.executemany(
        """
        UPDATE listings
           SET year_built = ?,
               object_category_raw = ?,
               house_number = ?,
               city_slug = ?,
               floor = ?
         WHERE listing_id = ?
        """,
        updates,
    )

    # Also translate object_category in-place to the English canonical enum
    # while preserving the German in object_category_raw (copied above).
    translations: list[tuple[str | None, str]] = []
    for lid, raw in conn.execute(
        "SELECT listing_id, object_category_raw FROM listings"
    ):
        translations.append((translate_object_category(raw), lid))
    conn.executemany(
        "UPDATE listings SET object_category = ? WHERE listing_id = ?",
        translations,
    )

    return {"columns_added": len(added), "rows_updated": len(updates)}


def _migrate_ranking_signal_columns(
    conn: sqlite3.Connection,
    landmarks: list[dict[str, Any]],
) -> dict[str, int]:
    added_landmark_cols = 0
    for lm in landmarks:
        col = f"dist_landmark_{lm['key']}_m"
        if _add_col_if_missing(conn, "listings_ranking_signals", col, "REAL"):
            added_landmark_cols += 1

    added_quality = 0
    for name, sql_type in QUALITY_COLS:
        if _add_col_if_missing(conn, "listings_ranking_signals", name, sql_type):
            added_quality += 1
    added_commute = 0
    for hb_key in HB_KEYS.values():
        col = f"commute_proxy_{hb_key}_min"
        if _add_col_if_missing(conn, "listings_ranking_signals", col, "REAL"):
            added_commute += 1

    return {
        "landmark_cols_added": added_landmark_cols,
        "quality_cols_added": added_quality,
        "commute_cols_added": added_commute,
    }


def _populate_landmark_distances(
    conn: sqlite3.Connection, landmarks: list[dict[str, Any]]
) -> int:
    if not landmarks:
        return 0

    rows = list(conn.execute(
        "SELECT listing_id, latitude, longitude FROM listings"
    ))
    listing_ids = [r[0] for r in rows]
    lats = np.array([r[1] if r[1] is not None else np.nan for r in rows], dtype=np.float64)
    lons = np.array([r[2] if r[2] is not None else np.nan for r in rows], dtype=np.float64)

    total_updates = 0
    for lm in landmarks:
        col = f"dist_landmark_{lm['key']}_m"
        d_km = _haversine_km_vec(lats, lons, float(lm["lat"]), float(lm["lon"]))
        d_m = d_km * 1000.0
        # Rows with NULL lat/lon get NaN -> write NULL for those.
        payload: list[tuple] = []
        for lid, value in zip(listing_ids, d_m):
            payload.append(
                (float(value) if np.isfinite(value) else None, lid)
            )
        conn.executemany(
            f"UPDATE listings_ranking_signals SET {col} = ? WHERE listing_id = ?",
            payload,
        )
        total_updates += len(payload)
    return total_updates


def _populate_quality_columns(conn: sqlite3.Connection) -> dict[str, int]:
    # Clamp nearest_stop_lines_count to [0, 100].
    conn.execute(
        """
        UPDATE listings_ranking_signals
           SET nearest_stop_lines_count_clamped = CASE
               WHEN nearest_stop_lines_count IS NULL THEN NULL
               WHEN nearest_stop_lines_count > 100 THEN 100
               WHEN nearest_stop_lines_count < 0 THEN 0
               ELSE nearest_stop_lines_count
           END
        """
    )
    clamped_rows = conn.execute(
        "SELECT COUNT(*) FROM listings_ranking_signals "
        "WHERE nearest_stop_lines_count_clamped IS NOT NULL"
    ).fetchone()[0]

    # Plausibility: 'suspect' when |delta| > 3 (300% off the bucket median).
    conn.execute(
        """
        UPDATE listings_ranking_signals
           SET price_plausibility = CASE
               WHEN price_delta_pct_canton_rooms IS NULL THEN NULL
               WHEN ABS(price_delta_pct_canton_rooms) > 3 THEN 'suspect'
               ELSE 'normal'
           END
        """
    )
    suspect_rows = conn.execute(
        "SELECT COUNT(*) FROM listings_ranking_signals WHERE price_plausibility = 'suspect'"
    ).fetchone()[0]

    # commute proxy: dist_nearest_stop_m / 80 (walk min to the stop) +
    # dist_landmark_hb_<city>_m / 1000 (rough train-leg min at ~60 km/h).
    for landmark_key, short in HB_KEYS.items():
        target_col = f"dist_landmark_{landmark_key}_m"
        proxy_col = f"commute_proxy_{short}_min"
        if target_col not in _columns(conn, "listings_ranking_signals"):
            continue
        conn.execute(
            f"""
            UPDATE listings_ranking_signals
               SET {proxy_col} = CASE
                   WHEN dist_nearest_stop_m IS NULL OR {target_col} IS NULL THEN NULL
                   ELSE (dist_nearest_stop_m / 80.0) + ({target_col} / 1000.0)
               END
            """
        )

    return {"clamped": clamped_rows, "suspect": suspect_rows}


def _rebuild_fts(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS listings_fts")
    conn.execute(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS listings_fts USING fts5(
            title, description, street, city, object_category_raw,
            content='listings',
            content_rowid='rowid',
            tokenize='unicode61 remove_diacritics 2'
        )
        """
    )
    conn.execute("INSERT INTO listings_fts(listings_fts) VALUES('rebuild')")


def _create_indexes(conn: sqlite3.Connection) -> None:
    for sql in (
        "CREATE INDEX IF NOT EXISTS idx_listings_city_slug  ON listings(city_slug)",
        "CREATE INDEX IF NOT EXISTS idx_listings_floor      ON listings(floor)",
        "CREATE INDEX IF NOT EXISTS idx_listings_year_built ON listings(year_built)",
        "CREATE INDEX IF NOT EXISTS idx_listings_price      ON listings(price)",
        "CREATE INDEX IF NOT EXISTS idx_listings_rooms      ON listings(rooms)",
        "CREATE INDEX IF NOT EXISTS idx_listings_area       ON listings(area)",
        "CREATE INDEX IF NOT EXISTS idx_listings_canton     ON listings(canton)",
        "CREATE INDEX IF NOT EXISTS idx_listings_postal     ON listings(postal_code)",
        "CREATE INDEX IF NOT EXISTS idx_listings_offer      ON listings(offer_type)",
        "CREATE INDEX IF NOT EXISTS idx_listings_category   ON listings(object_category)",
        "CREATE INDEX IF NOT EXISTS idx_listings_available  ON listings(available_from)",
        "CREATE INDEX IF NOT EXISTS idx_listings_platform   ON listings(platform_id)",
    ):
        conn.execute(sql)


# ---- entry point --------------------------------------------------------


def migrate(db_path: Path, landmarks_path: Path = DEFAULT_LANDMARKS) -> dict[str, Any]:
    """Run the full migration in one transaction. Returns a report dict."""
    if not db_path.exists():
        raise FileNotFoundError(f"migrate: db not found at {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        counts = _validate_source_tables(conn)
        landmarks = _load_landmarks(landmarks_path)

        with conn:
            listings_report = _migrate_listings_columns(conn)
            signals_report = _migrate_ranking_signal_columns(conn, landmarks)
            landmark_updates = _populate_landmark_distances(conn, landmarks)
            quality_report = _populate_quality_columns(conn)
            _rebuild_fts(conn)
            _create_indexes(conn)

        return {
            "source_counts": counts,
            "listings": listings_report,
            "ranking_signals": signals_report,
            "landmark_updates": landmark_updates,
            "quality": quality_report,
            "landmarks_count": len(landmarks),
        }
    finally:
        conn.close()


def _main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", type=Path, default=REPO_ROOT / "data" / "listings.db")
    ap.add_argument("--landmarks", type=Path, default=DEFAULT_LANDMARKS)
    args = ap.parse_args()

    report = migrate(args.db, args.landmarks)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
