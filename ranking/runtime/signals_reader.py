"""Query-time reader for `listings_ranking_signals`.

One-shot SELECT that attaches all tier-1 + tier-2 signals to a batch of
candidate listing_ids. Used by the ranker to compute geo_fit / price_fit /
poi_fit / noise_penalty signals in its linear blend.

Contract:
  * `load_signals(db_path, listing_ids)` → `dict[listing_id → SignalRow]`.
  * Listings missing from `listings_ranking_signals` (shouldn't happen
    normally — the table is seeded per listing_id) → value is None.
  * NULLs on individual columns are preserved; the ranker interprets them
    per column (usually: skip-weighting).
  * We DO NOT raise on missing table — we emit a [WARN] and return {} so
    the ranker keeps working with just the MVP signals.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from ranking.common.db import connect, table_exists
from ranking.schema import signal_names


@dataclass(slots=True)
class SignalRow:
    # Price
    price_baseline_canton:        float | None
    price_baseline_plz:            float | None
    price_delta_pct_canton:        float | None
    price_delta_pct_plz:           float | None
    price_baseline_n_canton:       int | None
    price_baseline_n_plz:          int | None
    # Transit
    dist_nearest_stop_m:           float | None
    nearest_stop_name:             str | None
    nearest_stop_id:               str | None
    nearest_stop_type:             str | None
    nearest_stop_lines_count:      int | None
    # POI
    poi_supermarket_300m:          int | None
    poi_supermarket_1km:           int | None
    poi_school_1km:                int | None
    poi_kindergarten_500m:         int | None
    poi_playground_500m:           int | None
    poi_pharmacy_500m:             int | None
    poi_clinic_1km:                int | None
    poi_gym_500m:                  int | None
    poi_park_500m:                 int | None
    poi_restaurant_300m:           int | None
    dist_motorway_m:               float | None
    dist_primary_road_m:           float | None
    dist_rail_m:                   float | None
    # Embedding-metadata
    embedding_row_index:           int | None
    embedding_model:               str | None
    embedding_doc_hash:            str | None


# Order MUST match the SignalRow field order below.
_SIGNAL_COLUMNS = [
    "price_baseline_chf_canton_rooms",
    "price_baseline_chf_plz_rooms",
    "price_delta_pct_canton_rooms",
    "price_delta_pct_plz_rooms",
    "price_baseline_n_canton_rooms",
    "price_baseline_n_plz_rooms",
    "dist_nearest_stop_m",
    "nearest_stop_name",
    "nearest_stop_id",
    "nearest_stop_type",
    "nearest_stop_lines_count",
    "poi_supermarket_300m",
    "poi_supermarket_1km",
    "poi_school_1km",
    "poi_kindergarten_500m",
    "poi_playground_500m",
    "poi_pharmacy_500m",
    "poi_clinic_1km",
    "poi_gym_500m",
    "poi_park_500m",
    "poi_restaurant_300m",
    "dist_motorway_m",
    "dist_primary_road_m",
    "dist_rail_m",
    "embedding_row_index",
    "embedding_model",
    "embedding_doc_hash",
]


def _validate_columns_against_registry():
    registry = set(signal_names())
    for c in _SIGNAL_COLUMNS:
        assert c in registry, f"signals_reader column not in schema.SIGNALS: {c}"


_validate_columns_against_registry()


def load_signals(db_path: Path, listing_ids: list[str]) -> dict[str, SignalRow]:
    """Return {listing_id: SignalRow} for every id that has a row. Missing ids
    are absent from the dict (callers should treat absence as "no ranking
    signals available — use MVP fallback").
    """
    if not listing_ids:
        return {}
    out: dict[str, SignalRow] = {}
    try:
        conn = connect(db_path)
    except sqlite3.Error as exc:
        print(
            f"[WARN] signals_reader.load_signals: expected=db connection, "
            f"got={type(exc).__name__}: {exc}, fallback=empty dict",
            flush=True,
        )
        return {}
    try:
        if not table_exists(conn, "listings_ranking_signals"):
            print(
                "[WARN] signals_reader.load_signals: expected=listings_ranking_signals table, "
                "got=missing, fallback=empty dict (run ranking/scripts/t1_create_table.py)",
                flush=True,
            )
            return {}
        cols_sql = ", ".join(_SIGNAL_COLUMNS)
        # Chunk to respect SQLite's 999-variable limit
        CHUNK = 800
        for i in range(0, len(listing_ids), CHUNK):
            chunk = listing_ids[i : i + CHUNK]
            placeholders = ",".join(["?"] * len(chunk))
            sql = (
                f"SELECT listing_id, {cols_sql} FROM listings_ranking_signals "
                f"WHERE listing_id IN ({placeholders})"
            )
            for row in conn.execute(sql, chunk):
                out[row["listing_id"]] = SignalRow(*[row[c] for c in _SIGNAL_COLUMNS])
    finally:
        conn.close()
    return out
