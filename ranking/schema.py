"""Single source of truth for the `listings_ranking_signals` side-table.

This table is populated by Tier 1 + Tier 2 scripts under `ranking/scripts/`
and consumed by the ranker at query time. It is strictly separate from the
harness-owned `listings` table and from `listings_enriched` (the null-fill
output).

Design mirrors `enrichment/schema.py`:
  * A single module-level `SIGNALS` list declares every column.
  * `validate_signals()` runs at import time — duplicate names, bad types,
    missing raw-source labels all `raise RuntimeError`.
  * `create_table_sql()` generates `CREATE TABLE IF NOT EXISTS …`.
  * Every writer runs a drift check before writing: if the DB has a column
    not in the registry OR the registry has a column missing from the DB,
    they `raise` instead of silently sentinel-filling.

Every column is NULLABLE — unlike the null-fill table, a missing signal here
means "we haven't computed it yet" rather than "truly unknown". The ranker
decides how to interpret NULL (usually: skip the signal for that listing).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

SignalKind = Literal["price", "geo", "poi", "embedding_meta", "commute"]
SignalType = Literal["INTEGER", "REAL", "TEXT"]


@dataclass(frozen=True, slots=True)
class RankingSignal:
    name: str
    sql_type: SignalType
    kind: SignalKind
    description: str                      # one-line human description
    produced_by: str                      # script path that writes it
    default_null_ok: bool = True          # True = NULL is an acceptable state


# Keep columns grouped by tier for readability. Downstream code treats them
# as a flat list — the grouping is purely cosmetic in this module.

SIGNALS: list[RankingSignal] = [
    # --- Tier 1.2 price baselines ------------------------------------------
    RankingSignal(
        "price_baseline_chf_canton_rooms", "REAL", "price",
        "Median rent (CHF) for this canton × rooms bucket. NULL if bucket<5 rows.",
        "ranking/scripts/t1_price_baselines.py",
    ),
    RankingSignal(
        "price_baseline_chf_plz_rooms", "REAL", "price",
        "Median rent (CHF) for this PLZ-prefix × rooms bucket. NULL if bucket<5 rows.",
        "ranking/scripts/t1_price_baselines.py",
    ),
    RankingSignal(
        "price_delta_pct_canton_rooms", "REAL", "price",
        "(price - canton_rooms_baseline) / canton_rooms_baseline. NULL if baseline NULL.",
        "ranking/scripts/t1_price_baselines.py",
    ),
    RankingSignal(
        "price_delta_pct_plz_rooms", "REAL", "price",
        "(price - plz_rooms_baseline) / plz_rooms_baseline. NULL if baseline NULL.",
        "ranking/scripts/t1_price_baselines.py",
    ),
    RankingSignal(
        "price_baseline_n_canton_rooms", "INTEGER", "price",
        "Sample size of the canton×rooms bucket this listing compares against.",
        "ranking/scripts/t1_price_baselines.py",
    ),
    RankingSignal(
        "price_baseline_n_plz_rooms", "INTEGER", "price",
        "Sample size of the PLZ×rooms bucket this listing compares against.",
        "ranking/scripts/t1_price_baselines.py",
    ),

    # --- Tier 2.1 GTFS nearest-stop ----------------------------------------
    RankingSignal(
        "dist_nearest_stop_m", "REAL", "geo",
        "Haversine metres to the nearest GTFS stop (SBB/PostBus/ZVV/trams).",
        "ranking/scripts/t2_gtfs_nearest.py",
    ),
    RankingSignal(
        "nearest_stop_name", "TEXT", "geo",
        "GTFS stop_name of the nearest stop (for audit / explanations).",
        "ranking/scripts/t2_gtfs_nearest.py",
    ),
    RankingSignal(
        "nearest_stop_id", "TEXT", "geo",
        "GTFS stop_id (or parent_station) of the nearest stop.",
        "ranking/scripts/t2_gtfs_nearest.py",
    ),
    RankingSignal(
        "nearest_stop_type", "TEXT", "geo",
        "Primary route_type served: train|tram|bus|ferry|gondola|funicular.",
        "ranking/scripts/t2_gtfs_nearest.py",
    ),
    RankingSignal(
        "nearest_stop_lines_count", "INTEGER", "geo",
        "Number of distinct routes serving the nearest stop.",
        "ranking/scripts/t2_gtfs_nearest.py",
    ),

    # --- Tier 2.2 OSM POI density ------------------------------------------
    # Paired radii per POI category. Kept as plain counts; the ranker normalises.
    RankingSignal("poi_supermarket_300m",  "INTEGER", "poi",
                  "OSM count of shop=supermarket within 300 m.",
                  "ranking/scripts/t2_osm_poi.py"),
    RankingSignal("poi_supermarket_1km",   "INTEGER", "poi",
                  "OSM count of shop=supermarket within 1 km.",
                  "ranking/scripts/t2_osm_poi.py"),
    RankingSignal("poi_school_1km",        "INTEGER", "poi",
                  "OSM count of amenity=school within 1 km.",
                  "ranking/scripts/t2_osm_poi.py"),
    RankingSignal("poi_kindergarten_500m", "INTEGER", "poi",
                  "OSM count of amenity=kindergarten within 500 m.",
                  "ranking/scripts/t2_osm_poi.py"),
    RankingSignal("poi_playground_500m",   "INTEGER", "poi",
                  "OSM count of leisure=playground within 500 m.",
                  "ranking/scripts/t2_osm_poi.py"),
    RankingSignal("poi_pharmacy_500m",     "INTEGER", "poi",
                  "OSM count of amenity=pharmacy within 500 m.",
                  "ranking/scripts/t2_osm_poi.py"),
    RankingSignal("poi_clinic_1km",        "INTEGER", "poi",
                  "OSM count of {amenity,healthcare}={clinic,hospital} within 1 km.",
                  "ranking/scripts/t2_osm_poi.py"),
    RankingSignal("poi_gym_500m",          "INTEGER", "poi",
                  "OSM count of leisure=fitness_centre within 500 m.",
                  "ranking/scripts/t2_osm_poi.py"),
    RankingSignal("poi_park_500m",         "INTEGER", "poi",
                  "OSM count of leisure=park within 500 m.",
                  "ranking/scripts/t2_osm_poi.py"),
    RankingSignal("poi_restaurant_300m",   "INTEGER", "poi",
                  "OSM count of amenity=restaurant within 300 m.",
                  "ranking/scripts/t2_osm_poi.py"),
    RankingSignal("dist_motorway_m",       "REAL",    "poi",
                  "Metres (EPSG:2056) to nearest motorway/trunk way. Noise-proxy.",
                  "ranking/scripts/t2_osm_poi.py"),
    RankingSignal("dist_primary_road_m",   "REAL",    "poi",
                  "Metres (EPSG:2056) to nearest primary road. Noise-proxy.",
                  "ranking/scripts/t2_osm_poi.py"),
    RankingSignal("dist_rail_m",           "REAL",    "poi",
                  "Metres (EPSG:2056) to nearest surface rail line (tunnel excluded). Noise-proxy.",
                  "ranking/scripts/t2_osm_poi.py"),

    # --- Tier 3.1 embedding-metadata (actual vectors live in .npy) --------
    RankingSignal(
        "embedding_row_index", "INTEGER", "embedding_meta",
        "Row index into data/ranking/embeddings.fp16.npy. NULL if never embedded.",
        "ranking/scripts/t3_embed_listings.py",
    ),
    RankingSignal(
        "embedding_model", "TEXT", "embedding_meta",
        "HF model id used. Pinned at write time for reproducibility.",
        "ranking/scripts/t3_embed_listings.py",
    ),
    RankingSignal(
        "embedding_doc_hash", "TEXT", "embedding_meta",
        "SHA-256 over the text that was embedded; lets us detect stale vectors.",
        "ranking/scripts/t3_embed_listings.py",
    ),

    # --- Bookkeeping --------------------------------------------------------
    RankingSignal(
        "last_updated_utc", "TEXT", "price",  # arbitrary kind; bookkeeping
        "UTC ISO timestamp of the last script that touched this row.",
        "ranking/scripts/*",
    ),
]


def validate_signals() -> None:
    """Fail loud at import time if the registry has obvious bugs."""
    names = [s.name for s in SIGNALS]
    if len(names) != len(set(names)):
        dupes = sorted({n for n in names if names.count(n) > 1})
        raise RuntimeError(f"Duplicate signal names in SIGNALS: {dupes}")
    for s in SIGNALS:
        if not s.name or not s.name.isidentifier():
            raise RuntimeError(f"Invalid signal name: {s.name!r}")
        if s.sql_type not in ("INTEGER", "REAL", "TEXT"):
            raise RuntimeError(f"Invalid sql_type for {s.name}: {s.sql_type!r}")


validate_signals()


def create_table_sql() -> str:
    cols: list[str] = ["listing_id TEXT PRIMARY KEY"]
    for s in SIGNALS:
        cols.append(f"{s.name} {s.sql_type}")
    cols.append("FOREIGN KEY (listing_id) REFERENCES listings(listing_id)")
    body = ",\n    ".join(cols)
    return f"CREATE TABLE IF NOT EXISTS listings_ranking_signals (\n    {body}\n);"


INDEX_SQL: list[str] = [
    # Ranker typically filters on these; keep them light-weight.
    "CREATE INDEX IF NOT EXISTS idx_lrs_dist_station ON listings_ranking_signals(dist_nearest_stop_m);",
    "CREATE INDEX IF NOT EXISTS idx_lrs_price_delta  ON listings_ranking_signals(price_delta_pct_canton_rooms);",
]


def signal_names() -> list[str]:
    """Every column name in registry order (excluding the PK)."""
    return [s.name for s in SIGNALS]


def check_db_matches_registry(conn) -> None:
    """Reject silently-divergent schemas.

    Must be called by every writer before any UPDATE.
    """
    rows = conn.execute("PRAGMA table_info(listings_ranking_signals);").fetchall()
    db_cols = {r[1] for r in rows}
    registry_cols = {"listing_id", *signal_names()}
    extra_in_db = db_cols - registry_cols
    missing_in_db = registry_cols - db_cols
    if extra_in_db:
        raise RuntimeError(
            f"Registry drift: DB has columns not in SIGNALS: {sorted(extra_in_db)}. "
            "Either add them to ranking/schema.py or drop + recreate the table."
        )
    if missing_in_db:
        raise RuntimeError(
            f"Registry drift: SIGNALS has columns not in DB: {sorted(missing_in_db)}. "
            "Run: python -m ranking.scripts.t1_create_table --db data/listings.db"
        )
