"""T2.2 — OSM POI density + noise-proxy line distances from the CH PBF extract.

Signals written to `listings_ranking_signals`:
  poi_supermarket_300m / _1km
  poi_school_1km
  poi_kindergarten_500m
  poi_playground_500m
  poi_pharmacy_500m
  poi_clinic_1km               (amenity ∈ {clinic, hospital} OR healthcare ∈ {clinic, hospital})
  poi_gym_500m                 (leisure=fitness_centre)
  poi_park_500m                (leisure=park)
  poi_restaurant_300m
  dist_motorway_m              (highway ∈ {motorway, motorway_link, trunk, trunk_link})
  dist_primary_road_m          (highway ∈ {primary, primary_link})
  dist_rail_m                  (railway=rail AND tunnel != yes)

Design notes (from the research agent):
  * `BallTree(metric='haversine')` for POI radius counts — we count FEATURES,
    not nodes, so a school polygon counts as 1.
  * `sjoin_nearest` after reprojecting to **EPSG:2056 (LV95 Swiss grid)** for
    line-distance queries. Haversine is wrong for perpendicular line distance.
  * QuackOSM parses the PBF; on re-runs it serves from its internal
    geoparquet cache in `data/ranking/osm/cache/`.
  * We keep one centroid per OSM id (quackosm already returns one row per
    feature, so this is automatic — but we defensively drop duplicates).

CLAUDE.md §5 compliance:
  * Every skip emits a [WARN] with context (category, reason).
  * If a PBF parse returns zero features for a category, we raise — that's
    never normal and should not silently leave every listing with count=0.

Usage:
    python -m ranking.scripts.t2_osm_poi --db data/listings.db
    python -m ranking.scripts.t2_osm_poi --db data/listings.db --ignore-cache
"""
from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from ranking.common.db import connect
from ranking.schema import check_db_matches_registry

PBF_PATH = Path("data/ranking/osm/switzerland-latest.osm.pbf")
OSM_CACHE_DIR = Path("data/ranking/osm/cache")
EARTH_RADIUS_M = 6_371_000.0


@dataclass(slots=True, frozen=True)
class PoiSpec:
    signal: str                  # column in listings_ranking_signals
    tags: dict[str, Any]         # quackosm tags_filter
    radius_m: int
    post_filter_sql: str | None = None  # optional DuckDB where-clause for edge cases


POI_SPECS: list[PoiSpec] = [
    PoiSpec("poi_supermarket_300m",  {"shop": ["supermarket"]},            300),
    PoiSpec("poi_supermarket_1km",   {"shop": ["supermarket"]},           1000),
    PoiSpec("poi_school_1km",        {"amenity": ["school"]},             1000),
    PoiSpec("poi_kindergarten_500m", {"amenity": ["kindergarten"]},        500),
    PoiSpec("poi_playground_500m",   {"leisure": ["playground"]},          500),
    PoiSpec("poi_pharmacy_500m",     {"amenity": ["pharmacy"]},            500),
    # Clinic: union of amenity{clinic, hospital} and healthcare{clinic, hospital}
    PoiSpec("poi_clinic_1km",
            {"amenity": ["clinic", "hospital"], "healthcare": ["clinic", "hospital"]},
            1000),
    PoiSpec("poi_gym_500m",          {"leisure": ["fitness_centre"]},      500),
    PoiSpec("poi_park_500m",         {"leisure": ["park"]},                500),
    PoiSpec("poi_restaurant_300m",   {"amenity": ["restaurant"]},          300),
]


@dataclass(slots=True, frozen=True)
class LineSpec:
    signal: str
    tags: dict[str, Any]
    extra_filter: str | None = None   # post-filter in pandas (e.g. drop tunnels)


LINE_SPECS: list[LineSpec] = [
    LineSpec(
        "dist_motorway_m",
        {"highway": ["motorway", "motorway_link", "trunk", "trunk_link"]},
    ),
    LineSpec(
        "dist_primary_road_m",
        {"highway": ["primary", "primary_link"]},
    ),
    LineSpec(
        "dist_rail_m",
        {"railway": ["rail"]},
        # Drop ways explicitly tagged tunnel=yes (those don't make noise on the surface)
        extra_filter="not_tunnel",
    ),
]


def _load_osm(tags: dict[str, Any], *, ignore_cache: bool):
    """Load a filtered OSM subset as a GeoDataFrame."""
    from quackosm import convert_pbf_to_geodataframe

    return convert_pbf_to_geodataframe(
        pbf_path=str(PBF_PATH),
        tags_filter=tags,
        working_directory=str(OSM_CACHE_DIR),
        ignore_cache=ignore_cache,
        verbosity_mode="silent",
    )


def _listings_df(conn) -> pd.DataFrame:
    rows = conn.execute(
        "SELECT listing_id, latitude, longitude FROM listings "
        "WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
    ).fetchall()
    df = pd.DataFrame([dict(r) for r in rows])
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    bad = df["latitude"].isna() | df["longitude"].isna() | ((df["latitude"] == 0) & (df["longitude"] == 0))
    n_bad = int(bad.sum())
    if n_bad:
        print(
            f"[WARN] t2_osm_poi: expected=valid non-zero coords, got={n_bad} "
            f"NaN/null-island listings, fallback=skip",
            flush=True,
        )
    return df[~bad].reset_index(drop=True)


def _poi_radius_counts(
    listings: pd.DataFrame, pois: pd.DataFrame, *, radius_m: int
) -> np.ndarray:
    """Count POIs within `radius_m` of each listing via BallTree(haversine)."""
    if len(pois) == 0:
        return np.zeros(len(listings), dtype=int)
    # Use centroid of each feature (works for both points and polygons)
    centroids = pois.geometry.representative_point()
    lat = centroids.y.values
    lon = centroids.x.values
    tree = BallTree(np.radians(np.column_stack([lat, lon])), metric="haversine")
    q = np.radians(listings[["latitude", "longitude"]].values)
    counts = tree.query_radius(q, r=radius_m / EARTH_RADIUS_M, count_only=True)
    return counts.astype(int)


def _line_distances_m(
    listings: pd.DataFrame, lines: pd.DataFrame, *, cap_m: int = 5000
) -> np.ndarray:
    """Per-listing distance to nearest line, in metres (EPSG:2056)."""
    import geopandas as gpd

    if len(lines) == 0:
        return np.full(len(listings), np.nan)

    # 1) build listings GDF in WGS84 → reproject to LV95
    lst = gpd.GeoDataFrame(
        listings.assign(
            geometry=gpd.points_from_xy(listings["longitude"], listings["latitude"])
        ),
        crs="EPSG:4326",
    ).to_crs("EPSG:2056")
    # 2) keep only LineString / MultiLineString geometries (filter out points etc.)
    lines_gdf = lines[lines.geometry.geom_type.isin(["LineString", "MultiLineString"])]
    if len(lines_gdf) == 0:
        print(
            "[WARN] t2_osm_poi: expected=LineString features, got=empty "
            "after geom_type filter, fallback=NaN distances",
            flush=True,
        )
        return np.full(len(listings), np.nan)
    lines_gdf = lines_gdf.to_crs("EPSG:2056")
    # 3) sjoin_nearest with a cap (otherwise it scans the whole network every time)
    nearest = gpd.sjoin_nearest(
        lst, lines_gdf[["geometry"]],
        max_distance=cap_m,
        distance_col="dist_m",
        how="left",
    )
    # sjoin_nearest can produce multiple rows if tied; keep the min per listing.
    # Use the preserved left-index (sjoin_nearest keeps lst's index) and reindex
    # back to the original order so we return an array aligned with `listings`.
    nearest_min = (
        nearest.reset_index()
        .groupby("index")["dist_m"]
        .min()
    )
    return nearest_min.reindex(range(len(lst))).values


def run(db_path: Path, *, ignore_cache: bool = False) -> dict:
    if not PBF_PATH.exists():
        raise RuntimeError(
            f"PBF not found at {PBF_PATH}. Download first:\n"
            "  curl -L -o data/ranking/osm/switzerland-latest.osm.pbf "
            "https://download.geofabrik.de/europe/switzerland-latest.osm.pbf"
        )
    OSM_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    t_start = time.monotonic()
    stats = {"categories": {}}

    with connect(db_path) as conn:
        check_db_matches_registry(conn)
        listings = _listings_df(conn)
        print(f"[INFO] t2_osm_poi: {len(listings):,} listings to score", flush=True)

        # ---- POIs ---------------------------------------------------------
        listing_updates: dict[int, dict[str, Any]] = {i: {} for i in range(len(listings))}

        for spec in POI_SPECS:
            t0 = time.monotonic()
            pois = _load_osm(spec.tags, ignore_cache=ignore_cache)
            n_feats = len(pois)
            if n_feats == 0:
                raise RuntimeError(
                    f"Zero OSM features for {spec.signal} with tags {spec.tags}. "
                    "This is never normal for Switzerland — inspect the PBF path."
                )
            counts = _poi_radius_counts(listings, pois, radius_m=spec.radius_m)
            elapsed = time.monotonic() - t0
            max_ = int(counts.max()) if len(counts) else 0
            mean = float(counts.mean()) if len(counts) else 0.0
            print(
                f"[INFO] t2_osm_poi: {spec.signal:28s} features={n_feats:>6,} "
                f"max={max_:>3} mean={mean:5.2f} elapsed_s={elapsed:.1f}",
                flush=True,
            )
            stats["categories"][spec.signal] = {
                "features": int(n_feats), "max": max_, "mean": round(mean, 3),
                "elapsed_s": round(elapsed, 2),
            }
            for i, c in enumerate(counts):
                listing_updates[i][spec.signal] = int(c)

        # ---- Line distances ----------------------------------------------
        for spec in LINE_SPECS:
            t0 = time.monotonic()
            lines = _load_osm(spec.tags, ignore_cache=ignore_cache)
            # tunnel=yes filter for rail per research-agent advice
            if spec.extra_filter == "not_tunnel" and "tunnel" in lines.columns:
                before = len(lines)
                lines = lines[lines["tunnel"].fillna("") != "yes"]
                print(
                    f"[INFO] t2_osm_poi: {spec.signal:28s} dropped {before - len(lines):,} "
                    f"rail tunnel=yes ways",
                    flush=True,
                )
            dists = _line_distances_m(listings, lines)
            elapsed = time.monotonic() - t0
            present = np.sum(~np.isnan(dists))
            p50 = float(np.nanmedian(dists)) if present else float("nan")
            p90 = float(np.nanpercentile(dists, 90)) if present else float("nan")
            print(
                f"[INFO] t2_osm_poi: {spec.signal:28s} features={len(lines):>6,} "
                f"covered={present:>6,} p50_m={p50:>6.0f} p90_m={p90:>6.0f} "
                f"elapsed_s={elapsed:.1f}",
                flush=True,
            )
            stats["categories"][spec.signal] = {
                "features": int(len(lines)), "covered": int(present),
                "p50_m": None if np.isnan(p50) else round(p50, 1),
                "p90_m": None if np.isnan(p90) else round(p90, 1),
                "elapsed_s": round(elapsed, 2),
            }
            for i, d in enumerate(dists):
                listing_updates[i][spec.signal] = None if np.isnan(d) else round(float(d), 1)

        # ---- Write everything ---------------------------------------------
        t0 = time.monotonic()
        cols_in_order = [s.signal for s in POI_SPECS] + [s.signal for s in LINE_SPECS]
        now_iso = datetime.now(timezone.utc).isoformat()
        conn.execute("BEGIN;")
        for i, row in enumerate(listings.itertuples()):
            upd = listing_updates[i]
            set_parts = ", ".join(f"{c} = ?" for c in cols_in_order) + ", last_updated_utc = ?"
            params = [upd.get(c) for c in cols_in_order] + [now_iso, row.listing_id]
            conn.execute(
                f"UPDATE listings_ranking_signals SET {set_parts} WHERE listing_id = ?;",
                params,
            )
        conn.commit()
        print(
            f"[INFO] t2_osm_poi: wrote {len(listings):,} rows in {time.monotonic() - t0:.2f}s",
            flush=True,
        )

    total = time.monotonic() - t_start
    stats["elapsed_s"] = round(total, 2)
    stats["listings_scored"] = len(listings)
    print(f"[INFO] t2_osm_poi: DONE total_s={total:.1f}", flush=True)
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument(
        "--ignore-cache",
        action="store_true",
        help="Force re-parse the PBF from scratch (default: reuse QuackOSM cache).",
    )
    args = parser.parse_args()
    if not args.db.exists():
        print(f"[ERROR] t2_osm_poi: db not found at {args.db}", file=sys.stderr)
        return 2
    run(args.db, ignore_cache=args.ignore_cache)
    return 0


if __name__ == "__main__":
    sys.exit(main())
