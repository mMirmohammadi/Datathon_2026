"""T2.1 — GTFS nearest-stop computation via BallTree(haversine).

Writes five columns to `listings_ranking_signals` per listing:
  * dist_nearest_stop_m
  * nearest_stop_name
  * nearest_stop_id                (parent_station if available; else the stop itself)
  * nearest_stop_type              (train|tram|bus|ferry|gondola|funicular|subway)
  * nearest_stop_lines_count       (# distinct route_ids serving that stop)

Design decisions (per the research agent + GTFS profile for Switzerland):
  * We collapse `location_type=0` child platforms onto their `parent_station`
    so Zürich HB shows up once, not as 17 neighbouring platforms.
  * We restrict to `location_type IN (0, 1)` — other types (entrances, etc.)
    aren't search endpoints.
  * route_type is computed per stop by joining stop_times → trips → routes;
    the *most common* type at that stop wins (a stop served by both S-Bahn
    and a bus shows as 'train' because rail has the bigger route_type bucket).
  * lines_count = distinct route_ids with a trip stopping at this stop.

Per CLAUDE.md §5:
  * Every listing with NULL or out-of-range coords → nearest_stop fields
    stay NULL + [WARN] log line per batch summarising the skip count.
  * If the GTFS feed is missing or malformed we fail loudly with a RuntimeError
    rather than silently writing NULLs to all 25k rows.

Usage:
    python -m ranking.scripts.t2_gtfs_nearest --db data/listings.db
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from ranking.common.db import connect
from ranking.schema import check_db_matches_registry

GTFS_DIR = Path("data/ranking/gtfs")
EARTH_RADIUS_M = 6_371_000.0

# GTFS route_type → human label (GTFS spec + Swiss extensions).
ROUTE_TYPE_LABEL = {
    0: "tram", 1: "subway", 2: "train", 3: "bus", 4: "ferry",
    5: "cable_car", 6: "gondola", 7: "funicular", 11: "trolleybus",
    12: "monorail",
}


def _load_stops(gtfs_dir: Path) -> pd.DataFrame:
    """Return a de-duplicated stop frame keyed on `stop_id` we'll search on.

    Rule: if a stop has a `parent_station`, we use the parent's coords and
    name (so Zürich HB's 17 child platforms collapse to one search anchor).
    """
    stops = pd.read_csv(
        gtfs_dir / "stops.txt",
        dtype=str,
        usecols=["stop_id","stop_name","stop_lat","stop_lon","location_type","parent_station"],
    )
    # Coerce types
    stops["lat"] = pd.to_numeric(stops["stop_lat"], errors="coerce")
    stops["lon"] = pd.to_numeric(stops["stop_lon"], errors="coerce")
    stops["location_type"] = stops["location_type"].fillna("0")

    # Keep only searchable stops (0 = platform, 1 = station)
    stops = stops[stops["location_type"].isin(["0", "1"])].copy()

    # Map child stops to their parent_station. If the parent is in the feed
    # and has coords, replace the child's coords with the parent's.
    parents = stops[stops["location_type"] == "1"][["stop_id","stop_name","lat","lon"]]
    parents = parents.rename(columns={
        "stop_id": "parent_station",
        "stop_name": "parent_name",
        "lat": "parent_lat",
        "lon": "parent_lon",
    })
    stops = stops.merge(parents, on="parent_station", how="left")

    # Resolve the canonical search row:
    #   - if it IS a station (location_type=1): keep as is
    #   - if it's a platform with a parent: use the parent's coords/name, key by parent_id
    #   - if it's a platform with no parent: keep as standalone
    platforms_with_parent = (
        (stops["location_type"] == "0") & stops["parent_station"].notna() & stops["parent_lat"].notna()
    )
    stops.loc[platforms_with_parent, "stop_id"] = stops.loc[platforms_with_parent, "parent_station"]
    stops.loc[platforms_with_parent, "stop_name"] = stops.loc[platforms_with_parent, "parent_name"]
    stops.loc[platforms_with_parent, "lat"] = stops.loc[platforms_with_parent, "parent_lat"]
    stops.loc[platforms_with_parent, "lon"] = stops.loc[platforms_with_parent, "parent_lon"]

    # De-dup by the (possibly-re-pointed) stop_id + drop coord-less rows
    stops = stops.dropna(subset=["lat", "lon"])
    stops = stops.drop_duplicates(subset=["stop_id"]).reset_index(drop=True)

    # Filter to Swiss stops by ID prefix — the OTD feed includes cross-border
    # stops in DE/AT/FR/IT/LI whose IDs start with other didok ranges.
    # Swiss didok numbers start with 85; LI stops start with 36 and are
    # legitimate "near" stops for listings in St.Margrethen etc.
    # We keep 85 (CH) + 36 (LI) + 0 (generic; often CH ad-hoc stops)
    # and drop the rest. Anything with a colon (':') is a platform indicator.
    keep_prefixes = ("85", "36", "0")
    stops["_base"] = stops["stop_id"].str.split(":").str[0]
    stops = stops[stops["_base"].str.startswith(keep_prefixes)].copy()

    return stops[["stop_id","stop_name","lat","lon"]].reset_index(drop=True)


def _compute_lines_and_types(gtfs_dir: Path, stop_ids: set[str]) -> pd.DataFrame:
    """Return DataFrame [stop_id, lines_count, top_route_type_label].

    stop_times.txt is ~812 MB. We stream it to avoid a 3 GB RAM spike, pulling
    only (trip_id, stop_id) per row and aggregating as we go.
    """
    t0 = time.monotonic()
    # routes.txt → {route_id: route_type}
    routes = pd.read_csv(
        gtfs_dir / "routes.txt",
        dtype=str,
        usecols=["route_id", "route_type"],
    )
    routes["route_type_int"] = pd.to_numeric(routes["route_type"], errors="coerce").astype("Int64")
    route_type_map = dict(zip(routes["route_id"], routes["route_type_int"]))

    # trips.txt → {trip_id: route_id}
    trips = pd.read_csv(
        gtfs_dir / "trips.txt",
        dtype=str,
        usecols=["trip_id", "route_id"],
    )
    trip_to_route = dict(zip(trips["trip_id"], trips["route_id"]))
    print(
        f"[INFO] t2_gtfs_nearest: routes={len(routes):,} trips={len(trips):,} "
        f"loaded in {time.monotonic() - t0:.1f}s",
        flush=True,
    )

    # Stream stop_times in chunks; per (stop_id, route_id), keep one row
    t0 = time.monotonic()
    stop_routes: dict[str, set[str]] = {}
    # For "top route type" we count trips per (stop_id, route_type) and keep the max
    stop_route_type_count: dict[str, dict[int, int]] = {}
    chunksize = 2_000_000
    total = 0
    for chunk in pd.read_csv(
        gtfs_dir / "stop_times.txt",
        dtype=str,
        usecols=["trip_id", "stop_id"],
        chunksize=chunksize,
    ):
        # Collapse platform-level stop_ids to their "base" id so counts
        # align with the collapsed stops set (e.g. 8503000:0:12 → 8503000)
        chunk["stop_id_base"] = chunk["stop_id"].str.split(":").str[0]
        # We only care about stops we actually searched. Also map trip → route
        chunk = chunk[chunk["stop_id_base"].isin(stop_ids)]
        chunk["route_id"] = chunk["trip_id"].map(trip_to_route)
        chunk = chunk.dropna(subset=["route_id"])
        chunk["route_type"] = chunk["route_id"].map(route_type_map).astype("Int64")

        for sid, grp in chunk.groupby("stop_id_base"):
            s = stop_routes.setdefault(sid, set())
            s.update(grp["route_id"].unique())
            rt_counts = stop_route_type_count.setdefault(sid, {})
            for rt, cnt in grp["route_type"].value_counts().items():
                if pd.notna(rt):
                    rt_counts[int(rt)] = rt_counts.get(int(rt), 0) + int(cnt)
        total += len(chunk)
    print(
        f"[INFO] t2_gtfs_nearest: stop_times aggregated ({total:,} relevant rows) "
        f"in {time.monotonic() - t0:.1f}s",
        flush=True,
    )

    rows = []
    for sid in stop_ids:
        lines = len(stop_routes.get(sid, set()))
        rt_counts = stop_route_type_count.get(sid, {})
        if rt_counts:
            top_rt = max(rt_counts, key=rt_counts.get)
            label = ROUTE_TYPE_LABEL.get(top_rt, f"route_type={top_rt}")
        else:
            label = None
        rows.append({"stop_id": sid, "lines_count": lines, "stop_type": label})
    return pd.DataFrame(rows)


def run(db_path: Path) -> dict:
    if not GTFS_DIR.exists():
        raise RuntimeError(
            f"GTFS not found at {GTFS_DIR}. Download first:\n"
            "  curl -L -o data/ranking/gtfs/gtfs_complete.zip https://gtfs.geops.ch/dl/gtfs_complete.zip\n"
            "  cd data/ranking/gtfs && unzip -o gtfs_complete.zip"
        )

    t_start = time.monotonic()
    print(f"[INFO] t2_gtfs_nearest: loading GTFS from {GTFS_DIR}", flush=True)
    stops = _load_stops(GTFS_DIR)
    print(
        f"[INFO] t2_gtfs_nearest: after filter+parent-collapse: {len(stops):,} searchable stops",
        flush=True,
    )

    # Lines / route-type aggregation
    stop_meta = _compute_lines_and_types(GTFS_DIR, set(stops["stop_id"]))
    stops = stops.merge(stop_meta, on="stop_id", how="left")
    stops["lines_count"] = stops["lines_count"].fillna(0).astype(int)

    # Build BallTree
    t0 = time.monotonic()
    tree = BallTree(np.radians(stops[["lat", "lon"]].values), metric="haversine")
    print(
        f"[INFO] t2_gtfs_nearest: BallTree built in {time.monotonic() - t0:.2f}s "
        f"({len(stops):,} stops)",
        flush=True,
    )

    # Query every listing's nearest stop
    with connect(db_path) as conn:
        check_db_matches_registry(conn)

        rows = conn.execute(
            "SELECT listing_id, latitude, longitude FROM listings "
            "WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
        ).fetchall()
        listings = pd.DataFrame([dict(r) for r in rows])
        listings["latitude"] = pd.to_numeric(listings["latitude"], errors="coerce")
        listings["longitude"] = pd.to_numeric(listings["longitude"], errors="coerce")
        bad = listings["latitude"].isna() | listings["longitude"].isna()
        if bad.any():
            print(
                f"[WARN] t2_gtfs_nearest: expected=valid coords, "
                f"got={bad.sum()} NaN after coercion, fallback=skip those listings",
                flush=True,
            )
            listings = listings[~bad].reset_index(drop=True)

        # Also drop the 9 null-island rows per data audit
        bad = (listings["latitude"] == 0) & (listings["longitude"] == 0)
        if bad.any():
            print(
                f"[WARN] t2_gtfs_nearest: expected=non-zero coords, got={bad.sum()} "
                f"(lat=0, lon=0) null-island, fallback=skip",
                flush=True,
            )
            listings = listings[~bad].reset_index(drop=True)

        t0 = time.monotonic()
        d_rad, idx = tree.query(
            np.radians(listings[["latitude", "longitude"]].values), k=1
        )
        listings["dist_m"] = d_rad[:, 0] * EARTH_RADIUS_M
        listings["stop_id"]   = stops.loc[idx[:, 0], "stop_id"].values
        listings["stop_name"] = stops.loc[idx[:, 0], "stop_name"].values
        listings["stop_type"] = stops.loc[idx[:, 0], "stop_type"].values
        listings["lines_count"] = stops.loc[idx[:, 0], "lines_count"].values
        print(
            f"[INFO] t2_gtfs_nearest: queried {len(listings):,} nearest stops "
            f"in {time.monotonic() - t0:.2f}s",
            flush=True,
        )

        # Write back
        t0 = time.monotonic()
        now_iso = datetime.now(timezone.utc).isoformat()
        conn.execute("BEGIN;")
        for r in listings.itertuples():
            conn.execute(
                """
                UPDATE listings_ranking_signals SET
                    dist_nearest_stop_m      = ?,
                    nearest_stop_id          = ?,
                    nearest_stop_name        = ?,
                    nearest_stop_type        = ?,
                    nearest_stop_lines_count = ?,
                    last_updated_utc         = ?
                WHERE listing_id = ?;
                """,
                (
                    round(float(r.dist_m), 1),
                    str(r.stop_id),
                    str(r.stop_name),
                    None if pd.isna(r.stop_type) else str(r.stop_type),
                    int(r.lines_count),
                    now_iso,
                    r.listing_id,
                ),
            )
        conn.commit()
        print(
            f"[INFO] t2_gtfs_nearest: wrote {len(listings):,} rows "
            f"in {time.monotonic() - t0:.2f}s",
            flush=True,
        )

    total = time.monotonic() - t_start
    print(f"[INFO] t2_gtfs_nearest: DONE total_s={total:.1f}", flush=True)
    return {
        "stops_total":     len(stops),
        "listings_scored": len(listings),
        "elapsed_s":       round(total, 2),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    args = parser.parse_args()
    if not args.db.exists():
        print(f"[ERROR] t2_gtfs_nearest: db not found at {args.db}", file=sys.stderr)
        return 2
    run(args.db)
    return 0


if __name__ == "__main__":
    sys.exit(main())
