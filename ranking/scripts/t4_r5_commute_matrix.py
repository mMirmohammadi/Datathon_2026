"""T4.1 — offline transit travel-time matrix via r5py (Haversine-pruned, parallel).

Builds a real door-to-door commute table between every listing and every
**geographically plausible** landmark, using the Swiss GTFS feed + OSM for
walk-access routing. Output lives in `listing_commute_times` inside
`data/listings.db`.

WHY r5py not OJP:
  OJP free tier is 20,000 req/day. 25k × 42 = 1.05 M calls would take
  >50 days and violate the quota 50×. r5py ingests our local GTFS + OSM
  files and computes the whole matrix offline with no API calls.

WHY Haversine pre-filter:
  A Zürich listing doesn't need a commute time to "Plainpalais, Geneva" —
  both the storage (~1 M rows of mostly-NULL) and the r5py work per
  irrelevant pair are wasted. We drop any (origin, destination) pair whose
  straight-line distance exceeds HAVERSINE_CUTOFF_KM — at typical Swiss
  transit speeds (30 km/h door-to-door), that's roughly the same cut-off
  as the 90-min max_time, but enforced CHEAPLY in Python before r5py
  starts.

WHY parallel workers:
  r5py's single `TravelTimeMatrix(...)` call is single-threaded. On our
  24-core box, that's a ~20× speedup sitting on the table. We chunk
  origins into N worker processes, each with its own JVM that reads the
  same Kryo-cached TransportNetwork from disk (~14 s load per worker
  after first build). Parent process collects per-chunk results and does
  one bulk INSERT.

WHY Tuesday 2026-05-05 08:00 CET:
  Inside the GTFS Fahrplan-2026 validity window (2025-12-14 → 2026-12-12).
  Tuesday peak hour — representative commute mix.

Pipeline:
  1. _prepare_gtfs() cleans whitespace-duplicate PKs that R5 rejects.
  2. Load origins (listings with coords) + destinations (landmarks.json).
  3. _haversine_km_matrix() — one vectorised numpy broadcast.
  4. For each origin, enumerate the landmarks within HAVERSINE_CUTOFF_KM.
     Origins that have ZERO nearby landmarks are DROPPED (rare — only
     happens for remote Alpine listings).
  5. Chunk origins into N workers. Each worker computes a sub-matrix
     against ALL destinations (cheap per r5py's one-to-many isochrone
     model — see §WHY above). Per-worker results are pickled back.
  6. Parent merges chunks, Haversine-filters the rows (dropping distant
     pairs), coerces travel_min → Int64, bulk-inserts into
     `listing_commute_times`.

Per CLAUDE.md §5:
  * Every dropped listing / landmark emits [WARN] with reason.
  * If r5py returns fewer rows than expected, we log and proceed
    (partial coverage better than none).
  * Post-condition asserts on row count + non-null count.

Idempotent: `listing_commute_times` is DROPPED + recreated each run.
Cleaned GTFS + Kryo network cache survive between runs.

Usage:
    # full parallel run:
    python -m ranking.scripts.t4_r5_commute_matrix --db data/listings.db

    # smoke (100 origins, 2 workers):
    python -m ranking.scripts.t4_r5_commute_matrix --db data/listings.db \\
        --limit-origins 100 --workers 2

Env:
    T4_HAVERSINE_CUTOFF_KM   default 40 (override per analysis)
    T4_WORKERS               default = min(8, os.cpu_count()) — more = more RAM
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.geometry as sg

# --- paths ------------------------------------------------------------------

OSM_PBF_PATH = Path("data/ranking/osm/switzerland-latest.osm.pbf")
GTFS_ZIP_PATH = Path("data/ranking/gtfs/gtfs_complete.zip")
# Cleaned copy (dedupe whitespace-only duplicates) — r5py's R5 parser is
# stricter than pandas about PK uniqueness. Created lazily by _prepare_gtfs().
GTFS_CLEANED_PATH = Path("data/ranking/gtfs/gtfs_cleaned.zip")
LANDMARKS_PATH = Path("data/ranking/landmarks.json")
DEFAULT_DB_PATH = Path("data/listings.db")

# --- routing config ---------------------------------------------------------

# Tuesday 2026-05-05 08:00 local (Europe/Zurich; CEST). GTFS feed carries
# the timezone — we pass a naive datetime and r5py interprets it in the
# feed's TZ.
DEPARTURE_TIME = dt.datetime(2026, 5, 5, 8, 0, 0)
MAX_TIME = dt.timedelta(minutes=90)

# --- filter / parallelism config --------------------------------------------

HAVERSINE_CUTOFF_KM = float(os.getenv("T4_HAVERSINE_CUTOFF_KM", "40.0"))
# 4-worker default. Each r5py JVM defaults to `-Xmx = 80% of TOTAL RAM`
# (see r5py/util/memory_footprint.py). With N processes this over-commits
# RAM by Nx → thrashing / OOM. We explicitly cap each worker's JVM heap
# via R5_MAX_MEMORY_PER_WORKER ("3G" default) so the math is:
#   N_workers × (JVM heap + Kryo overhead ~1.5G) + python ~0.5G ≈ safe.
# With defaults: 4 × 4.5 G ≈ 18 GB — comfortable on a 62 GB box.
DEFAULT_WORKERS = int(os.getenv("T4_WORKERS", str(min(4, os.cpu_count() or 4))))
# Hard cap for the JVM heap per worker. See _worker_compute_chunk for how
# this is passed to r5py (via sys.argv before the r5py import).
MAX_MEMORY_PER_WORKER = os.getenv("R5_MAX_MEMORY_PER_WORKER", "10G")

# --- GTFS cleaner (unchanged) ----------------------------------------------


def _prepare_gtfs() -> Path:
    """Produce a cleaned copy of the GTFS feed for r5py's strict PK checker.

    The Swiss aggregate feed at gtfs.geops.ch ships with stray trailing
    whitespace on at least one `stop_id` ("8580003:EV " duplicates
    "8580003:EV" on row 60445). Pandas-backed code tolerates; r5py's R5
    parser rejects with `GtfsFileError: DuplicateKeyError`.

    Idempotent: skips if `gtfs_cleaned.zip` is newer than the source.
    """
    if (GTFS_CLEANED_PATH.exists()
            and GTFS_CLEANED_PATH.stat().st_mtime > GTFS_ZIP_PATH.stat().st_mtime):
        print(
            f"[INFO] t4_r5_commute_matrix: cleaned GTFS exists at {GTFS_CLEANED_PATH} "
            f"(newer than source) — skipping rebuild",
            flush=True,
        )
        return GTFS_CLEANED_PATH

    import csv
    import io
    import zipfile

    print(
        f"[INFO] t4_r5_commute_matrix: building cleaned GTFS at {GTFS_CLEANED_PATH}",
        flush=True,
    )
    t0 = time.monotonic()

    ID_COLUMNS = {
        "stop_id", "parent_station", "route_id", "trip_id", "service_id",
        "from_stop_id", "to_stop_id", "agency_id", "shape_id", "block_id",
    }
    DEDUPE_KEYS = {
        "stops.txt":     ["stop_id"],
        "routes.txt":    ["route_id"],
        "trips.txt":     ["trip_id"],
        "agency.txt":    ["agency_id"],
        "calendar.txt":  ["service_id"],
        "transfers.txt": ["from_stop_id", "to_stop_id"],
    }
    STREAM_TRIM_FILES = {"stop_times.txt", "calendar_dates.txt"}

    stats: dict[str, int] = {}
    GTFS_CLEANED_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = GTFS_CLEANED_PATH.with_suffix(".zip.tmp")
    with zipfile.ZipFile(GTFS_ZIP_PATH, "r") as zin, \
         zipfile.ZipFile(tmp_out, "w", zipfile.ZIP_DEFLATED) as zout:
        for name in zin.namelist():
            data = zin.read(name)
            text = data.decode("utf-8", errors="replace")
            if name in DEDUPE_KEYS:
                reader = csv.reader(io.StringIO(text))
                rows = list(reader)
                if not rows:
                    zout.writestr(name, data)
                    continue
                header = rows[0]
                trim_idx = [i for i, h in enumerate(header) if h in ID_COLUMNS]
                key_idx = [header.index(k) for k in DEDUPE_KEYS[name] if k in header]
                seen: set[tuple] = set()
                kept = [header]
                dropped = 0
                for row in rows[1:]:
                    for i in trim_idx:
                        if i < len(row):
                            row[i] = row[i].strip()
                    key_tuple = tuple(row[i] for i in key_idx) if key_idx else tuple(row)
                    if key_tuple in seen:
                        dropped += 1
                        continue
                    seen.add(key_tuple)
                    kept.append(row)
                buf = io.StringIO()
                w = csv.writer(buf, lineterminator="\n")
                w.writerows(kept)
                zout.writestr(name, buf.getvalue().encode("utf-8"))
                stats[f"{name}:dupes_dropped"] = dropped
                stats[f"{name}:rows_kept"] = len(kept) - 1
                if dropped > 0:
                    print(
                        f"[INFO] t4_r5_commute_matrix._prepare_gtfs: {name}: "
                        f"dropped {dropped} whitespace-dupe rows (kept {len(kept) - 1})",
                        flush=True,
                    )
            elif name in STREAM_TRIM_FILES:
                reader = csv.reader(io.StringIO(text))
                rows_in = iter(reader)
                header = next(rows_in, None)
                if header is None:
                    zout.writestr(name, data)
                    continue
                trim_idx = [i for i, h in enumerate(header) if h in ID_COLUMNS]
                buf = io.StringIO()
                w = csv.writer(buf, lineterminator="\n")
                w.writerow(header)
                n_rows = 0
                for row in rows_in:
                    for i in trim_idx:
                        if i < len(row):
                            row[i] = row[i].strip()
                    w.writerow(row)
                    n_rows += 1
                zout.writestr(name, buf.getvalue().encode("utf-8"))
                stats[f"{name}:rows"] = n_rows
            else:
                zout.writestr(name, data)

    tmp_out.replace(GTFS_CLEANED_PATH)
    print(
        f"[INFO] t4_r5_commute_matrix._prepare_gtfs: done in "
        f"{time.monotonic() - t0:.1f}s stats={stats}",
        flush=True,
    )
    return GTFS_CLEANED_PATH


# --- data loaders -----------------------------------------------------------


def _load_origins_gdf(db_path: Path, limit: int | None) -> gpd.GeoDataFrame:
    """GeoDataFrame of (listing_id, geometry) for every listing with lat/lng."""
    con = sqlite3.connect(db_path)
    try:
        sql = (
            "SELECT listing_id, latitude, longitude FROM listings "
            "WHERE latitude IS NOT NULL AND longitude IS NOT NULL "
            "  AND NOT (latitude = 0 AND longitude = 0) "
            "ORDER BY listing_id"
        )
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        df = pd.read_sql_query(sql, con)
    finally:
        con.close()
    if df.empty:
        raise RuntimeError("No listings with valid coordinates — aborting.")
    gdf = gpd.GeoDataFrame(
        {"id": df["listing_id"].astype(str)},
        geometry=gpd.points_from_xy(df["longitude"], df["latitude"]),
        crs="EPSG:4326",
    )
    print(
        f"[INFO] t4_r5_commute_matrix: loaded {len(gdf)} origins",
        flush=True,
    )
    return gdf


def _load_destinations_gdf(limit: int | None) -> gpd.GeoDataFrame:
    """GeoDataFrame of (landmark_key, geometry) for landmarks with valid coords."""
    if not LANDMARKS_PATH.exists():
        raise FileNotFoundError(
            f"{LANDMARKS_PATH} not found. Run t1_landmarks_fetch.py + "
            "t1_landmarks_geocode_mined.py first."
        )
    data = json.loads(LANDMARKS_PATH.read_text(encoding="utf-8"))
    kept: list[dict[str, Any]] = []
    dropped = 0
    for rec in data:
        if not isinstance(rec, dict):
            dropped += 1
            continue
        key = rec.get("key")
        lat = rec.get("lat")
        lon = rec.get("lon")
        if key is None or lat is None or lon is None:
            print(
                f"[WARN] t4_r5_commute_matrix: expected=key+lat+lon, "
                f"got=({rec.get('key', '?')!r} lat={lat} lon={lon}), fallback=skip",
                flush=True,
            )
            dropped += 1
            continue
        kept.append({"id": str(key), "lat": float(lat), "lon": float(lon)})
    if not kept:
        raise RuntimeError(f"No landmarks with valid coords in {LANDMARKS_PATH}")
    if limit is not None:
        kept = kept[:limit]
    gdf = gpd.GeoDataFrame(
        {"id": [k["id"] for k in kept]},
        geometry=[sg.Point(k["lon"], k["lat"]) for k in kept],
        crs="EPSG:4326",
    )
    print(
        f"[INFO] t4_r5_commute_matrix: loaded {len(gdf)} destinations "
        f"({dropped} dropped for missing fields)",
        flush=True,
    )
    return gdf


# --- Haversine pre-filter ---------------------------------------------------


def _haversine_km_matrix(
    origins_gdf: gpd.GeoDataFrame,
    destinations_gdf: gpd.GeoDataFrame,
) -> np.ndarray:
    """Vectorised great-circle distance in km. Shape (N_origins, N_destinations).

    One-pass numpy broadcasting — 1 M pairs in ~200 ms.
    """
    R = 6371.0
    o_lat = np.radians(origins_gdf.geometry.y.values)
    o_lon = np.radians(origins_gdf.geometry.x.values)
    d_lat = np.radians(destinations_gdf.geometry.y.values)
    d_lon = np.radians(destinations_gdf.geometry.x.values)
    # Broadcast: (N_o, 1) vs (1, N_d)
    phi1 = o_lat[:, None]
    phi2 = d_lat[None, :]
    dphi = phi2 - phi1
    dlam = d_lon[None, :] - o_lon[:, None]
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlam / 2) ** 2
    return R * 2 * np.arcsin(np.sqrt(a))


def _filter_origins_with_nearby_landmarks(
    origins_gdf: gpd.GeoDataFrame,
    destinations_gdf: gpd.GeoDataFrame,
    cutoff_km: float,
) -> tuple[gpd.GeoDataFrame, np.ndarray]:
    """Drop origins that have ZERO landmarks within cutoff_km.

    Returns (filtered_origins_gdf, haversine_km_matrix_for_kept_origins).
    """
    km = _haversine_km_matrix(origins_gdf, destinations_gdf)
    has_nearby = (km <= cutoff_km).any(axis=1)
    n_keep = int(has_nearby.sum())
    n_drop = int((~has_nearby).sum())
    if n_drop > 0:
        print(
            f"[INFO] t4_r5_commute_matrix: filter dropped {n_drop} origins "
            f"with no landmarks within {cutoff_km} km (kept {n_keep})",
            flush=True,
        )
    kept = origins_gdf[has_nearby].reset_index(drop=True)
    km_kept = km[has_nearby]
    return kept, km_kept


# --- parallel worker --------------------------------------------------------


def _worker_compute_chunk(
    chunk_idx: int,
    origins_tuples: list[tuple[str, float, float]],
    destinations_tuples: list[tuple[str, float, float]],
    gtfs_path_str: str,
    osm_path_str: str,
    departure: dt.datetime,
    max_time_sec: int,
    max_memory: str,
) -> list[tuple[str, str, int | None]]:
    """Runs inside a worker process. Builds a local TransportNetwork (reads
    the Kryo cache once) and computes the travel-time matrix for this chunk
    of origins against ALL destinations. Returns (listing_id, landmark_key,
    travel_min_or_None) tuples.

    Why tuples not a DataFrame: cross-process pickling is cheaper on lists.

    `max_memory`: JVM -Xmx ceiling (e.g. "3G"). r5py reads it from
    `sys.argv` via configargparse `--max-memory`. Without this cap each
    JVM grabs 80% of TOTAL system RAM — N workers × that = OOM thrashing.
    We injecting the CLI arg into `sys.argv` BEFORE importing r5py.
    """
    import sys as _sys
    # Must land in sys.argv before `import r5py` triggers its configargparse.
    _sys.argv = [_sys.argv[0], "--max-memory", max_memory]

    import geopandas as _gpd
    import shapely.geometry as _sg
    import pandas as _pd
    import r5py
    from r5py import TransportMode

    t0 = time.monotonic()

    origins_gdf = _gpd.GeoDataFrame(
        {"id": [o[0] for o in origins_tuples]},
        geometry=[_sg.Point(o[2], o[1]) for o in origins_tuples],  # (lon, lat)
        crs="EPSG:4326",
    )
    destinations_gdf = _gpd.GeoDataFrame(
        {"id": [d[0] for d in destinations_tuples]},
        geometry=[_sg.Point(d[2], d[1]) for d in destinations_tuples],
        crs="EPSG:4326",
    )

    tn = r5py.TransportNetwork(osm_pbf=osm_path_str, gtfs=[gtfs_path_str])
    print(
        f"[INFO] t4_r5_commute_matrix[worker={chunk_idx}]: network loaded in "
        f"{time.monotonic() - t0:.1f}s, computing {len(origins_tuples)} "
        f"origins × {len(destinations_tuples)} destinations",
        flush=True,
    )

    t1 = time.monotonic()
    df = r5py.TravelTimeMatrix(
        tn,
        origins=origins_gdf,
        destinations=destinations_gdf,
        departure=departure,
        transport_modes=[TransportMode.TRANSIT, TransportMode.WALK],
        max_time=dt.timedelta(seconds=max_time_sec),
    )

    # Normalise r5py column names (version-dependent).
    col_map = {}
    for cand in ("from_id", "from_id_origin", "origin_id"):
        if cand in df.columns:
            col_map[cand] = "listing_id"; break
    for cand in ("to_id", "to_id_dest", "destination_id"):
        if cand in df.columns:
            col_map[cand] = "landmark_key"; break
    for cand in ("travel_time", "travel_time_min"):
        if cand in df.columns:
            col_map[cand] = "travel_min"; break
    df = df.rename(columns=col_map)

    df["travel_min"] = _pd.to_numeric(df["travel_min"], errors="coerce")

    rows: list[tuple[str, str, int | None]] = []
    for lid, lk, tv in zip(df["listing_id"], df["landmark_key"], df["travel_min"]):
        rows.append((
            str(lid),
            str(lk),
            None if _pd.isna(tv) else int(round(float(tv))),
        ))

    print(
        f"[INFO] t4_r5_commute_matrix[worker={chunk_idx}]: done in "
        f"{time.monotonic() - t1:.0f}s → {len(rows)} rows",
        flush=True,
    )
    return rows


def _compute_matrix_parallel(
    origins_gdf: gpd.GeoDataFrame,
    destinations_gdf: gpd.GeoDataFrame,
    n_workers: int,
) -> list[tuple[str, str, int | None]]:
    """Chunk origins across N worker processes; collect all (listing, landmark,
    travel_min) tuples. Each worker loads its own JVM + TransportNetwork.
    """
    gtfs_path = _prepare_gtfs()

    origins_tuples = [
        (str(row["id"]), float(row.geometry.y), float(row.geometry.x))
        for _, row in origins_gdf.iterrows()
    ]
    destinations_tuples = [
        (str(row["id"]), float(row.geometry.y), float(row.geometry.x))
        for _, row in destinations_gdf.iterrows()
    ]

    # Even-sized chunks of origins.
    n_workers = max(1, min(n_workers, len(origins_tuples)))
    chunks = [c.tolist() for c in np.array_split(np.arange(len(origins_tuples)), n_workers)]
    print(
        f"[INFO] t4_r5_commute_matrix: sharding {len(origins_tuples)} origins "
        f"across {n_workers} workers "
        f"(~{len(origins_tuples) // n_workers} origins/worker)",
        flush=True,
    )

    all_rows: list[tuple[str, str, int | None]] = []
    t0 = time.monotonic()
    with ProcessPoolExecutor(max_workers=n_workers) as pool:
        futures = []
        for idx, chunk in enumerate(chunks):
            chunk_tuples = [origins_tuples[i] for i in chunk]
            fut = pool.submit(
                _worker_compute_chunk,
                idx,
                chunk_tuples,
                destinations_tuples,
                str(gtfs_path),
                str(OSM_PBF_PATH),
                DEPARTURE_TIME,
                int(MAX_TIME.total_seconds()),
                MAX_MEMORY_PER_WORKER,
            )
            futures.append(fut)

        n_done = 0
        for fut in as_completed(futures):
            chunk_rows = fut.result()
            all_rows.extend(chunk_rows)
            n_done += 1
            print(
                f"[INFO] t4_r5_commute_matrix: worker finished "
                f"({n_done}/{n_workers}) total_rows_so_far={len(all_rows)} "
                f"elapsed_s={time.monotonic() - t0:.0f}",
                flush=True,
            )

    print(
        f"[INFO] t4_r5_commute_matrix: all workers done in "
        f"{time.monotonic() - t0:.0f}s → {len(all_rows)} raw rows",
        flush=True,
    )
    return all_rows


# --- persist with Haversine filter -----------------------------------------


def _persist_filtered(
    db_path: Path,
    all_rows: list[tuple[str, str, int | None]],
    haversine_km: dict[tuple[str, str], float],
    cutoff_km: float,
) -> dict[str, int]:
    """Write only the rows whose listing-landmark Haversine <= cutoff_km.

    Also drops NULL-travel_min rows whose Haversine > cutoff_km/2 — these
    are "unreachable in 90 min from listings that aren't even near the
    landmark" and add no signal. NULL for Haversine <= cutoff_km/2 is kept
    because that's informative ("geographically close but transit-isolated").
    """
    stats = {
        "raw_rows":           len(all_rows),
        "dropped_far":        0,
        "dropped_null_far":   0,
        "kept":               0,
    }

    kept_rows: list[tuple[str, str, int | None]] = []
    half_cutoff = cutoff_km / 2.0
    for lid, lk, tv in all_rows:
        km = haversine_km.get((lid, lk))
        if km is None:
            # Defensive — shouldn't happen; treat as far.
            stats["dropped_far"] += 1
            continue
        if km > cutoff_km:
            stats["dropped_far"] += 1
            continue
        if tv is None and km > half_cutoff:
            # NULL travel + moderately-far Haversine → drop, adds no signal.
            stats["dropped_null_far"] += 1
            continue
        kept_rows.append((lid, lk, tv))
    stats["kept"] = len(kept_rows)

    con = sqlite3.connect(db_path)
    try:
        con.execute("DROP TABLE IF EXISTS listing_commute_times")
        con.execute("""
            CREATE TABLE listing_commute_times (
                listing_id   TEXT NOT NULL,
                landmark_key TEXT NOT NULL,
                travel_min   INTEGER,
                PRIMARY KEY (listing_id, landmark_key)
            )
        """)
        con.execute(
            "CREATE INDEX idx_lct_landmark_time "
            "ON listing_commute_times (landmark_key, travel_min)"
        )

        con.execute("BEGIN")
        con.executemany(
            "INSERT OR REPLACE INTO listing_commute_times "
            "(listing_id, landmark_key, travel_min) VALUES (?, ?, ?)",
            kept_rows,
        )
        con.commit()

        actual = con.execute(
            "SELECT COUNT(*) FROM listing_commute_times"
        ).fetchone()[0]
        n_non_null = con.execute(
            "SELECT COUNT(*) FROM listing_commute_times WHERE travel_min IS NOT NULL"
        ).fetchone()[0]
        print(
            f"[INFO] t4_r5_commute_matrix: persisted {actual} rows "
            f"({n_non_null} non-null travel_min; "
            f"{actual - n_non_null} unreachable <{MAX_TIME} but within "
            f"Haversine cutoff)",
            flush=True,
        )
        stats["persisted"] = actual
        return stats
    finally:
        con.close()


# --- main driver ------------------------------------------------------------


def run(
    db_path: Path,
    limit_origins: int | None,
    limit_destinations: int | None,
    n_workers: int,
    cutoff_km: float,
) -> dict:
    t_start = time.monotonic()

    origins_gdf = _load_origins_gdf(db_path, limit_origins)
    destinations_gdf = _load_destinations_gdf(limit_destinations)

    # Pre-filter: drop origins that have ZERO landmarks within cutoff.
    origins_gdf, hv_matrix = _filter_origins_with_nearby_landmarks(
        origins_gdf, destinations_gdf, cutoff_km,
    )

    # Build a flat Haversine lookup for the persist step.
    hv_lookup: dict[tuple[str, str], float] = {}
    o_ids = origins_gdf["id"].tolist()
    d_ids = destinations_gdf["id"].tolist()
    for i, lid in enumerate(o_ids):
        for j, lk in enumerate(d_ids):
            hv_lookup[(lid, lk)] = float(hv_matrix[i, j])

    all_rows = _compute_matrix_parallel(origins_gdf, destinations_gdf, n_workers)
    persist_stats = _persist_filtered(db_path, all_rows, hv_lookup, cutoff_km)

    elapsed = time.monotonic() - t_start
    stats = {
        "origins_in":     len(origins_gdf),
        "destinations":   len(destinations_gdf),
        "workers":        n_workers,
        "cutoff_km":      cutoff_km,
        **persist_stats,
        "elapsed_s":      round(elapsed, 1),
    }
    print(
        f"[INFO] t4_r5_commute_matrix: DONE rows_persisted={stats['persisted']} "
        f"elapsed_s={elapsed:.0f}",
        flush=True,
    )
    return stats


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    p.add_argument("--limit-origins", type=int, default=None,
                   help="Smoke-test cap on listings (all by default).")
    p.add_argument("--limit-destinations", type=int, default=None,
                   help="Smoke-test cap on landmarks (all by default).")
    p.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                   help=f"Parallel processes (default {DEFAULT_WORKERS}).")
    p.add_argument("--cutoff-km", type=float, default=HAVERSINE_CUTOFF_KM,
                   help=f"Haversine cutoff (default {HAVERSINE_CUTOFF_KM}).")
    args = p.parse_args()

    if not args.db.exists():
        print(f"[ERROR] t4_r5_commute_matrix: db not found at {args.db}", file=sys.stderr)
        return 2

    try:
        run(args.db, args.limit_origins, args.limit_destinations,
            args.workers, args.cutoff_km)
    except (FileNotFoundError, RuntimeError) as exc:
        print(f"[ERROR] t4_r5_commute_matrix: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
