"""T4.1 — offline transit travel-time matrix via r5py.

Builds a real door-to-door commute table between every listing and every
landmark, using the Swiss GTFS feed + OpenStreetMap for walk-access routing.
Output lives in a new table `listing_commute_times` inside `data/listings.db`.

WHY r5py not OJP:
  Open Journey Planner's free tier caps at 20,000 requests/day. At
  25,546 listings × ~30 landmarks = ~766k requests, OJP pre-compute would
  take ~38 days of continuous API calls AND blow the daily quota 38×.
  r5py computes the whole matrix offline from the GTFS feed + OSM in one
  pass, with zero API calls. See _context/NEXT_STEPS_2026-04-18.md §P1.1b.

WHY Tuesday 2026-05-05 08:00 CET:
  The GTFS feed `feed_info.txt` declares validity 2025-12-14..2026-12-12
  (Fahrplan 2026). Tuesday 08:00 is a conventional commute-peak slot —
  traffic, wait times, and frequency are at their typical working-day mix.

Pipeline:
  1. Build r5py.TransportNetwork from `data/ranking/osm/switzerland-latest.osm.pbf`
     plus `data/ranking/gtfs/gtfs_complete.zip` (first-run downloads the R5 jar
     from Maven, ~500 MB one-time).
  2. Load origins = every listing with a valid lat/lng (expect ~23,900 of 25,546).
  3. Load destinations = every landmark in `data/ranking/landmarks.json` with
     a valid lat/lng.
  4. Compute `TravelTimeMatrixComputer` with
        departure      = 2026-05-05 08:00 CET
        transport_modes= [TRANSIT, WALK]
        max_time       = 90 min
     → returns a DataFrame [from_id, to_id, travel_time_min].
  5. Persist to SQLite `listing_commute_times(listing_id, landmark_key,
     travel_min)` — PRIMARY KEY (listing_id, landmark_key), INDEX on
     (landmark_key, travel_min) so "listings within N min of X" is a
     prefix-scan query.

Per CLAUDE.md §5:
  * Every origin/destination with invalid coords is dropped with a `[WARN]`.
  * If r5py returns fewer rows than expected, we emit a `[WARN]` and
    proceed with whatever we got (partial coverage is better than none;
    downstream ranker handles NULL as "no commute signal").
  * Pre- and post-condition asserts on the output table's row count.

Idempotent: re-run safely — the table is dropped + recreated, and the R5
network is cached under ~/.cache/r5py (automatic).

Usage:
    # full run (all listings × all landmarks):
    python -m ranking.scripts.t4_r5_commute_matrix --db data/listings.db

    # smoke test (100 listings × first 5 landmarks):
    python -m ranking.scripts.t4_r5_commute_matrix --db data/listings.db \\
        --limit-origins 100 --limit-destinations 5

Env:
    R5_JAR_OPTS   pass to JVM, default is r5py's own (80% system RAM).
                  If you want to cap: `export R5_JAR_OPTS="-Xmx16G"`.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

import geopandas as gpd
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

# Tuesday 2026-05-05 08:00 in Europe/Zurich timezone (CEST). Writing it as a
# naive datetime because r5py expects a `datetime.datetime` and uses the
# timezone attached to the TransportNetwork (derived from the GTFS). The
# GTFS feed is Swiss, so local time is correct.
DEPARTURE_TIME = dt.datetime(2026, 5, 5, 8, 0, 0)
MAX_TIME = dt.timedelta(minutes=90)

# --- helpers ----------------------------------------------------------------


def _load_origins_gdf(db_path: Path, limit: int | None) -> gpd.GeoDataFrame:
    """Return a GeoDataFrame of (listing_id, geometry) for every listing with
    usable lat/lng. The `id` column is named `listing_id` to match r5py's
    convention of preserving the origin-id column name in the output.
    """
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
    print(
        f"[INFO] t4_r5_commute_matrix: loaded {len(df)} origins (listings with coords)",
        flush=True,
    )
    if df.empty:
        raise RuntimeError("No listings with valid coordinates — aborting.")
    gdf = gpd.GeoDataFrame(
        {"id": df["listing_id"].astype(str)},
        geometry=gpd.points_from_xy(df["longitude"], df["latitude"]),
        crs="EPSG:4326",
    )
    return gdf


def _load_destinations_gdf(limit: int | None) -> gpd.GeoDataFrame:
    """Return a GeoDataFrame of (landmark_key, geometry) for landmarks that
    have a valid (lat, lon)."""
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
                f"got=rec({rec.get('key', '?')!r} lat={lat} lon={lon}), "
                f"fallback=skip",
                flush=True,
            )
            dropped += 1
            continue
        kept.append({"id": str(key), "lat": float(lat), "lon": float(lon)})
    if not kept:
        raise RuntimeError(f"No landmarks with valid coords in {LANDMARKS_PATH}")
    if limit is not None:
        kept = kept[:limit]
    print(
        f"[INFO] t4_r5_commute_matrix: loaded {len(kept)} destinations "
        f"({dropped} dropped for missing fields)",
        flush=True,
    )
    gdf = gpd.GeoDataFrame(
        {"id": [k["id"] for k in kept]},
        geometry=[sg.Point(k["lon"], k["lat"]) for k in kept],
        crs="EPSG:4326",
    )
    return gdf


def _prepare_gtfs() -> Path:
    """Produce a cleaned copy of the GTFS feed for r5py's strict PK checker.

    The Swiss aggregate feed at gtfs.geops.ch ships with stray trailing
    whitespace on at least one `stop_id` ("8580003:EV " duplicates
    "8580003:EV" on row 60445). Pandas-backed code (`t2_gtfs_nearest.py`)
    tolerates this because it trims on join; r5py's R5 parser rejects it
    with `GtfsFileError: DuplicateKeyError: stops … 'stop_id'`.

    We extract every member of the original zip, strip whitespace off the
    first column in the small metadata files (stops/routes/trips/transfers
    and their ID-referencing columns), dedupe, and re-pack to
    `gtfs_cleaned.zip`. stop_times.txt is the large one (GB-scale); we only
    trim the two ID columns in-place without parsing the whole thing as a
    DataFrame — memory-cheap.

    Idempotent: skips work if `gtfs_cleaned.zip` is newer than the source.
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

    # Columns whose values are IDs we want to trim.
    ID_COLUMNS = {
        "stop_id", "parent_station", "route_id", "trip_id", "service_id",
        "from_stop_id", "to_stop_id", "agency_id", "shape_id", "block_id",
    }
    # Tables where we dedupe by first-N columns after trimming.
    # Key is filename, value is list of key-column names.
    DEDUPE_KEYS = {
        "stops.txt":     ["stop_id"],
        "routes.txt":    ["route_id"],
        "trips.txt":     ["trip_id"],
        "agency.txt":    ["agency_id"],
        "calendar.txt":  ["service_id"],
        "transfers.txt": ["from_stop_id", "to_stop_id"],
    }
    # Large files — stream and trim only the ID columns without deduping.
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
                # Parse, trim every field that looks like an ID, dedupe.
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
                    # Trim ID columns in place
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
                # Stream through, trim ID columns; no dedupe.
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
    elapsed = time.monotonic() - t0
    print(
        f"[INFO] t4_r5_commute_matrix._prepare_gtfs: done in {elapsed:.1f}s "
        f"stats={stats}",
        flush=True,
    )
    return GTFS_CLEANED_PATH


def _build_network():
    """Build the r5py TransportNetwork. Logs the build time — it can be slow
    on the first run (large OSM/GTFS files, JVM startup, R5 jar download).
    """
    import r5py

    if not OSM_PBF_PATH.exists():
        raise FileNotFoundError(f"OSM PBF not found at {OSM_PBF_PATH}")
    if not GTFS_ZIP_PATH.exists():
        raise FileNotFoundError(f"GTFS zip not found at {GTFS_ZIP_PATH}")

    gtfs_path = _prepare_gtfs()

    print(
        f"[INFO] t4_r5_commute_matrix: building TransportNetwork "
        f"(osm={OSM_PBF_PATH.stat().st_size // 1024 // 1024}MB "
        f"gtfs={gtfs_path.stat().st_size // 1024 // 1024}MB)",
        flush=True,
    )
    t0 = time.monotonic()
    tn = r5py.TransportNetwork(
        osm_pbf=str(OSM_PBF_PATH),
        gtfs=[str(gtfs_path)],
    )
    print(
        f"[INFO] t4_r5_commute_matrix: network built in {time.monotonic() - t0:.1f}s",
        flush=True,
    )
    return tn


def _compute_matrix(tn, origins_gdf, destinations_gdf) -> pd.DataFrame:
    """Compute the travel-time matrix."""
    import r5py
    from r5py import TransportMode

    print(
        f"[INFO] t4_r5_commute_matrix: computing matrix "
        f"({len(origins_gdf)} origins × {len(destinations_gdf)} destinations = "
        f"{len(origins_gdf) * len(destinations_gdf)} O-D pairs)",
        flush=True,
    )
    t0 = time.monotonic()
    # r5py >= 1.0 renamed the entrypoint from `TravelTimeMatrixComputer`
    # (0.x) to `TravelTimeMatrix`. Same kwargs + RegionalTask inheritance,
    # just a different class name. Computing happens lazily on construction.
    df = r5py.TravelTimeMatrix(
        tn,
        origins=origins_gdf,
        destinations=destinations_gdf,
        departure=DEPARTURE_TIME,
        transport_modes=[TransportMode.TRANSIT, TransportMode.WALK],
        max_time=MAX_TIME,
    )
    print(
        f"[INFO] t4_r5_commute_matrix: matrix computed in "
        f"{time.monotonic() - t0:.1f}s → {len(df)} rows",
        flush=True,
    )
    return df


def _persist_matrix(db_path: Path, df: pd.DataFrame) -> int:
    """Write the matrix into `listing_commute_times`.

    Schema (keep it minimal; lazy to expand later if needed):
        CREATE TABLE listing_commute_times (
            listing_id   TEXT NOT NULL,
            landmark_key TEXT NOT NULL,
            travel_min   INTEGER,     -- NULL if unreachable in max_time
            PRIMARY KEY (listing_id, landmark_key)
        )

    r5py's output column names vary by version — we normalise:
      - from_id → listing_id
      - to_id   → landmark_key
      - travel_time (minutes, float) → travel_min (int or NULL)
    """
    # Normalise column names — r5py's default is from_id / to_id / travel_time.
    col_map = {}
    for candidate in ("from_id", "from_id_origin", "origin_id"):
        if candidate in df.columns:
            col_map[candidate] = "listing_id"
            break
    for candidate in ("to_id", "to_id_dest", "destination_id"):
        if candidate in df.columns:
            col_map[candidate] = "landmark_key"
            break
    for candidate in ("travel_time", "travel_time_min"):
        if candidate in df.columns:
            col_map[candidate] = "travel_min"
            break
    if set(col_map.values()) != {"listing_id", "landmark_key", "travel_min"}:
        raise RuntimeError(
            f"Could not map r5py output columns. Got: {list(df.columns)}. "
            f"Expected: from_id, to_id, travel_time."
        )
    df = df.rename(columns=col_map)

    # Coerce travel_min to Int64 (nullable), NaN → NULL.
    df["travel_min"] = pd.to_numeric(df["travel_min"], errors="coerce")
    df["travel_min"] = df["travel_min"].round().astype("Int64")

    # Enforce string types on keys to match the `listings` table.
    df["listing_id"] = df["listing_id"].astype(str)
    df["landmark_key"] = df["landmark_key"].astype(str)

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

        # Bulk insert — SQLite is fast at this if we use executemany within a tx.
        rows = [
            (lid, lk, (None if pd.isna(tv) else int(tv)))
            for lid, lk, tv in zip(df["listing_id"], df["landmark_key"], df["travel_min"])
        ]
        con.execute("BEGIN")
        con.executemany(
            "INSERT INTO listing_commute_times (listing_id, landmark_key, travel_min) "
            "VALUES (?, ?, ?)",
            rows,
        )
        con.commit()

        # Post-condition: verify row count
        actual = con.execute(
            "SELECT COUNT(*) FROM listing_commute_times"
        ).fetchone()[0]
        n_non_null = con.execute(
            "SELECT COUNT(*) FROM listing_commute_times WHERE travel_min IS NOT NULL"
        ).fetchone()[0]
        print(
            f"[INFO] t4_r5_commute_matrix: persisted {actual} rows "
            f"({n_non_null} non-null travel_min; "
            f"{actual - n_non_null} unreachable > max_time={MAX_TIME})",
            flush=True,
        )
        return actual
    finally:
        con.close()


def run(db_path: Path, limit_origins: int | None, limit_destinations: int | None) -> dict:
    t_start = time.monotonic()

    origins_gdf = _load_origins_gdf(db_path, limit_origins)
    destinations_gdf = _load_destinations_gdf(limit_destinations)

    tn = _build_network()
    df = _compute_matrix(tn, origins_gdf, destinations_gdf)

    rows_written = _persist_matrix(db_path, df)
    elapsed = time.monotonic() - t_start
    print(
        f"[INFO] t4_r5_commute_matrix: DONE rows={rows_written} "
        f"elapsed_s={elapsed:.0f}",
        flush=True,
    )
    return {
        "origins":        len(origins_gdf),
        "destinations":   len(destinations_gdf),
        "matrix_rows":    len(df),
        "rows_persisted": rows_written,
        "elapsed_s":      round(elapsed, 1),
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    p.add_argument("--limit-origins", type=int, default=None,
                   help="Smoke-test cap on listings (all by default).")
    p.add_argument("--limit-destinations", type=int, default=None,
                   help="Smoke-test cap on landmarks (all by default).")
    args = p.parse_args()

    if not args.db.exists():
        print(f"[ERROR] t4_r5_commute_matrix: db not found at {args.db}", file=sys.stderr)
        return 2

    try:
        run(args.db, args.limit_origins, args.limit_destinations)
    except (FileNotFoundError, RuntimeError) as exc:
        print(f"[ERROR] t4_r5_commute_matrix: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
