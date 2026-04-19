from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path

from app.db import get_connection
from app.harness.csv_import import create_indexes, create_schema
from app.harness.enriched_import import import_enriched_csv
from app.harness.sred_transform import ensure_sred_normalized_csv


logger = logging.getLogger(__name__)

ENRICHED_CSV_RELATIVE = Path("sample_data_enriched") / "sample_enriched_500.csv"


def _bundle_install_allowed() -> bool:
    """Tests set ``LISTINGS_SKIP_BUNDLE_INSTALL=1`` so each tmp DB does NOT
    decompress the 417 MB teammate bundle just to run a unit test.
    """
    return os.environ.get("LISTINGS_SKIP_BUNDLE_INSTALL", "0") != "1"


def bootstrap_database(*, db_path: Path, raw_data_dir: Path) -> None:
    """Three-step startup:

    1. If ``db_path`` already exists but the schema lacks the app-owned
       columns, run the in-place migration (teammate bundle path).
    2. If ``db_path`` is missing but the teammate bundle is on disk, install
       it (gunzip + copy ranking artefacts) and migrate.
    3. Otherwise fall back to the legacy 500-row CSV importer for smoke tests
       and fresh clones without the bundle.
    """
    ensure_sred_normalized_csv(raw_data_dir)

    if db_path.exists():
        if not _schema_matches(db_path):
            if _migration_is_possible(db_path):
                logger.info(
                    "\033[36mListings DB at %s is missing app-owned columns; "
                    "running scripts.migrate_db_to_app_schema.migrate() in place.\033[0m",
                    db_path,
                )
                _run_migration(db_path)
            else:
                logger.error(
                    "\033[31mListings DB schema mismatch at %s AND the migration "
                    "source tables (listings_enriched, listings_ranking_signals) "
                    "are absent. Preserving DB untouched; remove it and rerun to "
                    "rebuild from the bundle or the legacy CSV.\033[0m",
                    db_path,
                )
        return

    # DB missing. Try the teammate bundle first; fall back to legacy CSV.
    if _bundle_install_allowed():
        try:
            from scripts.install_dataset import ensure_installed

            ensure_installed(db_path=db_path)
            _run_migration(db_path)
            return
        except FileNotFoundError as exc:
            logger.warning(
                "\033[33m[WARN] using_legacy_500_row_fallback: expected=teammate "
                "bundle at datathon2026_dataset/, got=%s, fallback=CSV import of "
                "500 enriched rows from %s\033[0m",
                exc,
                raw_data_dir / ENRICHED_CSV_RELATIVE,
            )

    enriched_csv_path = raw_data_dir / ENRICHED_CSV_RELATIVE
    if not enriched_csv_path.exists():
        raise FileNotFoundError(
            f"Neither the teammate DB bundle nor the legacy CSV was found. "
            f"Expected one of: data/listings.db, "
            f"datathon2026_dataset/data/listings.db, {enriched_csv_path}."
        )

    with get_connection(db_path) as connection:
        create_schema(connection)
        import_enriched_csv(connection, enriched_csv_path)
        create_indexes(connection)
        connection.execute("INSERT INTO listings_fts(listings_fts) VALUES('rebuild')")
        connection.commit()


def _migration_is_possible(db_path: Path) -> bool:
    """True iff the two source tables the migration reads from exist."""
    try:
        with get_connection(db_path) as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ('listings_enriched', 'listings_ranking_signals')"
            ).fetchall()
            return len({r[0] for r in rows}) == 2
    except sqlite3.Error:
        return False


def _run_migration(db_path: Path) -> None:
    from scripts.migrate_db_to_app_schema import migrate

    try:
        report = migrate(db_path)
        logger.info("migrated DB at %s: %s", db_path, report)
    except (sqlite3.Error, RuntimeError) as exc:
        logger.error(
            "\033[31m[ERROR] migrate_db_to_app_schema failed: %s\033[0m",
            exc,
        )
        raise


def _schema_matches(db_path: Path) -> bool:
    required_columns = {
        "latitude",
        "longitude",
        "features_json",
        "platform_id",
        "scrape_source",
        "street",
        "house_number",
        "city_slug",
        "floor",
        "year_built",
        "object_category_raw",
        "object_type",
        "feature_wheelchair_accessible",
        "feature_private_laundry",
        "feature_minergie_certified",
    }

    with get_connection(db_path) as connection:
        table = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'listings'"
        ).fetchone()
        if table is None:
            return False

        columns = {
            column[1]
            for column in connection.execute("PRAGMA table_info(listings)").fetchall()
        }

    return required_columns <= columns


# ---------------------------------------------------------------------------
# Ranker-signal schema validator (Tier 1.2 — no silent fallbacks)
#
# The ranker and match-explanation modules read dozens of columns by name. If
# any are missing from the running DB, the `_safe_row_get` helper currently
# returns ``None`` quietly, meaning an entire ranking channel can vanish
# without any diagnostic. This validator is called from the FastAPI lifespan
# after ``bootstrap_database`` so every server start logs any gaps in a
# single consolidated ``[WARN]`` block — the operator sees what's broken
# before the first user query.


_EXPECTED_SIGNAL_COLUMNS: tuple[str, ...] = (
    "price_baseline_chf_canton_rooms",
    "price_baseline_chf_plz_rooms",
    "price_delta_pct_canton_rooms",
    "price_delta_pct_plz_rooms",
    "price_baseline_n_canton_rooms",
    "price_baseline_n_plz_rooms",
    "price_plausibility",
    "dist_nearest_stop_m",
    "nearest_stop_name",
    "nearest_stop_id",
    "nearest_stop_type",
    "nearest_stop_lines_count",
    "nearest_stop_lines_count_clamped",
    "nearest_stop_lines_log",
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
)

_EXPECTED_COMMUTE_PROXY = tuple(
    f"commute_proxy_{city}_min"
    for city in (
        "zurich", "bern", "basel", "geneve",
        "lausanne", "lugano", "winterthur", "st_gallen",
    )
)


def validate_ranker_schema(db_path: Path) -> dict[str, list[str]]:
    """Log one consolidated [WARN] per missing column family + table.

    Returns a dict of findings so callers / tests can inspect them. Never
    raises — a missing column is a degradation, not a startup-blocker — but
    every gap is announced loudly.

    Checks, in order:
      1. Required signal columns on ``listings_ranking_signals``.
      2. Every ``dist_landmark_<key>_m`` listed in ``data/ranking/landmarks.json``.
      3. Every ``commute_proxy_<city>_min`` used by the HB commute-target schema.
      4. ``listing_commute_times`` table (r5py GTFS truth — orphaned in the
         current wiring but read by the Tier 2 upgrade).
      5. ``listings_fts`` virtual table (FTS5 is the BM25 channel; its absence
         causes every ``POST /listings`` to 500).
    """
    from app.core import landmarks as _landmarks_mod

    findings: dict[str, list[str]] = {
        "missing_signal_columns": [],
        "missing_dist_landmark_columns": [],
        "missing_commute_proxy_columns": [],
        "missing_tables": [],
    }

    try:
        with get_connection(db_path) as conn:
            signal_cols = {
                row[1]
                for row in conn.execute(
                    "PRAGMA table_info(listings_ranking_signals)"
                ).fetchall()
            }
            table_names = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
                ).fetchall()
            }
    except sqlite3.Error as exc:
        print(
            f"[WARN] validate_ranker_schema: expected=readable DB at {db_path}, "
            f"got={type(exc).__name__}: {exc}, fallback=validator skipped",
            flush=True,
        )
        return findings

    for col in _EXPECTED_SIGNAL_COLUMNS:
        if col not in signal_cols:
            findings["missing_signal_columns"].append(col)

    # Landmark gazetteer: one dist_landmark_<key>_m column per resolved entry.
    try:
        for lm in _landmarks_mod.all_landmarks():
            col = _landmarks_mod.column_for(lm.key)
            if col not in signal_cols:
                findings["missing_dist_landmark_columns"].append(col)
    except Exception as exc:  # gazetteer file missing / malformed
        print(
            f"[WARN] validate_ranker_schema.landmarks: expected=readable "
            f"landmarks.json, got={type(exc).__name__}: {exc}, "
            f"fallback=landmark column check skipped",
            flush=True,
        )

    for col in _EXPECTED_COMMUTE_PROXY:
        if col not in signal_cols:
            findings["missing_commute_proxy_columns"].append(col)

    for required_table in ("listings_fts", "listing_commute_times"):
        if required_table not in table_names:
            findings["missing_tables"].append(required_table)

    # Emit ONE consolidated WARN per family so logs stay readable.
    for family, label, consequence in (
        (
            "missing_signal_columns", "ranker_signal_columns",
            "affected_channels=price/transit/POI/noise soft rankings + match_explain facts",
        ),
        (
            "missing_dist_landmark_columns", "landmark_distance_columns",
            "affected_channels=near_landmark soft ranking + landmark MatchFacts",
        ),
        (
            "missing_commute_proxy_columns", "commute_proxy_columns",
            "affected_channels=commute_target soft ranking (Tier 2 upgrade uses listing_commute_times instead)",
        ),
        (
            "missing_tables", "required_tables",
            "FTS5 missing -> every /listings request returns 500; "
            "listing_commute_times missing -> real r5py commute facts disabled",
        ),
    ):
        if findings[family]:
            missing_preview = ", ".join(findings[family][:10])
            n_missing = len(findings[family])
            print(
                f"[WARN] schema_validator.{label}: expected=present in "
                f"{db_path}, got={n_missing} missing ({missing_preview}"
                f"{'...' if n_missing > 10 else ''}), "
                f"consequence={consequence}",
                flush=True,
            )

    if not any(findings.values()):
        print(
            f"[INFO] schema_validator: all expected columns + tables present "
            f"in {db_path}",
            flush=True,
        )

    return findings
