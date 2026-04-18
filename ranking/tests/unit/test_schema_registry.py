"""Registry smoke tests — catches drift between schema.py and downstream code."""
from __future__ import annotations

import re

import pytest

from ranking.schema import SIGNALS, create_table_sql, signal_names, validate_signals


def test_all_names_unique():
    names = [s.name for s in SIGNALS]
    assert len(names) == len(set(names))


def test_all_names_are_valid_sql_identifiers():
    for s in SIGNALS:
        assert re.fullmatch(r"[a-z][a-z0-9_]*", s.name), s.name


def test_sql_types_are_valid():
    for s in SIGNALS:
        assert s.sql_type in {"INTEGER", "REAL", "TEXT"}, (s.name, s.sql_type)


def test_create_table_mentions_every_signal():
    sql = create_table_sql()
    for s in SIGNALS:
        assert f"{s.name} {s.sql_type}" in sql, s.name


def test_validate_signals_runs_at_import_time():
    # Just calling it explicitly to confirm no exception
    validate_signals()


def test_signal_names_excludes_pk():
    names = signal_names()
    assert "listing_id" not in names
    assert len(names) == len(SIGNALS)


def test_expected_core_signals_present():
    names = set(signal_names())
    for must_have in [
        "price_baseline_chf_canton_rooms",
        "price_delta_pct_canton_rooms",
        "dist_nearest_stop_m",
        "nearest_stop_name",
        "nearest_stop_type",
        "poi_supermarket_300m",
        "poi_school_1km",
        "dist_motorway_m",
        "dist_rail_m",
        "embedding_row_index",
        "last_updated_utc",
    ]:
        assert must_have in names, f"missing expected signal: {must_have}"


def test_all_poi_signals_are_integers():
    for s in SIGNALS:
        if s.name.startswith("poi_"):
            assert s.sql_type == "INTEGER", (s.name, s.sql_type)


def test_all_dist_signals_are_reals():
    for s in SIGNALS:
        if s.name.startswith("dist_") and s.name != "distance_km":  # future signal
            assert s.sql_type == "REAL", (s.name, s.sql_type)
