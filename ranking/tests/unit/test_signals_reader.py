"""Unit tests for signals_reader — no live DB required."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ranking.common.db import connect
from ranking.runtime.signals_reader import SignalRow, load_signals
from ranking.schema import create_table_sql, INDEX_SQL


@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    p = tmp_path / "test.db"
    con = sqlite3.connect(str(p))
    # Minimal `listings` stub so the FK constraint is satisfiable
    con.executescript(
        """
        CREATE TABLE listings (listing_id TEXT PRIMARY KEY);
        INSERT INTO listings VALUES ('a'), ('b'), ('c');
        """
    )
    con.executescript(create_table_sql())
    for idx in INDEX_SQL:
        con.execute(idx)
    # Seed 3 rows with varied signal data
    con.execute(
        """
        INSERT INTO listings_ranking_signals (
            listing_id,
            price_baseline_chf_canton_rooms, price_delta_pct_canton_rooms,
            dist_nearest_stop_m, nearest_stop_name, nearest_stop_type, nearest_stop_lines_count,
            poi_supermarket_300m, poi_school_1km, dist_motorway_m,
            embedding_row_index, embedding_model
        ) VALUES
            ('a', 2000.0, 0.1,  120, 'Zürich HB', 'train', 20,  3, 10, 2500, 0, 'arctic'),
            ('b',   NULL, NULL, 800, 'Bern Bf',   'train', 15,  0,  1, 1200, 1, 'arctic'),
            ('c', 1500.0, -0.05, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """
    )
    con.commit()
    con.close()
    return p


def test_load_signals_returns_rows(tmp_db):
    out = load_signals(tmp_db, ["a", "b", "c"])
    assert set(out.keys()) == {"a", "b", "c"}
    assert isinstance(out["a"], SignalRow)
    assert out["a"].price_baseline_canton == 2000.0
    assert out["a"].poi_supermarket_300m == 3
    assert out["a"].dist_nearest_stop_m == 120


def test_load_signals_preserves_nulls(tmp_db):
    out = load_signals(tmp_db, ["b", "c"])
    assert out["b"].price_baseline_canton is None
    assert out["c"].dist_nearest_stop_m is None
    assert out["c"].nearest_stop_name is None


def test_load_signals_missing_ids_absent(tmp_db):
    out = load_signals(tmp_db, ["a", "not_real"])
    assert "not_real" not in out
    assert "a" in out


def test_load_signals_empty_input_returns_empty(tmp_db):
    assert load_signals(tmp_db, []) == {}


def test_load_signals_missing_table_returns_empty(tmp_path, capsys):
    """If the signals table doesn't exist, emit [WARN] + empty dict."""
    p = tmp_path / "empty.db"
    con = sqlite3.connect(str(p))
    con.execute("CREATE TABLE listings (listing_id TEXT PRIMARY KEY)")
    con.execute("INSERT INTO listings VALUES ('a')")
    con.commit()
    con.close()

    result = load_signals(p, ["a"])
    assert result == {}
    out = capsys.readouterr().out
    assert "[WARN]" in out
    assert "listings_ranking_signals" in out


def test_load_signals_chunks_large_input(tmp_db):
    """Test IN-clause chunking for >999 ids."""
    # Add 1200 rows to the DB then query all at once
    con = sqlite3.connect(str(tmp_db))
    many = [("x" + str(i),) for i in range(1200)]
    con.executemany("INSERT OR IGNORE INTO listings VALUES (?)", many)
    con.executemany(
        "INSERT INTO listings_ranking_signals (listing_id) VALUES (?)",
        many,
    )
    con.commit()
    con.close()
    result = load_signals(tmp_db, [x[0] for x in many])
    assert len(result) == 1200
