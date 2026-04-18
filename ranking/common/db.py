"""SQLite helpers for the ranking layer.

Reuses the enrichment pipeline's WAL + busy_timeout pattern so ranking
scripts compose safely with in-flight enrichment passes on the same file.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path


def connect(db_path: Path, *, busy_timeout_ms: int = 30_000) -> sqlite3.Connection:
    """Return a Connection with WAL + busy_timeout configured.

    Ranking writes go to listings_ranking_signals; enrichment writes go to
    listings_enriched. Different tables, but same DB file — so concurrent
    writers need WAL + busy_timeout to avoid 'database is locked' errors.
    """
    conn = sqlite3.connect(str(db_path), timeout=busy_timeout_ms / 1000.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)};")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (name,)
    ).fetchone()
    return row is not None
