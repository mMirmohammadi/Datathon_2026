"""SQLite helpers. Uses the same path convention as app.config."""
from __future__ import annotations

import sqlite3
from pathlib import Path


def connect(db_path: Path) -> sqlite3.Connection:
    """Return a Connection with row factory = sqlite3.Row for dict-like access."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def enriched_column_names(conn: sqlite3.Connection) -> list[str]:
    """Return every column name in listings_enriched, in CREATE order."""
    return [r[1] for r in conn.execute("PRAGMA table_info(listings_enriched);").fetchall()]


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (name,)
    ).fetchone()
    return row is not None
