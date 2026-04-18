"""SQLite helpers. Uses the same path convention as app.config."""
from __future__ import annotations

import sqlite3
from pathlib import Path


def connect(db_path: Path, *, busy_timeout_ms: int = 30_000) -> sqlite3.Connection:
    """Return a Connection with row factory = sqlite3.Row for dict-like access.

    WAL mode + a generous busy_timeout are required because enrichment passes
    can run concurrently (pass 1b Nominatim at 1 req/s against pass 2 GPT at
    16 concurrent async writes). Without these, writes raise `database is
    locked` on contention — even though the two passes touch different columns.
    """
    conn = sqlite3.connect(str(db_path), timeout=busy_timeout_ms / 1000.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")      # readers never block writers
    conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)};")
    conn.execute("PRAGMA synchronous = NORMAL;")    # WAL-safe; much faster than FULL
    return conn


def enriched_column_names(conn: sqlite3.Connection) -> list[str]:
    """Return every column name in listings_enriched, in CREATE order."""
    return [r[1] for r in conn.execute("PRAGMA table_info(listings_enriched);").fetchall()]


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (name,)
    ).fetchone()
    return row is not None
