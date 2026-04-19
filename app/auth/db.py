"""Users-DB schema + idempotent bootstrap.

A separate SQLite file (``data/users.db``) holds accounts, sessions,
interactions and login attempts. Kept separate from ``data/listings.db`` so
teammate-shipped bundle migrations stay orthogonal to app auth state.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path


_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TEXT NOT NULL,
        last_login_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS user_sessions (
        token_hash TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        created_at TEXT NOT NULL,
        last_seen_at TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        revoked INTEGER NOT NULL DEFAULT 0
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sessions_user ON user_sessions(user_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS user_interactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        listing_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        value REAL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_interactions_user_time
        ON user_interactions(user_id, created_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_interactions_listing
        ON user_interactions(listing_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS user_login_attempts (
        username TEXT NOT NULL,
        ip TEXT NOT NULL,
        success INTEGER NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_login_attempts_recent
        ON user_login_attempts(username, ip, created_at DESC)
    """,
)


def connect(path: Path) -> sqlite3.Connection:
    """Open a connection with the settings we want everywhere in the auth stack.

    * ``row_factory = Row`` so routes/tests can index rows by column name.
    * ``foreign_keys = ON`` because we rely on ``ON DELETE CASCADE`` for
      account deletion to drop sessions + interactions atomically.
    * ``parent.mkdir`` so fresh clones can boot without manual setup.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def bootstrap_users_db(path: Path) -> None:
    """Create the users-DB schema if missing. Idempotent."""
    with connect(path) as conn:
        for stmt in _SCHEMA_STATEMENTS:
            conn.execute(stmt)
        conn.commit()
