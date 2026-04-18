"""Participant-owned bootstrap: builds the FTS5 virtual table for BM25 retrieval.

Idempotent: `CREATE ... IF NOT EXISTS` + `rebuild` on every startup. The FTS table
is content-linked to the harness-owned `listings` table via rowid — no data
duplication, no schema changes to `listings`.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

from app.db import get_connection

FTS_CREATE_SQL = """
CREATE VIRTUAL TABLE IF NOT EXISTS listings_fts USING fts5(
    title,
    description,
    street,
    city,
    content='listings',
    content_rowid='rowid',
    tokenize='unicode61 remove_diacritics 2'
);
"""


def _fts_exists(connection: sqlite3.Connection) -> bool:
    row = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='listings_fts'"
    ).fetchone()
    return row is not None


def bootstrap_participant(db_path: Path) -> None:
    """Create (if missing) and rebuild the listings_fts virtual table.

    Rebuild is idempotent and cheap (~seconds for ~22k rows). We do it on every
    startup so the FTS index always reflects the current state of `listings`.
    """
    t0 = time.monotonic()
    with get_connection(db_path) as connection:
        existed = _fts_exists(connection)
        connection.executescript(FTS_CREATE_SQL)
        # Rebuild regenerates the FTS index from the content table. Safe to run
        # whether the table was just created or already existed.
        connection.execute("INSERT INTO listings_fts(listings_fts) VALUES('rebuild')")
        connection.commit()
        row = connection.execute("SELECT COUNT(*) FROM listings_fts").fetchone()
        fts_count = row[0] if row else 0
    elapsed = time.monotonic() - t0
    print(
        f"[INFO] bootstrap_participant: fts_existed={existed} fts_rows={fts_count} "
        f"elapsed_s={elapsed:.2f}",
        flush=True,
    )
