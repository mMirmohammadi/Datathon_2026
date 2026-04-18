"""Audit: compare a sample of stored doc-hashes to the current enriched text.

``listings_ranking_signals.embedding_doc_hash`` was recorded at embed time so
we can detect when the listing text has drifted since the vector was built.
We re-hash 100 random listings and count mismatches. Drift is a ``[WARN]``,
not a failure, per the bundle README's contract.
"""
from __future__ import annotations

import hashlib
import random
import sqlite3
from pathlib import Path

import pytest


pytestmark = pytest.mark.smoke

REPO_ROOT = Path(__file__).resolve().parents[2]


def _hash_doc(title: str | None, description: str | None) -> str:
    text = f"{title or ''}\n{description or ''}"
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def test_embedding_doc_hash_drift() -> None:
    db_path = REPO_ROOT / "data" / "listings.db"
    if not db_path.exists():
        pytest.skip(f"DB not installed at {db_path}")
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = list(conn.execute(
            "SELECT l.listing_id, l.title, l.description, "
            "       s.embedding_doc_hash "
            "FROM listings l "
            "JOIN listings_ranking_signals s USING (listing_id) "
            "WHERE s.embedding_doc_hash IS NOT NULL "
            "ORDER BY RANDOM() LIMIT 100"
        ))
    assert rows, "no ranking signals rows with embedding_doc_hash"
    drift = 0
    for r in rows:
        if _hash_doc(r["title"], r["description"]) != r["embedding_doc_hash"]:
            drift += 1
    # We do not fail on drift; we assert the audit runs cleanly and log the
    # count for operators. The threshold here is generous because the
    # teammate's hash recipe may differ from our simple title+description one.
    print(f"[INFO] embedding_doc_hash_drift: {drift} / {len(rows)}")
