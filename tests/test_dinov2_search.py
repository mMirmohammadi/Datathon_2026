"""Unit tests for app.core.dinov2_search.

We can't load the real 289 MB matrix in the unit suite, so the tests stand
up a small synthetic store in a tmp dir and swap the store-dir constant via
monkeypatch. This pins the loader + similarity contract without touching
the production artefact.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np

from app.core import dinov2_search


def _write_fake_store(tmp: Path) -> Path:
    """Write a 6×4 L2-normalized float32 matrix + image index pointing to
    three listings, each with 2 photos. Layout mirrors the real store:
    one row per image, image_id = ``source/listing_id/idx-hash``.
    """
    store = tmp / "dinov2_store"
    store.mkdir()
    # 3 listings × 2 images each; 4-d vectors for test speed.
    # Same listing rows are similar; cross-listing rows are orthogonal.
    vecs = np.array(
        [
            [1.0, 0.0, 0.0, 0.0],  # L1 img 0
            [1.0, 0.01, 0.0, 0.0],  # L1 img 1 — near L1 img 0
            [0.0, 1.0, 0.0, 0.0],  # L2 img 0
            [0.0, 0.99, 0.01, 0.0],  # L2 img 1
            [0.0, 0.0, 1.0, 0.0],  # L3 img 0
            [0.1, 0.0, 0.99, 0.0],  # L3 img 1 — slightly biased toward L1
        ],
        dtype=np.float32,
    )
    # L2-normalise each row.
    vecs /= np.linalg.norm(vecs, axis=1, keepdims=True)
    np.save(store / "main.fp32.npy", vecs)

    db_path = store / "index.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE images (image_id TEXT PRIMARY KEY, index_kind TEXT NOT NULL, row_idx INTEGER NOT NULL)"
    )
    rows = [
        ("robinreal/L1/0-a", "main", 0),
        ("robinreal/L1/1-b", "main", 1),
        ("robinreal/L2/0-c", "main", 2),
        ("robinreal/L2/1-d", "main", 3),
        ("robinreal/L3/0-e", "main", 4),
        ("robinreal/L3/1-f", "main", 5),
    ]
    conn.executemany("INSERT INTO images VALUES (?, ?, ?)", rows)
    conn.commit()
    conn.close()
    return store


def test_load_and_find_similar_returns_self_first_when_not_excluded(
    tmp_path: Path,
) -> None:
    store = _write_fake_store(tmp_path)
    dinov2_search.reset_for_tests()
    dinov2_search.load_dinov2_index(store)

    # Self-match allowed: L1 vs all should put L1 first (cosine ≈ 1).
    results = dinov2_search.find_similar_listings("L1", k=3, exclude_self=False)
    assert results[0][0] == "L1"
    assert results[0][1] > 0.99


def test_find_similar_excludes_self_by_default(tmp_path: Path) -> None:
    store = _write_fake_store(tmp_path)
    dinov2_search.reset_for_tests()
    dinov2_search.load_dinov2_index(store)
    results = dinov2_search.find_similar_listings("L1", k=5)
    assert "L1" not in [lid for lid, _ in results]


def test_find_similar_orders_by_cosine_desc(tmp_path: Path) -> None:
    store = _write_fake_store(tmp_path)
    dinov2_search.reset_for_tests()
    dinov2_search.load_dinov2_index(store)
    results = dinov2_search.find_similar_listings("L1", k=3)
    # Cosines descending.
    cosines = [c for _, c in results]
    assert cosines == sorted(cosines, reverse=True)
    # L3 row 2 has a slight L1 bias; it should rank above L2 for an L1 query.
    ids = [lid for lid, _ in results]
    assert ids.index("L3") < ids.index("L2")


def test_find_similar_unknown_listing_returns_empty(tmp_path: Path) -> None:
    store = _write_fake_store(tmp_path)
    dinov2_search.reset_for_tests()
    dinov2_search.load_dinov2_index(store)
    assert dinov2_search.find_similar_listings("NOT_A_LISTING") == []


def test_find_similar_raises_before_load(tmp_path: Path) -> None:
    dinov2_search.reset_for_tests()
    import pytest

    with pytest.raises(RuntimeError):
        dinov2_search.find_similar_listings("L1")


def test_load_missing_store_raises_filenotfound(tmp_path: Path) -> None:
    dinov2_search.reset_for_tests()
    import pytest

    with pytest.raises(FileNotFoundError):
        dinov2_search.load_dinov2_index(tmp_path / "no_such_dir")
