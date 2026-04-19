"""Unit tests for dinov2_query functional helpers.

We skip the encode + model tests here (covered by test_dinov2_embed.py) and
focus on: search_topk, aggregate_per_listing, load_dinov2_index metadata join.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pytest

from image_search.common.dinov2_query import (
    Hit,
    RowInfo,
    aggregate_per_listing,
    load_dinov2_index,
    search_topk,
)
from image_search.common.dinov2_store import Dinov2Row, Dinov2Store


D = 4  # keep D small for easy hand-computed expected values


def _unit(v: list[float]) -> np.ndarray:
    a = np.asarray(v, dtype=np.float32)
    return a / np.linalg.norm(a)


def test_search_topk_returns_sorted_top_k():
    M = np.stack([
        _unit([1.0, 0.0, 0.0, 0.0]),
        _unit([0.0, 1.0, 0.0, 0.0]),
        _unit([0.5, 0.5, 0.0, 0.0]),  # cosine 0.707 with q=[1,0,0,0]
        _unit([0.0, 0.0, 1.0, 0.0]),
    ])
    q = _unit([1.0, 0.0, 0.0, 0.0])
    idx, scores = search_topk(q, M, top_k=3)
    assert list(idx) == [0, 2, 1]
    assert scores[0] > scores[1] > scores[2]
    np.testing.assert_allclose(scores[0], 1.0, atol=1e-6)
    np.testing.assert_allclose(scores[1], 1.0 / np.sqrt(2.0), atol=1e-6)


def test_search_topk_handles_k_larger_than_n():
    M = np.stack([_unit([1.0, 0.0, 0.0, 0.0]), _unit([0.0, 1.0, 0.0, 0.0])])
    q = _unit([1.0, 0.0, 0.0, 0.0])
    idx, scores = search_topk(q, M, top_k=10)
    assert idx.shape[0] == 2
    assert list(idx) == [0, 1]


def test_search_topk_empty_matrix():
    M = np.zeros((0, D), dtype=np.float32)
    q = _unit([1.0, 0.0, 0.0, 0.0])
    idx, scores = search_topk(q, M, top_k=5)
    assert idx.shape == (0,)
    assert scores.shape == (0,)


def test_aggregate_per_listing_max_pools_by_source_platform():
    """Same listing with multiple high-scoring images should appear once,
    with the max score and its best image_id."""
    rows = [
        RowInfo(image_id="a#c0", source="sred", platform_id="1",
                path="/1.jpeg", relevance_label="interior-room",
                index_kind="main"),
        RowInfo(image_id="a#c1", source="sred", platform_id="1",
                path="/1.jpeg", relevance_label="interior-room",
                index_kind="main"),
        RowInfo(image_id="b", source="robinreal", platform_id="2",
                path="/2.jpg", relevance_label="interior-room",
                index_kind="main"),
    ]
    indices = np.array([0, 1, 2], dtype=np.int64)
    scores = np.array([0.8, 0.9, 0.7], dtype=np.float32)

    hits = aggregate_per_listing(indices, scores, rows, top_k_listings=10)
    assert len(hits) == 2
    # First hit should be listing ('sred','1') with best image_id=a#c1 score 0.9
    assert hits[0].info.source == "sred"
    assert hits[0].info.platform_id == "1"
    assert hits[0].info.image_id == "a#c1"
    assert np.isclose(hits[0].score, 0.9)
    # Second hit should be listing ('robinreal','2') with score 0.7
    assert hits[1].info.platform_id == "2"
    assert np.isclose(hits[1].score, 0.7)


def test_aggregate_per_listing_respects_top_k_listings():
    rows = [
        RowInfo(image_id=f"i{i}", source="s", platform_id=str(i),
                path="/x", relevance_label="interior-room", index_kind="main")
        for i in range(5)
    ]
    indices = np.arange(5, dtype=np.int64)
    scores = np.array([0.5, 0.9, 0.1, 0.7, 0.3], dtype=np.float32)
    hits = aggregate_per_listing(indices, scores, rows, top_k_listings=2)
    assert len(hits) == 2
    assert [h.info.platform_id for h in hits] == ["1", "3"]


def test_load_dinov2_index_joins_metadata(tmp_path: Path):
    """End-to-end: write a tiny SigLIP-shaped sqlite + a Dinov2Store with
    matching image_ids, then load_dinov2_index must join metadata correctly."""
    # Build a minimal SigLIP index
    sl_path = tmp_path / "siglip.sqlite"
    con = sqlite3.connect(sl_path)
    con.executescript("""
        CREATE TABLE images (
            image_id TEXT PRIMARY KEY,
            source TEXT, platform_id TEXT, path TEXT, sred_cell INTEGER,
            relevance_label TEXT, relevance_confidence REAL,
            relevance_margin REAL, index_kind TEXT, row_idx INTEGER
        );
    """)
    rows = [
        ("a", "sred", "1", "/1.jpeg", 0, "interior-room", 0.9, 0.5, "main", 0),
        ("b", "robinreal", "2", "/2.jpg", None, "interior-room", 0.9, 0.5, "main", 1),
        ("f", "sred", "3", "/3.jpeg", 1, "floorplan", 0.9, 0.5, "floorplan", 0),
        ("dropped", "sred", "4", "/4.jpeg", 2, "logo-or-banner", 0.8, 0.3, "dropped", None),
    ]
    con.executemany("INSERT INTO images VALUES (?,?,?,?,?,?,?,?,?,?);", rows)
    con.commit()
    con.close()

    # Build a matching Dinov2Store
    dv_dir = tmp_path / "dinov2"
    D_full = 1024
    v1 = np.random.default_rng(0).standard_normal(D_full).astype(np.float32)
    v1 /= np.linalg.norm(v1)
    v2 = np.random.default_rng(1).standard_normal(D_full).astype(np.float32)
    v2 /= np.linalg.norm(v2)
    vf = np.random.default_rng(2).standard_normal(D_full).astype(np.float32)
    vf /= np.linalg.norm(vf)
    with Dinov2Store(dv_dir, projection_dim=D_full) as store:
        store.add_main_row(Dinov2Row(image_id="a"), v1)
        store.add_main_row(Dinov2Row(image_id="b"), v2)
        store.add_floorplan_row(Dinov2Row(image_id="f"), vf)
    (dv_dir / "build_report.json").write_text("{}")

    loaded = load_dinov2_index(dv_dir, sl_path)
    assert loaded.main_matrix.shape == (2, D_full)
    assert loaded.floor_matrix.shape == (1, D_full)

    assert loaded.main_row_info[0].image_id == "a"
    assert loaded.main_row_info[0].source == "sred"
    assert loaded.main_row_info[0].platform_id == "1"
    assert loaded.main_row_info[0].relevance_label == "interior-room"
    assert loaded.main_row_info[0].index_kind == "main"

    assert loaded.main_row_info[1].image_id == "b"
    assert loaded.main_row_info[1].platform_id == "2"

    assert loaded.floor_row_info[0].image_id == "f"
    assert loaded.floor_row_info[0].index_kind == "floorplan"


def test_load_dinov2_index_detects_desync(tmp_path: Path):
    """If the DINOv2 store references an image_id the SigLIP index doesn't
    know about, we must hard-fail --- silently populating None in a list and
    ranking against junk later is exactly the kind of bug this test catches."""
    sl_path = tmp_path / "siglip.sqlite"
    con = sqlite3.connect(sl_path)
    con.executescript("""
        CREATE TABLE images (
            image_id TEXT PRIMARY KEY,
            source TEXT, platform_id TEXT, path TEXT, sred_cell INTEGER,
            relevance_label TEXT, relevance_confidence REAL,
            relevance_margin REAL, index_kind TEXT, row_idx INTEGER
        );
        -- intentionally no 'a' row here
    """)
    con.commit()
    con.close()

    dv_dir = tmp_path / "dinov2"
    D_full = 1024
    v = np.ones(D_full, dtype=np.float32)
    v /= np.linalg.norm(v)
    with Dinov2Store(dv_dir, projection_dim=D_full) as store:
        store.add_main_row(Dinov2Row(image_id="a"), v)
    (dv_dir / "build_report.json").write_text("{}")

    with pytest.raises(KeyError, match="missing from SigLIP index"):
        load_dinov2_index(dv_dir, sl_path)
