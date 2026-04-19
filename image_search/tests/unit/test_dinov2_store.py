from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from image_search.common.dinov2_store import Dinov2Row, Dinov2Store, open_readonly


D = 1024  # ViT-L/14 embed dim


def _unit(seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    v = rng.normal(size=D).astype(np.float32)
    return v / np.linalg.norm(v)


def test_store_roundtrip_main_and_floorplan(tmp_path: Path):
    with Dinov2Store(tmp_path, projection_dim=D) as store:
        i0 = store.add_main_row(Dinov2Row(image_id="a"), _unit(1))
        i1 = store.add_main_row(Dinov2Row(image_id="b"), _unit(2))
        j0 = store.add_floorplan_row(Dinov2Row(image_id="c"), _unit(3))

    assert i0 == 0 and i1 == 1 and j0 == 0

    db, main, floor = open_readonly(tmp_path)
    try:
        assert main.shape == (2, D)
        assert floor.shape == (1, D)
        np.testing.assert_allclose(main[0], _unit(1), atol=1e-6)
        np.testing.assert_allclose(main[1], _unit(2), atol=1e-6)
        np.testing.assert_allclose(floor[0], _unit(3), atol=1e-6)

        rows = {r["image_id"]: dict(r) for r in
                db.execute("SELECT * FROM images;").fetchall()}
        assert rows["a"] == {"image_id": "a", "index_kind": "main", "row_idx": 0}
        assert rows["b"] == {"image_id": "b", "index_kind": "main", "row_idx": 1}
        assert rows["c"] == {"image_id": "c", "index_kind": "floorplan",
                             "row_idx": 0}
    finally:
        db.close()


def test_store_rejects_wrong_dim(tmp_path: Path):
    with Dinov2Store(tmp_path, projection_dim=D) as store:
        with pytest.raises(ValueError, match="embedding shape"):
            store.add_main_row(Dinov2Row(image_id="x"), np.zeros(100, dtype=np.float32))


def test_store_rejects_non_unit_embedding(tmp_path: Path):
    """The store is the last line of defense against a bug in GeM/normalize
    upstream. A non-unit vector must hard-fail."""
    v = _unit(7) * 2.0  # length 2, not 1
    with Dinov2Store(tmp_path, projection_dim=D) as store:
        with pytest.raises(ValueError, match="not L2-unit"):
            store.add_main_row(Dinov2Row(image_id="bad"), v)


def test_store_counts_by_kind(tmp_path: Path):
    with Dinov2Store(tmp_path, projection_dim=D) as store:
        for i in range(3):
            store.add_main_row(Dinov2Row(image_id=f"m{i}"), _unit(i + 10))
        for i in range(2):
            store.add_floorplan_row(Dinov2Row(image_id=f"f{i}"), _unit(i + 20))
        counts = store.count_by_kind()
    assert counts == {"main": 3, "floorplan": 2}


def test_store_empty_commits_empty_arrays(tmp_path: Path):
    """No writes -> on close, empty .npy files must still be emitted so
    downstream (open_readonly, verifier) doesn't crash on a partial run."""
    with Dinov2Store(tmp_path, projection_dim=D) as _:
        pass
    db, main, floor = open_readonly(tmp_path)
    try:
        assert main.shape == (0, D)
        assert floor.shape == (0, D)
        n = db.execute("SELECT COUNT(*) FROM images;").fetchone()[0]
        assert n == 0
    finally:
        db.close()


def test_store_upcasts_fp64_to_fp32(tmp_path: Path):
    """Caller may pass fp64 by accident; store must downcast, not crash."""
    v = _unit(5).astype(np.float64)
    with Dinov2Store(tmp_path, projection_dim=D) as store:
        store.add_main_row(Dinov2Row(image_id="fp64"), v)
    _, main, _ = open_readonly(tmp_path)
    assert main.dtype == np.float32
    np.testing.assert_allclose(main[0], v.astype(np.float32), atol=1e-6)


def test_store_row_idx_monotonic(tmp_path: Path):
    """row_idx must be 0, 1, 2, ... with no gaps --- used as an array index."""
    with Dinov2Store(tmp_path, projection_dim=D) as store:
        for i in range(5):
            idx = store.add_main_row(Dinov2Row(image_id=f"m{i}"), _unit(i))
            assert idx == i
