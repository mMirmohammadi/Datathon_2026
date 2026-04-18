from __future__ import annotations

from pathlib import Path

import numpy as np

from image_search.common.store import EmbeddingStore, ImageRow, open_readonly


D = 1536  # matches giant-opt-384, but the store is dim-agnostic


def _unit_vec(seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    v = rng.normal(size=D).astype(np.float32)
    return v / np.linalg.norm(v)


def test_store_roundtrip_main_and_floorplan(tmp_path: Path):
    with EmbeddingStore(tmp_path, projection_dim=D) as store:
        store.register_listing("abc", "robinreal")
        store.add_main_row(
            ImageRow(image_id="robinreal/abc/0",
                     source="robinreal", platform_id="abc", path="/x/0.jpg",
                     relevance_label="interior-room",
                     relevance_confidence=0.9),
            _unit_vec(1),
        )
        store.add_floorplan_row(
            ImageRow(image_id="robinreal/abc/1",
                     source="robinreal", platform_id="abc", path="/x/1.jpg",
                     relevance_label="floorplan",
                     relevance_confidence=0.95),
            _unit_vec(2),
        )
        store.add_dropped_row(
            ImageRow(image_id="robinreal/abc/logo",
                     source="robinreal", platform_id="abc", path="/x/logo.png",
                     relevance_label="logo-or-banner",
                     relevance_confidence=0.8),
        )

    db, main, floor = open_readonly(tmp_path)
    try:
        assert main.shape == (1, D)
        assert floor.shape == (1, D)
        assert np.allclose(main[0], _unit_vec(1), atol=1e-6)
        assert np.allclose(floor[0], _unit_vec(2), atol=1e-6)

        rows = db.execute("SELECT image_id, index_kind, row_idx "
                          "FROM images ORDER BY image_id;").fetchall()
        by_id = {r["image_id"]: dict(r) for r in rows}
        assert by_id["robinreal/abc/0"]["index_kind"] == "main"
        assert by_id["robinreal/abc/0"]["row_idx"] == 0
        assert by_id["robinreal/abc/1"]["index_kind"] == "floorplan"
        assert by_id["robinreal/abc/1"]["row_idx"] == 0
        assert by_id["robinreal/abc/logo"]["index_kind"] == "dropped"
        assert by_id["robinreal/abc/logo"]["row_idx"] is None
    finally:
        db.close()


def test_store_refuses_main_write_for_dropped_class(tmp_path: Path, capsys):
    with EmbeddingStore(tmp_path, projection_dim=D) as store:
        result = store.add_main_row(
            ImageRow(image_id="sneaky/logo",
                     source="robinreal", platform_id="x", path="/x.png",
                     relevance_label="logo-or-banner",
                     relevance_confidence=0.99),
            _unit_vec(3),
        )
    assert result is None
    err = capsys.readouterr().err
    assert "[WARN] store_dropped_class_leaked" in err

    db, main, _ = open_readonly(tmp_path)
    try:
        assert main.shape == (0, D)  # nothing was written to the main index
        n = db.execute("SELECT COUNT(*) FROM images;").fetchone()[0]
        assert n == 0, "leaked write must not create any image row"
    finally:
        db.close()


def test_store_refuses_floorplan_write_for_non_floorplan(tmp_path: Path, capsys):
    with EmbeddingStore(tmp_path, projection_dim=D) as store:
        result = store.add_floorplan_row(
            ImageRow(image_id="pretender",
                     source="robinreal", platform_id="y", path="/y.jpg",
                     relevance_label="interior-room",
                     relevance_confidence=0.9),
            _unit_vec(4),
        )
    assert result is None
    assert "[WARN] store_dropped_class_leaked" in capsys.readouterr().err


def test_store_rejects_wrong_dim_embedding(tmp_path: Path):
    import pytest
    with EmbeddingStore(tmp_path, projection_dim=D) as store:
        with pytest.raises(ValueError, match="embedding shape"):
            store.add_main_row(
                ImageRow(image_id="x", source="sred", platform_id="1",
                         path="/1.jpeg", relevance_label="interior-room",
                         relevance_confidence=0.8),
                np.zeros(100, dtype=np.float32),
            )


def test_store_count_by_kind(tmp_path: Path):
    with EmbeddingStore(tmp_path, projection_dim=D) as store:
        store.add_main_row(
            ImageRow(image_id="m1", source="sred", platform_id="1",
                     path="/1.jpeg", relevance_label="interior-room",
                     relevance_confidence=0.9),
            _unit_vec(1),
        )
        store.add_floorplan_row(
            ImageRow(image_id="f1", source="sred", platform_id="1",
                     path="/2.jpeg", relevance_label="floorplan",
                     relevance_confidence=0.95),
            _unit_vec(2),
        )
        store.add_dropped_row(
            ImageRow(image_id="d1", source="sred", platform_id="1",
                     path="/3.jpeg", relevance_label="logo-or-banner",
                     relevance_confidence=0.7),
        )
        counts = store.count_by_kind()

    assert counts == {"main": 1, "floorplan": 1, "dropped": 1}
