"""SQLite + .npy store for DINOv2 Tier-1 GeM descriptors.

Mirrors the shape of image_search/common/store.py but is a separate class on
purpose: the DINOv2 store's schema is intentionally LEAN --- it stores only
what's needed to join back to the authoritative SigLIP index by image_id.

Layout:
    data_dir/
      main.fp32.npy        (N_main, 1024) fp32 L2-unit
      floorplans.fp32.npy  (N_floor, 1024) fp32 L2-unit
      index.sqlite         image_id (PK) + index_kind + row_idx

Join contract:
    SELECT siglip.*, dinov2.row_idx
    FROM siglip.images siglip
    JOIN dinov2.images dinov2 USING (image_id)
    WHERE siglip.index_kind IN ('main','floorplan');

Safety-net: add_main_row / add_floorplan_row require correct shape and reject
anything else with ValueError. No label-based safety-net here; label filtering
happened upstream when the SigLIP index was built.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

import numpy as np


SCHEMA = """
CREATE TABLE IF NOT EXISTS images (
    image_id    TEXT PRIMARY KEY,
    index_kind  TEXT NOT NULL,   -- 'main' | 'floorplan'
    row_idx     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_images_index_kind ON images(index_kind);
"""


@dataclass
class Dinov2Row:
    image_id: str


class Dinov2Store:
    """Writer + read helper for the DINOv2 Tier-1 index."""

    def __init__(self, data_dir: Path, projection_dim: int):
        self.data_dir = data_dir
        self.projection_dim = projection_dim
        self.db_path = data_dir / "index.sqlite"
        self.main_npy_path = data_dir / "main.fp32.npy"
        self.floor_npy_path = data_dir / "floorplans.fp32.npy"
        self._main_vecs: list[np.ndarray] = []
        self._floor_vecs: list[np.ndarray] = []
        data_dir.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path)
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    # ---- Writes -----------------------------------------------------------

    def add_main_row(self, row: Dinov2Row, embedding: np.ndarray) -> int:
        return self._insert(row, embedding, index_kind="main",
                            bucket=self._main_vecs)

    def add_floorplan_row(self, row: Dinov2Row, embedding: np.ndarray) -> int:
        return self._insert(row, embedding, index_kind="floorplan",
                            bucket=self._floor_vecs)

    def _insert(self, row: Dinov2Row, embedding: np.ndarray, *,
                index_kind: str, bucket: list[np.ndarray]) -> int:
        if embedding.shape != (self.projection_dim,):
            raise ValueError(
                f"embedding shape {embedding.shape} != "
                f"({self.projection_dim},)"
            )
        if embedding.dtype != np.float32:
            embedding = embedding.astype(np.float32)
        # Hard-check that we were given a (normalized) unit vector. We do not
        # re-normalize here --- upstream encode_images L2-normalizes, and a
        # non-unit vector arriving here is a sign of an upstream bug.
        norm = float(np.linalg.norm(embedding))
        if not (0.999 <= norm <= 1.001):
            raise ValueError(
                f"embedding not L2-unit (||v||={norm:.6f}) --- upstream "
                f"must L2-normalize before calling Dinov2Store"
            )
        row_idx = len(bucket)
        bucket.append(embedding)
        self._conn.execute(
            "INSERT OR REPLACE INTO images(image_id, index_kind, row_idx) "
            "VALUES (?, ?, ?);",
            (row.image_id, index_kind, row_idx),
        )
        return row_idx

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.commit()
        if self._main_vecs:
            arr = np.stack(self._main_vecs, axis=0).astype(np.float32)
            np.save(self.main_npy_path, arr)
        else:
            np.save(self.main_npy_path,
                    np.zeros((0, self.projection_dim), dtype=np.float32))
        if self._floor_vecs:
            arr = np.stack(self._floor_vecs, axis=0).astype(np.float32)
            np.save(self.floor_npy_path, arr)
        else:
            np.save(self.floor_npy_path,
                    np.zeros((0, self.projection_dim), dtype=np.float32))
        self._conn.close()

    def __enter__(self) -> "Dinov2Store":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ---- Reads ------------------------------------------------------------

    def count_by_kind(self) -> dict[str, int]:
        rows = self._conn.execute(
            "SELECT index_kind, COUNT(*) FROM images GROUP BY index_kind;"
        ).fetchall()
        return {k: int(n) for k, n in rows}


def open_readonly(data_dir: Path) -> tuple[sqlite3.Connection, np.ndarray, np.ndarray]:
    db = sqlite3.connect(f"file:{data_dir / 'index.sqlite'}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    main = np.load(data_dir / "main.fp32.npy", mmap_mode="r")
    floor = np.load(data_dir / "floorplans.fp32.npy", mmap_mode="r")
    return db, main, floor
