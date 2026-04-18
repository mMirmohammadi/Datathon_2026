"""SQLite index + numpy .npy embedding store.

Two separate embedding stores (per user directive — floorplans live in their
own index so they don't pollute the 'big bright modern house' retrieval space):

    data/embeddings.fp32.npy      shape (N_main, D)
    data/floorplans.fp32.npy      shape (N_floor, D)
    data/index.sqlite             per-image rows + per-listing rows

Safety-net: the only way to write a main-index row is via add_main_row() which
refuses anything whose label is not in MAIN_INDEX_CLASSES, emitting
[WARN] store_dropped_class_leaked on violation. The floorplan path mirrors this.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from image_search.common.prompts import FLOORPLAN_CLASSES, MAIN_INDEX_CLASSES
from image_search.common.warn import warn


SCHEMA = """
CREATE TABLE IF NOT EXISTS listings (
    platform_id TEXT NOT NULL,
    source      TEXT NOT NULL,
    PRIMARY KEY (platform_id, source)
);

CREATE TABLE IF NOT EXISTS images (
    image_id             TEXT PRIMARY KEY,
    source               TEXT NOT NULL,
    platform_id          TEXT NOT NULL,
    path                 TEXT NOT NULL,
    sred_cell            INTEGER,
    relevance_label      TEXT NOT NULL,
    relevance_confidence REAL NOT NULL,
    relevance_margin     REAL,
    index_kind           TEXT NOT NULL,   -- 'main' | 'floorplan' | 'dropped'
    row_idx              INTEGER
);

CREATE INDEX IF NOT EXISTS idx_images_listing
    ON images(source, platform_id);
CREATE INDEX IF NOT EXISTS idx_images_index_kind
    ON images(index_kind);
"""


@dataclass
class ImageRow:
    image_id: str
    source: str
    platform_id: str
    path: str
    relevance_label: str
    relevance_confidence: float
    relevance_margin: float | None = None
    sred_cell: int | None = None


class EmbeddingStore:
    def __init__(self, data_dir: Path, projection_dim: int):
        self.data_dir = data_dir
        self.projection_dim = projection_dim
        self.db_path = data_dir / "index.sqlite"
        self.main_npy_path = data_dir / "embeddings.fp32.npy"
        self.floor_npy_path = data_dir / "floorplans.fp32.npy"
        self._main_vecs: list[np.ndarray] = []
        self._floor_vecs: list[np.ndarray] = []
        data_dir.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path)
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    # ---- Writes -----------------------------------------------------------

    def register_listing(self, platform_id: str, source: str) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO listings(platform_id, source) VALUES (?, ?);",
            (platform_id, source),
        )

    def add_main_row(self, row: ImageRow, embedding: np.ndarray) -> int | None:
        if row.relevance_label not in MAIN_INDEX_CLASSES:
            warn("store_dropped_class_leaked", image_id=row.image_id,
                 label=row.relevance_label, expected="main-index kept class",
                 fallback="refused_write")
            return None
        return self._insert(row, embedding, index_kind="main",
                            bucket=self._main_vecs)

    def add_floorplan_row(self, row: ImageRow, embedding: np.ndarray) -> int | None:
        if row.relevance_label not in FLOORPLAN_CLASSES:
            warn("store_dropped_class_leaked", image_id=row.image_id,
                 label=row.relevance_label, expected="floorplan",
                 fallback="refused_write")
            return None
        return self._insert(row, embedding, index_kind="floorplan",
                            bucket=self._floor_vecs)

    def add_dropped_row(self, row: ImageRow) -> None:
        """Record that we saw an image but dropped it (no embedding stored)."""
        self._conn.execute(
            """INSERT OR REPLACE INTO images(
                   image_id, source, platform_id, path, sred_cell,
                   relevance_label, relevance_confidence, relevance_margin,
                   index_kind, row_idx)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""",
            (row.image_id, row.source, row.platform_id, row.path, row.sred_cell,
             row.relevance_label, row.relevance_confidence, row.relevance_margin,
             "dropped", None),
        )

    def _insert(self, row: ImageRow, embedding: np.ndarray, *,
                index_kind: str, bucket: list[np.ndarray]) -> int:
        if embedding.shape != (self.projection_dim,):
            raise ValueError(
                f"embedding shape {embedding.shape} != ({self.projection_dim},)"
            )
        if embedding.dtype != np.float32:
            embedding = embedding.astype(np.float32)
        row_idx = len(bucket)
        bucket.append(embedding)
        self._conn.execute(
            """INSERT OR REPLACE INTO images(
                   image_id, source, platform_id, path, sred_cell,
                   relevance_label, relevance_confidence, relevance_margin,
                   index_kind, row_idx)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""",
            (row.image_id, row.source, row.platform_id, row.path, row.sred_cell,
             row.relevance_label, row.relevance_confidence, row.relevance_margin,
             index_kind, row_idx),
        )
        return row_idx

    # ---- Finalize ---------------------------------------------------------

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.commit()
        # Persist embedding arrays as .npy so downstream tooling can mmap them.
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

    def __enter__(self) -> "EmbeddingStore":
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
    main = np.load(data_dir / "embeddings.fp32.npy", mmap_mode="r")
    floor = np.load(data_dir / "floorplans.fp32.npy", mmap_mode="r")
    return db, main, floor
