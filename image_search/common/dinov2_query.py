"""Library-level query helpers for the DINOv2 Tier-1 index.

Provides three logical operations:

  1. ``load_dinov2_index(data_dir)``
        Opens the DINOv2 store read-only, loads the main + floorplan
        matrices, and builds a per-row metadata lookup by joining the
        DINOv2 index to the SigLIP index by image_id (so callers get the
        source, platform_id, path, relevance_label without having to
        maintain a second copy).

  2. ``encode_query_image(pil_image, lm)``
        Runs a single user-uploaded image through the same eval transform +
        GeM pipeline the indexer uses. Returns a 1024-d fp32 L2-unit vector.

  3. ``search(query_vec, main_matrix, top_k)``
        Cosine top-K against the main matrix. The matrix must be L2-unit
        (verified at index-build time), so we use a single matmul.

  4. ``aggregate_per_listing(hits, row_info)``
        Collapses per-image scores to per-listing (``source``, ``platform_id``)
        scores by max-pooling --- matching the convention in
        image_search/scripts/query.py.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from PIL import Image

from image_search.common.dinov2_embed import encode_images
from image_search.common.dinov2_model import LoadedDinov2


@dataclass
class RowInfo:
    image_id: str
    source: str
    platform_id: str
    path: str
    relevance_label: str
    index_kind: str  # 'main' or 'floorplan'


@dataclass
class LoadedIndex:
    data_dir: Path
    main_matrix: np.ndarray            # (N_main, 1024) fp32, L2-unit
    floor_matrix: np.ndarray           # (N_floor, 1024) fp32, L2-unit
    main_row_info: list[RowInfo]       # index main_row_info[row_idx] == info
    floor_row_info: list[RowInfo]      # index floor_row_info[row_idx] == info


@dataclass
class Hit:
    row_idx: int
    score: float
    info: RowInfo


def load_dinov2_index(
    data_dir: Path,
    siglip_index_path: Path,
    *,
    mmap: bool = True,
) -> LoadedIndex:
    """Load both matrices + joined metadata. Read-only on both SQLite files."""
    main = np.load(data_dir / "main.fp32.npy",
                   mmap_mode="r" if mmap else None)
    floor = np.load(data_dir / "floorplans.fp32.npy",
                    mmap_mode="r" if mmap else None)

    dv = sqlite3.connect(f"file:{data_dir / 'index.sqlite'}?mode=ro", uri=True)
    dv.row_factory = sqlite3.Row
    sl = sqlite3.connect(f"file:{siglip_index_path}?mode=ro", uri=True)
    sl.row_factory = sqlite3.Row

    # Build a dict from image_id -> SigLIP metadata
    sl_meta = {}
    for r in sl.execute(
        "SELECT image_id, source, platform_id, path, relevance_label "
        "FROM images WHERE index_kind IN ('main','floorplan');"
    ):
        sl_meta[r["image_id"]] = {
            "source": r["source"],
            "platform_id": r["platform_id"],
            "path": r["path"],
            "relevance_label": r["relevance_label"],
        }
    sl.close()

    # Populate ordered lists by DINOv2 row_idx
    main_info: list[RowInfo | None] = [None] * main.shape[0]
    floor_info: list[RowInfo | None] = [None] * floor.shape[0]
    for r in dv.execute("SELECT image_id, index_kind, row_idx FROM images;"):
        meta = sl_meta.get(r["image_id"])
        if meta is None:
            # Must never happen --- verifier already enforces subset. Hard-fail.
            raise KeyError(
                f"DINOv2 image_id {r['image_id']!r} missing from SigLIP index"
            )
        info = RowInfo(image_id=r["image_id"], index_kind=r["index_kind"],
                       **meta)
        if r["index_kind"] == "main":
            main_info[r["row_idx"]] = info
        else:
            floor_info[r["row_idx"]] = info
    dv.close()

    if any(x is None for x in main_info):
        n = sum(1 for x in main_info if x is None)
        raise RuntimeError(
            f"{n} main rows missing metadata --- DINOv2 store and "
            f"SigLIP index are desynchronized"
        )
    if any(x is None for x in floor_info):
        n = sum(1 for x in floor_info if x is None)
        raise RuntimeError(
            f"{n} floorplan rows missing metadata --- DINOv2 store and "
            f"SigLIP index are desynchronized"
        )

    return LoadedIndex(
        data_dir=data_dir,
        main_matrix=main,
        floor_matrix=floor,
        main_row_info=main_info,        # type: ignore[arg-type]
        floor_row_info=floor_info,      # type: ignore[arg-type]
    )


def encode_query_image(pil_image: Image.Image, lm: LoadedDinov2) -> np.ndarray:
    """Encode a single PIL image. Returns a (D,) fp32 L2-unit numpy array.

    Raises RuntimeError if the input produces a NaN/inf embedding (a
    silent NaN vector would otherwise score 0 against everything and give
    meaningless retrieval results).
    """
    feats, keep = encode_images([pil_image], lm, context="dinov2_query")
    if feats.shape[0] != 1:
        raise RuntimeError(f"encode returned {feats.shape[0]} rows, expected 1")
    if not keep[0]:
        raise RuntimeError(
            "query image produced a NaN/inf embedding --- see the "
            "[WARN] nan_embedding_dinov2 log line above"
        )
    return feats[0]


def search_topk(
    query_vec: np.ndarray,
    matrix: np.ndarray,
    top_k: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Cosine top-K (= dot-product, both sides L2-unit).

    Returns (indices, scores) both of length min(top_k, matrix.shape[0]).
    indices are sorted by descending score.
    """
    if matrix.shape[0] == 0:
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.float32)
    scores = matrix @ query_vec  # (N,)
    k = min(int(top_k), matrix.shape[0])
    # argpartition for O(N) top-k then sort the top-k slice
    idx = np.argpartition(-scores, k - 1)[:k]
    order = np.argsort(-scores[idx])
    idx = idx[order]
    return idx, scores[idx]


def aggregate_per_listing(
    indices: np.ndarray,
    scores: np.ndarray,
    row_info: list[RowInfo],
    *,
    top_k_listings: int,
) -> list[Hit]:
    """Max-pool per (source, platform_id). Returns top_k_listings Hits,
    each carrying the best image_id + score per listing."""
    best: dict[tuple[str, str], Hit] = {}
    for i, s in zip(indices.tolist(), scores.tolist()):
        info = row_info[int(i)]
        key = (info.source, info.platform_id)
        cur = best.get(key)
        if cur is None or s > cur.score:
            best[key] = Hit(row_idx=int(i), score=float(s), info=info)
    ranked = sorted(best.values(), key=lambda h: -h.score)
    return ranked[:top_k_listings]
