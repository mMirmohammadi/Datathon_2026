"""DINOv2 reverse-image similarity — image-to-image only.

Tier 4 of the demo plan. The DINOv2 store at
``image_search/data/full/dinov2_store/`` contains 1024-d global descriptors
(ViT-L/14 + GeM pooling, L2-normalized) for 70,548 main images + 617
floorplan images, built in Apr 2026 and verified with 22/22 invariants.

This module EXPOSES that store to the live API. Unlike SigLIP-2, DINOv2 is
vision-only — there is no text encoder — so this channel only handles
"find me listings that look like THIS listing". That maps to the natural UI
of a "Find similar" button on every card, not a text query.

Design:
  * Lazy singleton load (mmap matrix + SQLite index → in-memory dicts) on
    first ``find_similar_listings`` call. The main matrix is 289 MB; kept as
    a numpy memmap so the page cache handles warm lookups.
  * Query vector = mean of the source listing's main-image embeddings,
    renormalised. This is consistent with the cohort-centroid pattern the
    memory.visual channel already uses.
  * Similarity = max cosine per candidate listing (one strong image beats
    three mediocre ones, same aggregation choice as ``visual_search``).
  * Env flag ``LISTINGS_DINOV2_ENABLED`` (default on). Off at test / CI so
    unit tests don't mmap the matrix on every collection.
"""
from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path
from typing import Any

import numpy as np


DINOV2_STORE_DIR = (
    Path(__file__).resolve().parents[2] / "image_search" / "data" / "full" / "dinov2_store"
)


_LOCK = threading.Lock()
_STATE: dict[str, Any] = {
    "loaded": False,
    "matrix": None,           # (n_main, 1024) fp32 memmap
    "image_ids": None,        # list[str] ordered by row_idx
    "listing_to_rowids": None,  # {listing_id: [row_idx, ...]}
    "rowid_to_listing": None,   # [row_idx] -> listing_id
}


def dinov2_enabled() -> bool:
    return os.environ.get("LISTINGS_DINOV2_ENABLED", "1") == "1"


def is_loaded() -> bool:
    return bool(_STATE["loaded"])


def reset_for_tests() -> None:
    with _LOCK:
        for key in _STATE:
            _STATE[key] = False if key == "loaded" else None
        _STATE["loaded"] = False


def _parse_listing_id(image_id: str) -> str | None:
    """DINOv2 image_id looks like ``'source/platform_id/idx-hash'``.

    platform_id is the listing identifier used by the listings DB — we only
    keep the middle segment. Returns ``None`` if the id doesn't parse.
    """
    parts = image_id.split("/", 2)
    if len(parts) < 2:
        return None
    return parts[1] or None


def load_dinov2_index(store_dir: Path = DINOV2_STORE_DIR) -> None:
    """Eager startup load of the main-image matrix + platform-id map.

    Builds ``listing_id -> [row_idx]`` by splitting each image_id. Floorplans
    are intentionally excluded from the similarity search — we want building
    / interior look-alikes, not plan look-alikes.
    """
    with _LOCK:
        if _STATE["loaded"]:
            return
        if not store_dir.exists():
            raise FileNotFoundError(
                f"DINOv2 store dir not found: {store_dir}. Build via "
                f"image_search.scripts.build_dinov2_store or disable with "
                f"LISTINGS_DINOV2_ENABLED=0."
            )

        main_path = store_dir / "main.fp32.npy"
        if not main_path.exists():
            raise FileNotFoundError(f"Missing {main_path}")
        matrix = np.load(main_path, mmap_mode="r")

        conn = sqlite3.connect(f"file:{store_dir / 'index.sqlite'}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        image_ids: list[str | None] = [None] * matrix.shape[0]
        listing_to_rowids: dict[str, list[int]] = {}
        rowid_to_listing: list[str | None] = [None] * matrix.shape[0]
        for row in conn.execute(
            "SELECT image_id, row_idx FROM images WHERE index_kind='main'"
        ):
            row_idx = int(row["row_idx"])
            image_id = str(row["image_id"])
            if not (0 <= row_idx < matrix.shape[0]):
                continue
            image_ids[row_idx] = image_id
            listing_id = _parse_listing_id(image_id)
            if listing_id:
                listing_to_rowids.setdefault(listing_id, []).append(row_idx)
                rowid_to_listing[row_idx] = listing_id
        conn.close()

        _STATE["matrix"] = matrix
        _STATE["image_ids"] = image_ids
        _STATE["listing_to_rowids"] = listing_to_rowids
        _STATE["rowid_to_listing"] = rowid_to_listing
        _STATE["loaded"] = True

        print(
            f"[INFO] dinov2_index_loaded: store={store_dir} "
            f"matrix={matrix.shape} dtype={matrix.dtype} "
            f"listings_with_images={len(listing_to_rowids)}",
            flush=True,
        )


def _query_vector_for_listing(listing_id: str) -> np.ndarray | None:
    """Average the listing's main-image embeddings → one normalised query vec.

    Returns ``None`` if the listing has no images in the DINOv2 store (e.g.
    the listing was dropped during triage or has no photos at all).
    """
    listing_to_rowids: dict[str, list[int]] = _STATE["listing_to_rowids"]
    rowids = listing_to_rowids.get(listing_id)
    if not rowids:
        return None
    matrix = _STATE["matrix"]
    sub = np.asarray(matrix[np.array(rowids, dtype=np.int64)], dtype=np.float32)
    if sub.size == 0:
        return None
    mean = sub.mean(axis=0)
    norm = float(np.linalg.norm(mean))
    if norm < 1e-8:
        return None
    return mean / norm


def find_similar_listings(
    listing_id: str,
    *,
    k: int = 10,
    exclude_self: bool = True,
) -> list[tuple[str, float]]:
    """Return ``[(similar_listing_id, max_cosine), ...]`` sorted by
    cosine descending. Excludes the query listing itself by default.

    Raises ``RuntimeError`` if the index has not been loaded yet
    (loader failure must be loud, not silent).
    """
    if not _STATE["loaded"]:
        raise RuntimeError(
            "dinov2_search.find_similar_listings called before "
            "load_dinov2_index()"
        )
    query = _query_vector_for_listing(listing_id)
    if query is None:
        return []

    matrix = _STATE["matrix"]
    rowid_to_listing: list[str | None] = _STATE["rowid_to_listing"]

    # Cosine: matrix rows are already unit-normalised (verification.json
    # confirmed 22/22 invariants at build time); one dot product suffices.
    scores = (matrix @ query).astype(np.float32, copy=False)

    # Aggregate to max-per-listing.
    best: dict[str, float] = {}
    for row_idx in range(scores.shape[0]):
        other = rowid_to_listing[row_idx]
        if other is None:
            continue
        if exclude_self and other == listing_id:
            continue
        s = float(scores[row_idx])
        prev = best.get(other)
        if prev is None or s > prev:
            best[other] = s

    # Sort and trim.
    ranked = sorted(best.items(), key=lambda item: -item[1])
    return ranked[: max(1, int(k))]
