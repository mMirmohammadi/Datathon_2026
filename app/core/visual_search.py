"""Hybrid-search visual channel: SigLIP-2 Giant over the pre-built image index.

This module holds a process-global singleton of the image-search state: the
loaded model, the memmapped main embedding matrix, and a
(image_source, platform_id) -> [row_idx] map derived from the sqlite index.
Eager-loaded at FastAPI startup via `load_visual_index()`.

Environment flag `LISTINGS_VISUAL_ENABLED` defaults to "1". Set to "0" for
test/CI environments that must not pull the 3.7 GB checkpoint.

Torch / transformers / the image_search.common.* modules are imported lazily
inside `load_visual_index` / `encode_query` so the harness imports cleanly
without `uv sync --group image_search` as long as the visual path is disabled.
"""
from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path
from typing import Any

import numpy as np


SCRAPE_SOURCE_TO_IMAGE_SOURCE: dict[str, str] = {
    "COMPARIS": "structured",
    "ROBINREAL": "robinreal",
    "SRED": "sred",
}

VISUAL_STORE_DIR = Path(__file__).resolve().parents[2] / "image_search" / "data" / "full" / "store"
RRF_K = 60

_LOCK = threading.Lock()
_STATE: dict[str, Any] = {
    "loaded": False,
    "lm": None,
    "main_matrix": None,
    "pid_to_rowids": None,
}


def visual_enabled() -> bool:
    return os.environ.get("LISTINGS_VISUAL_ENABLED", "1") == "1"


def is_loaded() -> bool:
    return bool(_STATE["loaded"])


def reset_for_tests() -> None:
    """Drop the cached model + matrix. Test hook; not called in production."""
    with _LOCK:
        _STATE["loaded"] = False
        _STATE["lm"] = None
        _STATE["main_matrix"] = None
        _STATE["pid_to_rowids"] = None


def load_visual_index(store_dir: Path = VISUAL_STORE_DIR) -> None:
    """Eager load the SigLIP model + embeddings matrix + sqlite index.

    Call this once at app startup (FastAPI lifespan). Raises on failure so
    startup logs the root cause clearly rather than silently running without
    visual ranking.
    """
    with _LOCK:
        if _STATE["loaded"]:
            return
        if not store_dir.exists():
            raise FileNotFoundError(
                f"visual store dir not found: {store_dir}. "
                "Build the image index (see image_search/) or disable with "
                "LISTINGS_VISUAL_ENABLED=0"
            )

        # Lazy imports: only pay the torch / transformers cost when we actually load.
        from image_search.common.model import GIANT_MODEL_ID, load as load_model

        lm = load_model(GIANT_MODEL_ID)
        main_matrix = np.load(store_dir / "embeddings.fp32.npy", mmap_mode="r")

        conn = sqlite3.connect(f"file:{store_dir / 'index.sqlite'}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        pid_to_rowids: dict[tuple[str, str], list[int]] = {}
        for row in conn.execute(
            "SELECT source, platform_id, row_idx FROM images WHERE index_kind='main'"
        ):
            key = (row["source"], row["platform_id"])
            pid_to_rowids.setdefault(key, []).append(int(row["row_idx"]))
        conn.close()

        _STATE["lm"] = lm
        _STATE["main_matrix"] = main_matrix
        _STATE["pid_to_rowids"] = pid_to_rowids
        _STATE["loaded"] = True

        print(
            f"[INFO] visual_index_loaded: model={GIANT_MODEL_ID} "
            f"main_matrix={main_matrix.shape} "
            f"listings_with_images={len(pid_to_rowids)}",
            flush=True,
        )


def encode_query(text: str) -> np.ndarray:
    """Encode one query string into a (projection_dim,) L2-normalized vector."""
    if not _STATE["loaded"]:
        raise RuntimeError(
            "visual_search.encode_query called before load_visual_index()"
        )
    from image_search.common.embed import encode_text

    feats, keep = encode_text([text], _STATE["lm"], context="query")
    if feats.shape[0] == 0 or not bool(keep[0]):
        print(
            f"[WARN] visual_query_encoding: expected=finite 1536-d vector, "
            f"got=NaN or empty for query={text!r}, fallback=raise",
            flush=True,
        )
        raise RuntimeError("visual query encoding produced NaN / empty")
    return feats[0]


def score_candidates(
    query_text: str,
    candidates: list[dict[str, Any]],
) -> dict[str, float]:
    """Compute max-cosine per candidate listing.

    `candidates` rows must carry `listing_id`, `scrape_source`, `platform_id`.
    Returns `{listing_id: max_cosine_float}` for listings with >= 1 image in
    the main index. Listings with no image-index join are omitted (caller
    treats absence as no visual signal, not a filter).
    """
    if not candidates:
        return {}
    if not _STATE["loaded"]:
        raise RuntimeError(
            "visual_search.score_candidates called before load_visual_index()"
        )

    main = _STATE["main_matrix"]
    pid_to_rowids: dict[tuple[str, str], list[int]] = _STATE["pid_to_rowids"]

    # Resolve listing_id -> image-index key, keeping only candidates that have images.
    candidate_keys: dict[str, tuple[str, str]] = {}
    unknown_sources: set[str] = set()
    for row in candidates:
        listing_id = str(row["listing_id"])
        scrape_source = (row.get("scrape_source") or "").upper()
        image_source = SCRAPE_SOURCE_TO_IMAGE_SOURCE.get(scrape_source)
        if image_source is None:
            if scrape_source:
                unknown_sources.add(scrape_source)
            continue
        platform_id = row.get("platform_id")
        if not platform_id:
            continue
        candidate_keys[listing_id] = (image_source, str(platform_id))

    if unknown_sources:
        print(
            f"[WARN] visual_unknown_scrape_source: "
            f"expected=one of {sorted(SCRAPE_SOURCE_TO_IMAGE_SOURCE)}, "
            f"got={sorted(unknown_sources)}, fallback=listings skipped",
            flush=True,
        )

    relevant_rowids: list[int] = []
    rowid_owner: list[str] = []
    for listing_id, key in candidate_keys.items():
        for rid in pid_to_rowids.get(key, ()):
            relevant_rowids.append(rid)
            rowid_owner.append(listing_id)
    if not relevant_rowids:
        return {}

    q_vec = encode_query(query_text)
    subset = np.asarray(main[np.array(relevant_rowids, dtype=np.int64)])
    sims = subset @ q_vec

    out: dict[str, float] = {}
    for listing_id, sim in zip(rowid_owner, sims):
        sim_f = float(sim)
        if sim_f > out.get(listing_id, float("-inf")):
            out[listing_id] = sim_f
    return out


def fuse_rankings(
    rankings: list[list[str]],
    k: int = RRF_K,
) -> dict[str, float]:
    """Reciprocal Rank Fusion over an arbitrary number of rankings.

    Each ranking is a best-first list of listing_ids. Higher = better in the
    returned score dict. When a listing appears multiple times in a single
    ranking only its first position counts (the expected RRF semantics;
    prevents a pathological ranking from self-boosting).
    """
    scores: dict[str, float] = {}
    for ranking in rankings:
        seen: set[str] = set()
        for rank, listing_id in enumerate(ranking, start=1):
            if listing_id in seen:
                continue
            seen.add(listing_id)
            scores[listing_id] = scores.get(listing_id, 0.0) + 1.0 / (k + rank)
    return scores


def fuse_rrf(
    bm25_order: list[str],
    visual_order: list[str],
    k: int = RRF_K,
) -> dict[str, float]:
    """Back-compat two-arg shim. Delegates to :func:`fuse_rankings`."""
    return fuse_rankings([bm25_order, visual_order], k=k)
