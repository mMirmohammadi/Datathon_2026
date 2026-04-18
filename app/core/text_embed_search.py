"""Hybrid-search text channel: Arctic-Embed-L v2 over the pre-built listings matrix.

Thin adapter over :mod:`ranking.runtime.embedding_search` so that `app/` has a
stable call site with the same shape as :mod:`app.core.visual_search`. The
heavy lifting (matrix load, SentenceTransformer model, query-prefix handling)
lives in ``ranking.runtime``; this module just wires the env flag and the
eager-load entry point the lifespan calls.

The index is 25,546 x 1024 fp16 (50 MB on disk; ~100 MB once upcast to fp32
in RAM). It joins to listings by ``listing_id`` via
``data/ranking/embeddings_ids.json``, so no scrape-source mapping is needed
(unlike the visual channel).
"""
from __future__ import annotations

import os
import threading
from typing import Any


_STATE: dict[str, Any] = {"loaded": False, "matrix": None, "ids": None}
_LOCK = threading.Lock()


def text_embed_enabled() -> bool:
    return os.environ.get("LISTINGS_TEXT_EMBED_ENABLED", "1") == "1"


def is_loaded() -> bool:
    return bool(_STATE["loaded"])


def reset_for_tests() -> None:
    """Drop the cached matrix + ids. Test hook; never called in production."""
    with _LOCK:
        _STATE.clear()
        _STATE.update({"loaded": False, "matrix": None, "ids": None})


def load_text_embed_index() -> None:
    """Eager startup load: memmap the matrix + ids and warm the encoder.

    Raises on failure so startup fails loud rather than running with silently
    missing vectors. Operators opt out via ``LISTINGS_TEXT_EMBED_ENABLED=0``.
    """
    with _LOCK:
        if _STATE["loaded"]:
            return
    # Lazy import so the harness can import without sentence-transformers
    # installed (tests + BM25-only deployments).
    from ranking.runtime import embedding_search as _runtime

    matrix, ids = _runtime._lazy_init()
    _runtime._lazy_model()  # warm the encoder too so the first query is fast

    with _LOCK:
        _STATE["matrix"] = matrix
        _STATE["ids"] = ids
        _STATE["loaded"] = True
    print(
        f"[INFO] text_embed_index_loaded: matrix={matrix.shape} "
        f"ids={len(ids)}",
        flush=True,
    )


def score_candidates(
    query_text: str,
    candidates: list[dict[str, Any]],
) -> dict[str, float]:
    """Return {listing_id: cosine} for every candidate that has an embedding.

    Listings absent from ``embeddings_ids.json`` are omitted (caller treats
    absence as no-signal, never as a filter). No raise on underlying failure
    - the ``ranking.runtime`` layer emits ``[WARN]`` and returns empty.
    """
    if not candidates:
        return {}
    if not _STATE["loaded"]:
        raise RuntimeError(
            "text_embed_search.score_candidates called before "
            "load_text_embed_index()"
        )
    from ranking.runtime import embedding_search as _runtime

    listing_ids = [str(c["listing_id"]) for c in candidates]
    raw = _runtime.score_for_listings(query_text, listing_ids)
    return {lid: float(score) for lid, score in raw.items() if score is not None}
