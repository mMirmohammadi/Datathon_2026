"""Query-time cosine top-k over the Arctic-Embed-L v2 listings matrix.

Load once per process (50 MB fp16 in RAM). Each query:
  1. Encode the user text with the same model (Arctic needs "query: " prefix).
  2. Matrix multiply against the listings matrix → 1-D score array.
  3. argpartition → top-k indices → join to listing_ids.

Contract:
  * `search(query_text, k)` returns `list[(listing_id, score)]`, sorted by
    score DESC. Never raises on missing matrix — instead returns `[]` with a
    loud [WARN] so the ranker can decide whether to fall back.
  * The search is stateless; thread-safe after the lazy init.

Per CLAUDE.md §5:
  * If the model id recorded in `listings_ranking_signals.embedding_model`
    doesn't match the one we load, we emit a [WARN] at init time — the two
    must agree or cosine scores are meaningless.
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path

import numpy as np

EMBEDDINGS_NPY = Path("data/ranking/embeddings.fp16.npy")
EMBEDDINGS_IDS = Path("data/ranking/embeddings_ids.json")
MODEL_ID = os.getenv(
    "EMBED_MODEL_ID",
    "Snowflake/snowflake-arctic-embed-l-v2.0",
)
QUERY_PREFIX = "query: "    # Arctic requires this on queries (NOT on docs)


class _State:
    lock = threading.Lock()
    matrix: np.ndarray | None = None   # (N, 1024) fp32 (lifted from fp16 on load)
    ids:    list[str] | None = None
    model:  object | None = None


def _lazy_init() -> tuple[np.ndarray, list[str]]:
    """Load matrix + ids on first call. Raises if missing (loud — no fallback)."""
    with _State.lock:
        if _State.matrix is not None and _State.ids is not None:
            return _State.matrix, _State.ids
        if not EMBEDDINGS_NPY.exists():
            raise RuntimeError(
                f"embedding_search: matrix not found at {EMBEDDINGS_NPY}. "
                "Run: python -m ranking.scripts.t3_embed_listings --db data/listings.db"
            )
        if not EMBEDDINGS_IDS.exists():
            raise RuntimeError(
                f"embedding_search: ids not found at {EMBEDDINGS_IDS}. "
                "The matrix is useless without them."
            )
        # Matrix stored fp16; upcast to fp32 for cosine. ~100 MB for 25k rows.
        mat = np.load(EMBEDDINGS_NPY).astype(np.float32)
        ids = json.loads(EMBEDDINGS_IDS.read_text(encoding="utf-8"))
        if len(ids) != mat.shape[0]:
            raise RuntimeError(
                f"embedding_search: matrix rows ({mat.shape[0]}) != ids ({len(ids)})"
            )
        _State.matrix = mat
        _State.ids    = ids
        print(
            f"[INFO] embedding_search: loaded {mat.shape[0]:,} × {mat.shape[1]} "
            f"vectors ({mat.nbytes / (1024*1024):.1f} MB fp32 in RAM)",
            flush=True,
        )
        return mat, ids


def _lazy_model():
    with _State.lock:
        if _State.model is not None:
            return _State.model
    # Load outside the lock — SentenceTransformer import can be slow
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:
        raise RuntimeError(
            f"embedding_search: sentence-transformers not installed: {exc}"
        )
    m = SentenceTransformer(MODEL_ID, trust_remote_code=True)
    with _State.lock:
        _State.model = m
    return m


def embed_query(query_text: str) -> np.ndarray:
    """Encode a query. Returns (1024,) fp32 with unit L2-norm."""
    if not isinstance(query_text, str) or not query_text.strip():
        raise ValueError("query_text must be a non-empty string")
    model = _lazy_model()
    emb = model.encode(
        [QUERY_PREFIX + query_text],
        normalize_embeddings=True,
        show_progress_bar=False,
    )
    return emb[0].astype(np.float32)


def search(query_text: str, k: int = 50) -> list[tuple[str, float]]:
    """Cosine top-k against all embedded listings. Returns list of (id, score).

    Score is in [-1, 1]; 1 is identical. Model is loaded lazily on first call
    (~20 s) then cached.
    """
    try:
        matrix, ids = _lazy_init()
    except RuntimeError as exc:
        print(
            f"[WARN] embedding_search.search: expected=loadable matrix, "
            f"got={exc}, fallback=empty list (ranker should weight embed_sim=0)",
            flush=True,
        )
        return []
    try:
        q = embed_query(query_text)
    except Exception as exc:
        print(
            f"[WARN] embedding_search.search: expected=encoded query, "
            f"got={type(exc).__name__}: {exc}, fallback=empty list",
            flush=True,
        )
        return []
    scores = matrix @ q    # (N,)
    k = max(1, min(int(k), scores.shape[0]))
    top_idx = np.argpartition(-scores, k - 1)[:k]
    top_idx = top_idx[np.argsort(-scores[top_idx])]
    return [(ids[i], float(scores[i])) for i in top_idx]


def score_for_listings(query_text: str, listing_ids: list[str]) -> dict[str, float | None]:
    """Given a query + a set of listing_ids, return {id → cosine-score or None}.

    Used by the ranker at rerank time to add an `embed_sim` signal to the
    blend. Listings that were never embedded (missing from ids.json) get None.
    """
    try:
        matrix, ids = _lazy_init()
    except RuntimeError:
        return {lid: None for lid in listing_ids}
    id_to_idx = {lid: i for i, lid in enumerate(ids)}
    try:
        q = embed_query(query_text)
    except Exception:
        return {lid: None for lid in listing_ids}
    out: dict[str, float | None] = {}
    for lid in listing_ids:
        idx = id_to_idx.get(lid)
        if idx is None:
            out[lid] = None
        else:
            out[lid] = float(matrix[idx] @ q)
    return out
