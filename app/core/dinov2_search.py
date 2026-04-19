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
from PIL import Image


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

_MODEL_LOCK = threading.Lock()
_MODEL_STATE: dict[str, Any] = {"lm": None}


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


def _load_dinov2_model_once() -> Any:
    """Lazy-load the DINOv2 ViT-L/14 reg encoder on first upload call.

    The index loader only needs the pre-computed matrix (fp32 memmap), so at
    server start we skip pulling the 1.13 GB backbone. The arbitrary-photo
    upload path needs the live encoder, so we load on first use. ~22 s cold
    model load on CUDA; after that every call is a single forward + matmul
    (sub-100 ms typical on an RTX 5090).
    """
    with _MODEL_LOCK:
        lm = _MODEL_STATE.get("lm")
        if lm is not None:
            return lm
        # Lazy import so the harness imports clean without torch / torchvision
        # when the DINOv2 channel is disabled.
        from image_search.common.dinov2_model import load as load_model
        lm = load_model()
        _MODEL_STATE["lm"] = lm
        print(
            f"[INFO] dinov2_model_loaded_for_upload: entry={lm.entry} "
            f"device={lm.device} dtype={lm.dtype}",
            flush=True,
        )
        return lm


def _encode_query_image_to_vec(pil_image: Image.Image) -> np.ndarray:
    """Encode a PIL image → 1024-d L2-unit fp32 numpy vector via DINOv2.

    Shared by :func:`find_similar_by_image` (image-only reverse search) and
    :func:`score_candidates_for_image` (text+image hybrid ranking).
    """
    from image_search.common.dinov2_query import encode_query_image

    lm = _load_dinov2_model_once()
    return encode_query_image(pil_image, lm)


def score_candidates_for_image(
    pil_image: Image.Image,
    candidates: list[dict[str, Any]],
    *,
    return_best_image_ids: bool = False,
) -> (
    dict[str, float]
    | tuple[dict[str, float], dict[str, str]]
):
    """Return ``{listing_id: max_cosine}`` for each candidate that has images
    in the DINOv2 store. Caller treats absence as no-signal (same contract as
    ``text_embed_search.score_candidates``).

    Used by ``_rerank_hybrid`` to fuse a text query + an uploaded photo: the
    image channel contributes one ranking (by descending cosine) to the RRF
    alongside BM25 / Arctic / SigLIP / soft / memory. Listings without images
    are omitted from THIS ranking only — they can still win on other channels.

    When ``return_best_image_ids=True`` the call also returns
    ``{listing_id: best_image_id}`` — the ``image_id`` whose embedding scored
    highest, so callers can reorder the listing's images to show the one that
    actually matched first. Identity only; no URL lookup.
    """
    if not candidates:
        return ({}, {}) if return_best_image_ids else {}
    if not _STATE["loaded"]:
        raise RuntimeError(
            "dinov2_search.score_candidates_for_image called before "
            "load_dinov2_index()"
        )
    listing_to_rowids: dict[str, list[int]] = _STATE["listing_to_rowids"]
    matrix = _STATE["matrix"]
    image_ids: list[str | None] = _STATE["image_ids"]

    # Collect every row_idx that belongs to a candidate we need to score;
    # one matmul over the subset is cheaper than per-listing subset matmuls.
    row_to_lid: dict[int, str] = {}
    for c in candidates:
        lid = str(c["listing_id"])
        for row_idx in listing_to_rowids.get(lid, ()):
            row_to_lid[int(row_idx)] = lid
    if not row_to_lid:
        return ({}, {}) if return_best_image_ids else {}

    query_vec = _encode_query_image_to_vec(pil_image)
    row_idxs = np.fromiter(row_to_lid.keys(), dtype=np.int64)
    sub = np.asarray(matrix[row_idxs], dtype=np.float32)
    scores = (sub @ query_vec).astype(np.float32, copy=False)

    best: dict[str, float] = {}
    best_row: dict[str, int] = {}
    for i, row_idx in enumerate(row_idxs.tolist()):
        lid = row_to_lid[int(row_idx)]
        s = float(scores[i])
        prev = best.get(lid)
        if prev is None or s > prev:
            best[lid] = s
            best_row[lid] = int(row_idx)
    if not return_best_image_ids:
        return best
    best_image_ids = {
        lid: (image_ids[rid] if 0 <= rid < len(image_ids) else "") or ""
        for lid, rid in best_row.items()
    }
    return best, best_image_ids


def _best_image_id_vs_vector(
    platform_id: str,
    query_vec: np.ndarray,
) -> str | None:
    """For a single listing, find the image_id whose DINOv2 vector has the
    highest cosine with ``query_vec``. Returns None if the listing has no
    images in the store.

    Used by the find-similar-listings flow to pick which photo of each result
    to surface first on the card (the one that actually drove the match).
    """
    if not _STATE["loaded"]:
        return None
    listing_to_rowids: dict[str, list[int]] = _STATE["listing_to_rowids"]
    rowids = listing_to_rowids.get(platform_id)
    if not rowids:
        return None
    matrix = _STATE["matrix"]
    image_ids: list[str | None] = _STATE["image_ids"]
    sub = np.asarray(matrix[np.array(rowids, dtype=np.int64)], dtype=np.float32)
    scores = (sub @ query_vec).astype(np.float32, copy=False)
    best_local = int(np.argmax(scores))
    best_rowid = rowids[best_local]
    if 0 <= best_rowid < len(image_ids):
        return image_ids[best_rowid]
    return None


def _arctic_scores_for_listing(
    listing_id: str,
    *,
    top_n: int = 500,
) -> dict[str, float]:
    """Semantic ``{other_id: cosine}`` using the query listing's own Arctic row.

    Avoids a round-trip through the text encoder — the listing already has a
    precomputed 1024-d vector in the shared embeddings matrix, so we just dot
    it against every other row. Used by the "look-alike homes" flow to rank
    listings by description similarity (fuses with DINOv2 image + feature
    score below).

    Returns ``{}`` when the listing has no embedding row (e.g. dropped during
    triage) — the caller should fall back to the other channels.
    """
    try:
        from ranking.runtime import embedding_search as _es
        matrix, ids = _es._lazy_init()
    except Exception as exc:
        print(
            f"[WARN] _arctic_scores_for_listing: expected=Arctic matrix, "
            f"got={type(exc).__name__}: {exc}, fallback=empty",
            flush=True,
        )
        return {}
    try:
        query_idx = ids.index(str(listing_id))
    except ValueError:
        return {}
    query_vec = matrix[query_idx]
    scores = (matrix @ query_vec).astype(np.float32, copy=False)
    n = scores.shape[0]
    k = max(1, min(int(top_n), n))
    top_idx = np.argpartition(-scores, k - 1)[:k]
    top_idx = top_idx[np.argsort(-scores[top_idx])]
    out: dict[str, float] = {}
    for i in top_idx.tolist():
        out[str(ids[i])] = float(scores[i])
    out.pop(str(listing_id), None)
    return out


def _feature_scores_for_listing(
    listing_id: str,
    db_path: Any,
    *,
    top_n: int = 500,
) -> dict[str, float]:
    """SQL-driven feature-similarity ``{other_id: score}``.

    Scalar similarity:
      +2.0 same canton
      +0.5 same object_category
      +1/(1+|Δrooms|) for rooms proximity
      +1/(1+|Δprice|/price0) for price proximity (bounded, 0 when base price is missing)

    Returns top_n by score, excluding the query listing. Empty dict on DB
    error (caller falls back to image / text channels).
    """
    import sqlite3
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            q = conn.execute(
                "SELECT canton, rooms, price, object_category FROM listings "
                "WHERE listing_id = ?",
                (str(listing_id),),
            ).fetchone()
            if q is None:
                return {}
            canton = q["canton"]
            rooms = q["rooms"]
            price = q["price"]
            cat = q["object_category"]
            # Guard against NULL rooms/price which would break the arithmetic.
            if rooms is None or price is None or not price:
                rooms_scalar = "0.0"
                price_scalar = "0.0"
                params = [canton or "", cat or "", str(listing_id), int(top_n)]
            else:
                rooms_scalar = "(1.0 / (1.0 + ABS(COALESCE(rooms, 0) - ?)))"
                price_scalar = (
                    "(CASE WHEN price > 0 "
                    "THEN 1.0 / (1.0 + ABS(price - ?) / ?) ELSE 0.0 END)"
                )
                params = [
                    canton or "",
                    cat or "",
                    rooms,
                    price,
                    float(price),
                    str(listing_id),
                    int(top_n),
                ]
            sql = (
                "SELECT listing_id, "
                "(CASE WHEN canton = ? THEN 2.0 ELSE 0.0 END) + "
                "(CASE WHEN object_category = ? THEN 0.5 ELSE 0.0 END) + "
                f"{rooms_scalar} + {price_scalar} AS score "
                "FROM listings WHERE listing_id != ? "
                "ORDER BY score DESC LIMIT ?"
            )
            rows = conn.execute(sql, params).fetchall()
            return {str(r["listing_id"]): float(r["score"]) for r in rows}
        finally:
            conn.close()
    except Exception as exc:
        print(
            f"[WARN] _feature_scores_for_listing: expected=SQL feature score, "
            f"got={type(exc).__name__}: {exc}, fallback=empty",
            flush=True,
        )
        return {}


def _load_platform_to_listing(db_path: Any) -> dict[str, str]:
    """Build ``{platform_id: listing_id}`` from the listings table once.

    DINOv2 image_ids are keyed on ``platform_id``; everything else in the
    ranker (Arctic embeddings, SQL queries, ListingData) is keyed on
    ``listing_id``. For ROBINREAL the two match; for COMPARIS and SRED they
    differ. The fused similar-listings flow translates one → the other so
    the RRF can fuse all three channels in a single ID space.
    """
    import sqlite3
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT platform_id, listing_id FROM listings "
            "WHERE platform_id IS NOT NULL AND platform_id != ''"
        ).fetchall()
    finally:
        conn.close()
    return {str(pid): str(lid) for pid, lid in rows}


def find_similar_listings_fused(
    listing_id: str,
    platform_id: str,
    *,
    db_path: Any,
    k: int = 10,
) -> tuple[list[tuple[str, float]], dict[str, str], dict[str, float]]:
    """Fused image + text + feature similarity for the "look-alikes" button.

    Works even when the query listing has zero images in the DINOv2 store
    (the old ``find_similar_listings`` returned empty in that case). Returns
    ``([(other_listing_id, fused_rrf_score), ...],
      {other_listing_id: best_image_id_or_''},
      {other_listing_id: max_image_cosine})``.

    RRF over up to three rankings, all translated into the ``listing_id``
    namespace before fusion:
      1. DINOv2 centroid vs all images → per-listing max cosine (if indexed).
         Platform_id → listing_id mapped via the listings table.
      2. Arctic description cosine vs the query listing's own embedding row.
      3. SQL feature-similarity scalar (canton + category + rooms + price).

    ``best_image_id`` is the image_id of the result listing whose embedding
    best matches the query listing's centroid, so the UI can surface that
    photo first on each card. Empty string when DINOv2 didn't contribute.

    The third dict — the max DINOv2 cosine per result listing — is returned
    separately from the fused score so UI can display an honest "match X%"
    visual-similarity number. A fused RRF score (typical range 0.01–0.05) is
    meaningless as a "% match". Listings not present in the image index are
    absent from this dict; callers should treat that as "no visual score".
    """
    from app.core.visual_search import fuse_rankings

    rankings: list[list[str]] = []
    platform_to_listing = _load_platform_to_listing(db_path)

    # Channel 1 — DINOv2 image centroid of the query listing vs every image.
    # Result keys are platform_ids; translate to listing_ids so RRF fuses
    # against the Arctic / feature channels in the same namespace.
    image_scores: dict[str, float] = {}
    query_vec: np.ndarray | None = None
    if _STATE["loaded"]:
        query_vec = _query_vector_for_listing(platform_id)
        if query_vec is not None:
            matrix = _STATE["matrix"]
            rowid_to_listing: list[str | None] = _STATE["rowid_to_listing"]
            scores = (matrix @ query_vec).astype(np.float32, copy=False)
            # Max cosine per candidate listing (keyed on platform_id internally).
            best_pid: dict[str, float] = {}
            for ridx in range(scores.shape[0]):
                other_pid = rowid_to_listing[ridx]
                if other_pid is None or other_pid == platform_id:
                    continue
                s = float(scores[ridx])
                prev = best_pid.get(other_pid)
                if prev is None or s > prev:
                    best_pid[other_pid] = s
            # Translate to listing_id namespace.
            for pid, s in best_pid.items():
                lid = platform_to_listing.get(pid)
                if lid:
                    prev = image_scores.get(lid)
                    if prev is None or s > prev:
                        image_scores[lid] = s
            if image_scores:
                rankings.append(
                    sorted(image_scores, key=lambda lid: -image_scores[lid])
                )

    # Channel 2 — Arctic semantic (listing's own description vector).
    arctic_scores = _arctic_scores_for_listing(listing_id, top_n=500)
    if arctic_scores:
        rankings.append(sorted(arctic_scores, key=lambda lid: -arctic_scores[lid]))

    # Channel 3 — SQL feature similarity (canton + rooms + price + category).
    feature_scores = _feature_scores_for_listing(listing_id, db_path, top_n=500)
    if feature_scores:
        rankings.append(sorted(feature_scores, key=lambda lid: -feature_scores[lid]))

    if not rankings:
        return [], {}, {}

    fused = fuse_rankings(rankings)
    fused.pop(str(listing_id), None)
    ranked = sorted(fused.items(), key=lambda item: -item[1])[: max(1, int(k))]

    # Best image per result listing (only populated when DINOv2 contributed).
    # Build listing_id → platform_id reverse map for the lookups.
    listing_to_platform = {lid: pid for pid, lid in platform_to_listing.items()}
    best_image_ids: dict[str, str] = {}
    if query_vec is not None:
        listing_to_rowids: dict[str, list[int]] = _STATE["listing_to_rowids"]
        matrix = _STATE["matrix"]
        image_ids: list[str | None] = _STATE["image_ids"]
        for rid, _ in ranked:
            pid = listing_to_platform.get(rid, rid)
            rowids = listing_to_rowids.get(pid)
            if not rowids:
                best_image_ids[rid] = ""
                continue
            sub = np.asarray(matrix[np.array(rowids, dtype=np.int64)], dtype=np.float32)
            local_scores = (sub @ query_vec).astype(np.float32, copy=False)
            best_local = int(np.argmax(local_scores))
            img_id = image_ids[rowids[best_local]] if rowids else None
            best_image_ids[rid] = img_id or ""
    # Only expose cosines for listings we actually ranked (i.e. kept after the
    # top-K cut). `image_scores` is keyed on listing_id with cosines in [-1, 1].
    returned_cosines = {
        rid: image_scores[rid] for rid, _ in ranked if rid in image_scores
    }
    return ranked, best_image_ids, returned_cosines


def find_similar_by_image(
    pil_image: Image.Image,
    *,
    k: int = 10,
) -> list[tuple[str, float]]:
    """Encode an arbitrary photo through DINOv2 and return top-K similar listings.

    Same aggregation as :func:`find_similar_listings` (max cosine per candidate
    listing). Used by the ``POST /listings/search/image`` endpoint so the user
    can upload any photo (not just pick an existing listing) and find matches.

    Raises ``RuntimeError`` if the index has not been loaded yet (same
    loud-fallback contract as ``find_similar_listings``).
    """
    if not _STATE["loaded"]:
        raise RuntimeError(
            "dinov2_search.find_similar_by_image called before "
            "load_dinov2_index()"
        )
    query = _encode_query_image_to_vec(pil_image)

    matrix = _STATE["matrix"]
    rowid_to_listing: list[str | None] = _STATE["rowid_to_listing"]

    # Stored matrix rows are L2-unit; one dot product = cosine.
    scores = (matrix @ query).astype(np.float32, copy=False)

    best: dict[str, float] = {}
    for row_idx in range(scores.shape[0]):
        other = rowid_to_listing[row_idx]
        if other is None:
            continue
        s = float(scores[row_idx])
        prev = best.get(other)
        if prev is None or s > prev:
            best[other] = s

    ranked = sorted(best.items(), key=lambda item: -item[1])
    return ranked[: max(1, int(k))]
