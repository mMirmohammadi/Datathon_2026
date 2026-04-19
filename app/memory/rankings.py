"""Turn a :class:`UserProfile` into RRF-ready rankings over a candidate pool.

Each call to :func:`build_memory_rankings` returns **0 to 5** best-first lists
of listing_ids:

1. **Semantic taste**   - cosine of candidate Arctic-Embed vector vs the
   user's positive-minus-negative centroid.
2. **Visual taste**     - cosine of candidate best-image SigLIP vector vs
   the user's positive visual centroid.
3. **Feature taste**    - dot product of candidate feature-presence vector
   vs the user's per-feature signed preference.
4. **Price preference** - inverse distance of ``log(candidate_price)`` to
   the user's ``(mu, sigma)``.
5. **Dismissal demotion** - listings within the candidate pool that look
   very similar (semantic cosine > 0.85) to explicit dismissals sink to
   the bottom of this one ranking.

Each channel is skipped independently (with a loud ``[WARN]``) when its
prerequisite data is missing: cold-start, no text index loaded, no visual
index loaded, etc. That matches CLAUDE.md §5 (no silent fallbacks) and the
existing convention in ``_rerank_hybrid``.

All rankings share the same ``candidate_ids`` restriction: memory never
introduces a listing into the pool that wasn't in the hard-filter output.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np

from app.core.hard_filters import FEATURE_COLUMN_MAP
from app.memory.profile import UserProfile


SEMANTIC_DISMISSAL_THRESHOLD = 0.85


@dataclass(slots=True)
class MemorySignals:
    """Per-candidate numeric scores for UI explainability. Everything is
    either a ``[-1, 1]``-ish cosine or ``None`` when the channel didn't fire.
    """
    semantic: dict[str, float]
    visual: dict[str, float]
    feature: dict[str, float]
    price: dict[str, float]

    def composite(self, listing_id: str) -> float | None:
        """Mean of available channel scores; None when all four were absent."""
        parts = [
            d.get(listing_id)
            for d in (self.semantic, self.visual, self.feature, self.price)
        ]
        kept = [v for v in parts if v is not None]
        if not kept:
            return None
        return float(sum(kept) / len(kept))


def _text_matrix_and_index(
    text_state: Any,
) -> tuple[np.ndarray, dict[str, int]] | None:
    """Pull ``(matrix, {listing_id: row_idx})`` out of text_embed_search state.

    ``text_state`` is the module-level ``_STATE`` dict of
    :mod:`app.core.text_embed_search`; callers own the lazy-load decision.
    """
    matrix = text_state.get("matrix") if text_state else None
    ids = text_state.get("ids") if text_state else None
    if matrix is None or ids is None:
        return None
    return matrix, {lid: i for i, lid in enumerate(ids)}


def _visual_matrix_and_owners(
    visual_state: Any,
) -> tuple[np.ndarray, dict[tuple[str, str], list[int]]] | None:
    matrix = visual_state.get("main_matrix") if visual_state else None
    pid_to_rowids = visual_state.get("pid_to_rowids") if visual_state else None
    if matrix is None or not pid_to_rowids:
        return None
    return matrix, pid_to_rowids


def _mean_of_rows(matrix: np.ndarray, rows: list[int]) -> np.ndarray | None:
    if not rows:
        return None
    sub = np.asarray(matrix[np.array(rows, dtype=np.int64)], dtype=np.float32)
    if sub.size == 0:
        return None
    mean = sub.mean(axis=0)
    return mean


def _normalize(vec: np.ndarray) -> np.ndarray | None:
    norm = float(np.linalg.norm(vec))
    if norm <= 1e-12:
        return None
    return (vec / norm).astype(np.float32)


def _sort_desc(scores: dict[str, float]) -> list[str]:
    return [
        lid for lid, _ in sorted(scores.items(), key=lambda kv: -kv[1])
    ]


def _sort_asc(scores: dict[str, float]) -> list[str]:
    return [
        lid for lid, _ in sorted(scores.items(), key=lambda kv: kv[1])
    ]


# ---------- Channel 1: semantic taste ---------------------------------------


def _semantic_taste(
    profile: UserProfile,
    candidate_ids: list[str],
    text_matrix: np.ndarray,
    id_to_idx: dict[str, int],
) -> tuple[list[str], dict[str, float]] | None:
    """Cosine between candidate text-embeddings and the user's taste centroid."""
    pos_rows = [
        id_to_idx[lid]
        for lid in profile.positive_ids
        if lid in id_to_idx
    ]
    neg_rows = [
        id_to_idx[lid]
        for lid in profile.negative_ids
        if lid in id_to_idx
    ]
    if not pos_rows:
        return None
    c_pos = _mean_of_rows(text_matrix, pos_rows)
    if c_pos is None:
        return None
    c_neg = _mean_of_rows(text_matrix, neg_rows) if neg_rows else None
    taste = c_pos - 0.5 * c_neg if c_neg is not None else c_pos
    taste_n = _normalize(taste)
    if taste_n is None:
        return None

    scores: dict[str, float] = {}
    for lid in candidate_ids:
        idx = id_to_idx.get(lid)
        if idx is None:
            continue
        vec = np.asarray(text_matrix[idx], dtype=np.float32)
        vec_n = _normalize(vec)
        if vec_n is None:
            continue
        scores[lid] = float(vec_n @ taste_n)
    if not scores:
        return None
    return _sort_desc(scores), scores


# ---------- Channel 2: visual taste -----------------------------------------


def _visual_taste(
    profile: UserProfile,
    candidates: list[dict[str, Any]],
    main_matrix: np.ndarray,
    pid_to_rowids: dict[tuple[str, str], list[int]],
    scrape_to_image_source: dict[str, str],
    visited: dict[str, list[int]] | None = None,
) -> tuple[list[str], dict[str, float]] | None:
    """Cosine between candidate best-image centroid and the user's
    positive-image centroid.

    We need (scrape_source, platform_id) per listing to look up image row
    indices, so this channel requires candidate rows, not just listing_ids.
    """
    # Build (image_source, platform_id) map for candidates that have images.
    candidate_image_keys: dict[str, tuple[str, str]] = {}
    for row in candidates:
        lid = str(row.get("listing_id"))
        scrape = str(row.get("scrape_source") or "").upper()
        img_source = scrape_to_image_source.get(scrape)
        platform_id = row.get("platform_id")
        if img_source is None or not platform_id:
            continue
        candidate_image_keys[lid] = (img_source, str(platform_id))

    if not candidate_image_keys:
        return None

    # Average across all rowids per listing for the profile centroid.
    pos_rows: list[int] = []
    # Since the profile doesn't carry (source, platform_id), we need a map
    # from listing_id to rowids for the positives. We only have it for the
    # candidates we've seen so far. If a positive isn't in this candidate
    # pool, we fall back to relying on the text centroid alone (skip here).
    pos_ids_set = set(profile.positive_ids)
    for lid, key in candidate_image_keys.items():
        if lid in pos_ids_set:
            pos_rows.extend(pid_to_rowids.get(key, ()))
    if not pos_rows:
        return None
    c_pos = _mean_of_rows(main_matrix, pos_rows)
    if c_pos is None:
        return None
    c_pos_n = _normalize(c_pos)
    if c_pos_n is None:
        return None

    scores: dict[str, float] = {}
    for lid, key in candidate_image_keys.items():
        rids = pid_to_rowids.get(key, ())
        if not rids:
            continue
        cand_vec = _mean_of_rows(main_matrix, list(rids))
        if cand_vec is None:
            continue
        cand_n = _normalize(cand_vec)
        if cand_n is None:
            continue
        scores[lid] = float(cand_n @ c_pos_n)
    if not scores:
        return None
    return _sort_desc(scores), scores


# ---------- Channel 3: feature taste ----------------------------------------


def _feature_taste(
    profile: UserProfile,
    candidates: list[dict[str, Any]],
) -> tuple[list[str], dict[str, float]] | None:
    """Dot product between candidate feature vector and the user's taste."""
    if not profile.feature_taste:
        return None
    # Is there any signal? (all-zero taste → skip)
    if all(abs(v) < 1e-9 for v in profile.feature_taste.values()):
        return None

    feature_keys = list(FEATURE_COLUMN_MAP)
    taste = np.asarray(
        [profile.feature_taste.get(k, 0.0) for k in feature_keys],
        dtype=np.float32,
    )
    scores: dict[str, float] = {}
    for row in candidates:
        lid = str(row.get("listing_id"))
        feats = set(row.get("features") or [])
        vec = np.asarray(
            [1.0 if k in feats else -1.0 for k in feature_keys],
            dtype=np.float32,
        )
        scores[lid] = float(vec @ taste)
    if not scores:
        return None
    return _sort_desc(scores), scores


# ---------- Channel 4: price preference -------------------------------------


def _price_taste(
    profile: UserProfile,
    candidates: list[dict[str, Any]],
) -> tuple[list[str], dict[str, float]] | None:
    if profile.price_mu is None or profile.price_sigma is None:
        return None
    mu = profile.price_mu
    sigma = max(profile.price_sigma, 0.05)
    scores: dict[str, float] = {}
    for row in candidates:
        lid = str(row.get("listing_id"))
        price = row.get("price")
        if price is None:
            continue
        try:
            p = float(price)
        except (TypeError, ValueError):
            continue
        if p <= 0.0:
            continue
        # Score = -z (closer to mu is better). Caller sorts descending so
        # ranks line up with the other channels.
        scores[lid] = -abs(math.log(p) - mu) / sigma
    if not scores:
        return None
    return _sort_desc(scores), scores


# ---------- Channel 5: dismissal demotion -----------------------------------


def _dismissal_demotion(
    profile: UserProfile,
    candidate_ids: list[str],
    text_matrix: np.ndarray | None,
    id_to_idx: dict[str, int] | None,
) -> list[str] | None:
    """Return candidate_ids with near-duplicates of dismissed listings at the end.

    Listings that *are* dismissed sink unconditionally. Listings whose text
    vector has cosine > 0.85 with any dismissed vector also sink. Everything
    else preserves the input order.
    """
    if not profile.dismissed_ids:
        return None

    sunken: set[str] = {lid for lid in candidate_ids if lid in profile.dismissed_ids}

    if text_matrix is not None and id_to_idx:
        dismiss_rows = [
            id_to_idx[lid] for lid in profile.dismissed_ids if lid in id_to_idx
        ]
        if dismiss_rows:
            dismiss_mat = np.asarray(
                text_matrix[np.array(dismiss_rows, dtype=np.int64)],
                dtype=np.float32,
            )
            norms = np.linalg.norm(dismiss_mat, axis=1, keepdims=True)
            norms = np.where(norms > 1e-12, norms, 1.0)
            dismiss_mat = dismiss_mat / norms
            for lid in candidate_ids:
                if lid in sunken:
                    continue
                idx = id_to_idx.get(lid)
                if idx is None:
                    continue
                vec = np.asarray(text_matrix[idx], dtype=np.float32)
                n = float(np.linalg.norm(vec))
                if n <= 1e-12:
                    continue
                vec = vec / n
                max_sim = float((dismiss_mat @ vec).max())
                if max_sim > SEMANTIC_DISMISSAL_THRESHOLD:
                    sunken.add(lid)

    if not sunken:
        return None

    kept = [lid for lid in candidate_ids if lid not in sunken]
    demoted = [lid for lid in candidate_ids if lid in sunken]
    return kept + demoted


# ---------- Public entry point ----------------------------------------------


def build_memory_rankings(
    *,
    candidates: list[dict[str, Any]],
    profile: UserProfile,
    text_state: Any = None,
    visual_state: Any = None,
    scrape_to_image_source: dict[str, str] | None = None,
) -> tuple[list[list[str]], MemorySignals]:
    """Return ``(rankings, signals)`` for RRF fusion and per-candidate explainability.

    The tuple is safe to unpack even on cold-start: ``rankings`` is ``[]`` and
    every ``signals`` dict is empty.
    """
    empty_signals = MemorySignals(
        semantic={}, visual={}, feature={}, price={}
    )
    rankings: list[list[str]] = []
    if not candidates:
        return rankings, empty_signals

    candidate_ids = [str(c["listing_id"]) for c in candidates]

    if profile.is_cold_start:
        # Cold-start: the only signal worth firing is dismissal-demotion,
        # which is independent of positives.
        demotion = _dismissal_demotion(
            profile, candidate_ids, text_matrix=None, id_to_idx=None
        )
        if demotion is not None:
            rankings.append(demotion)
        else:
            print(
                f"[WARN] memory.build_memory_rankings: user {profile.user_id} "
                f"is cold-start and has no dismissals, "
                f"fallback=no memory rankings added",
                flush=True,
            )
        return rankings, empty_signals

    text_pack = _text_matrix_and_index(text_state)
    visual_pack = _visual_matrix_and_owners(visual_state)

    sem_scores: dict[str, float] = {}
    vis_scores: dict[str, float] = {}
    feat_scores: dict[str, float] = {}
    pri_scores: dict[str, float] = {}

    # 1. Semantic taste
    if text_pack is not None:
        text_matrix, id_to_idx = text_pack
        out = _semantic_taste(profile, candidate_ids, text_matrix, id_to_idx)
        if out is None:
            print(
                f"[WARN] memory.semantic: user {profile.user_id} positive ids "
                f"not in text index, fallback=skip channel",
                flush=True,
            )
        else:
            rank, sem_scores = out
            rankings.append(rank)
    else:
        print(
            f"[WARN] memory.semantic: text_embed index not loaded, "
            f"fallback=skip channel",
            flush=True,
        )

    # 2. Visual taste
    if visual_pack is not None and scrape_to_image_source:
        main_matrix, pid_to_rowids = visual_pack
        out = _visual_taste(
            profile, candidates, main_matrix, pid_to_rowids, scrape_to_image_source
        )
        if out is None:
            print(
                f"[WARN] memory.visual: no positive listing images intersect "
                f"current candidate pool, fallback=skip channel",
                flush=True,
            )
        else:
            rank, vis_scores = out
            rankings.append(rank)
    else:
        print(
            f"[WARN] memory.visual: visual index not loaded, "
            f"fallback=skip channel",
            flush=True,
        )

    # 3. Feature taste
    out = _feature_taste(profile, candidates)
    if out is None:
        print(
            f"[WARN] memory.feature: user {profile.user_id} has no feature "
            f"signal (all-zero taste vector), fallback=skip channel",
            flush=True,
        )
    else:
        rank, feat_scores = out
        rankings.append(rank)

    # 4. Price preference
    out = _price_taste(profile, candidates)
    if out is None:
        print(
            f"[WARN] memory.price: user {profile.user_id} has no positive "
            f"price samples, fallback=skip channel",
            flush=True,
        )
    else:
        rank, pri_scores = out
        rankings.append(rank)

    # 5. Dismissal demotion
    text_matrix = text_pack[0] if text_pack else None
    id_to_idx = text_pack[1] if text_pack else None
    demotion = _dismissal_demotion(
        profile, candidate_ids, text_matrix=text_matrix, id_to_idx=id_to_idx
    )
    if demotion is not None:
        rankings.append(demotion)

    signals = MemorySignals(
        semantic=sem_scores,
        visual=vis_scores,
        feature=feat_scores,
        price=pri_scores,
    )
    return rankings, signals
