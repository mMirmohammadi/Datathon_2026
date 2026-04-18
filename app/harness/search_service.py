"""Top-level orchestration.

Flow for `query_from_text`:
    query
     → Claude QueryPlan (shared LRU across hard/soft extraction)
     → SQL hard-filter GATE
     → soft-filter (drop negatives)
     → BM25 candidates over the allowed set
     → rank (4+1 signal linear blend)
     → if zero candidates, walk the relaxation ladder
     → paginate and populate `meta`

`meta` is always non-empty and always carries:
  - `extracted_filters` (field: value of each populated hard filter)
  - `soft`              (keywords, negatives, sentiment, rewrites)
  - `relaxations`       (list of human-readable step descriptions, [] if none)
  - `warnings`          (list of strings, [] if none)
  - `confidence`        (float 0..1 from Claude)
  - `clarification`     ({question, needed} if Claude flagged ambiguity)
  - `timings_ms`        (per-stage latency)
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from app.core.hard_filters import HardFilterParams, search_listings
from app.models.schemas import HardFilters, ListingsResponse
from app.participant.hard_fact_extraction import extract_hard_facts
from app.participant.query_plan import get_plan
from app.participant.ranking import rank_listings
from app.participant.relaxation import relax
from app.participant.retrieval import bm25_candidates
from app.participant.soft_fact_extraction import extract_soft_facts
from app.participant.soft_filtering import filter_soft_facts

# Minimum candidates we try to secure before giving up on relaxation
MIN_HITS = 5
# How many candidates we retrieve from BM25 before ranking (ranking limits further)
BM25_POOL_K = 100


def filter_hard_facts(db_path: Path, hard_facts: HardFilters) -> list[dict[str, Any]]:
    return search_listings(db_path, to_hard_filter_params(hard_facts))


def query_from_text(
    *,
    db_path: Path,
    query: str,
    limit: int,
    offset: int,
) -> ListingsResponse:
    t_total_start = time.monotonic()
    warnings: list[str] = []
    relaxations: list[str] = []
    timings: dict[str, float] = {}

    # ---- 1. query understanding (shared LRU, one API call for both) --------
    t0 = time.monotonic()
    try:
        plan = get_plan(query)
    except (ValueError, TypeError) as exc:
        # Bad user input — bubble up as empty result + warning rather than crash
        warnings.append(f"query_plan_bad_input: {exc}")
        return ListingsResponse(
            listings=[],
            meta={
                "extracted_filters": {},
                "soft": {},
                "relaxations": [],
                "warnings": warnings,
                "confidence": 0.0,
                "clarification": {"needed": False, "question": None},
                "timings_ms": {"total": round((time.monotonic() - t_total_start) * 1000, 1)},
            },
        )
    timings["query_plan_ms"] = round((time.monotonic() - t0) * 1000, 1)

    hard_facts = extract_hard_facts(query)
    hard_facts.limit = max(limit * 3, BM25_POOL_K)  # retrieve a wider pool than returned
    hard_facts.offset = 0  # pagination happens after ranking
    soft_facts = extract_soft_facts(query)

    if plan.confidence < 0.4:
        warnings.append(
            f"low_confidence_plan: confidence={plan.confidence:.2f} "
            f"(regex fallback likely used; check [WARN] logs)"
        )

    # ---- 2. SQL hard-filter GATE -----------------------------------------
    t0 = time.monotonic()
    candidates = filter_hard_facts(db_path, hard_facts)
    timings["sql_gate_ms"] = round((time.monotonic() - t0) * 1000, 1)

    # ---- 2b. relaxation ladder if empty ----------------------------------
    if not candidates:
        for relaxed_hf, description in relax(hard_facts):
            relaxed_hf.limit = hard_facts.limit
            relaxed_hf.offset = 0
            candidates = filter_hard_facts(db_path, relaxed_hf)
            relaxations.append(description)
            if len(candidates) >= MIN_HITS:
                hard_facts = relaxed_hf
                break
        if relaxations:
            print(
                f"[INFO] search_service: relaxations_applied={len(relaxations)} "
                f"final_count={len(candidates)} steps={relaxations}",
                flush=True,
            )
        if not candidates:
            warnings.append("no_results_after_relaxation")

    # ---- 3. soft-filter (drop negative-keyword hits) ---------------------
    t0 = time.monotonic()
    pre_soft = len(candidates)
    candidates = filter_soft_facts(candidates, soft_facts)
    timings["soft_filter_ms"] = round((time.monotonic() - t0) * 1000, 1)
    if len(candidates) < pre_soft:
        print(
            f"[INFO] search_service: soft_filter dropped {pre_soft - len(candidates)} "
            f"(kept {len(candidates)})",
            flush=True,
        )

    # ---- 4. BM25 retrieval (re-rank within allowed set) ------------------
    t0 = time.monotonic()
    allowed_ids = [str(c["listing_id"]) for c in candidates]
    bm25_cands = bm25_candidates(db_path, plan, allowed_ids, k=BM25_POOL_K)
    timings["bm25_ms"] = round((time.monotonic() - t0) * 1000, 1)
    # If BM25 returned nothing but candidates exist, fall back to the allowed
    # candidates themselves (pass-through). Emit a [WARN] so it's never silent.
    if not bm25_cands and candidates:
        warnings.append("bm25_empty_fallback_to_sql_gate")
        print(
            "[WARN] search_service: expected=bm25_results, got=empty, "
            f"fallback=sql_gate_passthrough allowed={len(candidates)}",
            flush=True,
        )
        bm25_cands = candidates[:BM25_POOL_K]

    # ---- 5. rank with 4+1 linear blend -----------------------------------
    t0 = time.monotonic()
    ranked = rank_listings(bm25_cands, soft_facts)
    timings["rank_ms"] = round((time.monotonic() - t0) * 1000, 1)

    # ---- 6. paginate ------------------------------------------------------
    paged = ranked[offset : offset + limit]

    # ---- 7. build meta ---------------------------------------------------
    timings["total_ms"] = round((time.monotonic() - t_total_start) * 1000, 1)
    meta = {
        "extracted_filters": _filter_summary(hard_facts),
        "soft": {
            "keywords": list(soft_facts.get("keywords") or []),
            "negatives": list(soft_facts.get("negatives") or []),
            "price_sentiment": soft_facts.get("price_sentiment"),
            "soft_features": list(soft_facts.get("soft_features") or []),
            "rewrites": list(soft_facts.get("rewrites") or []),
        },
        "relaxations": relaxations,
        "warnings": warnings,
        "confidence": plan.confidence,
        "clarification": {
            "needed": plan.clarification_needed,
            "question": plan.clarification_question,
        },
        "pool_size": len(ranked),
        "timings_ms": timings,
    }

    return ListingsResponse(listings=paged, meta=meta)


def query_from_filters(
    *,
    db_path: Path,
    hard_facts: HardFilters | None,
) -> ListingsResponse:
    """Direct structured-filter endpoint. No Claude, no BM25 — just SQL + stub rank."""
    t0 = time.monotonic()
    structured = hard_facts or HardFilters()
    soft_facts = {"keywords": [], "negatives": [], "rewrites": [], "soft_features": []}
    candidates = filter_hard_facts(db_path, structured)
    candidates = filter_soft_facts(candidates, soft_facts)
    ranked = rank_listings(candidates, soft_facts)
    return ListingsResponse(
        listings=ranked[structured.offset : structured.offset + structured.limit],
        meta={
            "extracted_filters": _filter_summary(structured),
            "timings_ms": {"total_ms": round((time.monotonic() - t0) * 1000, 1)},
        },
    )


def to_hard_filter_params(hard_facts: HardFilters) -> HardFilterParams:
    return HardFilterParams(
        city=hard_facts.city,
        postal_code=hard_facts.postal_code,
        canton=hard_facts.canton,
        min_price=hard_facts.min_price,
        max_price=hard_facts.max_price,
        min_rooms=hard_facts.min_rooms,
        max_rooms=hard_facts.max_rooms,
        latitude=hard_facts.latitude,
        longitude=hard_facts.longitude,
        radius_km=hard_facts.radius_km,
        features=hard_facts.features,
        offer_type=hard_facts.offer_type,
        object_category=hard_facts.object_category,
        limit=hard_facts.limit,
        offset=hard_facts.offset,
        sort_by=hard_facts.sort_by,
    )


def _filter_summary(hf: HardFilters) -> dict[str, Any]:
    """Only populated fields; keeps `meta.extracted_filters` readable."""
    d = hf.model_dump(exclude_none=True)
    d.pop("limit", None)
    d.pop("offset", None)
    d.pop("sort_by", None)
    return d
