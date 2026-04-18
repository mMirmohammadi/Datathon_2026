"""MVP ranking: 4 positive + 1 negative signal, percentile-normalized linear blend.

Inputs:
  - candidates: list of dict from retrieval.bm25_candidates (already filtered
    through the SQL hard-filter gate; each may have `bm25_score` or None).
  - soft_facts: dict produced by extract_soft_facts; has the QueryPlan's
    soft.keywords, soft.negatives, price_sentiment, soft_features, etc.

Signals (all percentile-normalized within the candidate pool):
  +0.40  BM25F percentile
  +0.20  feature_match (required hits counted fully, preferred at half weight)
  +0.20  price_fit (triangle centered on pool p25/median/p75 by sentiment)
  +0.10  freshness (linear on days-to-available)
  -0.10  negative_penalty (substring hit of any `soft.negatives` term)

Output: list[RankedListingResult] sorted by score DESC. Each carries a
templated `reason` string summarizing why it ranked where it did.
"""
from __future__ import annotations

import datetime as dt
import json
from typing import Any

from app.models.schemas import ListingData, RankedListingResult
from app.participant import scoring_config as CFG

# --- main API ---------------------------------------------------------------


def rank_listings(
    candidates: list[dict[str, Any]],
    soft_facts: dict[str, Any],
) -> list[RankedListingResult]:
    if not candidates:
        return []

    n = len(candidates)
    keywords = [str(k).lower() for k in soft_facts.get("keywords", [])]
    negatives = [str(x).lower() for x in soft_facts.get("negatives", [])]
    soft_features = list(soft_facts.get("soft_features") or [])
    price_sentiment = soft_facts.get("price_sentiment")

    # --- raw signals --------------------------------------------------------
    bm25_raw: list[float | None] = [c.get("bm25_score") for c in candidates]
    feat_raw: list[float] = [
        _feature_match_raw(c, soft_features) for c in candidates
    ]
    price_raw: list[float | None] = [
        _coerce_price(c.get("price")) for c in candidates
    ]
    fresh_raw: list[float | None] = [
        _freshness_raw(c.get("available_from")) for c in candidates
    ]
    neg_hit: list[int] = [_negative_hit(c, negatives) for c in candidates]

    # --- percentile normalization ------------------------------------------
    # BM25: more negative = more relevant → negate so larger is better
    bm25_pct = _percentile_higher_is_better([
        -x if x is not None else None for x in bm25_raw
    ])
    feat_pct = _percentile_higher_is_better(feat_raw)
    fresh_pct = _percentile_higher_is_better(fresh_raw)

    # Price fit: triangle → already in [0, 1]
    price_fit = _price_fit_scores(price_raw, price_sentiment)

    # --- combine -----------------------------------------------------------
    scored: list[tuple[float, dict[str, float], dict[str, Any]]] = []
    for i, cand in enumerate(candidates):
        components = {
            "bm25_pct": bm25_pct[i],
            "feat_pct": feat_pct[i],
            "price_fit": price_fit[i],
            "fresh_pct": fresh_pct[i],
            "neg_hit": float(neg_hit[i]),
        }
        score = (
            CFG.W_BM25 * components["bm25_pct"]
            + CFG.W_FEATURE_MATCH * components["feat_pct"]
            + CFG.W_PRICE_FIT * components["price_fit"]
            + CFG.W_FRESHNESS * components["fresh_pct"]
            + CFG.W_NEGATIVE_PENALTY * components["neg_hit"]
        )
        scored.append((score, components, cand))

    scored.sort(key=lambda t: (-t[0], t[2].get("listing_id", "")))
    scored = scored[: CFG.MAX_RANKED]

    # BM25 rank for the reason string
    bm25_rank_idx = _ranks_lower_is_better(bm25_raw)
    id_to_rank = {
        c.get("listing_id"): bm25_rank_idx[i] for i, c in enumerate(candidates)
    }

    results: list[RankedListingResult] = []
    for score, comp, cand in scored:
        bm25_rank = id_to_rank.get(cand.get("listing_id"))
        reason = _render_reason(
            cand,
            soft_facts,
            comp,
            bm25_rank=bm25_rank,
            pool_size=n,
        )
        results.append(
            RankedListingResult(
                listing_id=str(cand.get("listing_id", "")),
                score=round(score, 6),
                reason=reason,
                listing=_to_listing_data(cand),
            )
        )
    return results


# --- signal computations ---------------------------------------------------


def _feature_match_raw(cand: dict[str, Any], soft_features: list[dict]) -> float:
    """Raw feature-match score ∈ [0, 1]. Returns 0.5 neutral if no features requested."""
    if not soft_features:
        return 0.5
    cand_features = {str(f).lower() for f in cand.get("features") or []}
    total = 0.0
    max_total = 0.0
    for feat in soft_features:
        name = str(feat.get("name", "")).lower()
        weight = 1.0 if bool(feat.get("required", False)) else 0.5
        max_total += weight
        if name and name in cand_features:
            total += weight
    if max_total == 0:
        return 0.5
    return total / max_total


def _coerce_price(v: Any) -> float | None:
    try:
        p = float(v) if v is not None else None
    except (TypeError, ValueError):
        return None
    if p is None or p <= 0 or p >= 1_000_000:
        # Data has known bad values (0, 1, 1_111_111 sentinels).
        return None
    return p


def _freshness_raw(available_from: Any) -> float | None:
    """Parse `available_from` → freshness score ∈ [0, 1] or None on parse fail."""
    if not available_from or not isinstance(available_from, str):
        return None
    s = available_from.strip()
    if not s or s.lower() in {"sofort", "immediately", "nach vereinbarung", "n/a"}:
        return 1.0
    parsed: dt.date | None = None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            parsed = dt.datetime.strptime(s[:10], fmt).date()
            break
        except ValueError:
            continue
    if parsed is None:
        return None
    days = abs((parsed - dt.date.today()).days)
    if days <= CFG.FRESHNESS_FULL_DAYS:
        return 1.0
    if days >= CFG.FRESHNESS_ZERO_DAYS:
        return 0.0
    span = CFG.FRESHNESS_ZERO_DAYS - CFG.FRESHNESS_FULL_DAYS
    return 1.0 - (days - CFG.FRESHNESS_FULL_DAYS) / span


def _negative_hit(cand: dict[str, Any], negatives: list[str]) -> int:
    if not negatives:
        return 0
    title = str(cand.get("title") or "").lower()
    desc = str(cand.get("description") or "").lower()
    text = f"{title}\n{desc}"
    for kw in negatives:
        if kw and kw in text:
            return 1
    return 0


# --- percentile helpers ----------------------------------------------------


def _percentile_higher_is_better(values: list[float | None]) -> list[float]:
    """Percentile rank in [0, 1]. None values get neutral 0.5. Ties → average rank."""
    n = len(values)
    if n <= 1:
        return [0.5] * n
    present = [(i, v) for i, v in enumerate(values) if v is not None]
    if len(present) <= 1:
        out = [0.5] * n
        if len(present) == 1:
            out[present[0][0]] = 1.0
        return out
    present.sort(key=lambda t: t[1])
    rank_of: dict[int, float] = {}
    i = 0
    while i < len(present):
        j = i
        while j + 1 < len(present) and present[j + 1][1] == present[i][1]:
            j += 1
        mid = (i + j) / 2.0
        for k in range(i, j + 1):
            rank_of[present[k][0]] = mid
        i = j + 1
    denom = max(len(present) - 1, 1)
    return [rank_of[idx] / denom if idx in rank_of else 0.5 for idx in range(n)]


def _ranks_lower_is_better(values: list[float | None]) -> list[int | None]:
    """1-based rank, lowest (most negative) → rank 1. None stays None."""
    n = len(values)
    indexed = [(i, v) for i, v in enumerate(values) if v is not None]
    indexed.sort(key=lambda t: t[1])
    out: list[int | None] = [None] * n
    for rank, (orig_i, _) in enumerate(indexed, start=1):
        out[orig_i] = rank
    return out


def _price_fit_scores(
    prices: list[float | None], sentiment: str | None
) -> list[float]:
    """Triangle fit to candidate pool distribution, centered by sentiment."""
    n = len(prices)
    if sentiment not in CFG.PRICE_SENTIMENT_Q:
        return [0.5] * n
    present = sorted(p for p in prices if p is not None)
    if len(present) < 2:
        return [0.5] * n
    q = CFG.PRICE_SENTIMENT_Q[sentiment]
    target = _quantile(present, q)
    half_range = max(target - present[0], present[-1] - target)
    if half_range <= 0:
        return [0.5] * n
    out: list[float] = []
    for p in prices:
        if p is None:
            out.append(0.5)
            continue
        score = 1.0 - abs(p - target) / half_range
        out.append(max(0.0, min(1.0, score)))
    return out


def _quantile(sorted_vals: list[float], q: float) -> float:
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    pos = q * (n - 1)
    lo = int(pos)
    hi = min(lo + 1, n - 1)
    frac = pos - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


# --- reason rendering -----------------------------------------------------


def _render_reason(
    cand: dict[str, Any],
    soft_facts: dict[str, Any],
    comp: dict[str, float],
    *,
    bm25_rank: int | None,
    pool_size: int,
) -> str:
    bits: list[str] = []
    hard_bits: list[str] = []
    if cand.get("rooms") is not None:
        hard_bits.append(f"{_fmt_rooms(cand['rooms'])} rooms")
    if cand.get("city"):
        hard_bits.append(str(cand["city"]))
    if cand.get("price") is not None:
        hard_bits.append(f"CHF {int(cand['price'])}")
    feats = [f for f in (cand.get("features") or []) if f]
    if feats:
        hard_bits.append(", ".join(feats[:3]))
    if hard_bits:
        bits.append("Matches " + " · ".join(hard_bits) + ".")

    if bm25_rank is not None:
        bits.append(f"BM25 rank {bm25_rank}/{pool_size}.")

    sent = soft_facts.get("price_sentiment")
    if sent and comp["price_fit"] >= 0.7:
        bits.append(f"Price fits {sent} sentiment.")
    elif sent and comp["price_fit"] < 0.3:
        bits.append(f"Price off {sent} target.")

    if comp["neg_hit"] > 0:
        bits.append("⚠ contains a negated keyword (penalized).")

    if soft_facts.get("keywords"):
        hits = _which_keywords_hit(cand, soft_facts["keywords"])
        if hits:
            bits.append(f"Soft-match: {', '.join(hits[:3])}.")

    return " ".join(bits) or "Matches hard filters."


def _which_keywords_hit(cand: dict[str, Any], keywords: list[str]) -> list[str]:
    title = str(cand.get("title") or "").lower()
    desc = str(cand.get("description") or "").lower()
    text = f"{title} {desc}"
    return [k for k in keywords if k and k.lower() in text]


def _fmt_rooms(v: Any) -> str:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    if f == int(f):
        return str(int(f))
    return f"{f:g}"


# --- candidate → ListingData ------------------------------------------------


def _to_listing_data(c: dict[str, Any]) -> ListingData:
    return ListingData(
        id=str(c.get("listing_id", "")),
        title=c.get("title") or "",
        description=c.get("description"),
        street=c.get("street"),
        city=c.get("city"),
        postal_code=c.get("postal_code"),
        canton=c.get("canton"),
        latitude=c.get("latitude"),
        longitude=c.get("longitude"),
        price_chf=_coerce_int(c.get("price")),
        rooms=_coerce_float(c.get("rooms")),
        living_area_sqm=_coerce_int(c.get("area")),
        available_from=c.get("available_from"),
        image_urls=_coerce_image_urls(c.get("image_urls")),
        hero_image_url=c.get("hero_image_url"),
        original_listing_url=c.get("original_url"),
        features=list(c.get("features") or []),
        offer_type=c.get("offer_type"),
        object_category=c.get("object_category"),
        object_type=c.get("object_type"),
    )


def _coerce_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(round(float(v)))
    except (TypeError, ValueError):
        return None


def _coerce_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _coerce_image_urls(v: Any) -> list[str] | None:
    if v is None:
        return None
    if isinstance(v, list):
        return [str(item) for item in v]
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
        except json.JSONDecodeError:
            return [v]
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    return None
