"""T5.2 — Pareto + MMR diversification of the ranked top-K.

Problem (slide 10 in the organizer's deck): a query like
*"cheap AND central AND quiet"* is often impossible to satisfy jointly.
Naïve linear blending picks 10 near-duplicates that all make the same
compromise. Pareto + MMR together:

  * **Pareto** — compute the non-dominated frontier over the axes the user's
    query mentioned. Returns the listings where no other listing is strictly
    better on all chosen axes. This surfaces the REAL tradeoffs.
  * **MMR** (Maximal Marginal Relevance) — from the Pareto set plus the rest
    of the candidates, pick a top-K that balances (relevance) against
    (dissimilarity to already-picked). Dissimilarity is measured on
    city / price band / size to avoid "10 variants of the same building".

Both functions are pure — they take a list of candidate dicts (each with a
base score + the dimensions to optimise over) and return a re-ordered list.
The ranker in `app/participant/ranking.py` owns which axes to use per query.

No external deps beyond numpy.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

import numpy as np


# ---------- Pareto --------------------------------------------------------


def pareto_frontier(
    items: Sequence[dict[str, Any]],
    *,
    minimise: Sequence[str],
    maximise: Sequence[str] = (),
) -> list[dict[str, Any]]:
    """Return only non-dominated items on the given axes.

    An item A is dominated if some other item B is:
      - ≤ on every minimise axis AND ≥ on every maximise axis AND
      - strictly better on at least one axis.
    NULLs are treated as "worst possible" (never dominate, always dominated)
    so missing signals don't spuriously take a listing off the frontier.
    """
    n = len(items)
    if n <= 1:
        return list(items)
    axes_min = list(minimise)
    axes_max = list(maximise)
    if not axes_min and not axes_max:
        return list(items)

    BIG = 1e18
    def get_vals(it: dict) -> tuple[list[float], list[float]]:
        mins = [float(it.get(a)) if it.get(a) is not None else BIG for a in axes_min]
        maxs = [float(it.get(a)) if it.get(a) is not None else -BIG for a in axes_max]
        return mins, maxs

    vals = [get_vals(it) for it in items]
    keep = [True] * n
    for i in range(n):
        if not keep[i]:
            continue
        m_i, x_i = vals[i]
        for j in range(n):
            if i == j or not keep[j]:
                continue
            m_j, x_j = vals[j]
            # Does j dominate i?
            le_min = all(m_j[k] <= m_i[k] for k in range(len(axes_min)))
            ge_max = all(x_j[k] >= x_i[k] for k in range(len(axes_max)))
            strict = any(m_j[k] < m_i[k] for k in range(len(axes_min))) or \
                     any(x_j[k] > x_i[k] for k in range(len(axes_max)))
            if le_min and ge_max and strict:
                keep[i] = False
                break
    return [items[i] for i in range(n) if keep[i]]


# ---------- MMR -----------------------------------------------------------


@dataclass(slots=True, frozen=True)
class MMRDimension:
    """One dimension along which to diversify.

    `extractor(item) -> any comparable value`. Items with identical values
    on this dimension are considered "close" (distance=0); otherwise 1.
    For numeric dimensions (price, area) you can bucket before passing in,
    or pass in a float and use `numeric_bucket_size` for bucketing.
    """
    name: str
    extractor: "callable"           # type: ignore[valid-type]
    numeric_bucket_size: float | None = None


def _mmr_distance(a: dict, b: dict, dims: list[MMRDimension]) -> float:
    """0 = identical on every dimension; 1 = all dimensions differ."""
    if not dims:
        return 1.0
    diffs = 0
    for d in dims:
        va = d.extractor(a)
        vb = d.extractor(b)
        if va is None or vb is None:
            continue                 # missing dim doesn't count as similar
        if d.numeric_bucket_size is not None:
            try:
                va_b = round(float(va) / d.numeric_bucket_size)
                vb_b = round(float(vb) / d.numeric_bucket_size)
                if va_b != vb_b:
                    diffs += 1
            except (TypeError, ValueError):
                diffs += 1
        else:
            if va != vb:
                diffs += 1
    return diffs / len(dims)


def mmr(
    items: Sequence[dict[str, Any]],
    *,
    k: int,
    relevance_key: str,
    dims: Sequence[MMRDimension],
    lambda_: float = 0.7,
) -> list[dict[str, Any]]:
    """Greedy MMR: pick k items balancing relevance vs diversity.

    score(i) = lambda * relevance(i) - (1 - lambda) * max sim(i, already picked)

    `lambda_=1.0` → pure relevance (no diversification). `0.5` → balanced.
    Requires every item to have `relevance_key` in [0, 1].
    """
    if not items:
        return []
    k = max(1, min(int(k), len(items)))
    if lambda_ < 0 or lambda_ > 1:
        raise ValueError(f"lambda_ must be in [0, 1], got {lambda_}")

    items = list(items)
    picked: list[dict] = []
    remaining = list(items)
    dims_list = list(dims)

    while remaining and len(picked) < k:
        best_i = 0
        best_score = -np.inf
        for i, cand in enumerate(remaining):
            rel = float(cand.get(relevance_key) or 0.0)
            if picked:
                max_sim = max(
                    1.0 - _mmr_distance(cand, p, dims_list) for p in picked
                )
            else:
                max_sim = 0.0
            score = lambda_ * rel - (1.0 - lambda_) * max_sim
            if score > best_score:
                best_score = score
                best_i = i
        picked.append(remaining.pop(best_i))

    return picked
