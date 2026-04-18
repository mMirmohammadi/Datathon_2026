"""Soft-filter stage: drop candidates that hit a user-declared hard negative.

Only operates on `soft_facts["negatives"]` — phrases the user declared as
excluded (via Claude's QueryPlan extraction; the negation has already been
stripped so `negatives = ["ground floor"]` means "user does NOT want ground
floor"). Match is case-insensitive substring in title OR description.

Every drop emits a single aggregated `[WARN]` log per call so the audit trail
is non-silent per CLAUDE.md §5. Individual per-row log lines would flood logs
on large candidate pools.

This is intentionally a binary filter (drop), not a demote. The ranking stage
has a separate `negative_penalty` signal; if a listing makes it past this
filter it should still be ranked honestly.
"""
from __future__ import annotations

from typing import Any


def filter_soft_facts(
    candidates: list[dict[str, Any]],
    soft_facts: dict[str, Any],
) -> list[dict[str, Any]]:
    negatives_raw = soft_facts.get("negatives") or []
    negatives = [str(n).strip().lower() for n in negatives_raw if str(n).strip()]
    if not negatives:
        return candidates

    kept: list[dict[str, Any]] = []
    dropped_ids: list[str] = []
    drops_by_kw: dict[str, int] = {kw: 0 for kw in negatives}

    for cand in candidates:
        hit_kw = _first_negative_hit(cand, negatives)
        if hit_kw is None:
            kept.append(cand)
            continue
        dropped_ids.append(str(cand.get("listing_id", "?")))
        drops_by_kw[hit_kw] += 1

    if dropped_ids:
        sample = ", ".join(dropped_ids[:5])
        more = f" (+{len(dropped_ids) - 5} more)" if len(dropped_ids) > 5 else ""
        hit_summary = ", ".join(f"{kw}={n}" for kw, n in drops_by_kw.items() if n)
        print(
            f"[WARN] soft_filtering: expected=no_neg_match, got=neg_hit, "
            f"fallback=dropped n={len(dropped_ids)} by_kw={{{hit_summary}}} "
            f"sample=[{sample}]{more}",
            flush=True,
        )
    return kept


def _first_negative_hit(cand: dict[str, Any], negatives: list[str]) -> str | None:
    title = str(cand.get("title") or "").lower()
    desc = str(cand.get("description") or "").lower()
    text = f"{title}\n{desc}"
    for kw in negatives:
        if kw in text:
            return kw
    return None
