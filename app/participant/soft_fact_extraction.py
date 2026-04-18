"""Delegator: natural-language query → soft-preferences dict via QueryPlan.

Shares one Claude call with `extract_hard_facts` via the LRU cache on
`query_plan.get_plan`. The returned dict is consumed by `filter_soft_facts`
and `rank_listings` downstream; we include the BM25 rewrites + keywords + raw
query so retrieval and ranking don't need to re-call Claude.
"""
from __future__ import annotations

from typing import Any

from app.participant.query_plan import get_plan


def extract_soft_facts(query: str) -> dict[str, Any]:
    plan = get_plan(query)
    return {
        "raw_query": plan.raw_query,
        "confidence": plan.confidence,
        "keywords": list(plan.soft.keywords),
        "negatives": list(plan.soft.negatives),
        "price_sentiment": plan.soft.price_sentiment,
        "soft_features": [
            {"name": f.name, "required": f.required} for f in plan.soft.features
        ],
        "rewrites": list(plan.rewrites),
        "clarification_needed": plan.clarification_needed,
        "clarification_question": plan.clarification_question,
    }
