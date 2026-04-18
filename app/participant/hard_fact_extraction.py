"""Delegator: natural-language query → HardFilters via QueryPlan."""
from __future__ import annotations

from app.models.schemas import HardFilters
from app.participant.query_plan import get_plan, queryplan_to_hard_filters


def extract_hard_facts(query: str) -> HardFilters:
    plan = get_plan(query)
    return queryplan_to_hard_filters(plan)
