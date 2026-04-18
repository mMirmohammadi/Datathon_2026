from __future__ import annotations

from typing import Any


def extract_soft_facts(query: str) -> dict[str, Any]:
    # Intentionally stubbed. The harness keeps the raw query so teams
    # have an obvious place to plug in preference interpretation.
    return {"raw_query": query}
