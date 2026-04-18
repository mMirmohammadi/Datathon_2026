from __future__ import annotations

from typing import Any


def filter_soft_facts(
    candidates: list[dict[str, Any]],
    soft_facts: dict[str, Any],
) -> list[dict[str, Any]]:
    # Intentionally stubbed. All hard-filtered candidates pass through.
    return candidates
