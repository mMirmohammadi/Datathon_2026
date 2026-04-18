"""Confidence scoring for text-regex extractions (pass 2)."""
from __future__ import annotations


def compute_confidence(
    base: float,
    *,
    lang_match: bool,
    negated: bool = False,
) -> float:
    """Return a confidence in [0, 1] given signal quality.

    - `base`: the field-level prior (e.g. 0.75 for a balcony regex hit, 0.9 for
      a year-built match with a specific "Baujahr" prefix).
    - `lang_match`: True if the match came from the same language as the
      detected description language. Cross-language matches are down-weighted
      because they're often false positives (e.g. an Italian word happens to
      match a German regex).
    - `negated`: True if a negation token appeared in the 3-token lookback
      window before the match. Fully discards the match (returns 0.0).

    `0.6` cross-language multiplier matches the plan's §2 confidence formula.
    """
    if not 0.0 <= base <= 1.0:
        raise ValueError(f"base confidence {base} not in [0,1]")
    if negated:
        return 0.0
    cross_lang_penalty = 1.0 if lang_match else 0.6
    return round(base * cross_lang_penalty, 3)
