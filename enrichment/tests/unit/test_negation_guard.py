"""Unit tests for common.text_extract.is_negated (3-token lookback window)."""
from __future__ import annotations

import pytest

from enrichment.common.text_extract import is_negated

DE_NEG = ("kein(?:e|en|er|es)?", "ohne", "nicht")
FR_NEG = (r"pas\s+de", "sans", "aucun(?:e|es|s)?")
IT_NEG = ("senza", "non")
EN_NEG = ("no", "without", "not")


@pytest.mark.parametrize("text,match_start,negs,expected", [
    # --- German ---
    ("Wohnung ohne Balkon", len("Wohnung ohne "), DE_NEG, True),
    ("kein Balkon vorhanden", len("kein "), DE_NEG, True),
    ("Leider kein eigener Balkon", len("Leider kein eigener "), DE_NEG, True),
    ("Schöner Balkon mit Aussicht", len("Schöner "), DE_NEG, False),
    ("Kein Larm, dafür ein Balkon", len("Kein Larm, dafür ein "), DE_NEG, False),  # neg out of window
    # --- French ---
    ("appartement sans balcon", len("appartement sans "), FR_NEG, True),
    ("logement pas de balcon", len("logement pas de "), FR_NEG, True),
    ("joli balcon ensoleillé", len("joli "), FR_NEG, False),
    # --- Italian ---
    ("appartamento senza balcone", len("appartamento senza "), IT_NEG, True),
    ("bellissimo balcone", len("bellissimo "), IT_NEG, False),
    # --- English ---
    ("apartment without balcony", len("apartment without "), EN_NEG, True),
    ("no balcony available", len("no "), EN_NEG, True),
    ("spacious balcony overlooking", len("spacious "), EN_NEG, False),
    # --- Edge cases ---
    ("", 0, DE_NEG, False),
    ("Balkon", 0, DE_NEG, False),  # nothing before the match
])
def test_negation_guard(text: str, match_start: int, negs: tuple, expected: bool):
    result = is_negated(text, match_start, negs, lookback_tokens=3)
    assert result is expected, f"is_negated({text!r}, {match_start}, ...) -> {result}, expected {expected}"


def test_lookback_window_is_respected():
    # 4 tokens before: "aus einem fernen kein Balkon" — "kein" is outside a 3-token window
    text = "aus einem fernen kein Balkon"
    match_start = len("aus einem fernen kein ")
    # With lookback=3, the 3 tokens before match are: "einem fernen kein" → "kein" is inside the window
    assert is_negated(text, match_start, DE_NEG, lookback_tokens=3) is True
    # With lookback=2, the 2 tokens before are: "fernen kein" → still contains kein
    # But if match_start is further out, e.g. 5 tokens of fluff then match:
    text2 = "a b c kein d e Balkon"
    match_start2 = len("a b c kein d e ")  # match = "Balkon"
    # Lookback 3 = ["c","kein","d","e"][-3:] = ["kein","d","e"] -> contains kein -> True
    assert is_negated(text2, match_start2, DE_NEG, lookback_tokens=3) is True
    # Lookback 2 = ["d","e"] -> no kein -> False
    assert is_negated(text2, match_start2, DE_NEG, lookback_tokens=2) is False
