"""Unit tests for common.confidence.compute_confidence."""
from __future__ import annotations

import pytest

from enrichment.common.confidence import compute_confidence


def test_base_with_lang_match_no_negation():
    assert compute_confidence(0.8, lang_match=True, negated=False) == 0.8


def test_cross_language_reduces_to_60pct():
    assert compute_confidence(0.8, lang_match=False, negated=False) == pytest.approx(0.48)


def test_negated_is_zero():
    assert compute_confidence(0.8, lang_match=True, negated=True) == 0.0
    assert compute_confidence(0.8, lang_match=False, negated=True) == 0.0


def test_base_0_stays_0():
    assert compute_confidence(0.0, lang_match=True) == 0.0


def test_base_1_stays_1_with_lang_match():
    assert compute_confidence(1.0, lang_match=True) == 1.0


def test_rejects_out_of_range_base():
    with pytest.raises(ValueError):
        compute_confidence(-0.1, lang_match=True)
    with pytest.raises(ValueError):
        compute_confidence(1.1, lang_match=True)


def test_result_always_in_0_1():
    for base in (0.0, 0.25, 0.5, 0.75, 1.0):
        for lm in (True, False):
            for neg in (True, False):
                r = compute_confidence(base, lang_match=lm, negated=neg)
                assert 0.0 <= r <= 1.0
