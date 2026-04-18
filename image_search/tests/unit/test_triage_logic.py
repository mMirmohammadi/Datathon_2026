"""Fast unit tests for triage decision logic.

These do NOT load a real SigLIP model. They test the scoring function directly
with hand-constructed probability rows. The "does it correctly classify ETH
Juniors as a logo?" assertion lives in the integration pilot, not here,
because it requires downloading ~400 MB of weights.
"""
from __future__ import annotations

import pytest
import torch

from image_search.common.prompts import ALL_CLASSES
from image_search.common.triage import (
    AMBIGUOUS_MARGIN,
    CONFIDENCE_THRESHOLD,
    _decide_from_scores,
)


def _idx(label: str) -> int:
    return ALL_CLASSES.index(label)


def _one_hot(label: str, *, confidence: float = 0.9) -> torch.Tensor:
    # Build a softmax-like row: `confidence` on the target, rest uniform.
    rest = (1.0 - confidence) / (len(ALL_CLASSES) - 1)
    row = torch.full((len(ALL_CLASSES),), rest)
    row[_idx(label)] = confidence
    return row.unsqueeze(0)


def test_confident_kept_class_passes_through():
    probs = _one_hot("interior-room", confidence=0.8)
    out = _decide_from_scores(probs, parent_ids=["unit-1"])
    assert out[0].label == "interior-room"
    assert out[0].confidence == pytest.approx(0.8, abs=1e-5)


def test_confident_dropped_class_stays_dropped():
    probs = _one_hot("logo-or-banner", confidence=0.7)
    out = _decide_from_scores(probs, parent_ids=["unit-2"])
    assert out[0].label == "logo-or-banner"


def test_below_threshold_falls_back_to_other_uninformative(capsys):
    # All classes tied at ~1/7 ≈ 0.14, which is below the 0.35 confidence floor.
    row = torch.full((1, len(ALL_CLASSES)), 1.0 / len(ALL_CLASSES))
    out = _decide_from_scores(row, parent_ids=["lowconf-1"])
    assert out[0].label == "other-uninformative"
    assert out[0].confidence < CONFIDENCE_THRESHOLD
    err = capsys.readouterr().err
    assert "[WARN] triage_lowconf" in err
    assert "parent='lowconf-1'" in err


def test_ambiguous_between_kept_and_dropped_biases_toward_kept(capsys):
    # Top = logo-or-banner (dropped), runner = interior-room (kept), margin < 0.05.
    # User directive: bias toward keep.
    row = torch.zeros(1, len(ALL_CLASSES))
    row[0, _idx("logo-or-banner")] = 0.40
    row[0, _idx("interior-room")] = 0.38
    other = (1.0 - 0.40 - 0.38) / (len(ALL_CLASSES) - 2)
    for c in ALL_CLASSES:
        if c not in ("logo-or-banner", "interior-room"):
            row[0, _idx(c)] = other

    out = _decide_from_scores(row, parent_ids=["ambig-1"])
    assert out[0].label == "interior-room", "ambiguous kept-vs-dropped must keep"
    assert out[0].margin < AMBIGUOUS_MARGIN
    assert "[WARN] triage_ambiguous_kept" in capsys.readouterr().err


def test_ambiguous_between_two_kept_stays_with_top1(capsys):
    row = torch.zeros(1, len(ALL_CLASSES))
    row[0, _idx("interior-room")] = 0.40
    row[0, _idx("building-exterior")] = 0.38
    other = (1.0 - 0.78) / (len(ALL_CLASSES) - 2)
    for c in ALL_CLASSES:
        if c not in ("interior-room", "building-exterior"):
            row[0, _idx(c)] = other

    out = _decide_from_scores(row, parent_ids=["ambig-2"])
    # Both kept, so we don't override — top1 wins.
    assert out[0].label == "interior-room"
    err = capsys.readouterr().err
    assert "[WARN] triage_ambiguous" in err
    assert "[WARN] triage_ambiguous_kept" not in err


def test_empty_input_returns_empty():
    probs = torch.zeros(0, len(ALL_CLASSES))
    out = _decide_from_scores(probs, parent_ids=[])
    assert out == []
