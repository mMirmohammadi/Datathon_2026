"""Tests for the regex fallback path of query_plan.get_plan.

Live Claude calls are exercised separately by `scripts/eval_mvp.py`; these
unit tests validate the offline, deterministic parts:

  - Regex fallback extraction (rooms, price, city)
  - canonicalizers (canton, features, object_category)
  - QueryPlan → HardFilters adapter
"""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from app.models.schemas import QueryPlan


@pytest.fixture
def qp_module():
    # Defer import so env-loading happens in this process
    import app.participant.query_plan as qp

    qp.get_plan.cache_clear()
    return qp


def test_regex_fallback_extracts_rooms_price_city(qp_module):
    plan = qp_module._regex_fallback(
        "3-room apartment in Zurich under 2800 CHF with balcony"
    )
    assert plan.rooms.min_value == 3.0
    assert plan.rooms.max_value == 3.0
    assert plan.price.max_value == 2800.0
    assert plan.city == ["Zürich"]
    assert plan.confidence == 0.3  # fallback signal


def test_regex_fallback_handles_half_rooms_de(qp_module):
    plan = qp_module._regex_fallback("3.5-Zimmer in Zürich max 2500 Fr.")
    assert plan.rooms.min_value == 3.5
    assert plan.price.max_value == 2500.0


def test_regex_fallback_handles_range(qp_module):
    plan = qp_module._regex_fallback("2 to 4 rooms in Basel")
    assert plan.rooms.min_value == 2.0
    assert plan.rooms.max_value == 4.0
    assert plan.city == ["Basel"]


def test_regex_fallback_italian_locali(qp_module):
    plan = qp_module._regex_fallback("bilocale a Lugano")
    # "bi" prefix not handled — regex just matches the first number-locali pattern.
    # With no explicit "2 locali", we expect no rooms extracted. This documents
    # the limit of the fallback (Claude is the real path for Italian numerals).
    assert plan.city == ["Lugano"]


def test_canonicalize_canton(qp_module):
    assert qp_module._canonicalize_canton("zh") == "ZH"
    assert qp_module._canonicalize_canton("GENEVA") == "GE"  # first 2 chars
    assert qp_module._canonicalize_canton("xx") is None
    assert qp_module._canonicalize_canton(None) is None
    assert qp_module._canonicalize_canton("") is None


def test_canonicalize_features_known_and_unknown(qp_module, capsys):
    out = qp_module._canonicalize_features(["balcony", "xyz_bogus", "ELEVATOR", "parking"])
    assert out == ["balcony", "elevator", "parking"]
    # unknown feature must produce a [WARN] log — no silent drops
    err = capsys.readouterr().out
    assert "[WARN] query_plan._canonicalize_features" in err
    assert "xyz_bogus" in err


def test_canonicalize_object_category_passes_through_canonical(qp_module):
    out = qp_module._canonicalize_object_category(["Wohnung", "Studio"])
    assert out == ["Wohnung", "Studio"]


def test_canonicalize_object_category_maps_english(qp_module, capsys):
    out = qp_module._canonicalize_object_category(["Apartment"])
    assert out == ["Wohnung"]


def test_canonicalize_object_category_drops_rooms_synonyms(qp_module, capsys):
    # "bilocale" should NOT become a category — it's a room count
    out = qp_module._canonicalize_object_category(["bilocale"])
    assert out is None
    err = capsys.readouterr().out
    assert "[WARN]" in err


def test_canonicalize_object_category_drops_unknown(qp_module, capsys):
    out = qp_module._canonicalize_object_category(["SomeWeirdCategory"])
    assert out is None
    err = capsys.readouterr().out
    assert "[WARN]" in err
    assert "SomeWeirdCategory" in err


def test_queryplan_to_hard_filters_roundtrip(qp_module):
    plan = QueryPlan(
        city=["Zürich"],
        canton="ZH",
        raw_query="test",
        required_features=["balcony", "elevator"],
    )
    plan.rooms.min_value = 3
    plan.rooms.max_value = 3
    plan.price.max_value = 2800
    hf = qp_module.queryplan_to_hard_filters(plan)
    assert hf.city == ["Zürich"]
    assert hf.canton == "ZH"
    assert hf.min_rooms == 3.0
    assert hf.max_rooms == 3.0
    assert hf.max_price == 2800
    assert hf.features == ["balcony", "elevator"]


def test_get_plan_rejects_empty_query(qp_module):
    with pytest.raises(ValueError):
        qp_module.get_plan("")
    with pytest.raises(ValueError):
        qp_module.get_plan("   ")


def test_get_plan_rejects_long_query(qp_module):
    long_q = "a" * 2000
    with pytest.raises(ValueError):
        qp_module.get_plan(long_q)


def test_get_plan_rejects_non_string(qp_module):
    with pytest.raises(TypeError):
        qp_module.get_plan(123)  # type: ignore[arg-type]


def test_get_plan_falls_back_on_missing_api_key(qp_module, capsys):
    """When no API key, get_plan() must return a regex plan + log [WARN]."""
    with patch.object(qp_module, "_client", None):
        qp_module.get_plan.cache_clear()
        plan = qp_module.get_plan("3 rooms in Zurich under 2800 CHF")
    assert plan.rooms.min_value == 3.0
    assert plan.price.max_value == 2800.0
    out = capsys.readouterr().out
    assert "[WARN] query_plan.get_plan" in out
    assert "no_api_key" in out
