from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from app.models.schemas import HardFilters
from app.participant import hard_fact_extraction
from app.participant.hard_fact_extraction import (
    SYSTEM_PROMPT,
    _HARD_FILTERS_SCHEMA,
    extract_hard_facts,
)


def _fake_openai_response(payload: dict[str, Any] | str) -> SimpleNamespace:
    content = payload if isinstance(payload, str) else json.dumps(payload)
    return SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=content))]
    )


class _FakeClient:
    def __init__(self, response: Any = None, raise_with: Exception | None = None) -> None:
        self._response = response
        self._raise_with = raise_with
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))
        self.calls: list[dict[str, Any]] = []

    def _create(self, **kwargs: Any) -> SimpleNamespace:
        self.calls.append(kwargs)
        if self._raise_with is not None:
            raise self._raise_with
        return self._response


@pytest.fixture(autouse=True)
def _api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")


def _install_client(monkeypatch: pytest.MonkeyPatch, client: _FakeClient) -> None:
    monkeypatch.setattr(hard_fact_extraction, "OpenAI", lambda *a, **kw: client)


def test_happy_path_populates_new_and_old_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _FakeClient(
        response=_fake_openai_response(
            {
                "city": ["zurich"],
                "postal_code": None,
                "canton": "ZH",
                "min_price": None,
                "max_price": 2800,
                "min_rooms": 3.0,
                "max_rooms": 3.5,
                "min_area": 70,
                "max_area": None,
                "min_floor": 1,
                "max_floor": None,
                "min_year_built": 2015,
                "max_year_built": None,
                "available_from_after": "2026-06-01",
                "features": ["balcony"],
                "features_excluded": ["fireplace"],
                "object_category": ["apartment"],
            }
        )
    )
    _install_client(monkeypatch, client)

    result = extract_hard_facts("3-3.5 rooms in Zurich, min 70m^2, balcony, no fireplace, from June, modern")

    assert isinstance(result, HardFilters)
    assert result.city == ["zurich"]
    assert result.canton == "ZH"
    assert result.max_price == 2800
    assert result.min_rooms == 3.0
    assert result.max_rooms == 3.5
    assert result.min_area == 70
    assert result.min_floor == 1
    assert result.min_year_built == 2015
    assert result.available_from_after == "2026-06-01"
    assert result.features == ["balcony"]
    assert result.features_excluded == ["fireplace"]
    assert result.object_category == ["apartment"]
    assert len(client.calls) == 1
    assert client.calls[0]["model"] == "gpt-4o-mini"
    assert client.calls[0]["response_format"]["type"] == "json_schema"


def test_empty_payload_returns_default_hard_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _FakeClient(response=_fake_openai_response({}))
    _install_client(monkeypatch, client)

    result = extract_hard_facts("something vague")

    assert isinstance(result, HardFilters)
    assert result.city is None
    assert result.max_price is None
    assert result.features is None
    assert result.features_excluded is None
    assert result.min_area is None


def test_sdk_error_logs_warn_and_reraises(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    client = _FakeClient(raise_with=RuntimeError("boom"))
    _install_client(monkeypatch, client)

    with pytest.raises(RuntimeError, match="boom"):
        extract_hard_facts("whatever")

    out = capsys.readouterr().out
    assert "[WARN] extract_hard_facts failed" in out
    assert "RuntimeError" in out
    assert "fallback=raise" in out


def test_invalid_payload_logs_warn_and_reraises(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    client = _FakeClient(
        response=_fake_openai_response({"min_price": -1})
    )
    _install_client(monkeypatch, client)

    with pytest.raises(Exception):
        extract_hard_facts("weird")

    out = capsys.readouterr().out
    assert "[WARN] extract_hard_facts failed" in out


def test_missing_api_key_logs_warn_and_reraises(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        extract_hard_facts("anything")

    out = capsys.readouterr().out
    assert "[WARN] extract_hard_facts failed" in out


# ---------- schema contract ----------

class TestSchema:
    def test_schema_is_strict(self) -> None:
        assert _HARD_FILTERS_SCHEMA["strict"] is True
        assert _HARD_FILTERS_SCHEMA["schema"]["additionalProperties"] is False

    def test_offer_type_is_dropped(self) -> None:
        props = _HARD_FILTERS_SCHEMA["schema"]["properties"]
        assert "offer_type" not in props

    def test_canton_enum_has_26_codes(self) -> None:
        canton = _HARD_FILTERS_SCHEMA["schema"]["properties"]["canton"]
        enum = canton["enum"]
        codes = [v for v in enum if v is not None]
        assert len(codes) == 26
        assert {"ZH", "GE", "BE", "BS", "VD", "TI", "UR"} <= set(codes)

    def test_object_category_items_are_english_enum(self) -> None:
        oc = _HARD_FILTERS_SCHEMA["schema"]["properties"]["object_category"]
        items_enum = oc["items"]["enum"]
        assert "apartment" in items_enum
        assert "house" in items_enum
        assert "furnished_apartment" in items_enum
        assert "studio" in items_enum
        # No German residue.
        for v in items_enum:
            assert "Wohnung" not in v
            assert "Haus" not in v

    def test_features_excluded_mirrors_features_enum(self) -> None:
        props = _HARD_FILTERS_SCHEMA["schema"]["properties"]
        assert "features_excluded" in props
        assert (
            props["features_excluded"]["items"]["enum"]
            == props["features"]["items"]["enum"]
        )

    def test_new_filter_fields_present(self) -> None:
        props = _HARD_FILTERS_SCHEMA["schema"]["properties"]
        for field in (
            "min_area", "max_area", "min_floor", "max_floor",
            "min_year_built", "max_year_built", "available_from_after",
        ):
            assert field in props, f"missing {field}"
            assert field in _HARD_FILTERS_SCHEMA["schema"]["required"]


# ---------- prompt pins ----------

class TestSystemPrompt:
    def test_contains_emission_rules(self) -> None:
        assert "Kreis" in SYSTEM_PROMPT
        assert "furnished" in SYSTEM_PROMPT or "moebliert" in SYSTEM_PROMPT.lower()
        assert "bedrooms" in SYSTEM_PROMPT.lower() or "schlafzimmer" in SYSTEM_PROMPT.lower()

    def test_contains_non_emission_rules(self) -> None:
        # Commute-is-soft rule and don't-hallucinate rule.
        assert "commute" in SYSTEM_PROMPT.lower() or "min zum hb" in SYSTEM_PROMPT.lower()
        assert "family-friendly" in SYSTEM_PROMPT.lower()
        assert "modern" in SYSTEM_PROMPT.lower()

    def test_contains_both_few_shot_examples(self) -> None:
        assert "3-room bright apartment in Zurich" in SYSTEM_PROMPT
        assert "2.5 bis 3.5 Zimmer" in SYSTEM_PROMPT

    def test_canton_hint_lists_codes(self) -> None:
        assert "ZH" in SYSTEM_PROMPT and "GE" in SYSTEM_PROMPT
