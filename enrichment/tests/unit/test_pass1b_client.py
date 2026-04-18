"""Unit tests for pass 1b Nominatim client — mocked HTTP, no live API calls."""
from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest

from enrichment.scripts.pass1b_nominatim import (
    NominatimClient,
    NominatimConfig,
    _coord_key,
    _extract_postcode,
    _extract_street,
    _is_ch_response,
)


@pytest.fixture
def cfg() -> NominatimConfig:
    return NominatimConfig(
        base_url="https://nominatim.test",
        user_agent="test-agent/1.0",
        rate_sec=0.001,  # bypass rate limit for unit tests
    )


@pytest.fixture
def mock_client() -> MagicMock:
    return MagicMock(spec=httpx.Client)


# --- coord_key ---

def test_coord_key_rounds_to_4dp():
    assert _coord_key(47.369712, 8.538623) == "47.3697,8.5386"


def test_coord_key_negative_and_zero():
    assert _coord_key(0.0, 0.0) == "0.0,0.0"
    assert _coord_key(-1.23456, -7.89012) == "-1.2346,-7.8901"


def test_coord_key_same_within_11m():
    # ~11m = 4th decimal place. Two points within that range hash identically.
    assert _coord_key(47.36970, 8.53860) == _coord_key(47.36971, 8.53864)


# --- extract_postcode ---

def test_extract_postcode_happy():
    resp = {"address": {"postcode": "8001", "road": "Bahnhofstr.", "country_code": "ch"}}
    assert _extract_postcode(resp) == "8001"


def test_extract_postcode_strips_whitespace():
    resp = {"address": {"postcode": "  8001  "}}
    assert _extract_postcode(resp) == "8001"


def test_extract_postcode_missing():
    assert _extract_postcode({}) is None
    assert _extract_postcode({"address": {}}) is None
    assert _extract_postcode({"address": {"postcode": ""}}) is None
    assert _extract_postcode({"address": {"postcode": 1234}}) is None  # not a string


# --- extract_street ---

def test_extract_street_road_plus_housenumber():
    resp = {"address": {"road": "Bahnhofstrasse", "house_number": "42"}}
    assert _extract_street(resp) == "Bahnhofstrasse 42"


def test_extract_street_road_only():
    resp = {"address": {"road": "Bahnhofstrasse"}}
    assert _extract_street(resp) == "Bahnhofstrasse"


def test_extract_street_fallback_pedestrian():
    resp = {"address": {"pedestrian": "Niederdorfstrasse", "house_number": "12"}}
    assert _extract_street(resp) == "Niederdorfstrasse 12"


def test_extract_street_empty():
    assert _extract_street({}) is None
    assert _extract_street({"address": {}}) is None


# --- is_ch_response ---

def test_is_ch_response():
    assert _is_ch_response({"address": {"country_code": "ch"}}) is True
    assert _is_ch_response({"address": {"country_code": "CH"}}) is True
    assert _is_ch_response({"address": {"country_code": "de"}}) is False
    assert _is_ch_response({}) is False
    assert _is_ch_response({"address": {}}) is False


# --- NominatimClient retry + success paths (mocked) ---

def _mock_response(status_code: int, json_data=None, text: str = ""):
    r = MagicMock(spec=httpx.Response)
    r.status_code = status_code
    if json_data is not None:
        r.json = MagicMock(return_value=json_data)
    else:
        r.json = MagicMock(side_effect=ValueError("no JSON"))
    r.text = text
    return r


def test_reverse_returns_parsed_json_on_200(cfg, mock_client):
    mock_client.get.return_value = _mock_response(
        200,
        {"address": {"postcode": "8001", "country_code": "ch"}},
    )
    client = NominatimClient(cfg, client=mock_client)
    result = client.reverse(47.37, 8.54)
    assert result == {"address": {"postcode": "8001", "country_code": "ch"}}
    assert mock_client.get.call_count == 1


def test_reverse_retries_on_429_then_succeeds(cfg, mock_client, monkeypatch):
    monkeypatch.setattr("time.sleep", lambda _: None)  # skip waits
    mock_client.get.side_effect = [
        _mock_response(429),
        _mock_response(200, {"address": {"postcode": "8001"}}),
    ]
    client = NominatimClient(cfg, client=mock_client)
    result = client.reverse(47.37, 8.54)
    assert result == {"address": {"postcode": "8001"}}
    assert mock_client.get.call_count == 2


def test_reverse_exhausts_retries_returns_none(cfg, mock_client, monkeypatch):
    monkeypatch.setattr("time.sleep", lambda _: None)
    mock_client.get.return_value = _mock_response(503)
    client = NominatimClient(cfg, client=mock_client)
    result = client.reverse(47.37, 8.54)
    assert result is None
    assert mock_client.get.call_count == 3  # MAX_RETRIES


def test_reverse_4xx_non_429_returns_none_without_retry(cfg, mock_client):
    mock_client.get.return_value = _mock_response(403, text="forbidden")
    client = NominatimClient(cfg, client=mock_client)
    result = client.reverse(47.37, 8.54)
    assert result is None
    assert mock_client.get.call_count == 1  # no retry on 403


def test_reverse_non_json_body_returns_empty_dict(cfg, mock_client):
    r = MagicMock(spec=httpx.Response)
    r.status_code = 200
    r.json = MagicMock(side_effect=ValueError("not JSON"))
    r.text = "<html>error page</html>"
    mock_client.get.return_value = r
    client = NominatimClient(cfg, client=mock_client)
    result = client.reverse(47.37, 8.54)
    assert result == {}  # graceful empty — caller keeps row pending


def test_reverse_http_error_retries(cfg, mock_client, monkeypatch):
    monkeypatch.setattr("time.sleep", lambda _: None)
    mock_client.get.side_effect = [
        httpx.ConnectError("net down"),
        httpx.ConnectError("net down"),
        _mock_response(200, {"address": {"postcode": "8001"}}),
    ]
    client = NominatimClient(cfg, client=mock_client)
    result = client.reverse(47.37, 8.54)
    assert result == {"address": {"postcode": "8001"}}
    assert mock_client.get.call_count == 3


def test_config_clamps_rate_below_1sec(monkeypatch):
    from enrichment.scripts.pass1b_nominatim import _load_config
    monkeypatch.setenv("NOMINATIM_RATE_SEC", "0.1")
    cfg = _load_config()
    assert cfg.rate_sec == 1.0  # clamped up


def test_config_user_agent_includes_contact(monkeypatch):
    from enrichment.scripts.pass1b_nominatim import _load_config
    monkeypatch.setenv("NOMINATIM_CONTACT_EMAIL", "rzninvo@gmail.com")
    cfg = _load_config()
    assert "rzninvo@gmail.com" in cfg.user_agent
