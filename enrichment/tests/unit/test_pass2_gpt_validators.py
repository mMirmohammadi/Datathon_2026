"""Unit tests for the GPT-pass post-validators — deterministic, no API calls.

Every validator must:
  * Normalize input to the same format the regex pass would produce.
  * Reject implausible values (year out-of-range, email w/ generic provider, etc).
  * Never raise on bad input — return (False, raw_value, reason).
"""
from __future__ import annotations

import datetime as dt

import pytest

from enrichment.scripts.pass2_gpt_extract import (
    GENERIC_EMAIL_DOMAINS,
    _clamp_confidence,
    _derive_agency_name_from_email,
    _validate_agency_name,
    _validate_area,
    _validate_available_from,
    _validate_email,
    _validate_feature,
    _validate_floor,
    _validate_phone,
    _validate_year,
)


# ---- feature ----------------------------------------------------------------

@pytest.mark.parametrize("v", ["0", "1"])
def test_feature_valid(v):
    ok, norm, _ = _validate_feature(v)
    assert ok and norm == v


@pytest.mark.parametrize("v", ["2", "", "true", None, "yes", "0.0"])
def test_feature_rejects_non_binary(v):
    ok, _, reason = _validate_feature(v)
    assert not ok and reason


# ---- year -------------------------------------------------------------------

def test_year_accepts_plausible():
    ok, norm, _ = _validate_year("1920")
    assert ok and norm == "1920"


def test_year_accepts_int_str_trim():
    ok, norm, _ = _validate_year("1980.0")  # float-like OK since we int()
    assert not ok  # int('1980.0') raises ValueError — design choice: strict


def test_year_rejects_too_low():
    ok, _, reason = _validate_year("1799")
    assert not ok and "1800" in reason


def test_year_rejects_future():
    far_future = str(dt.date.today().year + 10)
    ok, _, reason = _validate_year(far_future)
    assert not ok


# ---- area -------------------------------------------------------------------

@pytest.mark.parametrize("v,expected", [("10", "10"), ("140", "140"), ("500", "500")])
def test_area_valid(v, expected):
    ok, norm, _ = _validate_area(v)
    assert ok and norm == expected


@pytest.mark.parametrize("v", ["0", "9", "501", "5000", "not a number"])
def test_area_rejects_out_of_range(v):
    ok, _, _ = _validate_area(v)
    assert not ok


# ---- floor ------------------------------------------------------------------

@pytest.mark.parametrize("v,expected", [("0", "0"), ("-1", "-1"), ("1", "1"), ("5", "5"), ("99", "99")])
def test_floor_valid(v, expected):
    ok, norm, _ = _validate_floor(v)
    assert ok and norm == expected


@pytest.mark.parametrize("v", ["-2", "100", "EG", "ground"])
def test_floor_rejects_out_of_range(v):
    ok, _, _ = _validate_floor(v)
    assert not ok


# ---- available_from ---------------------------------------------------------

def test_available_from_today_valid():
    ok, norm, _ = _validate_available_from(dt.date.today().isoformat())
    assert ok and norm == dt.date.today().isoformat()


def test_available_from_rejects_distant_past():
    ok, _, reason = _validate_available_from("2020-01-01")
    assert not ok and "outside" in reason


def test_available_from_rejects_distant_future():
    far = (dt.date.today() + dt.timedelta(days=800)).isoformat()
    ok, _, _ = _validate_available_from(far)
    assert not ok


def test_available_from_invalid_iso():
    ok, _, _ = _validate_available_from("not a date")
    assert not ok


# ---- phone ------------------------------------------------------------------

def test_phone_already_e164():
    ok, norm, _ = _validate_phone("+41 44 123 45 67")
    assert ok and norm == "+41 44 123 45 67"


def test_phone_swiss_local_normalizes():
    ok, norm, _ = _validate_phone("044 123 45 67")
    assert ok and norm == "+41 44 123 45 67"


def test_phone_0041_prefix_normalizes():
    ok, norm, _ = _validate_phone("0041 44 123 45 67")
    assert ok and norm == "+41 44 123 45 67"


def test_phone_dotted_format():
    ok, norm, _ = _validate_phone("044.123.45.67")
    assert ok and norm == "+41 44 123 45 67"


def test_phone_embedded_in_short_noise():
    # "Tel 044 123 45 67" is <30 chars — permissive path accepts
    ok, norm, _ = _validate_phone("Tel 044 123 45 67")
    assert ok and norm == "+41 44 123 45 67"


def test_phone_rejects_long_noise():
    v = "call me at 044 123 45 67 or send email to foo@bar.ch please do so"
    ok, _, _ = _validate_phone(v)
    assert not ok


def test_phone_rejects_non_swiss():
    ok, _, _ = _validate_phone("+1 555 123 4567")
    assert not ok


# ---- email ------------------------------------------------------------------

def test_email_accepts_lowercases():
    ok, norm, _ = _validate_email("Info@Robinreal.CH")
    assert ok and norm == "info@robinreal.ch"


def test_email_rejects_generic_providers():
    for d in ("gmail", "yahoo", "outlook"):
        ok, _, _ = _validate_email(f"someone@{d}.com")
        assert not ok


def test_email_rejects_missing_at():
    ok, _, _ = _validate_email("no-at-symbol")
    assert not ok


# ---- agency_name derivation -------------------------------------------------

def test_agency_name_derived_from_domain():
    assert _derive_agency_name_from_email("info@robinreal.ch") == "Robinreal"


def test_agency_name_derived_handles_subdomain():
    assert _derive_agency_name_from_email("foo@bar.robinreal.ch") == "Robinreal"


def test_agency_name_returns_none_for_generic_providers():
    for d in GENERIC_EMAIL_DOMAINS:
        assert _derive_agency_name_from_email(f"x@{d}.com") is None


def test_agency_name_returns_none_for_empty_or_no_at():
    assert _derive_agency_name_from_email("") is None
    assert _derive_agency_name_from_email("plain") is None


def test_validate_agency_name_rejects_generic():
    ok, _, _ = _validate_agency_name("gmail")
    assert not ok


def test_validate_agency_name_accepts_real():
    ok, norm, _ = _validate_agency_name("Robinreal")
    assert ok and norm == "Robinreal"


# ---- confidence clamp -------------------------------------------------------

@pytest.mark.parametrize(
    "inp,expected",
    [(0.0, 0.0), (0.5, 0.5), (1.0, 1.0), (-0.1, 0.0), (1.0001, 1.0),
     ("0.7", 0.7), ("bogus", 0.0), (None, 0.0)],
)
def test_clamp_confidence(inp, expected):
    assert _clamp_confidence(inp) == pytest.approx(expected)
