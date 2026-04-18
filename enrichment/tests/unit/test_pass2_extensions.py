"""Unit tests for pass 2 extensions: floor, area, available_from, agency_name."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from enrichment.common.text_extract import find_first_match
from enrichment.scripts import pass2_text_extract as p2

PATTERNS_DIR = Path(__file__).resolve().parents[2] / "patterns"


def _langs(spec: dict) -> dict[str, list[str]]:
    if "all" in spec:
        all_pats = spec["all"]
        return {"de": all_pats, "fr": all_pats, "it": all_pats, "en": all_pats}
    return {k: spec.get(k, []) for k in ("de", "fr", "it", "en")}


@pytest.fixture(scope="module")
def floor_patterns() -> dict:
    with (PATTERNS_DIR / "floor.yaml").open() as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def neg() -> dict[str, list[str]]:
    with (PATTERNS_DIR / "negation.yaml").open() as f:
        data = yaml.safe_load(f)
    return {k: data.get(k, []) for k in ("de", "fr", "it", "en")}


# =============================================================================
# Floor
# =============================================================================

@pytest.mark.parametrize("text,lang,expected_value", [
    # Ground floor → "0"
    ("Helle Wohnung im Erdgeschoss", "de", "0"),
    ("charmant appartement au rez-de-chaussée", "fr", "0"),
    ("appartamento al piano terra", "it", "0"),
    ("apartment on ground floor", "en", "0"),
    # Basement → "-1"
    ("Wohnung im Souterrain mit Aussicht", "de", "-1"),
    ("appartement au sous-sol", "fr", "-1"),
    ("appartamento al piano interrato", "it", "-1"),
    ("basement unit", "en", "-1"),
    # Numeric
    ("Wohnung im 3. Stock", "de", "3"),
    ("Dachgeschoss im 5. OG", "de", "5"),
    ("3e étage avec ascenseur", "fr", "3"),
    ("4° piano", "it", "4"),
    ("2nd floor flat", "en", "2"),
])
def test_floor_extraction(floor_patterns, neg, text, lang, expected_value):
    patterns = {
        k: (float(floor_patterns[k]["base_confidence"]), _langs(floor_patterns[k]))
        for k in ("floor_ground", "floor_basement", "floor_numeric")
    }
    result = p2._extract_floor(text, lang, patterns, neg, lookback=3)
    assert result is not None, f"no match for: {text!r}"
    assert result[0] == expected_value


def test_floor_priority_ground_over_numeric(floor_patterns, neg):
    """When both are present, ground wins."""
    patterns = {
        k: (float(floor_patterns[k]["base_confidence"]), _langs(floor_patterns[k]))
        for k in ("floor_ground", "floor_basement", "floor_numeric")
    }
    # "Erdgeschoss, aber auch Zugang zum 3. Stock" — ground should win per priority.
    text = "Erdgeschoss mit Zugang zum 3. Stock"
    result = p2._extract_floor(text, "de", patterns, neg, lookback=3)
    assert result is not None
    assert result[0] == "0"


def test_floor_numeric_rejects_implausible(floor_patterns, neg):
    """The numeric extractor drops >99 via regex; the script also validates 1..99."""
    patterns = {
        k: (float(floor_patterns[k]["base_confidence"]), _langs(floor_patterns[k]))
        for k in ("floor_ground", "floor_basement", "floor_numeric")
    }
    # "150. Stock" shouldn't match the 1-2 digit pattern
    result = p2._extract_floor("150. Stock", "de", patterns, neg, lookback=3)
    # Regex \d{1,2} won't capture 3 digits as a whole; might capture "15" or "50".
    # Even if so, validated 1..99. Accept either no-match or a plausible 15/50.
    if result is not None:
        assert 1 <= int(result[0]) <= 99


def test_floor_no_match(floor_patterns, neg):
    patterns = {
        k: (float(floor_patterns[k]["base_confidence"]), _langs(floor_patterns[k]))
        for k in ("floor_ground", "floor_basement", "floor_numeric")
    }
    result = p2._extract_floor("Schöne Wohnung mit Küche", "de", patterns, neg, lookback=3)
    assert result is None


# =============================================================================
# Area validation
# =============================================================================

@pytest.mark.parametrize("raw,expected", [
    ("85", "85"),
    ("10", "10"),
    ("500", "500"),
    ("1",   None),       # too small
    ("501", None),       # too big
    ("0",   None),
    ("9",   None),
    ("abc", None),
    ("",    None),
])
def test_validate_area(raw, expected):
    assert p2._validate_area(raw) == expected


# =============================================================================
# Available-from parsing
# =============================================================================

@pytest.fixture(scope="module")
def avail_patterns():
    with (PATTERNS_DIR / "available_from.yaml").open() as f:
        data = yaml.safe_load(f)
    return {
        k: (float(data[k]["base_confidence"]), _langs(data[k]))
        for k in ("available_from_immediate", "available_from_iso", "available_from_european")
    }


def test_available_from_immediate_de(avail_patterns, neg):
    result = p2._extract_available_from("Ab sofort verfügbar", "de", avail_patterns, neg, 3)
    assert result is not None
    today = datetime.now(timezone.utc).date().isoformat()
    assert result[0] == today


@pytest.mark.parametrize("phrase,lang", [
    ("per sofort", "de"),
    ("disponible immédiatement", "fr"),
    ("subito disponibile", "it"),
    ("immediately available", "en"),
])
def test_available_from_immediate_all_langs(avail_patterns, neg, phrase, lang):
    result = p2._extract_available_from(phrase, lang, avail_patterns, neg, 3)
    assert result is not None
    today = datetime.now(timezone.utc).date().isoformat()
    assert result[0] == today


def test_available_from_iso(avail_patterns, neg):
    # Pick a date in the near future — within [today-90d, today+2y]
    future = (datetime.now(timezone.utc).date() + timedelta(days=60)).isoformat()
    result = p2._extract_available_from(f"Bezug ab {future}", "de", avail_patterns, neg, 3)
    assert result is not None
    assert result[0] == future


def test_available_from_european_date(avail_patterns, neg):
    future = datetime.now(timezone.utc).date() + timedelta(days=60)
    text = f"Einzug per {future.day}.{future.month:02d}.{future.year}"
    result = p2._extract_available_from(text, "de", avail_patterns, neg, 3)
    assert result is not None
    assert result[0] == future.isoformat()


def test_available_from_rejects_ancient_date(avail_patterns, neg):
    """1990 should be rejected (outside [today-90d, today+2y])."""
    result = p2._extract_available_from("ab 01.05.1990", "de", avail_patterns, neg, 3)
    assert result is None


def test_available_from_rejects_distant_future(avail_patterns, neg):
    result = p2._extract_available_from("ab 2099-05-01", "de", avail_patterns, neg, 3)
    assert result is None


def test_available_from_rejects_invalid_date(avail_patterns, neg):
    """Feb 30 doesn't exist."""
    result = p2._extract_available_from("per 30.02.2026", "de", avail_patterns, neg, 3)
    assert result is None


# =============================================================================
# Agency name derivation
# =============================================================================

@pytest.mark.parametrize("email,expected", [
    ("info@robinreal.ch",       "Robinreal"),
    ("contact@comparis.ch",     "Comparis"),
    ("anna.mueller@mueller-ag.ch", "Mueller-ag"),
    ("INFO@EXAMPLE.COM",        "Example"),
])
def test_derive_agency_name_from_email(email, expected):
    assert p2._derive_agency_name_from_email(email) == expected


@pytest.mark.parametrize("email", [
    "private@gmail.com",
    "someone@hotmail.com",
    "x@bluewin.ch",
    "y@icloud.com",
    "test@outlook.com",
])
def test_derive_rejects_personal_email_providers(email):
    assert p2._derive_agency_name_from_email(email) is None


@pytest.mark.parametrize("bad", [
    "",
    None,
    "noatsign",
    "nodomain@",
    "@nolocal",
    "@.",
    "no-tld@x",
])
def test_derive_rejects_invalid_email(bad):
    assert p2._derive_agency_name_from_email(bad) is None
