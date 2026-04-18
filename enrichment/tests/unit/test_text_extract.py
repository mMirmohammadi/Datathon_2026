"""Unit tests for find_first_match against real YAML patterns."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from enrichment.common.text_extract import find_first_match

PATTERNS_DIR = Path(__file__).resolve().parents[2] / "patterns"


@pytest.fixture(scope="module")
def features_patterns() -> dict:
    with (PATTERNS_DIR / "features.yaml").open() as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def negation_patterns() -> dict[str, list[str]]:
    with (PATTERNS_DIR / "negation.yaml").open() as f:
        data = yaml.safe_load(f)
    return {k: data.get(k, []) for k in ("de", "fr", "it", "en")}


def _langs(spec: dict) -> dict[str, list[str]]:
    """Convert a YAML spec to {lang -> [regex]}. Supports 'all' for lang-agnostic."""
    if "all" in spec:
        all_pats = spec["all"]
        return {"de": all_pats, "fr": all_pats, "it": all_pats, "en": all_pats}
    return {k: spec.get(k, []) for k in ("de", "fr", "it", "en")}


# --- Positive matches across all 4 languages for a representative feature ---
@pytest.mark.parametrize("text,detected_lang,expected_lang_used", [
    ("Schöne Wohnung mit Balkon",             "de", "de"),
    ("Terrasse mit Aussicht",                  "de", "de"),
    ("appartement avec balcon ensoleillé",    "fr", "fr"),
    ("appartamento con balcone panoramico",    "it", "it"),
    ("flat with balcony",                      "en", "en"),
])
def test_balcony_positive(text, detected_lang, expected_lang_used, features_patterns, negation_patterns):
    hit = find_first_match(text, _langs(features_patterns["balcony"]), detected_lang, negation_patterns)
    assert hit is not None
    assert hit.lang_used == expected_lang_used
    assert hit.negated is False


# --- Negation detection ---
@pytest.mark.parametrize("text,lang", [
    ("Wohnung ohne Balkon",        "de"),
    ("kein Balkon vorhanden",      "de"),
    ("appartement sans balcon",    "fr"),
    ("logement pas de balcon",     "fr"),
    ("appartamento senza balcone", "it"),
    ("apartment without balcony",  "en"),
    ("no balcony available",       "en"),
])
def test_balcony_negated(text, lang, features_patterns, negation_patterns):
    hit = find_first_match(text, _langs(features_patterns["balcony"]), lang, negation_patterns)
    assert hit is not None
    assert hit.negated is True


# --- No-match cases ---
@pytest.mark.parametrize("text,lang", [
    ("Schöne Wohnung mit Küche",  "de"),
    ("bel appartement meublé",     "fr"),
    ("casa spaziosa",              "it"),
    ("modern flat with kitchen",   "en"),
    ("",                           "en"),
])
def test_balcony_no_match(text, lang, features_patterns, negation_patterns):
    hit = find_first_match(text, _langs(features_patterns["balcony"]), lang, negation_patterns)
    assert hit is None


# --- Elevator per language ---
@pytest.mark.parametrize("text,lang", [
    ("Haus mit Lift im Treppenhaus",   "de"),
    ("mit Aufzug und Parkplatz",       "de"),
    ("immeuble avec ascenseur",        "fr"),
    ("palazzo con ascensore",          "it"),
    ("building with elevator",         "en"),
    ("flat with lift access",          "en"),
])
def test_elevator_positive(text, lang, features_patterns, negation_patterns):
    hit = find_first_match(text, _langs(features_patterns["elevator"]), lang, negation_patterns)
    assert hit is not None and not hit.negated


# --- Fireplace positive ---
@pytest.mark.parametrize("text,lang", [
    ("Wohnzimmer mit Cheminée",   "de"),
    ("salon avec cheminée",        "fr"),
    ("soggiorno con caminetto",    "it"),
    ("living room with fireplace", "en"),
])
def test_fireplace_positive(text, lang, features_patterns, negation_patterns):
    hit = find_first_match(text, _langs(features_patterns["fireplace"]), lang, negation_patterns)
    assert hit is not None and not hit.negated


# --- Minergie brand (language-agnostic, should match in any language) ---
@pytest.mark.parametrize("text,detected_lang", [
    ("Minergie-A zertifiziertes Gebäude", "de"),
    ("immeuble Minergie-P certifié",      "fr"),
    ("edificio Minergie certificato",     "it"),
    ("Minergie-certified building",       "en"),
])
def test_minergie_positive(text, detected_lang, features_patterns, negation_patterns):
    hit = find_first_match(text, _langs(features_patterns["minergie_certified"]), detected_lang, negation_patterns)
    assert hit is not None and not hit.negated


# --- Year built captures group 1 ---
def test_year_built_captures_year(negation_patterns):
    with (PATTERNS_DIR / "year_built.yaml").open() as f:
        spec = yaml.safe_load(f)["year_built"]
    langs = _langs(spec)
    cases = [
        ("Baujahr: 1987",                             "de", "1987"),
        ("erbaut im Jahr 2010",                        "de", "2010"),
        ("année de construction: 1965",                "fr", "1965"),
        ("construit en 2001",                          "fr", "2001"),
        ("anno di costruzione 1978",                   "it", "1978"),
        ("built in 2015",                              "en", "2015"),
    ]
    for text, lang, expected_year in cases:
        hit = find_first_match(text, langs, lang, negation_patterns)
        assert hit is not None, text
        assert hit.groups[0] == expected_year, f"{text}: got {hit.groups}"


# --- Agency phone capturing groups yield correct normalization ---
def test_agency_phone_captures_4_groups(negation_patterns):
    with (PATTERNS_DIR / "agency_phone.yaml").open() as f:
        spec = yaml.safe_load(f)["agency_phone"]
    langs = _langs(spec)
    cases = [
        ("Kontakt: +41 44 123 45 67 gerne", ("44", "123", "45", "67")),
        ("Tel: 044 123 45 67",                ("44", "123", "45", "67")),
        ("phone 0041 22 345 67 89",           ("22", "345", "67", "89")),
        ("tel. 044.123.45.67",                ("44", "123", "45", "67")),
    ]
    for text, expected_groups in cases:
        hit = find_first_match(text, langs, "de", negation_patterns)
        assert hit is not None, text
        assert hit.groups == expected_groups, f"{text}: got {hit.groups}"


# --- Agency email ---
def test_agency_email(negation_patterns):
    with (PATTERNS_DIR / "agency_email.yaml").open() as f:
        spec = yaml.safe_load(f)["agency_email"]
    langs = _langs(spec)
    cases = [
        ("Contact us: info@robinreal.ch",     "info@robinreal.ch"),
        ("email: mail.an.mich@comparis.ch",  "mail.an.mich@comparis.ch"),
        ("Schreiben Sie an hallo@mueller.de", "hallo@mueller.de"),
    ]
    for text, expected_email in cases:
        hit = find_first_match(text, langs, "de", negation_patterns)
        assert hit is not None, text
        assert hit.value.lower() == expected_email.lower()


def test_email_rejects_file_extensions(negation_patterns):
    """Patterns must not match 'plan.pdf' style paths, since TLD allowlist excludes pdf."""
    with (PATTERNS_DIR / "agency_email.yaml").open() as f:
        spec = yaml.safe_load(f)["agency_email"]
    langs = _langs(spec)
    hit = find_first_match("see plan.pdf attached", langs, "en", negation_patterns)
    assert hit is None


# --- Language priority: detected lang tried first ---
def test_detected_lang_is_tried_first(features_patterns, negation_patterns):
    """When description is mixed, the detected lang's patterns win."""
    # "Balkon" hits DE; "balcon" hits FR. Place both in one text; detected_lang decides order.
    text = "Wohnung mit Balkon, appartement avec balcon"
    hit_de = find_first_match(text, _langs(features_patterns["balcony"]), "de", negation_patterns)
    hit_fr = find_first_match(text, _langs(features_patterns["balcony"]), "fr", negation_patterns)
    assert hit_de is not None and hit_de.lang_used == "de"
    assert hit_fr is not None and hit_fr.lang_used == "fr"
