"""Unit tests on the admin1 → canton-code map."""
from __future__ import annotations

from enrichment.common.cantons import ADMIN1_TO_CANTON_CODE, admin1_to_canton_code


def test_has_26_cantons():
    assert len(ADMIN1_TO_CANTON_CODE) == 26


def test_all_codes_are_2_letter_upper():
    for code in ADMIN1_TO_CANTON_CODE.values():
        assert len(code) == 2 and code.isupper() and code.isalpha(), code


def test_all_26_iso_codes_present():
    expected = {
        "ZH", "BE", "LU", "UR", "SZ", "OW", "NW", "GL", "ZG",
        "FR", "SO", "BS", "BL", "SH", "AR", "AI", "SG", "GR",
        "AG", "TG", "TI", "VD", "VS", "NE", "GE", "JU",
    }
    assert set(ADMIN1_TO_CANTON_CODE.values()) == expected


def test_known_admin1_values_map_correctly():
    # These strings come from live reverse_geocoder 1.5.1 smoke tests.
    cases = {
        "Zurich": "ZH",
        "Bern": "BE",
        "Geneva": "GE",
        "Vaud": "VD",
        "Ticino": "TI",
        "Basel-City": "BS",
        "Basel-Landschaft": "BL",
        "Saint Gallen": "SG",
        "Grisons": "GR",
        "Valais": "VS",
        "Appenzell Ausserrhoden": "AR",
        "Appenzell Innerrhoden": "AI",
    }
    for admin1, expected_code in cases.items():
        assert admin1_to_canton_code(admin1) == expected_code, admin1


def test_unmapped_returns_none():
    assert admin1_to_canton_code("Lombardy") is None
    assert admin1_to_canton_code("Ile-de-France") is None
    assert admin1_to_canton_code("Western") is None  # Ghana — for (0,0) null-island
    assert admin1_to_canton_code("") is None
    assert admin1_to_canton_code(None) is None


def test_whitespace_is_tolerated():
    assert admin1_to_canton_code("  Zurich  ") == "ZH"
    assert admin1_to_canton_code("Zurich\n") == "ZH"
