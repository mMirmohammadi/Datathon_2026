from __future__ import annotations

import pytest

from app.core.normalize import (
    OBJECT_CATEGORY_ENGLISH,
    slug,
    split_street,
    translate_object_category,
)


class TestSlug:
    def test_zurich_umlaut_folds_to_english(self) -> None:
        assert slug("Zürich") == "zurich"

    def test_english_zurich_passes_through(self) -> None:
        assert slug("Zurich") == "zurich"

    def test_geneva_french_folds_to_english(self) -> None:
        assert slug("Genève") == "geneva"

    def test_geneva_german_routes_to_english(self) -> None:
        assert slug("Genf") == "geneva"

    def test_biel_slash_bienne_collapses(self) -> None:
        assert slug("Biel/Bienne") == "biel"
        assert slug("Bienne") == "biel"

    def test_st_gallen_variants_collapse(self) -> None:
        assert slug("St. Gallen") == "st-gallen"
        assert slug("St Gallen") == "st-gallen"
        assert slug("Saint-Gall") == "st-gallen"

    def test_bern_variants(self) -> None:
        assert slug("Bern") == "bern"
        assert slug("Berne") == "bern"

    def test_unknown_city_gets_pure_ascii_fold(self) -> None:
        assert slug("Chêne-Bourg") == "chene-bourg"
        assert slug("Brütten") == "brutten"
        assert slug("Delémont") == "delemont"

    def test_whitespace_and_case_are_stripped(self) -> None:
        assert slug("  zurich  ") == "zurich"
        assert slug("ZURICH") == "zurich"

    def test_none_returns_none(self) -> None:
        assert slug(None) is None

    def test_empty_returns_none(self) -> None:
        assert slug("") is None
        assert slug("   ") is None


class TestTranslateObjectCategory:
    def test_top_mappings(self) -> None:
        assert translate_object_category("Wohnung") == "apartment"
        assert translate_object_category("Möblierte Wohnung") == "furnished_apartment"
        assert translate_object_category("Dachwohnung") == "attic_apartment"
        assert translate_object_category("Haus") == "house"
        assert translate_object_category("Studio") == "studio"
        assert translate_object_category("Loft") == "loft"
        assert translate_object_category("Attika") == "penthouse"
        assert translate_object_category("Maisonette") == "maisonette"
        assert translate_object_category("Villa") == "villa"
        assert translate_object_category("Gewerbeobjekt") == "commercial"

    def test_unknown_value_logs_warn_and_returns_other(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        result = translate_object_category("NotARealCategory")
        assert result == "other"
        out = capsys.readouterr().out
        assert "[WARN] translate_object_category" in out
        assert "NotARealCategory" in out
        assert "fallback='other'" in out

    def test_empty_returns_none(self) -> None:
        assert translate_object_category("") is None
        assert translate_object_category(None) is None

    def test_english_enum_includes_other(self) -> None:
        assert "other" in OBJECT_CATEGORY_ENGLISH
        assert "apartment" in OBJECT_CATEGORY_ENGLISH


class TestSplitStreet:
    def test_pure_int_house_number(self) -> None:
        assert split_street("Bettlachstrasse 43") == ("bettlachstrasse", "43")

    def test_int_with_letter_suffix(self) -> None:
        assert split_street("Rebgasse 12a") == ("rebgasse", "12a")
        assert split_street("Rehweg 5B") == ("rehweg", "5B")

    def test_int_plus_int(self) -> None:
        # Two trailing numbers (apartment within house); greedy regex captures the pair.
        street, house = split_street("Lupinenweg 8 8A")
        assert street == "lupinenweg"
        assert house == "8 8A"

    def test_complex_tail(self) -> None:
        street, house = split_street("Rue des Pavillons 5Bis 4")
        assert street == "rue des pavillons"
        assert house == "5Bis 4"

    def test_no_trailing_number(self) -> None:
        assert split_street("Piazza Della Riscossa") == ("piazza della riscossa", None)
        assert split_street("Mühlemattstrasse") == ("mühlemattstrasse", None)

    def test_whitespace_is_stripped(self) -> None:
        assert split_street("  Bahnhofstrasse 1  ") == ("bahnhofstrasse", "1")

    def test_none_and_empty(self) -> None:
        assert split_street(None) == (None, None)
        assert split_street("") == (None, None)
        assert split_street("   ") == (None, None)
