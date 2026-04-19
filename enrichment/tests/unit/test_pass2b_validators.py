"""Unit tests for pass 2b validators + the 5 auditor fixtures.

These tests are PURE — no DB, no network. They exercise:
  * `_validate_bathroom_count` — int range check
  * `_validate_bool` — "true"/"false" normalisation
  * `_validate_raw_snippet` — the hallucination guard (explicit vs inferred-default)
  * `_normalize_whitespace` — the nbsp/space fix
  * `_row_is_in_scope` — the pre-filter
  * End-to-end mock runs against the 5 auditor fixtures from _context/PASS2B_PLAN.md §8.5
"""
from __future__ import annotations

import pytest

from enrichment.scripts.pass2b_bathroom_cellar_kitchen import (
    FieldValue,
    Pass2bExtraction,
    _normalize_whitespace,
    _row_is_in_scope,
    _validate_bathroom_count,
    _validate_bool,
    _validate_extraction,
    _validate_raw_snippet,
)


# ---------------------------------------------------------------------------
# _normalize_whitespace — the nbsp / multi-space fix
# ---------------------------------------------------------------------------


class TestNormalizeWhitespace:
    def test_regular_space_unchanged(self):
        assert _normalize_whitespace("hello world") == "hello world"

    def test_nbsp_collapses_to_space(self):
        assert _normalize_whitespace("hello\xa0world") == "hello world"

    def test_multi_nbsp_collapses(self):
        assert _normalize_whitespace("a\xa0\xa0b") == "a b"

    def test_tab_newline_collapses(self):
        assert _normalize_whitespace("a\tb\nc") == "a b c"

    def test_preserves_content(self):
        assert _normalize_whitespace("2 Bäder/Dusche") == "2 Bäder/Dusche"


# ---------------------------------------------------------------------------
# _validate_bathroom_count
# ---------------------------------------------------------------------------


class TestValidateBathroomCount:
    @pytest.mark.parametrize("v", ["1", "2", "3", "10"])
    def test_valid(self, v):
        ok, norm, _ = _validate_bathroom_count(v)
        assert ok and norm == v

    @pytest.mark.parametrize("v", ["0", "11", "100", "-1"])
    def test_out_of_range(self, v):
        ok, norm, why = _validate_bathroom_count(v)
        assert not ok
        assert norm is None
        assert "out of" in why

    @pytest.mark.parametrize("v", ["", "abc", "1.5", None])
    def test_non_int(self, v):
        ok, norm, why = _validate_bathroom_count(v)
        assert not ok
        assert norm is None


# ---------------------------------------------------------------------------
# _validate_bool
# ---------------------------------------------------------------------------


class TestValidateBool:
    @pytest.mark.parametrize("v, expected", [
        ("true", "true"), ("false", "false"),
        ("True", "true"), ("FALSE", "false"),
        ("  true  ", "true"),
    ])
    def test_valid(self, v, expected):
        ok, norm, _ = _validate_bool(v)
        assert ok and norm == expected

    @pytest.mark.parametrize("v", ["yes", "no", "1", "0", "", None, "maybe"])
    def test_invalid(self, v):
        ok, norm, _ = _validate_bool(v)
        assert not ok and norm is None


# ---------------------------------------------------------------------------
# _row_is_in_scope
# ---------------------------------------------------------------------------


class TestRowIsInScope:
    def test_bathroom_keyword_matches(self):
        assert _row_is_in_scope("Schöne Wohnung mit Badezimmer", None)

    def test_cellar_keyword_matches(self):
        assert _row_is_in_scope("Mit Kellerabteil.", None)

    def test_shared_keyword_matches(self):
        assert _row_is_in_scope("Zimmer in 4er-WG.", None)

    def test_residential_category_matches_without_keywords(self):
        assert _row_is_in_scope("Vague teaser text.", "apartment")

    def test_gewerbeobjekt_no_match(self):
        assert not _row_is_in_scope("Büro in zentraler Lage.", "Gewerbeobjekt")

    def test_raw_bad_not_a_false_positive(self):
        """Auditor §8.4 — 'Bad' as substring of 'Baden' must not trigger."""
        # No keyword, non-residential category → should be OUT of scope.
        assert not _row_is_in_scope("Located in Baden-Baden", "Parkplatz")

    def test_case_sensitive(self):
        # Our keyword list uses exact-case; 'badezimmer' (lowercase) would miss.
        assert not _row_is_in_scope("badezimmer im keller", None)  # lowercase


# ---------------------------------------------------------------------------
# _validate_raw_snippet — hallucination guard
# ---------------------------------------------------------------------------


class TestValidateRawSnippet:
    DESC = "Helle 3.5-Zimmer-Wohnung im Altbau mit Badezimmer und Kellerabteil."

    def test_null_value_accepts_null_snippet(self):
        ok, _ = _validate_raw_snippet(
            raw_snippet=None, description=self.DESC,
            value=None, confidence=0.0,
            field_name="bathroom_count", object_category="apartment",
        )
        assert ok

    def test_explicit_match_accepted(self):
        ok, _ = _validate_raw_snippet(
            raw_snippet="Badezimmer", description=self.DESC,
            value="1", confidence=0.9,
            field_name="bathroom_count", object_category="apartment",
        )
        assert ok

    def test_explicit_nbsp_mismatch_normalised_and_accepted(self):
        # Description has nbsp; GPT normalised to regular space.
        desc_nbsp = "2\xa0Bäder/Dusche in der Wohnung."
        ok, _ = _validate_raw_snippet(
            raw_snippet="2 Bäder/Dusche", description=desc_nbsp,
            value="2", confidence=0.9,
            field_name="bathroom_count", object_category="apartment",
        )
        assert ok

    def test_explicit_miss_rejected(self):
        # GPT invented a snippet — reject.
        ok, why = _validate_raw_snippet(
            raw_snippet="Jacuzzi im Badezimmer", description=self.DESC,
            value="1", confidence=0.9,
            field_name="bathroom_count", object_category="apartment",
        )
        assert not ok
        assert "not a substring" in why

    def test_empty_snippet_rejected(self):
        ok, why = _validate_raw_snippet(
            raw_snippet="", description=self.DESC,
            value="1", confidence=0.9,
            field_name="bathroom_count", object_category="apartment",
        )
        assert not ok

    def test_inferred_default_false_shared_on_apartment(self):
        """Full apartment → bathroom_shared=false @ 0.75, no snippet required.
        (Category slug is 'apartment', not German 'apartment' — matches DB.)
        """
        ok, _ = _validate_raw_snippet(
            raw_snippet=None, description=self.DESC,
            value="false", confidence=0.75,
            field_name="bathroom_shared", object_category="apartment",
        )
        assert ok

    def test_inferred_default_true_shared_on_shared_room(self):
        """shared_room → bathroom_shared=true @ 0.75, no snippet required."""
        ok, _ = _validate_raw_snippet(
            raw_snippet=None, description=self.DESC,
            value="true", confidence=0.75,
            field_name="bathroom_shared", object_category="shared_room",
        )
        assert ok

    def test_inferred_default_wrong_direction_rejected(self):
        """kitchen_shared=true on a full apartment @ low conf without snippet → reject."""
        ok, why = _validate_raw_snippet(
            raw_snippet=None, description=self.DESC,
            value="true", confidence=0.70,
            field_name="kitchen_shared", object_category="apartment",
        )
        assert not ok  # full unit + true without snippet is contradictory

    def test_inferred_default_high_confidence_without_snippet_rejected(self):
        """High-confidence claims must cite text. 0.95 + no snippet → reject."""
        ok, why = _validate_raw_snippet(
            raw_snippet=None, description=self.DESC,
            value="false", confidence=0.95,
            field_name="bathroom_shared", object_category="apartment",
        )
        assert not ok

    def test_has_cellar_context_clue_allowed_no_snippet(self):
        """has_cellar inferred from "Waschmaschine im Keller" context may skip snippet."""
        ok, _ = _validate_raw_snippet(
            raw_snippet=None, description=self.DESC,
            value="true", confidence=0.70,
            field_name="has_cellar", object_category="apartment",
        )
        assert ok

    def test_null_object_category_accepts_inferred_default(self):
        """When the raw CSV has no object_category, trust GPT's description-based
        inference at conf ≤ 0.80 (both directions)."""
        for value in ("false", "true"):
            ok, _ = _validate_raw_snippet(
                raw_snippet=None, description=self.DESC,
                value=value, confidence=0.75,
                field_name="bathroom_shared", object_category=None,
            )
            assert ok, f"NULL-category + {value!r}@0.75 should be accepted"

    def test_null_category_still_rejects_high_confidence_without_snippet(self):
        """NULL category OR residential doesn't excuse high-confidence no-snippet."""
        ok, _ = _validate_raw_snippet(
            raw_snippet=None, description=self.DESC,
            value="true", confidence=0.95,
            field_name="bathroom_shared", object_category=None,
        )
        assert not ok, "0.95 conf without snippet must be rejected regardless"

    def test_private_category_rejects_contradictory_true_without_snippet(self):
        """On a full apartment, bathroom_shared=true@low-conf without snippet
        contradicts the category. Reject."""
        ok, _ = _validate_raw_snippet(
            raw_snippet=None, description=self.DESC,
            value="true", confidence=0.75,
            field_name="bathroom_shared", object_category="apartment",
        )
        assert not ok


# ---------------------------------------------------------------------------
# _validate_extraction — end-to-end on the 5 auditor fixtures
# ---------------------------------------------------------------------------


def _mk(value, conf, snippet):
    return FieldValue(value=value, confidence=conf, raw_snippet=snippet)


class TestAuditorFixtures:
    """5 challenging multilingual fixtures from the auditor (PASS2B_PLAN §8.5)."""

    def test_T1_DE_separate_WC_does_not_add(self):
        """`Badezimmer + separates WC` → bathroom_count=1, not 2."""
        desc = ("Helle 3.5-Zimmer-Wohnung im Altbau, 85 m². Grosses Badezimmer "
                "mit Dusche und Wanne sowie separates WC im Flur. Moderne Einbauküche.")
        ext = Pass2bExtraction(
            bathroom_count=_mk("1", 0.90, "Grosses Badezimmer mit Dusche und Wanne"),
            bathroom_shared=_mk("false", 0.75, None),
            has_cellar=_mk(None, 0.0, None),
            kitchen_shared=_mk("false", 0.75, None),
        )
        writes, warnings = _validate_extraction(ext, desc, "apartment")
        assert warnings == []
        assert "bathroom_count" in writes and writes["bathroom_count"][0] == "1"
        assert writes["bathroom_shared"][0] == "false"
        assert "has_cellar" not in writes
        assert writes["kitchen_shared"][0] == "false"

    def test_T2_DE_WG_shared_explicit_cites_snippets(self):
        desc = ("WG-Zimmer in 4er-WG, 18 m². Gemeinsames Badezimmer und "
                "Gemeinschaftsküche im Flur. Eigener Kellerabteil zur Mitbenützung im UG.")
        ext = Pass2bExtraction(
            bathroom_count=_mk("1", 0.80, "Gemeinsames Badezimmer"),
            bathroom_shared=_mk("true", 0.90, "Gemeinsames Badezimmer"),
            has_cellar=_mk("true", 0.85, "Eigener Kellerabteil zur Mitbenützung"),
            kitchen_shared=_mk("true", 0.90, "Gemeinschaftsküche"),
        )
        writes, warnings = _validate_extraction(ext, desc, "shared_room")
        assert warnings == []
        assert writes["bathroom_count"] == ("1", 0.80, "Gemeinsames Badezimmer")
        assert writes["bathroom_shared"] == ("true", 0.90, "Gemeinsames Badezimmer")
        assert writes["has_cellar"][0] == "true"
        assert writes["kitchen_shared"][0] == "true"

    def test_T3_FR_full_apt_no_mention_defaults(self):
        desc = ("Bel appartement 4 pièces entièrement rénové au 2e étage, "
                "balcon sud, ascenseur, proche transports publics.")
        ext = Pass2bExtraction(
            bathroom_count=_mk(None, 0.0, None),
            bathroom_shared=_mk("false", 0.75, None),
            has_cellar=_mk(None, 0.0, None),
            kitchen_shared=_mk("false", 0.75, None),
        )
        writes, warnings = _validate_extraction(ext, desc, "apartment")
        assert warnings == []
        assert "bathroom_count" not in writes
        assert writes["bathroom_shared"] == ("false", 0.75, None)
        assert "has_cellar" not in writes
        assert writes["kitchen_shared"] == ("false", 0.75, None)

    def test_T4_IT_shared_cellar_still_has_cellar(self):
        """Per plan §1.3: 'Cantina ad uso comune' → has_cellar=true."""
        desc = ("Appartamento di 3.5 locali, 90 mq, cucina abitabile, un bagno "
                "completo con doccia. Cantina ad uso comune nel seminterrato. "
                "Lavanderia condominiale.")
        ext = Pass2bExtraction(
            bathroom_count=_mk("1", 0.90, "un bagno completo con doccia"),
            bathroom_shared=_mk("false", 0.75, None),
            has_cellar=_mk("true", 0.85, "Cantina ad uso comune"),
            kitchen_shared=_mk("false", 0.75, None),
        )
        writes, warnings = _validate_extraction(ext, desc, "apartment")
        assert warnings == []
        assert writes["bathroom_count"][0] == "1"
        assert writes["has_cellar"][0] == "true"  # shared cellar still counts

    def test_T5_EN_teaser_all_null_returns_nothing(self):
        desc = "Charming property in prime location. Contact us for full details."
        ext = Pass2bExtraction(
            bathroom_count=_mk(None, 0.0, None),
            bathroom_shared=_mk(None, 0.0, None),
            has_cellar=_mk(None, 0.0, None),
            kitchen_shared=_mk(None, 0.0, None),
        )
        writes, warnings = _validate_extraction(ext, desc, None)
        assert writes == {}
        assert warnings == []

    def test_hallucinated_snippet_dropped(self):
        """Regression: GPT invents a snippet not in the description."""
        desc = "Small studio near main station."
        ext = Pass2bExtraction(
            bathroom_count=_mk("3", 0.95, "three bathrooms with jacuzzis"),
            bathroom_shared=_mk(None, 0.0, None),
            has_cellar=_mk(None, 0.0, None),
            kitchen_shared=_mk(None, 0.0, None),
        )
        writes, warnings = _validate_extraction(ext, desc, None)
        # bathroom_count write dropped because snippet isn't in desc
        assert "bathroom_count" not in writes
        assert len(warnings) == 1
        assert "not a substring" in warnings[0]

    def test_nbsp_in_description_matches_normalised_snippet(self):
        """Common Comparis artefact: nbsp in the description."""
        desc = "Wohnung\xa0mit\xa01 Badezimmer\xa0und Keller."
        ext = Pass2bExtraction(
            bathroom_count=_mk("1", 0.90, "mit 1 Badezimmer und Keller"),
            bathroom_shared=_mk("false", 0.75, None),
            has_cellar=_mk("true", 0.85, "Keller"),
            kitchen_shared=_mk("false", 0.75, None),
        )
        writes, warnings = _validate_extraction(ext, desc, "apartment")
        assert warnings == []
        assert writes["bathroom_count"][0] == "1"
        assert writes["has_cellar"][0] == "true"
