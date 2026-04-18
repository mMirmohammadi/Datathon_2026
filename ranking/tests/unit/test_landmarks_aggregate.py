"""Unit tests for the pure-logic paths in t1_landmarks_aggregate.py.

No DB, no network — exercises the fuzzy-merge + best-query picker on
hand-crafted in-memory records. The cache-loading and geocoding paths
have their own integration lifecycles and are tested live when the
pipeline runs end-to-end.
"""
from __future__ import annotations

from collections import Counter

import pytest

from ranking.scripts.t1_landmarks_aggregate import (
    _best_query,
    _fuzzy_merge,
    _group_by_canonical,
    _is_token_boundary_substring,
)


# ---------- _group_by_canonical ---------------------------------------------


def test_group_by_canonical_counts_distinct_listings():
    """Same canonical mentioned twice by the same listing counts ONCE."""
    recs = [
        {"listing_id": "1", "mentions": [
            {"name": "ETH", "kind": "university", "canonical": "eth_zurich"},
            {"name": "ETH Zürich", "kind": "university", "canonical": "eth_zurich"},
        ]},
        {"listing_id": "2", "mentions": [
            {"name": "ETH", "kind": "university", "canonical": "eth_zurich"},
        ]},
    ]
    groups = _group_by_canonical(recs, city_map={})
    g = groups[("eth_zurich", "university")]
    assert g["listing_ids"] == {"1", "2"}
    assert len(g["listing_ids"]) == 2  # NOT 3


def test_group_by_canonical_distinguishes_kinds():
    """Same canonical with different kinds stays separate (GPT bug catch)."""
    recs = [
        {"listing_id": "1", "mentions": [
            {"name": "Zürichsee", "kind": "lake", "canonical": "zurichsee"},
            {"name": "Zürichsee", "kind": "park", "canonical": "zurichsee"},
        ]},
    ]
    groups = _group_by_canonical(recs, city_map={})
    assert ("zurichsee", "lake") in groups
    assert ("zurichsee", "park") in groups


def test_group_by_canonical_skips_empty_canonical():
    recs = [
        {"listing_id": "1", "mentions": [
            {"name": "foo", "kind": "transit", "canonical": ""},
            {"name": "bar", "kind": "transit", "canonical": "   "},
            {"name": "good", "kind": "transit", "canonical": "hb_zurich"},
        ]},
    ]
    groups = _group_by_canonical(recs, city_map={})
    assert set(groups.keys()) == {("hb_zurich", "transit")}


def test_group_by_canonical_attaches_city_from_map():
    """The city from the listing's enriched row becomes a disambiguator hint."""
    recs = [
        {"listing_id": "1", "mentions": [
            {"name": "Bahnhof", "kind": "transit", "canonical": "bahnhof"},
        ]},
        {"listing_id": "2", "mentions": [
            {"name": "Bahnhof", "kind": "transit", "canonical": "bahnhof"},
        ]},
    ]
    groups = _group_by_canonical(
        recs, city_map={"1": "Bern", "2": "Zürich"},
    )
    g = groups[("bahnhof", "transit")]
    assert g["cities"]["Bern"] == 1
    assert g["cities"]["Zürich"] == 1


# ---------- _fuzzy_merge ----------------------------------------------------


def _mk_group(canonical, kind, listing_ids, names=None, cities=None):
    return {
        "canonical": canonical,
        "kind":      kind,
        "listing_ids": set(listing_ids),
        "names":     Counter(names or {}),
        "cities":    Counter(cities or {}),
    }


def test_fuzzy_merge_substring_same_kind_merges_to_shorter():
    """`eth_zentrum_zurich` (superset) merges INTO `eth_zentrum` (shorter)."""
    groups = {
        ("eth_zentrum", "university"):        _mk_group(
            "eth_zentrum", "university", {"a", "b"}, {"ETH": 2}, {}),
        ("eth_zentrum_zurich", "university"): _mk_group(
            "eth_zentrum_zurich", "university", {"c"}, {"ETH Zürich": 1}, {}),
    }
    merged = _fuzzy_merge(groups)
    # Only the shorter canonical survives
    assert ("eth_zentrum", "university") in merged
    assert ("eth_zentrum_zurich", "university") not in merged
    g = merged[("eth_zentrum", "university")]
    # listing_ids unioned
    assert g["listing_ids"] == {"a", "b", "c"}
    # names unioned
    assert g["names"]["ETH"] == 2
    assert g["names"]["ETH Zürich"] == 1


def test_fuzzy_merge_unrelated_canonicals_not_merged():
    """`eth_zentrum` and `epfl` are both short + same kind but not substrings."""
    groups = {
        ("eth_zentrum", "university"): _mk_group("eth_zentrum", "university", {"a"}),
        ("epfl",        "university"): _mk_group("epfl",        "university", {"b"}),
    }
    merged = _fuzzy_merge(groups)
    assert len(merged) == 2  # no merge


def test_fuzzy_merge_different_kinds_not_merged():
    """Same canonical string but different kind should NOT merge."""
    groups = {
        ("zurich", "neighborhood"): _mk_group("zurich", "neighborhood", {"a"}),
        ("zurich", "other"):        _mk_group("zurich", "other", {"b"}),
    }
    merged = _fuzzy_merge(groups)
    assert ("zurich", "neighborhood") in merged
    assert ("zurich", "other") in merged


def test_fuzzy_merge_char_substring_not_at_token_boundary_rejected():
    """'a' is a char-substring of 'abcdefg' but NOT at a token boundary.
    With the underscore-delimited gate we should NOT merge.

    This guards against spurious merges on 1-2 char canonicals inside
    unrelated longer tokens.
    """
    groups = {
        ("a",       "transit"): _mk_group("a",       "transit", {"a"}),
        ("abcdefg", "transit"): _mk_group("abcdefg", "transit", {"b"}),
    }
    merged = _fuzzy_merge(groups)
    assert len(merged) == 2


def test_is_token_boundary_substring_positive_and_negative_cases():
    # Positives
    assert _is_token_boundary_substring("eth_zentrum", "eth_zentrum_zurich")
    assert _is_token_boundary_substring("bahnhof_bern", "bahnhof_bern_sbb")
    assert _is_token_boundary_substring("hb_zurich", "hb_zurich_sbb_terminal")
    # Negatives
    assert not _is_token_boundary_substring("a", "abcdefg")
    assert not _is_token_boundary_substring("hb", "hbz_zurich")  # hb is not whole token
    assert not _is_token_boundary_substring("", "anything")
    assert not _is_token_boundary_substring("same", "same")      # equal rejected
    assert not _is_token_boundary_substring("xy", "xy_zurich")   # below min-length gate


def test_fuzzy_merge_chain_of_three_collapses_to_shortest():
    """`bahnhof` ⊂ `bahnhof_bern` ⊂ `bahnhof_bern_sbb` (all transit).
    The union-find resolver must collapse all three to the shortest.
    """
    groups = {
        ("bahnhof",          "transit"): _mk_group("bahnhof",          "transit", {"x"}),
        ("bahnhof_bern",     "transit"): _mk_group("bahnhof_bern",     "transit", {"y"}),
        ("bahnhof_bern_sbb", "transit"): _mk_group("bahnhof_bern_sbb", "transit", {"z"}),
    }
    merged = _fuzzy_merge(groups)
    assert set(merged.keys()) == {("bahnhof", "transit")}
    assert merged[("bahnhof", "transit")]["listing_ids"] == {"x", "y", "z"}


# ---------- _best_query -----------------------------------------------------


def test_best_query_picks_most_frequent_queryable_name_and_appends_top_city():
    """Most-frequent surface (tiebreak shortest) beats longest adjective-laden
    variant — observed 90% Nominatim miss rate on naively-longest picks."""
    g = _mk_group(
        canonical="altstadt",
        kind="cultural",
        listing_ids={"1", "2", "3", "4"},
        names={
            "Altstadt": 3,
            "UNESCO-geschützte Altstadt von Bern": 1,  # noisy, Nominatim 0-hit
        },
        cities={"Bern": 4},
    )
    q = _best_query(g)
    assert q == "Altstadt, Bern"


def test_best_query_appends_city_when_name_lacks_it():
    g = _mk_group(
        canonical="bahnhof",
        kind="transit",
        listing_ids={"1"},
        names={"Bahnhof": 5},
        cities={"Bern": 4, "Zürich": 1},
    )
    q = _best_query(g)
    assert q == "Bahnhof, Bern"


def test_best_query_skips_non_queryable_surface_forms():
    """Generic lower-case single-word names shouldn't win — fall back to
    canonical.replace('_',' ').title()."""
    g = _mk_group(
        canonical="some_park",
        kind="park",
        listing_ids={"1"},
        names={"park": 5, "der park": 2},  # both non-queryable (all lower/too generic)
        cities={"Basel": 3},
    )
    q = _best_query(g)
    # Falls back to canonical-derived; then prepends top city.
    assert q == "Some Park, Basel"


def test_best_query_fallback_to_canonical_if_no_names():
    g = _mk_group(
        canonical="some_park",
        kind="park",
        listing_ids={"1"},
        names={},
        cities={},
    )
    q = _best_query(g)
    assert q == "Some Park"  # underscores → spaces + title-case
