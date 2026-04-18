"""Curated-landmark list sanity checks. No network calls."""
from __future__ import annotations

from ranking.scripts.t1_landmarks_fetch import CURATED


def test_no_duplicate_keys():
    keys = [l["key"] for l in CURATED]
    assert len(keys) == len(set(keys)), f"duplicate landmark keys: {keys}"


def test_every_entry_has_required_fields():
    for l in CURATED:
        for k in ("key", "query", "kind", "aliases"):
            assert k in l, f"{l.get('key', '?')} missing field: {k}"
        assert isinstance(l["aliases"], list)
        assert len(l["aliases"]) >= 1


def test_kinds_are_from_allowed_set():
    allowed = {"university", "transit", "lake", "oldtown", "employer"}
    for l in CURATED:
        assert l["kind"] in allowed, (l["key"], l["kind"])


def test_eth_has_three_campus_entries():
    """ETH has distinct campuses → multiple entries (agent push-back #2)."""
    eth_keys = {l["key"] for l in CURATED if l["key"].startswith("eth_")}
    assert len(eth_keys) >= 3, f"expected ≥3 ETH campus entries, got {eth_keys}"


def test_core_landmarks_present():
    keys = {l["key"] for l in CURATED}
    for must_have in [
        "eth_zentrum", "eth_hoengg", "epfl", "uzh_zentrum",
        "hb_zurich", "hb_geneve", "hb_bern", "hb_basel",
        "zurich_airport", "geneva_airport",
    ]:
        assert must_have in keys, f"missing canonical landmark: {must_have}"


def test_aliases_contain_canonical_and_alternates():
    """Each landmark should have at least one alias that's not identical to the key."""
    for l in CURATED:
        # e.g. "eth_zentrum" → aliases include "ETH", "ETH Zürich" etc.
        assert any(a.replace(" ", "").lower() != l["key"].lower() for a in l["aliases"])
