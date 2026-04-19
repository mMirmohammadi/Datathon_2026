"""Unit tests for app.core.match_explain.

The route-level test pins the response shape; these tests pin the
fact-generation logic independently so a change in, say, the "good" threshold
for quiet is caught even if the route test still passes.
"""
from __future__ import annotations

import sqlite3
from typing import Any

import pytest

from app.core.match_explain import build_match_detail
from app.models.schemas import HardFilters, SoftPreferences


def _make_row(values: dict[str, Any]) -> sqlite3.Row:
    """Build a sqlite3.Row via an in-memory DB so _safe_row_get works."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cols = list(values.keys())
    col_defs = ", ".join(f"{c}" for c in cols)
    conn.execute(f"CREATE TABLE t ({col_defs})")
    placeholders = ", ".join("?" for _ in cols)
    conn.execute(
        f"INSERT INTO t VALUES ({placeholders})",
        [values[c] for c in cols],
    )
    row = conn.execute("SELECT * FROM t").fetchone()
    conn.close()
    return row


def _listing(**overrides: Any) -> dict[str, Any]:
    base = {
        "listing_id": "L1",
        "title": "Modern 3.5 Zimmerwohnung in Zürich mit Balkon",
        "description": "Helle, ruhige Wohnung mit Balkon und Lift.",
        "street": "Bahnhofstrasse",
        "city": "Zürich",
        "postal_code": "8001",
        "canton": "ZH",
        "price": 2500,
        "rooms": 3.5,
        "area": 90,
        "floor": 3,
        "year_built": 2005,
        "object_category": "apartment",
        "features": ["balcony", "elevator"],
    }
    base.update(overrides)
    return base


def test_hard_checks_cover_every_requested_constraint() -> None:
    listing = _listing()
    hard = HardFilters(
        city=["zurich"],
        postal_code=["8001"],
        min_rooms=3.0,
        max_rooms=4.0,
        max_price=3000,
        features=["balcony"],
        features_excluded=["fireplace"],
    )
    md = build_match_detail(listing=listing, hard=hard, signal_row=None)
    labels = [h.label for h in md.hard_checks]
    assert "city" in labels
    assert "postal_code" in labels
    assert "rooms" in labels
    assert "price" in labels
    assert "feature: balcony" in labels
    assert "feature: fireplace" in labels
    # All the synthesized rows must carry ok=True — the listing is here, so
    # it passed the gate.
    assert all(h.ok for h in md.hard_checks)
    # Price check must show the actual value.
    price_row = next(h for h in md.hard_checks if h.label == "price")
    assert "2500" in price_row.value


def test_keyword_hits_are_split_correctly() -> None:
    listing = _listing()  # description contains "ruhig", "Balkon"
    hard = HardFilters(bm25_keywords=["ruhig", "Balkon", "Jacuzzi"])
    md = build_match_detail(listing=listing, hard=hard, signal_row=None)
    # matching is case-insensitive; "Balkon" in listing = match.
    assert set(md.matched_keywords) == {"ruhig", "Balkon"}
    assert md.unmatched_keywords == ["Jacuzzi"]


def test_soft_fact_quiet_interpretation() -> None:
    listing = _listing()
    hard = HardFilters(soft_preferences=SoftPreferences(quiet=True))

    # composite = motorway + primary = 1500 → good
    md = build_match_detail(
        listing=listing,
        hard=hard,
        signal_row=_make_row({"dist_motorway_m": 1000, "dist_primary_road_m": 500, "dist_rail_m": None}),
    )
    fact = next(f for f in md.soft_facts if f.axis == "quiet")
    assert fact.interpretation == "good"

    # composite = 150 → poor
    md = build_match_detail(
        listing=listing,
        hard=hard,
        signal_row=_make_row({"dist_motorway_m": 100, "dist_primary_road_m": 50, "dist_rail_m": None}),
    )
    fact = next(f for f in md.soft_facts if f.axis == "quiet")
    assert fact.interpretation == "poor"


def test_soft_fact_transit_close_is_good_and_omits_lines_count() -> None:
    """After the "100 lines" strip, the transit fact:

    * interpretation is determined by distance alone (< 300m → good)
    * the string NEVER includes the meaningless clamped lines count even if
      the row carries one
    * stop name + distance still render
    """
    listing = _listing()
    hard = HardFilters(soft_preferences=SoftPreferences(near_public_transport=True))
    row = _make_row({
        "dist_nearest_stop_m": 50.0,
        "nearest_stop_name": "Zurich HB",
        "nearest_stop_type": "train",
        "nearest_stop_lines_count_clamped": 80,
        "nearest_stop_lines_count": 500,
    })
    md = build_match_detail(listing=listing, hard=hard, signal_row=row)
    fact = next(f for f in md.soft_facts if f.axis == "near_public_transport")
    assert fact.interpretation == "good"
    assert "Zurich HB" in fact.value
    assert "50 m" in fact.value
    # Guard against regression: the string must not contain any "N line(s)"
    # fragment even when the row provides both the clamped and raw counts.
    assert "lines" not in fact.value, (
        f"transit fact must not include the lines count: {fact.value!r}"
    )
    assert "line " not in fact.value, (
        f"transit fact must not include singular 'line ': {fact.value!r}"
    )


def test_soft_fact_transit_interp_is_distance_only() -> None:
    """Single-line stop at 250 m must still be 'good' (tier is distance-only)."""
    listing = _listing()
    hard = HardFilters(soft_preferences=SoftPreferences(near_public_transport=True))
    row = _make_row({
        "dist_nearest_stop_m": 250.0,
        "nearest_stop_name": "Rural Stop",
        "nearest_stop_type": "bus",
        "nearest_stop_lines_count_clamped": 1,  # old logic would demote to "ok"
        "nearest_stop_lines_count": 1,
    })
    md = build_match_detail(listing=listing, hard=hard, signal_row=row)
    fact = next(f for f in md.soft_facts if f.axis == "near_public_transport")
    assert fact.interpretation == "good", (
        "250 m stop must be 'good' under distance-only tier"
    )
    assert "lines" not in fact.value


def test_soft_fact_price_sentiment_cheap_when_below_baseline() -> None:
    listing = _listing()
    hard = HardFilters(soft_preferences=SoftPreferences(price_sentiment="cheap"))
    # `price_delta_pct_canton_rooms` is stored as a fraction (−0.15 == 15%
    # below); see ranking/schema.py:56-64. The display/threshold code in
    # _price_fact converts to percent exactly once.
    row = _make_row({
        "price_delta_pct_canton_rooms": -0.15,
        "price_delta_pct_plz_rooms": None,
        "price_plausibility": "plausible",
    })
    md = build_match_detail(listing=listing, hard=hard, signal_row=row)
    fact = next(f for f in md.soft_facts if f.axis == "price")
    assert fact.interpretation == "good"
    assert "15% below" in fact.value

    # suspect flag always demotes to poor
    row = _make_row({
        "price_delta_pct_canton_rooms": -0.40,
        "price_delta_pct_plz_rooms": None,
        "price_plausibility": "suspect",
    })
    md = build_match_detail(listing=listing, hard=hard, signal_row=row)
    fact = next(f for f in md.soft_facts if f.axis == "price")
    assert fact.interpretation == "poor"
    assert "suspect" in fact.value


def test_unknown_landmark_yields_unknown_fact_not_skipped() -> None:
    listing = _listing()
    hard = HardFilters(
        soft_preferences=SoftPreferences(near_landmark=["NotARealPlaceXYZ"]),
    )
    md = build_match_detail(listing=listing, hard=hard, signal_row=None)
    facts = [f for f in md.soft_facts if f.axis.startswith("landmark")]
    assert len(facts) == 1
    assert facts[0].interpretation == "unknown"
    # Silent-disable is forbidden; the fact is emitted even though unresolved.


def _make_landmark_row(**overrides: Any) -> sqlite3.Row:
    """Signal row with a handful of `dist_landmark_<key>_m` columns so the
    nearby-landmark pass has real data to sort. Defaults mirror the
    distances a Zurich-centre listing would see: HB very close, ETH close,
    Zurichsee close, others further out."""
    base = {
        "dist_landmark_hb_zurich_m": 600.0,
        "dist_landmark_eth_zentrum_m": 1300.0,
        "dist_landmark_zurichsee_m": 800.0,
        "dist_landmark_altstadt_zurich_m": 400.0,
        "dist_landmark_hb_bern_m": 120_000.0,  # outside the 20 km radius
        "dist_landmark_plainpalais_m": 200.0,  # close but kind=neighborhood (skipped)
        "dist_landmark_altstadt_m": 100.0,     # close but kind=cultural (skipped)
    }
    base.update(overrides)
    return _make_row(base)


def test_nearby_landmarks_emitted_without_explicit_request() -> None:
    """Top-N nearest trusted landmarks show up even with no `near_landmark`."""
    listing = _listing()
    hard = HardFilters(soft_preferences=SoftPreferences())  # no explicit landmark
    row = _make_landmark_row()
    md = build_match_detail(listing=listing, hard=hard, signal_row=row)

    landmark_facts = [f for f in md.soft_facts if f.axis.startswith("landmark_")]
    assert len(landmark_facts) == 3, (
        f"expected top-3 nearest landmarks, got {len(landmark_facts)}: "
        f"{[f.axis for f in landmark_facts]}"
    )
    # Ordered by ascending Haversine distance in the seed row.
    assert landmark_facts[0].axis == "landmark_altstadt_zurich"  # 400 m
    assert landmark_facts[1].axis == "landmark_hb_zurich"        # 600 m
    assert landmark_facts[2].axis == "landmark_zurichsee"        # 800 m
    # 400 m is under the 1.5 km "good" threshold; 600 m too; 800 m too.
    for f in landmark_facts:
        assert f.interpretation == "good", (
            f"{f.axis} @ {f.value!r} should be good (<1.5 km), got {f.interpretation}"
        )


def test_nearby_landmarks_respect_kind_allowlist() -> None:
    """`neighborhood` + `cultural` entries are skipped even when closest."""
    listing = _listing()
    hard = HardFilters(soft_preferences=SoftPreferences())
    # plainpalais (neighborhood) @ 200 m, altstadt (cultural) @ 100 m are
    # closer than anything else but must not appear in the nearby list.
    row = _make_landmark_row(
        dist_landmark_plainpalais_m=200.0,
        dist_landmark_altstadt_m=100.0,
    )
    md = build_match_detail(listing=listing, hard=hard, signal_row=row)
    axes = {f.axis for f in md.soft_facts if f.axis.startswith("landmark_")}
    assert "landmark_plainpalais" not in axes
    assert "landmark_altstadt" not in axes
    # But the curated oldtown entry does appear — same kind allowlist hits.
    assert "landmark_altstadt_zurich" in axes


def test_nearby_landmarks_drop_beyond_max_radius() -> None:
    """Entries beyond the 50 km cap don't clutter the nearby list."""
    listing = _listing()
    hard = HardFilters(soft_preferences=SoftPreferences())
    # Only hb_bern is present and it's 120 km away → no nearby facts.
    row = _make_row({"dist_landmark_hb_bern_m": 120_000.0})
    md = build_match_detail(listing=listing, hard=hard, signal_row=row)
    landmark_facts = [f for f in md.soft_facts if f.axis.startswith("landmark_")]
    assert landmark_facts == []


def test_nearby_landmarks_reach_rural_listings_within_cap() -> None:
    """A rural listing with its nearest HB 40 km away still gets context."""
    listing = _listing()
    hard = HardFilters(soft_preferences=SoftPreferences())
    # Locarno → Lugano shape: the nearest curated reference points are 30-40
    # km away. All within the 50 km cap, so we emit them as "ok" context.
    row = _make_row({
        "dist_landmark_hb_lugano_m": 40_000.0,
        "dist_landmark_lago_lugano_m": 38_000.0,
        "dist_landmark_usi_lugano_m": 39_500.0,
    })
    md = build_match_detail(listing=listing, hard=hard, signal_row=row)
    landmark_facts = [f for f in md.soft_facts if f.axis.startswith("landmark_")]
    assert len(landmark_facts) == 3
    for f in landmark_facts:
        assert f.interpretation == "ok", (
            f"{f.axis} @ {f.value!r} is beyond 1.5 km, should be ok (informational)"
        )


def test_nearby_landmarks_dedup_explicit_request() -> None:
    """A landmark emitted via `near_landmark` is not re-emitted by the pass."""
    listing = _listing()
    hard = HardFilters(
        soft_preferences=SoftPreferences(near_landmark=["ETH"]),
    )
    row = _make_landmark_row()
    md = build_match_detail(listing=listing, hard=hard, signal_row=row)

    eth_facts = [f for f in md.soft_facts if f.axis == "landmark_eth_zentrum"]
    # Exactly one ETH fact (the explicit one), not duplicated by the pass.
    assert len(eth_facts) == 1
    # 3 more nearby facts in addition to the explicit ETH request.
    nearby = [
        f for f in md.soft_facts
        if f.axis.startswith("landmark_") and f.axis != "landmark_eth_zentrum"
    ]
    assert len(nearby) == 3


def test_nearby_landmarks_skipped_for_missing_signal_row() -> None:
    """No signal row → no nearby landmark facts (nothing to measure)."""
    listing = _listing()
    hard = HardFilters(soft_preferences=SoftPreferences())
    md = build_match_detail(listing=listing, hard=hard, signal_row=None)
    landmark_facts = [f for f in md.soft_facts if f.axis.startswith("landmark_")]
    assert landmark_facts == []


def test_no_signal_row_still_produces_hard_checks_and_keywords() -> None:
    listing = _listing()
    hard = HardFilters(
        city=["zurich"],
        bm25_keywords=["ruhig"],
        soft_preferences=SoftPreferences(quiet=True),
    )
    md = build_match_detail(listing=listing, hard=hard, signal_row=None)
    # hard_checks present
    assert len(md.hard_checks) == 1
    # matched_keywords present
    assert md.matched_keywords == ["ruhig"]
    # soft_facts has quiet=unknown (no row)
    assert len(md.soft_facts) == 1
    assert md.soft_facts[0].axis == "quiet"
    assert md.soft_facts[0].interpretation == "unknown"
