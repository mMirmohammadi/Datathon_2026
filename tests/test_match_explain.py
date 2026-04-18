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


def test_soft_fact_transit_close_and_many_lines_is_good() -> None:
    listing = _listing()
    hard = HardFilters(soft_preferences=SoftPreferences(near_public_transport=True))
    row = _make_row({
        "dist_nearest_stop_m": 50.0,
        "nearest_stop_name": "Zurich HB",
        "nearest_stop_lines_count_clamped": 80,
        "nearest_stop_lines_count": 500,
    })
    md = build_match_detail(listing=listing, hard=hard, signal_row=row)
    fact = next(f for f in md.soft_facts if f.axis == "near_public_transport")
    assert fact.interpretation == "good"
    assert "Zurich HB" in fact.value
    assert "50 m" in fact.value


def test_soft_fact_price_sentiment_cheap_when_below_baseline() -> None:
    listing = _listing()
    hard = HardFilters(soft_preferences=SoftPreferences(price_sentiment="cheap"))
    row = _make_row({
        "price_delta_pct_canton_rooms": -15.0,
        "price_delta_pct_plz_rooms": None,
        "price_plausibility": "plausible",
    })
    md = build_match_detail(listing=listing, hard=hard, signal_row=row)
    fact = next(f for f in md.soft_facts if f.axis == "price")
    assert fact.interpretation == "good"
    assert "15% below" in fact.value

    # suspect flag always demotes to poor
    row = _make_row({
        "price_delta_pct_canton_rooms": -40.0,
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
