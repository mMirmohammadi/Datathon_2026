"""Tests for the relaxation ladder iterator."""
from __future__ import annotations

from app.models.schemas import HardFilters
from app.participant.relaxation import relax


def test_no_preconditions_yields_nothing():
    """If the plan has none of the relaxable fields, relax() yields no rungs."""
    hf = HardFilters()
    rungs = list(relax(hf))
    assert rungs == []


def test_price_only_yields_one_rung():
    hf = HardFilters(max_price=2000)
    rungs = list(relax(hf))
    assert len(rungs) == 1
    new, desc = rungs[0]
    assert new.max_price == 2200  # +10%
    assert hf.max_price == 2000   # input not mutated
    assert "price" in desc.lower()


def test_min_and_max_price_both_relaxed():
    hf = HardFilters(min_price=1500, max_price=2500)
    new, _ = next(iter(relax(hf)))
    assert new.min_price == 1350  # -10% (int)
    assert new.max_price == 2750  # +10%


def test_city_relax_keeps_canton():
    hf = HardFilters(city=["Zürich"], canton="ZH")
    rungs = list(relax(hf))
    # Rung 1: drop city. Rung 2: drop canton.
    assert len(rungs) == 2
    new1, _ = rungs[0]
    assert new1.city is None
    assert new1.canton == "ZH"
    new2, _ = rungs[1]
    assert new2.canton is None


def test_cumulative_relaxation():
    hf = HardFilters(
        city=["Zürich"], canton="ZH",
        max_price=2000,
        features=["balcony", "elevator"],
    )
    rungs = list(relax(hf))
    # price → drop_city → drop_canton → drop_features (radius skipped: no lat/lng)
    assert len(rungs) == 4
    last_filter, _ = rungs[-1]
    # Everything should be relaxed in the last rung
    assert last_filter.city is None
    assert last_filter.canton is None
    assert last_filter.features is None
    assert last_filter.max_price == 2200  # price was only expanded, not dropped


def test_radius_expansion_requires_lat_lng():
    # radius without lat/lng → skipped
    hf = HardFilters(radius_km=5.0)
    assert list(relax(hf)) == []

    hf = HardFilters(latitude=47.37, longitude=8.54, radius_km=2.0)
    rungs = list(relax(hf))
    assert len(rungs) == 1
    new, desc = rungs[0]
    assert new.radius_km == 3.0  # 2 * 1.5
    assert "radius" in desc.lower()


def test_input_never_mutated():
    hf = HardFilters(city=["Basel"], canton="BS", max_price=3000, features=["elevator"])
    snapshot = hf.model_dump()
    list(relax(hf))
    assert hf.model_dump() == snapshot
