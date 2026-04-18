"""Unit tests for pass 1 geo-guards (pure functions, no DB or rg)."""
from __future__ import annotations

from enrichment.scripts.pass1_geocode import _is_in_ch_bbox, _is_null_island


def test_null_island_detected():
    assert _is_null_island(0.0, 0.0) is True


def test_near_null_is_not_null_island():
    assert _is_null_island(0.0001, 0.0) is False
    assert _is_null_island(0.0, 0.0001) is False


def test_real_swiss_coords_are_not_null_island():
    # Paradeplatz Zurich
    assert _is_null_island(47.3697, 8.5386) is False


def test_ch_bbox_accepts_real_swiss_coords():
    # Covers all corners of CH
    assert _is_in_ch_bbox(47.3697, 8.5386)   # Zurich
    assert _is_in_ch_bbox(46.2103, 6.1429)   # Geneva
    assert _is_in_ch_bbox(46.1700, 8.7990)   # Locarno
    assert _is_in_ch_bbox(47.5608, 7.5898)   # Basel
    assert _is_in_ch_bbox(46.9466, 7.4440)   # Bern


def test_ch_bbox_rejects_neighbour_cities():
    assert _is_in_ch_bbox(45.4642, 9.1900)  is False  # Milan
    assert _is_in_ch_bbox(48.1351, 11.5820) is False  # Munich
    assert _is_in_ch_bbox(48.8566, 2.3522)  is False  # Paris
    assert _is_in_ch_bbox(48.2082, 16.3738) is False  # Vienna


def test_ch_bbox_rejects_null_island():
    assert _is_in_ch_bbox(0.0, 0.0) is False
