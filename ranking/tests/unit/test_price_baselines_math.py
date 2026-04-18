"""Pure-math tests for t1_price_baselines — no DB."""
from __future__ import annotations

import pytest

from ranking.scripts.t1_price_baselines import (
    MIN_BUCKET_SIZE,
    _compute_baselines,
    _plz_prefix,
    _rooms_bucket,
)


# ---------- helpers -----------------------------------------------------------


def _row(price, canton, plz, rooms):
    return {"listing_id": "x", "price": price, "rooms": rooms,
            "canton": canton, "postal_code": plz}


# ---------- _plz_prefix -------------------------------------------------------


def test_plz_prefix_typical():
    assert _plz_prefix("8001") == "80"
    assert _plz_prefix("1204") == "12"


def test_plz_prefix_handles_whitespace_and_null():
    assert _plz_prefix(" 8001 ") == "80"
    assert _plz_prefix(None) is None
    assert _plz_prefix("") is None
    assert _plz_prefix("1") is None


# ---------- _rooms_bucket -----------------------------------------------------


@pytest.mark.parametrize("r, expected", [
    (3.0, 3.0), (3.5, 3.5), (4.5, 4.5),
    (3.25, 3.5),  # snap to nearest 0.5
    (2.75, 3.0),
    (1.0, 1.0),
])
def test_rooms_bucket_valid(r, expected):
    assert _rooms_bucket(r) == expected


@pytest.mark.parametrize("r", [0, -1, 16, None, "not a number"])
def test_rooms_bucket_rejects_bad(r):
    assert _rooms_bucket(r) is None


# ---------- _compute_baselines ------------------------------------------------


def test_bucket_needs_at_least_min_size():
    # 4 rows in ZH/3 = below threshold; should NOT produce a baseline.
    rows = [_row(2000, "ZH", "8001", 3) for _ in range(MIN_BUCKET_SIZE - 1)]
    canton_med, plz_med, _ = _compute_baselines(rows)
    assert canton_med == {}
    assert plz_med == {}


def test_bucket_at_exact_threshold_produces_baseline():
    rows = [_row(p, "ZH", "8001", 3) for p in range(2000, 2000 + MIN_BUCKET_SIZE)]
    canton_med, plz_med, _ = _compute_baselines(rows)
    assert ("ZH", 3.0) in canton_med
    med, n = canton_med[("ZH", 3.0)]
    assert n == MIN_BUCKET_SIZE
    # Median of [2000, 2001, 2002, 2003, 2004] = 2002
    assert med == 2002.0


def test_medians_are_robust_to_outliers():
    # 5 rents at ~2000 + one at 500k → median should ignore the outlier
    rows = [_row(2000, "ZH", "8001", 3),
            _row(2050, "ZH", "8001", 3),
            _row(2100, "ZH", "8001", 3),
            _row(2150, "ZH", "8001", 3),
            _row(2200, "ZH", "8001", 3),
            _row(500_000, "ZH", "8001", 3)]
    canton_med, _, _ = _compute_baselines(rows)
    med, _ = canton_med[("ZH", 3.0)]
    assert med == 2125.0  # median of 6 values


def test_null_price_ignored():
    rows = [_row(None, "ZH", "8001", 3)] * 10
    canton_med, plz_med, _ = _compute_baselines(rows)
    assert canton_med == {}
    assert plz_med == {}


def test_null_canton_still_gets_plz_bucket():
    rows = [_row(2000 + i, None, "8001", 3) for i in range(MIN_BUCKET_SIZE)]
    canton_med, plz_med, _ = _compute_baselines(rows)
    assert canton_med == {}
    assert ("80", 3.0) in plz_med
