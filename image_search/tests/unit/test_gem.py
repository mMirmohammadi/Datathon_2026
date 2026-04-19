from __future__ import annotations

import math

import numpy as np
import pytest
import torch

from image_search.common.gem import DEFAULT_EPS, DEFAULT_P, gem_pool


def _rand_patches(B: int, N: int, D: int, *, seed: int = 0,
                  positive_only: bool = False) -> torch.Tensor:
    g = torch.Generator().manual_seed(seed)
    x = torch.randn(B, N, D, generator=g, dtype=torch.float32)
    if positive_only:
        x = x.abs() + 1e-3  # strictly positive so p=1 exactly matches arithmetic mean
    return x


def test_gem_output_shape_and_dtype():
    x = _rand_patches(4, 16, 8)
    out = gem_pool(x)
    assert out.shape == (4, 8)
    assert out.dtype == torch.float32


def test_gem_rejects_wrong_rank():
    with pytest.raises(ValueError, match="expects"):
        gem_pool(torch.randn(10, 8))  # missing batch dim
    with pytest.raises(ValueError, match="expects"):
        gem_pool(torch.randn(2, 3, 4, 5))  # extra dim


def test_gem_rejects_bad_p_and_eps():
    x = _rand_patches(1, 4, 4)
    with pytest.raises(ValueError, match="p must be"):
        gem_pool(x, p=0.0)
    with pytest.raises(ValueError, match="p must be"):
        gem_pool(x, p=-1.0)
    with pytest.raises(ValueError, match="eps must be"):
        gem_pool(x, eps=0.0)


def test_gem_p1_matches_arithmetic_mean_on_positive_input():
    """For non-negative input, p=1 GeM-no-normalize == arithmetic mean over N."""
    x = _rand_patches(2, 32, 16, seed=7, positive_only=True)
    got = gem_pool(x, p=1.0, l2_normalize=False)
    expected = x.mean(dim=1)
    assert torch.allclose(got, expected, atol=1e-6)


def test_gem_large_p_approaches_scaled_max_on_positive_input():
    """For non-negative input with p -> infty:
        GeM = ((1/N) * sum x_i^p)^(1/p)  -->  max(x) * (1/N)^(1/p)

    At p=64, N=32 the scale factor is (1/32)^(1/64) ~= 0.9473 -- far from 1.0,
    so the naive "GeM ~~ max" intuition is wrong at any practical p. We test
    the actual closed-form limit instead. This guards against a real bug where
    someone strips the (1/N) factor thinking it is cosmetic.
    """
    B, N, D = 1, 32, 16
    p = 64.0
    x = _rand_patches(B, N, D, seed=11, positive_only=True)
    got = gem_pool(x, p=p, l2_normalize=False)
    scale = (1.0 / N) ** (1.0 / p)
    expected = x.amax(dim=1) * scale
    # At p=64, the non-max terms contribute ~1% relative on random uniforms;
    # 2e-2 rel-tolerance is empirically adequate and guards the scale factor.
    assert torch.allclose(got, expected, atol=2e-2, rtol=2e-2)


def test_gem_l2_normalize_produces_unit_vectors():
    x = _rand_patches(5, 16, 32, seed=3)
    out = gem_pool(x, l2_normalize=True)
    norms = out.norm(dim=1)
    # All rows must have L2 norm == 1 (within fp32 rounding).
    assert torch.allclose(norms, torch.ones(5), atol=1e-6)


def test_gem_negatives_are_clamped_to_eps():
    """A patch grid of all-negative values should pool to a vector of shape (1, D)
    whose magnitude is dominated by eps (i.e. very close to 0 pre-normalize)."""
    x = -torch.ones(1, 16, 8)  # all -1
    got = gem_pool(x, p=DEFAULT_P, eps=DEFAULT_EPS, l2_normalize=False)
    # clamp(min=eps) -> eps, pow(p) -> eps^p, mean -> eps^p, pow(1/p) -> eps
    expected = torch.full((1, 8), DEFAULT_EPS)
    assert torch.allclose(got, expected, atol=1e-9)


def test_gem_mixed_signs_positive_pathway_dominates():
    """Positive entries survive the clamp; negatives become eps. So a patch grid
    with one strongly-positive token per feature should produce a pooled vector
    that tracks that strong token far more than its negative neighbors."""
    B, N, D = 1, 8, 4
    x = torch.full((B, N, D), -1.0)
    x[0, 0, :] = 5.0  # one strong positive patch per feature
    got = gem_pool(x, p=3.0, eps=DEFAULT_EPS, l2_normalize=False)
    # For this setup: mean = (5^3 + 7*eps^3) / 8 -> pow(1/3) ~= 5 * (1/8)^(1/3)
    approx = 5.0 * (1.0 / N) ** (1.0 / 3.0)
    assert torch.allclose(got, torch.full((B, D), approx), atol=1e-5)


def test_gem_deterministic_same_input_same_output():
    x = _rand_patches(3, 16, 8, seed=42)
    a = gem_pool(x.clone())
    b = gem_pool(x.clone())
    assert torch.equal(a, b)


def test_gem_float64_input_preserves_dtype():
    """If caller gives fp64 input (e.g. reference computation), stay in fp64."""
    x = _rand_patches(1, 16, 8).to(torch.float64)
    out = gem_pool(x)
    assert out.dtype == torch.float64


def test_gem_matches_closed_form_on_scalar_input():
    """Trivial hand-computable case: patch grid [1, 2, 3] single feature.
    GeM p=3, no L2: ((1^3 + 2^3 + 3^3) / 3)^(1/3) = (36/3)^(1/3) = 12^(1/3)."""
    x = torch.tensor([[[1.0], [2.0], [3.0]]])  # [1, 3, 1]
    got = gem_pool(x, p=3.0, l2_normalize=False).item()
    expected = (36.0 / 3.0) ** (1.0 / 3.0)
    assert math.isclose(got, expected, abs_tol=1e-6)
