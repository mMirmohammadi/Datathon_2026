"""Unit tests for DINOv2 image encoder.

We do NOT download the real DINOv2 weights here (that would require ~300 MB
download and a GPU). Instead we build a mock LoadedDinov2 whose `.model`
mimics the canonical forward_features(x) -> {"x_norm_patchtokens": ...} contract
and verify:

    - output shape, dtype, unit-norm
    - deterministic output for identical input
    - NaN patch tokens -> row is masked out with [WARN]
    - empty input -> empty output
    - shape-mismatch D -> hard failure

The real-model end-to-end check runs on qolam, not here.
"""
from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pytest
import torch
from PIL import Image

from image_search.common.dinov2_embed import encode_images


EMBED_DIM = 1024
NUM_PATCHES = 256  # 224/14 squared


class _MockModel(torch.nn.Module):
    """Mimics the DINOv2 forward_features contract for testing."""

    def __init__(self, *, constant_value: float | None = None,
                 nan_every: int | None = None,
                 wrong_dim: bool = False):
        super().__init__()
        self.constant_value = constant_value
        self.nan_every = nan_every
        self.wrong_dim = wrong_dim
        self._calls = 0

    def forward_features(self, x: torch.Tensor) -> dict:
        self._calls += 1
        B = x.shape[0]
        D = EMBED_DIM if not self.wrong_dim else EMBED_DIM - 1
        if self.constant_value is not None:
            patches = torch.full((B, NUM_PATCHES, D), self.constant_value,
                                 dtype=torch.float32, device=x.device)
        else:
            # Deterministic pseudo-random per pixel sum -- same input -> same out
            pixel_sum = x.flatten(1).sum(dim=1, keepdim=True)  # [B, 1]
            base = torch.arange(NUM_PATCHES * D, dtype=torch.float32,
                                device=x.device).reshape(NUM_PATCHES, D)
            patches = (base.unsqueeze(0) + pixel_sum.unsqueeze(-1)) / 1e4
        if self.nan_every is not None:
            for i in range(B):
                if i % self.nan_every == 0:
                    patches[i, 0, 0] = float("nan")
        return {"x_norm_patchtokens": patches}


def _mock_lm(**kwargs) -> SimpleNamespace:
    model = _MockModel(**kwargs)
    return SimpleNamespace(
        device="cpu",
        dtype=torch.float32,
        model=model,
        embed_dim=EMBED_DIM,
        patch_size=14,
        input_size=224,
        entry="mock",
    )


def _make_img(color: tuple[int, int, int] = (128, 128, 128)) -> Image.Image:
    return Image.new("RGB", (400, 300), color)


def test_encode_empty_returns_empty():
    feats, keep = encode_images([], _mock_lm())
    assert feats.shape == (0, EMBED_DIM)
    assert feats.dtype == np.float32
    assert keep.shape == (0,)
    assert keep.dtype == bool


def test_encode_shape_dtype_and_unit_norm():
    lm = _mock_lm()
    imgs = [_make_img((100, 100, 100)), _make_img((200, 50, 50))]
    feats, keep = encode_images(imgs, lm)
    assert feats.shape == (2, EMBED_DIM)
    assert feats.dtype == np.float32
    assert keep.all()
    norms = np.linalg.norm(feats, axis=1)
    assert np.allclose(norms, 1.0, atol=1e-5), norms


def test_encode_deterministic():
    lm = _mock_lm()
    imgs = [_make_img((42, 42, 42))]
    a, _ = encode_images(imgs, lm)
    b, _ = encode_images(imgs, lm)
    np.testing.assert_array_equal(a, b)


def test_encode_masks_out_nan_rows_and_warns(capsys):
    """Contract matches image_search/common/embed.py: encode returns the FULL
    (N, D) array plus a keep_mask. Caller applies the mask. A NaN-containing
    row is flagged in keep_mask=False AND a [WARN] is emitted for it."""
    lm = _mock_lm(nan_every=1)  # every row has a NaN patch
    imgs = [_make_img(), _make_img((1, 1, 1))]
    feats, keep = encode_images(imgs, lm, context="unit_test")
    err = capsys.readouterr().err
    assert feats.shape == (len(imgs), EMBED_DIM)
    assert keep.shape == (len(imgs),)
    assert not keep.any(), "all rows had NaN -> keep_mask must be all False"
    assert err.count("[WARN] nan_embedding_dinov2") == len(imgs)


def test_encode_partial_nan_mask_preserves_good_rows(capsys):
    """A batch with some NaN rows should return keep_mask with True for clean
    rows and False for dirty rows, and ONLY dirty rows should produce warns."""
    lm = _mock_lm(nan_every=2)  # rows 0, 2, 4, ... get NaN
    imgs = [_make_img((i, i, i)) for i in range(4)]
    feats, keep = encode_images(imgs, lm, context="unit_test")
    err = capsys.readouterr().err
    assert feats.shape == (4, EMBED_DIM)
    assert list(keep) == [False, True, False, True]
    assert err.count("[WARN] nan_embedding_dinov2") == 2
    # Clean rows must still be L2-unit
    clean = feats[keep]
    norms = np.linalg.norm(clean, axis=1)
    assert np.allclose(norms, 1.0, atol=1e-5)


def test_encode_rejects_wrong_model_dim():
    lm = _mock_lm(wrong_dim=True)
    with pytest.raises(ValueError, match="patch tokens D="):
        encode_images([_make_img()], lm)


def test_encode_raises_on_missing_patch_tokens_key():
    """If the hub checkpoint changes keys we want an immediate hard failure."""
    class BrokenModel(torch.nn.Module):
        def forward_features(self, x):
            return {"x_norm_clstoken": torch.zeros(x.shape[0], EMBED_DIM)}

    lm = SimpleNamespace(
        device="cpu", dtype=torch.float32, model=BrokenModel(),
        embed_dim=EMBED_DIM, patch_size=14, input_size=224, entry="broken",
    )
    with pytest.raises(KeyError, match="x_norm_patchtokens"):
        encode_images([_make_img()], lm)


def test_encode_constant_patches_produce_unit_vector():
    """When all patch tokens are the same positive constant, GeM collapses to a
    constant vector, and L2 normalize produces uniform unit vectors (all dims
    equal to 1/sqrt(D))."""
    lm = _mock_lm(constant_value=1.0)
    feats, keep = encode_images([_make_img(), _make_img((1, 2, 3))], lm)
    assert feats.shape == (2, EMBED_DIM)
    expected = 1.0 / np.sqrt(EMBED_DIM)
    np.testing.assert_allclose(feats, expected, atol=1e-5)


def test_encode_rows_align_with_input_order():
    """encode_images must preserve order: input[i] -> feat[i] (or a drop)."""
    lm = _mock_lm()
    imgs = [_make_img(c) for c in [(10, 20, 30), (200, 100, 50), (0, 0, 0)]]
    feats, keep = encode_images(imgs, lm)
    assert keep.all()
    # Re-encode one image at a time and compare row-wise
    for i, img in enumerate(imgs):
        single, _ = encode_images([img], _mock_lm())
        np.testing.assert_allclose(feats[i], single[0], atol=1e-6)
