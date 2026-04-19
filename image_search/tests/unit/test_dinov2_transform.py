from __future__ import annotations

import math

import pytest
import torch
from PIL import Image

from image_search.common.dinov2_transform import (
    CROP_SIZE,
    IMAGENET_MEAN,
    IMAGENET_STD,
    RESIZE_SIZE,
    make_eval_transform,
    preprocess_batch,
    preprocess_one,
)


def test_constants_match_dinov2_canonical():
    """Cross-check: these must match facebookresearch/dinov2/dinov2/data/transforms.py.
    If this fails, someone edited a constant and the whole 71k index goes stale."""
    assert IMAGENET_MEAN == (0.485, 0.456, 0.406)
    assert IMAGENET_STD == (0.229, 0.224, 0.225)
    assert RESIZE_SIZE == 256
    assert CROP_SIZE == 224


def test_preprocess_one_shape_and_dtype():
    img = Image.new("RGB", (400, 300), (128, 128, 128))
    out = preprocess_one(img)
    assert out.shape == (3, 224, 224)
    assert out.dtype == torch.float32


def test_preprocess_one_handles_rgba_to_rgb():
    img = Image.new("RGBA", (400, 300), (128, 128, 128, 255))
    out = preprocess_one(img)
    assert out.shape == (3, 224, 224)


def test_preprocess_one_handles_small_input():
    """SRED cells after crop are 112x112. Resize(256) upscales them to 256x256,
    then CenterCrop(224) yields 224x224 --- must not error."""
    img = Image.new("RGB", (112, 112), (100, 150, 200))
    out = preprocess_one(img)
    assert out.shape == (3, 224, 224)


def test_preprocess_one_handles_rectangular_input():
    """Resize takes the shorter side to 256, preserving aspect ratio."""
    tall = Image.new("RGB", (100, 400), (0, 255, 0))
    wide = Image.new("RGB", (400, 100), (0, 0, 255))
    assert preprocess_one(tall).shape == (3, 224, 224)
    assert preprocess_one(wide).shape == (3, 224, 224)


def test_preprocess_normalize_moves_flat_color_to_expected_value():
    """A flat-gray image at pixel value 128 becomes 128/255 = 0.5019... post-ToTensor.
    After Normalize: (0.5019 - mean) / std per channel."""
    img = Image.new("RGB", (300, 300), (128, 128, 128))
    out = preprocess_one(img)

    pre_normalize = 128.0 / 255.0
    expected = [(pre_normalize - m) / s for m, s in zip(IMAGENET_MEAN, IMAGENET_STD)]

    # Center pixel (any interior pixel) should match the per-channel expected value
    for c in range(3):
        got = out[c, 112, 112].item()
        assert math.isclose(got, expected[c], abs_tol=1e-4), (
            f"channel {c}: got {got}, expected {expected[c]}"
        )


def test_preprocess_batch_empty():
    out = preprocess_batch([])
    assert out.shape == (0, 3, 224, 224)
    assert out.dtype == torch.float32


def test_preprocess_batch_multi():
    imgs = [Image.new("RGB", (400, 300), c) for c in
            [(0, 0, 0), (255, 255, 255), (128, 64, 32)]]
    out = preprocess_batch(imgs)
    assert out.shape == (3, 3, 224, 224)
    assert out.dtype == torch.float32


def test_eval_transform_is_deterministic():
    img = Image.new("RGB", (400, 300), (200, 100, 50))
    tf = make_eval_transform()
    a = tf(img)
    b = tf(img)
    assert torch.equal(a, b), "eval transform must be deterministic"
