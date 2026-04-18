from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from PIL import Image

from image_search.common.sred import (
    CELL_SIZE,
    SRED_MONTAGE_SIZE,
    UPSCALE_SIZE,
    split_sred_2x2,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_JSON = REPO_ROOT / "image_search/fixtures/sred_known/1154156.jpeg.expected.json"
SRED_IMAGE = REPO_ROOT / "raw_data/sred_images/1154156.jpeg"


def test_sred_split_exact_pixel_hashes():
    """Every cell's raw RGB bytes must match the committed SHA-256 fixture."""
    img = Image.open(SRED_IMAGE)
    assert img.size == SRED_MONTAGE_SIZE, "fixture must be a 224x224 SRED montage"
    crops = split_sred_2x2(img, parent_image_id="1154156")
    expected = json.loads(FIXTURE_JSON.read_text())["hashes"]
    assert len(crops) == 4
    for c in crops:
        got = hashlib.sha256(c.pre_resize_bytes).hexdigest()
        assert got == expected[f"c{c.cell}"], (
            f"cell {c.cell} hash mismatch — "
            f"expected {expected[f'c{c.cell}'][:16]}..., got {got[:16]}..."
        )


def test_sred_split_geometry_corner_order():
    """TL, TR, BL, BR ordering — synthetic 4-color montage makes order testable."""
    # Build a 224x224 image where each 112x112 cell is a distinct flat color.
    montage = Image.new("RGB", SRED_MONTAGE_SIZE)
    colors = [(255, 0, 0), (0, 255, 0), (0, 0, 255), (255, 255, 0)]  # TL, TR, BL, BR
    for idx, box in enumerate([(0, 0, 112, 112),
                               (112, 0, 224, 112),
                               (0, 112, 112, 224),
                               (112, 112, 224, 224)]):
        cell = Image.new("RGB", (112, 112), colors[idx])
        montage.paste(cell, box)

    crops = split_sred_2x2(montage, parent_image_id="synthetic")
    assert len(crops) == 4
    for c in crops:
        assert c.pre_resize_size == (CELL_SIZE, CELL_SIZE)
        assert c.resized.size == (UPSCALE_SIZE, UPSCALE_SIZE)
        # Center pixel of each pre-resize cell must equal the painted color.
        cell_img = Image.frombytes("RGB", (CELL_SIZE, CELL_SIZE), c.pre_resize_bytes)
        center = cell_img.getpixel((56, 56))
        assert center == colors[c.cell], (
            f"cell {c.cell} center color {center} != expected {colors[c.cell]} "
            f"— corner ordering is wrong"
        )


def test_sred_split_rejects_wrong_size(capsys):
    """Non-224x224 images must raise ValueError + emit [WARN] sred_guard."""
    wrong = Image.new("RGB", (300, 300), (128, 128, 128))
    with pytest.raises(ValueError, match=r"expects \(224, 224\)"):
        split_sred_2x2(wrong, parent_image_id="wrong-size")
    err = capsys.readouterr().err
    assert "[WARN] sred_guard" in err
    assert "got=(300, 300)" in err


def test_sred_split_rejects_224x224_from_non_sred_source():
    """Guard is by SIZE only here; the source-level check happens upstream in the
    pipeline. But a 224x224 robinreal image still feeds through the splitter cleanly
    — the caller (pipeline) is responsible for only invoking this on SRED.
    This test pins the contract: splitter is purely size-gated, not source-gated.
    """
    synthetic = Image.new("RGB", (224, 224), (10, 20, 30))
    crops = split_sred_2x2(synthetic, parent_image_id="any")
    assert len(crops) == 4  # size matched, so splitter runs; source-guarding is elsewhere


def test_sred_split_upscales_to_384_rgb():
    img = Image.new("RGB", (224, 224), (50, 60, 70))
    crops = split_sred_2x2(img)
    for c in crops:
        assert c.resized.size == (384, 384)
        assert c.resized.mode == "RGB"
