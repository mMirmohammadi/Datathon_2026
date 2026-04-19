from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from PIL import Image

from image_search.common.dinov2_sred import (
    CELL_SIZE,
    SRED_MONTAGE_SIZE,
    _CORNERS,
    crop_sred_cell,
)
from image_search.common.sred import _CORNERS as SIGLIP_CORNERS
from image_search.common.sred import split_sred_2x2


def test_dinov2_and_siglip_corners_match_exactly():
    """If these two corner tuples ever drift, the DINOv2 and SigLIP indices
    will no longer be joinable by image_id#c<cell>. This is the load-bearing
    cross-check between the two preprocessing paths."""
    assert _CORNERS == SIGLIP_CORNERS


def test_crop_each_cell_returns_112x112_rgb():
    img = Image.new("RGB", SRED_MONTAGE_SIZE, (128, 64, 32))
    for cell in range(4):
        sub = crop_sred_cell(img, cell)
        assert sub.size == (CELL_SIZE, CELL_SIZE)
        assert sub.mode == "RGB"


def test_crop_corner_order_matches_colored_fixture():
    """TL, TR, BL, BR ordering --- 4 flat-color cells make the order testable."""
    montage = Image.new("RGB", SRED_MONTAGE_SIZE)
    colors = [(255, 0, 0), (0, 255, 0), (0, 0, 255), (255, 255, 0)]
    for idx, box in enumerate(_CORNERS):
        cell = Image.new("RGB", (CELL_SIZE, CELL_SIZE), colors[idx])
        montage.paste(cell, box)

    for i in range(4):
        sub = crop_sred_cell(montage, i)
        # Center pixel of each cell must equal the painted color.
        assert sub.getpixel((56, 56)) == colors[i], (
            f"cell {i} center {sub.getpixel((56, 56))} != {colors[i]}"
        )


def test_crop_rejects_wrong_size(capsys):
    wrong = Image.new("RGB", (300, 300), (0, 0, 0))
    with pytest.raises(ValueError, match=r"expects \(224, 224\)"):
        crop_sred_cell(wrong, 0, parent_image_id="bad")
    assert "[WARN] sred_guard" in capsys.readouterr().err


def test_crop_rejects_bad_cell_index(capsys):
    img = Image.new("RGB", SRED_MONTAGE_SIZE, (0, 0, 0))
    with pytest.raises(ValueError, match="cell out of range"):
        crop_sred_cell(img, 4, parent_image_id="bad-cell")
    assert "[WARN] sred_guard" in capsys.readouterr().err


def test_dinov2_crop_matches_siglip_pre_resize_bytes():
    """The 112x112 bytes that DINOv2 feeds into its transform must be BIT-EXACT
    identical to what SigLIP's split_sred_2x2 produced at its pre-resize stage.
    This pins that the two pipelines share the same pixel region for the same
    cell index --- a regression here would silently desynchronize the indices.
    """
    real_fixture = (
        Path(__file__).resolve().parents[2]
        / "fixtures/sred_known/1154156.jpeg.expected.json"
    )
    real_image = (
        Path(__file__).resolve().parents[3]
        / "raw_data/sred_images/1154156.jpeg"
    )
    if not real_image.exists():
        pytest.skip(f"fixture raw image missing: {real_image}")

    import json
    expected_hashes = json.loads(real_fixture.read_text())["hashes"]
    img = Image.open(real_image)
    # SigLIP's pre-resize bytes
    siglip_crops = split_sred_2x2(img, parent_image_id="1154156")
    siglip_bytes = {c.cell: c.pre_resize_bytes for c in siglip_crops}

    for cell in range(4):
        dinov2_sub = crop_sred_cell(img, cell, parent_image_id="1154156")
        dinov2_bytes = dinov2_sub.tobytes()
        assert dinov2_bytes == siglip_bytes[cell], (
            f"cell {cell}: DINOv2 crop bytes differ from SigLIP pre-resize bytes"
        )
        # Also check against the committed pixel-hash fixture
        got_hash = hashlib.sha256(dinov2_bytes).hexdigest()
        assert got_hash == expected_hashes[f"c{cell}"], (
            f"cell {cell}: hash mismatch vs committed fixture"
        )
