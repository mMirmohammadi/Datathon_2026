"""SRED cell cropper for the DINOv2 pipeline.

Background: image_search/common/sred.py splits a 224x224 SRED montage into
four 112x112 cells and upscales each to 384x384 for SigLIP (which wants 384x384
input). The DINOv2 eval transform wants 224x224 input (and does its own
resize-256 + center-crop-224), so we emit the raw 112x112 cell here and let the
transform pipeline handle upscaling.

This module is intentionally a thin, separate helper so that:
  (1) any mismatch between SRED->SigLIP and SRED->DINOv2 cell layout is
      caught by unit tests (corner ordering and indices are pinned by the
      shared constants below and verified against the existing SigLIP
      sred.py corners).
  (2) there is no path by which changing DINOv2 preprocessing can
      accidentally change the SigLIP preprocessing (the two stores must
      remain joinable by image_id, which depends on a stable cell mapping).

Cell index order must match image_search/common/sred.py:

    +-------+-------+
    |   0   |   1   |
    +-------+-------+
    |   2   |   3   |
    +-------+-------+
"""
from __future__ import annotations

from PIL import Image

from image_search.common.warn import warn


SRED_MONTAGE_SIZE: tuple[int, int] = (224, 224)
CELL_SIZE: int = 112

_CORNERS: tuple[tuple[int, int, int, int], ...] = (
    (0, 0, 112, 112),       # 0 TL
    (112, 0, 224, 112),     # 1 TR
    (0, 112, 112, 224),     # 2 BL
    (112, 112, 224, 224),   # 3 BR
)


def crop_sred_cell(
    img: Image.Image,
    cell: int,
    *,
    parent_image_id: str = "unknown",
) -> Image.Image:
    """Return the 112x112 RGB sub-image at `cell` of a 224x224 SRED montage.

    Raises ValueError (with a [WARN] sred_guard) if the montage is not 224x224
    or `cell` is out of range --- never silently returns a wrong region.
    """
    if img.size != SRED_MONTAGE_SIZE:
        warn("sred_guard", parent_image_id=parent_image_id,
             expected=SRED_MONTAGE_SIZE, got=img.size,
             fallback="raise ValueError")
        raise ValueError(
            f"crop_sred_cell expects {SRED_MONTAGE_SIZE}, got {img.size}"
        )
    if not 0 <= cell < 4:
        warn("sred_guard", parent_image_id=parent_image_id,
             expected="cell in [0,3]", got=cell,
             fallback="raise ValueError")
        raise ValueError(f"crop_sred_cell cell out of range 0..3: {cell}")

    rgb = img if img.mode == "RGB" else img.convert("RGB")
    sub = rgb.crop(_CORNERS[cell])
    assert sub.size == (CELL_SIZE, CELL_SIZE)  # invariant
    return sub
