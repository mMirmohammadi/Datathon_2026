"""SRED 2x2 montage splitter.

SRED images are uniformly 224x224 JPEGs. Each is a 2x2 grid of four unrelated
small photos (typically bathroom / room / kitchen / facade). Feeding the whole
montage into SigLIP blends the four cells into one pooled embedding, which
destroys retrieval — so every SRED image must be split before triage/embed.

Cell order is TL, TR, BL, BR (left-to-right, top-to-bottom):

    +-------+-------+
    |   0   |   1   |
    +-------+-------+
    |   2   |   3   |
    +-------+-------+
"""
from __future__ import annotations

from dataclasses import dataclass

from PIL import Image

from image_search.common.warn import warn


SRED_MONTAGE_SIZE: tuple[int, int] = (224, 224)
CELL_SIZE: int = 112
UPSCALE_SIZE: int = 384

_CORNERS: tuple[tuple[int, int, int, int], ...] = (
    (0, 0, 112, 112),       # 0 TL
    (112, 0, 224, 112),     # 1 TR
    (0, 112, 112, 224),     # 2 BL
    (112, 112, 224, 224),   # 3 BR
)


@dataclass(frozen=True)
class SredCrop:
    parent_image_id: str
    cell: int
    pre_resize_size: tuple[int, int]
    pre_resize_bytes: bytes  # raw RGB bytes at 112x112 (for fixture hashing)
    resized: Image.Image     # 384x384 RGB (model input)


def split_sred_2x2(img: Image.Image, *, parent_image_id: str = "unknown") -> list[SredCrop]:
    """Split a 224x224 SRED montage into four 112x112 cells, each upscaled to 384x384.

    Raises ValueError (with a [WARN] sred_guard) if the image is not 224x224.
    """
    if img.size != SRED_MONTAGE_SIZE:
        warn("sred_guard", parent_image_id=parent_image_id,
             expected=SRED_MONTAGE_SIZE, got=img.size)
        raise ValueError(
            f"split_sred_2x2 expects {SRED_MONTAGE_SIZE}, got {img.size}"
        )

    rgb = img if img.mode == "RGB" else img.convert("RGB")
    crops: list[SredCrop] = []
    for cell_idx, box in enumerate(_CORNERS):
        sub = rgb.crop(box)  # 112x112 RGB
        assert sub.size == (CELL_SIZE, CELL_SIZE), sub.size  # invariant
        resized = sub.resize((UPSCALE_SIZE, UPSCALE_SIZE), Image.Resampling.BICUBIC)
        crops.append(SredCrop(
            parent_image_id=parent_image_id,
            cell=cell_idx,
            pre_resize_size=sub.size,
            pre_resize_bytes=sub.tobytes(),
            resized=resized,
        ))
    return crops
