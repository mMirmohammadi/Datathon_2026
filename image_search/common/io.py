"""Safe image opener for the embedding pipeline.

Returns a PIL.Image in mode 'RGB' on success, None on failure (and a [WARN]
describing the failure — never a silent error per project CLAUDE.md §5).
"""
from __future__ import annotations

from pathlib import Path

from PIL import Image, UnidentifiedImageError

from image_search.common.warn import warn


def safe_open_image(path: Path) -> Image.Image | None:
    try:
        if path.stat().st_size == 0:
            warn("skip_zero_byte", path=str(path))
            return None
    except FileNotFoundError:
        warn("skip_missing", path=str(path))
        return None

    try:
        img = Image.open(path)
        img.load()  # force decode so we catch truncation now, not later
    except UnidentifiedImageError as e:
        warn("skip_corrupt", path=str(path), err=str(e))
        return None
    except (OSError, ValueError) as e:
        warn("skip_corrupt", path=str(path), err=str(e))
        return None

    if img.mode == "RGBA":
        # Composite onto white so downstream sees clean RGB.
        bg = Image.new("RGB", img.size, (255, 255, 255))
        bg.paste(img, mask=img.split()[3])
        warn("png_rgba_to_rgb", path=str(path))
        return bg

    if img.mode != "RGB":
        return img.convert("RGB")

    return img
