"""Enumerate images under raw_data/.

Three known trees (verified by direct inspection):

    raw_data/sred_images/<platform_id>.jpeg                         (flat, 11105 files, 224x224)
    raw_data/robinreal_images/platform_id=<pid>/<image>.jpg|.png    (792 dirs, 5385 files)
    raw_data/structured_data_images/platform_id=<pid>/<image>.jpg   (3842 dirs, 25555 files)

We use the folder-embedded `platform_id` as the listing key in this pipeline,
since it is derivable without DB access and is 1:1 with listings in our data.
If a caller needs the canonical `listing_id` from SQLite they can map later.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}

RAW_DATA = Path(__file__).resolve().parents[2] / "raw_data"
SRED_DIR       = RAW_DATA / "sred_images"
ROBINREAL_DIR  = RAW_DATA / "robinreal_images"
STRUCTURED_DIR = RAW_DATA / "structured_data_images"


@dataclass(frozen=True)
class ImageRef:
    source: str       # "sred" | "robinreal" | "structured"
    platform_id: str  # dir-embedded or filename-stem; treated as listing key
    image_id: str     # unique within (source, platform_id); filename stem for now
    path: Path        # absolute path on disk


def _is_image(p: Path) -> bool:
    return p.is_file() and p.suffix.lower() in IMAGE_EXTS


def _platform_id_from_dir(d: Path) -> str | None:
    name = d.name
    prefix = "platform_id="
    if not name.startswith(prefix):
        return None
    return name[len(prefix):]


def iter_sred(root: Path = SRED_DIR) -> Iterator[ImageRef]:
    if not root.is_dir():
        return
    for p in sorted(root.iterdir()):
        if not _is_image(p):
            continue
        pid = p.stem  # e.g. "1154156"
        yield ImageRef(source="sred", platform_id=pid, image_id=pid, path=p)


def _iter_platform_tree(root: Path, source_name: str) -> Iterator[ImageRef]:
    if not root.is_dir():
        return
    for sub in sorted(root.iterdir()):
        if not sub.is_dir():
            continue
        pid = _platform_id_from_dir(sub)
        if pid is None:
            continue
        for p in sorted(sub.iterdir()):
            if not _is_image(p):
                continue
            yield ImageRef(source=source_name, platform_id=pid, image_id=p.stem, path=p)


def iter_robinreal(root: Path = ROBINREAL_DIR) -> Iterator[ImageRef]:
    yield from _iter_platform_tree(root, "robinreal")


def iter_structured(root: Path = STRUCTURED_DIR) -> Iterator[ImageRef]:
    yield from _iter_platform_tree(root, "structured")


def iter_all() -> Iterator[ImageRef]:
    yield from iter_sred()
    yield from iter_robinreal()
    yield from iter_structured()
