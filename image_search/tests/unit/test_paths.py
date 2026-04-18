from __future__ import annotations

from pathlib import Path

from image_search.common.paths import (
    iter_robinreal,
    iter_sred,
    iter_structured,
)


def _mkimg(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    # Minimal valid JPEG header — tests only care about file extension / existence.
    p.write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 16)


def test_iter_sred_flat_files(tmp_path: Path):
    root = tmp_path / "sred_images"
    _mkimg(root / "1001.jpeg")
    _mkimg(root / "1002.jpeg")
    root.joinpath("notes.txt").write_text("ignore")

    refs = list(iter_sred(root))
    assert [r.platform_id for r in refs] == ["1001", "1002"]
    assert all(r.source == "sred" for r in refs)
    assert all(r.image_id == r.platform_id for r in refs)


def test_iter_robinreal_platform_tree(tmp_path: Path):
    root = tmp_path / "robinreal_images"
    _mkimg(root / "platform_id=abc/0-img.jpg")
    _mkimg(root / "platform_id=abc/1-img.jpg")
    _mkimg(root / "platform_id=def/0-img.png")
    (root / "stray_file.jpg").parent.mkdir(parents=True, exist_ok=True)
    (root / "stray_file.jpg").write_bytes(b"")  # not under a platform_id= dir

    refs = list(iter_robinreal(root))
    platforms = sorted({r.platform_id for r in refs})
    assert platforms == ["abc", "def"]
    assert all(r.source == "robinreal" for r in refs)
    assert len(refs) == 3


def test_iter_structured_ignores_non_image_files(tmp_path: Path):
    root = tmp_path / "structured_data_images"
    _mkimg(root / "platform_id=123/a.jpg")
    (root / "platform_id=123/metadata.json").write_text("{}")

    refs = list(iter_structured(root))
    assert len(refs) == 1
    assert refs[0].platform_id == "123"
    assert refs[0].source == "structured"
