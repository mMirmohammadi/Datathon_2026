from __future__ import annotations

from pathlib import Path

from PIL import Image

from image_search.common.io import safe_open_image


def test_safe_open_image_returns_rgb_for_jpeg(tmp_path: Path):
    p = tmp_path / "ok.jpg"
    Image.new("RGB", (10, 10), (123, 45, 67)).save(p, "JPEG")
    img = safe_open_image(p)
    assert img is not None
    assert img.mode == "RGB"
    assert img.size == (10, 10)


def test_safe_open_image_composites_rgba_png(tmp_path: Path, capsys):
    p = tmp_path / "alpha.png"
    Image.new("RGBA", (5, 5), (0, 0, 0, 0)).save(p, "PNG")
    img = safe_open_image(p)
    assert img is not None
    assert img.mode == "RGB"
    assert "[WARN] png_rgba_to_rgb" in capsys.readouterr().err


def test_safe_open_image_returns_none_for_zero_byte(tmp_path: Path, capsys):
    p = tmp_path / "empty.jpg"
    p.write_bytes(b"")
    assert safe_open_image(p) is None
    assert "[WARN] skip_zero_byte" in capsys.readouterr().err


def test_safe_open_image_returns_none_for_corrupt(tmp_path: Path, capsys):
    p = tmp_path / "garbage.jpg"
    p.write_bytes(b"not a jpeg")
    assert safe_open_image(p) is None
    assert "[WARN] skip_corrupt" in capsys.readouterr().err


def test_safe_open_image_returns_none_for_missing(tmp_path: Path, capsys):
    p = tmp_path / "nope.jpg"
    assert safe_open_image(p) is None
    assert "[WARN] skip_missing" in capsys.readouterr().err
