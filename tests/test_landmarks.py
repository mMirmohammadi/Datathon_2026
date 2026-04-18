from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core import landmarks


@pytest.fixture(autouse=True)
def _reset_landmarks_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Each test starts with a fresh in-memory landmark cache."""
    landmarks._STATE.update({"by_slug": None, "by_key": None, "path": None})
    yield
    landmarks._STATE.update({"by_slug": None, "by_key": None, "path": None})


def _write_landmarks(tmp_path: Path, data: list[dict]) -> Path:
    path = tmp_path / "landmarks.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def test_resolve_exact_key(tmp_path: Path) -> None:
    path = _write_landmarks(tmp_path, [
        {"key": "eth_zentrum", "kind": "university", "lat": 47.3765,
         "lon": 8.5482, "aliases": ["ETH", "ETH Zürich", "ETHZ"]},
    ])
    landmarks.load(path)
    lm = landmarks.resolve("eth_zentrum")
    assert lm is not None
    assert lm.key == "eth_zentrum"
    assert lm.kind == "university"
    assert lm.lat == pytest.approx(47.3765)


def test_resolve_alias_case_insensitive(tmp_path: Path) -> None:
    path = _write_landmarks(tmp_path, [
        {"key": "eth_zentrum", "kind": "university", "lat": 47.3765,
         "lon": 8.5482, "aliases": ["ETH", "ETH Zürich", "ETHZ"]},
    ])
    landmarks.load(path)
    for name in ("ETH", "eth", "ETHZ", "eth zürich", "ETH ZÜRICH"):
        lm = landmarks.resolve(name)
        assert lm is not None, name
        assert lm.key == "eth_zentrum", f"{name!r} did not resolve"


def test_resolve_accent_fold(tmp_path: Path) -> None:
    path = _write_landmarks(tmp_path, [
        {"key": "zurichsee", "kind": "lake", "lat": 47.30, "lon": 8.55,
         "aliases": ["Zürichsee", "Lac de Zurich", "Lake Zurich"]},
    ])
    landmarks.load(path)
    # ASCII fold should match the umlaut aliases.
    assert landmarks.resolve("Zurichsee").key == "zurichsee"
    assert landmarks.resolve("zuerichsee") is None  # not an alias; slug lookup fails
    assert landmarks.resolve("Lake Zurich").key == "zurichsee"


def test_resolve_returns_none_for_unknown(tmp_path: Path) -> None:
    path = _write_landmarks(tmp_path, [])
    landmarks.load(path)
    assert landmarks.resolve("nope") is None
    assert landmarks.resolve("") is None
    assert landmarks.resolve(None) is None  # type: ignore[arg-type]


def test_column_for_returns_distance_signal_name() -> None:
    assert landmarks.column_for("eth_zentrum") == "dist_landmark_eth_zentrum_m"
    assert landmarks.column_for("zurich_hb") == "dist_landmark_zurich_hb_m"


def test_missing_gazetteer_warns_and_returns_empty(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "missing.json"
    landmarks.load(path)
    out = capsys.readouterr().out
    assert "[WARN] landmarks.load" in out
    assert landmarks.resolve("ETH") is None
