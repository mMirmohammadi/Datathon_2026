from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pytest

from app.core import text_embed_search


@pytest.fixture(autouse=True)
def _reset_state() -> None:
    text_embed_search.reset_for_tests()
    yield
    text_embed_search.reset_for_tests()


def _fake_matrix_and_ids() -> tuple[np.ndarray, list[str]]:
    # 4-dim unit-norm embeddings; row 0 is exactly aligned with "bright".
    mat = np.array([
        [1.0, 0.0, 0.0, 0.0],
        [0.0, 1.0, 0.0, 0.0],
        [0.5, 0.5, 0.5, 0.5],
        [0.0, 0.0, 0.0, 1.0],
    ], dtype=np.float32)
    mat = mat / np.linalg.norm(mat, axis=1, keepdims=True)
    ids = ["L1", "L2", "L3", "L4"]
    return mat, ids


# ---------- enabled env toggle ----------

def test_defaults_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LISTINGS_TEXT_EMBED_ENABLED", raising=False)
    assert text_embed_search.text_embed_enabled() is True


def test_env_zero_disables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LISTINGS_TEXT_EMBED_ENABLED", "0")
    assert text_embed_search.text_embed_enabled() is False


# ---------- load_text_embed_index ----------

def test_load_delegates_to_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    matrix, ids = _fake_matrix_and_ids()

    from ranking.runtime import embedding_search as runtime

    monkeypatch.setattr(runtime, "_lazy_init", lambda: (matrix, ids))
    monkeypatch.setattr(runtime, "_lazy_model", lambda: object())

    text_embed_search.load_text_embed_index()

    assert text_embed_search.is_loaded()
    assert text_embed_search._STATE["matrix"] is matrix
    assert text_embed_search._STATE["ids"] is ids


def test_load_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    matrix, ids = _fake_matrix_and_ids()
    from ranking.runtime import embedding_search as runtime

    calls = {"init": 0, "model": 0}

    def _init():
        calls["init"] += 1
        return matrix, ids

    def _model():
        calls["model"] += 1
        return object()

    monkeypatch.setattr(runtime, "_lazy_init", _init)
    monkeypatch.setattr(runtime, "_lazy_model", _model)
    text_embed_search.load_text_embed_index()
    text_embed_search.load_text_embed_index()
    assert calls == {"init": 1, "model": 1}


# ---------- score_candidates ----------

def test_score_candidates_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    matrix, ids = _fake_matrix_and_ids()
    from ranking.runtime import embedding_search as runtime

    monkeypatch.setattr(runtime, "_lazy_init", lambda: (matrix, ids))
    monkeypatch.setattr(runtime, "_lazy_model", lambda: object())
    text_embed_search.load_text_embed_index()

    def fake_score_for_listings(query_text: str, listing_ids: list[str]):
        mapping = {"L1": 0.9, "L2": 0.1, "L3": 0.5, "L4": None}
        return {lid: mapping.get(lid) for lid in listing_ids}

    monkeypatch.setattr(runtime, "score_for_listings", fake_score_for_listings)

    cands = [{"listing_id": lid} for lid in ("L1", "L2", "L3", "L4", "LX")]
    scores = text_embed_search.score_candidates("bright", cands)
    assert scores == {"L1": 0.9, "L2": 0.1, "L3": 0.5}
    # L4 (None) and LX (unknown) are omitted.


def test_score_candidates_empty_returns_empty() -> None:
    assert text_embed_search.score_candidates("x", []) == {}


def test_score_candidates_raises_when_not_loaded() -> None:
    text_embed_search.reset_for_tests()
    with pytest.raises(RuntimeError, match="before load_text_embed_index"):
        text_embed_search.score_candidates("x", [{"listing_id": "L1"}])
