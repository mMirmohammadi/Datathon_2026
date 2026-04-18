from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from app.core import visual_search


# ---------- fuse_rrf (pure function) ----------

class TestFuseRrf:
    def test_item_in_both_rankings_scores_highest(self) -> None:
        fused = visual_search.fuse_rrf(
            bm25_order=["A", "B", "C"],
            visual_order=["A", "C", "B"],
        )
        assert fused["A"] > fused["B"]
        assert fused["A"] > fused["C"]

    def test_item_only_in_one_ranking_still_scores(self) -> None:
        fused = visual_search.fuse_rrf(
            bm25_order=["A", "B"],
            visual_order=["C"],
        )
        assert set(fused) == {"A", "B", "C"}
        assert fused["A"] > 0
        assert fused["C"] > 0

    def test_formula_matches_1_over_k_plus_rank(self) -> None:
        fused = visual_search.fuse_rrf(
            bm25_order=["A"],
            visual_order=["A"],
            k=60,
        )
        expected = 1.0 / (60 + 1) + 1.0 / (60 + 1)
        assert fused["A"] == pytest.approx(expected)

    def test_empty_orders_gives_empty_result(self) -> None:
        assert visual_search.fuse_rrf([], []) == {}

    def test_later_ranks_score_lower(self) -> None:
        fused = visual_search.fuse_rrf(
            bm25_order=["A", "B", "C", "D"],
            visual_order=["A", "B", "C", "D"],
        )
        assert fused["A"] > fused["B"] > fused["C"] > fused["D"]


# ---------- visual_enabled env flag ----------

class TestVisualEnabled:
    def test_defaults_on(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("LISTINGS_VISUAL_ENABLED", raising=False)
        assert visual_search.visual_enabled() is True

    def test_zero_disables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LISTINGS_VISUAL_ENABLED", "0")
        assert visual_search.visual_enabled() is False

    def test_one_enables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LISTINGS_VISUAL_ENABLED", "1")
        assert visual_search.visual_enabled() is True


# ---------- scrape-source mapping ----------

def test_scrape_source_mapping_covers_all_three_sources() -> None:
    assert visual_search.SCRAPE_SOURCE_TO_IMAGE_SOURCE["COMPARIS"] == "structured"
    assert visual_search.SCRAPE_SOURCE_TO_IMAGE_SOURCE["ROBINREAL"] == "robinreal"
    assert visual_search.SCRAPE_SOURCE_TO_IMAGE_SOURCE["SRED"] == "sred"


# ---------- score_candidates ----------

@pytest.fixture
def fake_visual_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Populate visual_search._STATE with a tiny in-memory index.

    Layout: 4-d unit-L2 vectors. Five rows owned by three listings:
      row 0, 1 -> (structured, "36493173")  -- L1
      row 2, 3 -> (structured, "36276309")  -- L2
      row 4    -> (robinreal, "697caa67")   -- L3 (one image only)
    Row 0 is aligned with a "bright" query vector, row 3 is aligned with a
    "view" query vector; we pick values so max-pool semantics are obvious.
    """
    matrix = np.array(
        [
            [1.0, 0.0, 0.0, 0.0],  # L1 best image for "bright"
            [0.0, 1.0, 0.0, 0.0],  # L1 second image
            [0.5, 0.5, 0.5, 0.5],  # L2 ok for any query
            [0.0, 0.0, 0.0, 1.0],  # L2 best image for "view"
            [0.0, 0.5, 0.5, np.sqrt(0.5)],  # L3 middling
        ],
        dtype=np.float32,
    )
    # ensure rows are unit-norm
    matrix = matrix / np.linalg.norm(matrix, axis=1, keepdims=True)

    pid_to_rowids = {
        ("structured", "36493173"): [0, 1],
        ("structured", "36276309"): [2, 3],
        ("robinreal", "697caa67"): [4],
    }

    visual_search.reset_for_tests()
    visual_search._STATE["loaded"] = True
    visual_search._STATE["lm"] = object()  # sentinel
    visual_search._STATE["main_matrix"] = matrix
    visual_search._STATE["pid_to_rowids"] = pid_to_rowids

    def _fake_encode_query(text: str) -> np.ndarray:
        # Deterministic: the word "bright" produces [1,0,0,0], "view" -> [0,0,0,1], else [0.5]*4.
        vec = np.array([0.5, 0.5, 0.5, 0.5], dtype=np.float32)
        if "bright" in text.lower():
            vec = np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32)
        elif "view" in text.lower():
            vec = np.array([0.0, 0.0, 0.0, 1.0], dtype=np.float32)
        return vec / np.linalg.norm(vec)

    monkeypatch.setattr(visual_search, "encode_query", _fake_encode_query)
    yield
    visual_search.reset_for_tests()


class TestScoreCandidates:
    def test_bright_query_prefers_L1(self, fake_visual_state) -> None:
        candidates = [
            {"listing_id": "L1", "scrape_source": "COMPARIS", "platform_id": "36493173"},
            {"listing_id": "L2", "scrape_source": "COMPARIS", "platform_id": "36276309"},
            {"listing_id": "L3", "scrape_source": "ROBINREAL", "platform_id": "697caa67"},
        ]
        scores = visual_search.score_candidates("bright apartment", candidates)
        assert scores["L1"] == pytest.approx(1.0)  # row 0 is exactly aligned
        assert scores["L1"] > scores["L2"]
        assert scores["L1"] > scores["L3"]

    def test_view_query_prefers_L2_over_L1(self, fake_visual_state) -> None:
        candidates = [
            {"listing_id": "L1", "scrape_source": "COMPARIS", "platform_id": "36493173"},
            {"listing_id": "L2", "scrape_source": "COMPARIS", "platform_id": "36276309"},
        ]
        scores = visual_search.score_candidates("view", candidates)
        assert scores["L2"] == pytest.approx(1.0)  # row 3 is exactly aligned with [0,0,0,1]
        assert scores["L2"] > scores["L1"]

    def test_max_pool_uses_best_image_per_listing(self, fake_visual_state) -> None:
        # L1 has row 0 aligned with the query and row 1 orthogonal. Max should
        # be 1.0 (from row 0), not the average of the two.
        candidates = [
            {"listing_id": "L1", "scrape_source": "COMPARIS", "platform_id": "36493173"},
        ]
        scores = visual_search.score_candidates("bright", candidates)
        assert scores["L1"] == pytest.approx(1.0)

    def test_missing_listing_is_omitted(self, fake_visual_state) -> None:
        candidates = [
            {"listing_id": "L1", "scrape_source": "COMPARIS", "platform_id": "36493173"},
            # L4 has no images in the fake index.
            {"listing_id": "L4", "scrape_source": "COMPARIS", "platform_id": "99999999"},
        ]
        scores = visual_search.score_candidates("bright", candidates)
        assert "L4" not in scores
        assert "L1" in scores

    def test_unknown_scrape_source_warns_and_skips(
        self, fake_visual_state, capsys: pytest.CaptureFixture[str]
    ) -> None:
        candidates = [
            {"listing_id": "LX", "scrape_source": "NONESUCH", "platform_id": "foo"},
        ]
        scores = visual_search.score_candidates("bright", candidates)
        assert scores == {}
        out = capsys.readouterr().out
        assert "[WARN] visual_unknown_scrape_source" in out

    def test_empty_candidates_returns_empty(self, fake_visual_state) -> None:
        assert visual_search.score_candidates("bright", []) == {}

    def test_raises_when_not_loaded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        visual_search.reset_for_tests()
        with pytest.raises(RuntimeError, match="before load_visual_index"):
            visual_search.score_candidates(
                "bright",
                [{"listing_id": "L1", "scrape_source": "COMPARIS", "platform_id": "x"}],
            )


# ---------- load_visual_index guard rails ----------

def test_load_visual_index_raises_when_store_missing(tmp_path: Path) -> None:
    visual_search.reset_for_tests()
    missing = tmp_path / "does-not-exist"
    with pytest.raises(FileNotFoundError, match="visual store dir not found"):
        visual_search.load_visual_index(missing)
