from __future__ import annotations

import pytest

from app.core.visual_search import fuse_rankings, fuse_rrf


class TestFuseRankings:
    def test_empty_list_returns_empty_dict(self) -> None:
        assert fuse_rankings([]) == {}

    def test_single_ranking_reproduces_reciprocal_rank(self) -> None:
        out = fuse_rankings([["A", "B", "C"]], k=60)
        assert out == pytest.approx({
            "A": 1 / 61,
            "B": 1 / 62,
            "C": 1 / 63,
        })

    def test_two_way_fuse_matches_shim(self) -> None:
        a = ["A", "B", "C"]
        b = ["B", "A", "D"]
        assert fuse_rankings([a, b]) == fuse_rrf(a, b)

    def test_three_rankings_sum_correctly(self) -> None:
        out = fuse_rankings([
            ["A", "B"],
            ["A", "C"],
            ["B", "A"],
        ])
        # A appears rank 1 twice, rank 2 once -> 2/(60+1) + 1/(60+2).
        # B appears rank 1 once, rank 2 once -> 1/(60+1) + 1/(60+2).
        # C appears rank 2 once -> 1/(60+2).
        assert out["A"] > out["B"] > out["C"]
        assert out["A"] == pytest.approx(2 / 61 + 1 / 62)
        assert out["B"] == pytest.approx(1 / 61 + 1 / 62)
        assert out["C"] == pytest.approx(1 / 62)

    def test_duplicates_in_one_ranking_count_only_first(self) -> None:
        # A appearing twice in the same ranking must not double-count (self-boost).
        out = fuse_rankings([["A", "A", "B"]])
        assert out == pytest.approx({"A": 1 / 61, "B": 1 / 63})

    def test_k_parameter_respected(self) -> None:
        assert fuse_rankings([["A"]], k=10) == pytest.approx({"A": 1 / 11})

    def test_custom_ranking_overlap(self) -> None:
        # Listing present in all rankings should strictly dominate.
        out = fuse_rankings([
            ["A", "B", "C"],
            ["A", "D", "E"],
            ["A", "F", "G"],
        ])
        assert max(out, key=out.get) == "A"
