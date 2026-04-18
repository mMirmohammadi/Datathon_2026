from __future__ import annotations

from pathlib import Path

import pytest

from app.core import visual_search, text_embed_search
from app.harness import search_service
from app.harness.search_service import _rerank_hybrid, query_from_text
from app.models.schemas import HardFilters, SoftPreferences


# ---------------- fakes ----------------


class _FakeChannel:
    """Tiny stand-in for visual_search / text_embed_search."""

    def __init__(self, score_map: dict[str, float]) -> None:
        self.score_map = score_map
        self.loaded = True

    def score_candidates(self, query, candidates):
        return {
            str(c["listing_id"]): s
            for c in candidates
            if (s := self.score_map.get(str(c["listing_id"]))) is not None
        }


def _activate_visual(monkeypatch, score_map):
    fake = _FakeChannel(score_map)
    monkeypatch.setattr(search_service, "visual_enabled", lambda: True)
    monkeypatch.setattr(search_service, "visual_is_loaded", lambda: True)
    monkeypatch.setattr(
        search_service, "visual_score_candidates", fake.score_candidates
    )


def _activate_text_embed(monkeypatch, score_map):
    fake = _FakeChannel(score_map)
    monkeypatch.setattr(search_service, "text_embed_enabled", lambda: True)
    monkeypatch.setattr(search_service, "text_embed_is_loaded", lambda: True)
    monkeypatch.setattr(
        search_service, "text_embed_score_candidates", fake.score_candidates
    )


def _disable_visual(monkeypatch) -> None:
    monkeypatch.setattr(search_service, "visual_enabled", lambda: False)
    monkeypatch.setattr(search_service, "visual_is_loaded", lambda: False)


def _disable_text_embed(monkeypatch) -> None:
    monkeypatch.setattr(search_service, "text_embed_enabled", lambda: False)
    monkeypatch.setattr(search_service, "text_embed_is_loaded", lambda: False)


# ---------------- _rerank_hybrid ----------------


class TestRerankHybridChannels:
    @pytest.fixture(autouse=True)
    def _defaults(self, monkeypatch):
        _disable_visual(monkeypatch)
        _disable_text_embed(monkeypatch)
        # build_soft_rankings gets called with db_path; patch it to return
        # controllable rankings here so we don't need a real DB.
        monkeypatch.setattr(
            search_service, "build_soft_rankings",
            lambda cands, soft, db_path: [],
        )

    def test_empty_candidates_returns_empty(self, tmp_path) -> None:
        assert _rerank_hybrid([], "q", None, tmp_path / "db") == []

    def test_bm25_only_attaches_rrf_score(self, tmp_path) -> None:
        cands = [{"listing_id": "L1"}, {"listing_id": "L2"}]
        out = _rerank_hybrid(cands, "q", None, tmp_path / "db")
        # BM25 alone: L1 rank 1 -> 1/61; L2 rank 2 -> 1/62. L1 wins.
        assert out[0]["listing_id"] == "L1"
        assert out[0]["rrf_score"] == pytest.approx(1 / 61)
        assert out[1]["rrf_score"] == pytest.approx(1 / 62)
        assert out[0]["visual_score"] is None
        assert out[0]["text_embed_score"] is None
        assert out[0]["soft_signals_activated"] == 0

    def test_visual_boost_reorders(self, tmp_path, monkeypatch) -> None:
        _activate_visual(monkeypatch, {"L2": 0.9})
        cands = [{"listing_id": "L1"}, {"listing_id": "L2"}]
        out = _rerank_hybrid(cands, "q", None, tmp_path / "db")
        # L1 only in BM25 rank 1; L2 in BM25 rank 2 AND visual rank 1 -> sum wins.
        assert out[0]["listing_id"] == "L2"
        assert out[0]["visual_score"] == 0.9

    def test_text_embed_boost_reorders(self, tmp_path, monkeypatch) -> None:
        _activate_text_embed(monkeypatch, {"L3": 0.8})
        cands = [{"listing_id": lid} for lid in ("L1", "L2", "L3")]
        out = _rerank_hybrid(cands, "q", None, tmp_path / "db")
        # L3 appears in BM25 rank 3 AND text_embed rank 1. L1 BM25 rank 1.
        # L3 rrf = 1/63 + 1/61, L1 rrf = 1/61. L3 wins.
        assert out[0]["listing_id"] == "L3"
        assert out[0]["text_embed_score"] == 0.8

    def test_all_channels_plus_soft_prefs(self, tmp_path, monkeypatch) -> None:
        _activate_visual(monkeypatch, {"L2": 0.9})
        _activate_text_embed(monkeypatch, {"L3": 0.8})
        monkeypatch.setattr(
            search_service, "build_soft_rankings",
            lambda c, s, d: [["L1"], ["L2"]],  # two soft rankings
        )
        cands = [{"listing_id": lid} for lid in ("L1", "L2", "L3")]
        out = _rerank_hybrid(cands, "q", SoftPreferences(quiet=True), tmp_path / "db")
        # soft_signals_activated should equal 2.
        assert all(c["soft_signals_activated"] == 2 for c in out)


# ---------------- query_from_text ----------------


class TestQueryFromText:
    def _setup(self, monkeypatch, tmp_path):
        """Stand up the enriched-500 fallback DB so query_from_text has a real
        listings table to gate against; disable all ML channels."""
        import os
        repo_root = Path(__file__).resolve().parents[1]
        os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")
        os.environ["LISTINGS_DB_PATH"] = str(tmp_path / "listings.db")
        _disable_visual(monkeypatch)
        _disable_text_embed(monkeypatch)
        monkeypatch.setattr(
            search_service, "build_soft_rankings",
            lambda c, s, d: [],
        )

    def test_pool_size_respected(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._setup(monkeypatch, tmp_path)

        captured = {}

        def fake_extract(query: str) -> HardFilters:
            return HardFilters()

        def spy_filter_hard_facts(db_path, hard_facts):
            captured["limit"] = hard_facts.limit
            captured["offset"] = hard_facts.offset
            return []

        monkeypatch.setattr(search_service, "extract_hard_facts", fake_extract)
        monkeypatch.setattr(search_service, "filter_hard_facts", spy_filter_hard_facts)

        response = query_from_text(
            db_path=tmp_path / "listings.db",
            query="any",
            limit=5,
            offset=3,
        )
        # Pool is MAX(limit, HYBRID_POOL=300). Offset is zeroed before the gate
        # so pagination happens after fusion.
        assert captured["limit"] >= 300
        assert captured["offset"] == 0
        assert response.listings == []

    def test_pagination_applied_after_fusion(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._setup(monkeypatch, tmp_path)
        _activate_visual(monkeypatch, {"L3": 0.99})

        def fake_extract(q):
            return HardFilters()

        # Inject a fixed candidate pool of 5 with bm25 order L1..L5.
        def fake_filter(db_path, hard_facts):
            return [{"listing_id": f"L{i}", "title": f"T{i}"} for i in range(1, 6)]

        monkeypatch.setattr(search_service, "extract_hard_facts", fake_extract)
        monkeypatch.setattr(search_service, "filter_hard_facts", fake_filter)

        # The candidate dicts need a few fields for ListingData; add them.
        def fake_filter_enriched(db_path, hard_facts):
            rows = fake_filter(db_path, hard_facts)
            for r in rows:
                r.update({"city": "x", "latitude": None, "longitude": None})
            return rows

        monkeypatch.setattr(search_service, "filter_hard_facts", fake_filter_enriched)

        # limit=2, offset=0 -> visual-boosted L3 first, then BM25-first L1.
        response = query_from_text(
            db_path=tmp_path / "listings.db",
            query="visual please",
            limit=2,
            offset=0,
        )
        ids = [item.listing_id for item in response.listings]
        assert ids[0] == "L3"            # visual-boosted to the top
        assert len(ids) == 2             # paginated to 2

        # offset=1 -> skip the top, return the next page.
        response2 = query_from_text(
            db_path=tmp_path / "listings.db",
            query="visual please",
            limit=2,
            offset=1,
        )
        ids2 = [item.listing_id for item in response2.listings]
        assert ids2[0] != "L3"           # page 2 doesn't start with the winner
        assert len(ids2) == 2
