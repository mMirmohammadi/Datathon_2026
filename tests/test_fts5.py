from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core.hard_filters import HardFilterParams, search_listings
from app.db import get_connection
from app.harness.csv_import import create_indexes, create_schema


def _insert(
    connection,
    *,
    listing_id: str,
    title: str = "",
    description: str = "",
    city_slug: str = "zurich",
    canton: str = "ZH",
    object_category: str = "apartment",
    object_category_raw: str | None = "Wohnung",
    price: int | None = 2000,
    rooms: float | None = 3.0,
    **overrides,
) -> None:
    cols = {
        "listing_id": listing_id,
        "title": title or listing_id,
        "description": description,
        "city": city_slug,
        "city_slug": city_slug,
        "canton": canton,
        "object_category": object_category,
        "object_category_raw": object_category_raw,
        "price": price,
        "rooms": rooms,
        "features_json": "[]",
        "offer_type": "RENT",
        "raw_json": "{}",
    }
    cols.update(overrides)
    placeholders = ", ".join("?" for _ in cols)
    columns = ", ".join(cols.keys())
    connection.execute(
        f"INSERT INTO listings ({columns}) VALUES ({placeholders})",
        tuple(cols.values()),
    )


@pytest.fixture(scope="module")
def fts_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    db_path = tmp_path_factory.mktemp("fts") / "listings.db"
    with get_connection(db_path) as connection:
        create_schema(connection)
        # L1: Minergie + Altbau + balcony (high-signal)
        _insert(
            connection,
            listing_id="L1",
            title="Minergie loft",
            description="Modern Minergie-certified apartment with Altbau charm and balcony.",
            city_slug="zurich",
        )
        # L2: Zurich with Zürich umlaut in description, balcony, Helle Wohnung
        _insert(
            connection,
            listing_id="L2",
            title="Helle Wohnung in Zürich",
            description="Helle 3-Zimmer Wohnung mit Balkon in Zürich Altstadt.",
            city_slug="zurich",
        )
        # L3: balcony only, Winterthur
        _insert(
            connection,
            listing_id="L3",
            title="Balcony flat",
            description="Bright apartment with balcony near the lake.",
            city_slug="winterthur",
            canton="ZH",
        )
        # L4: French listing, Genève, no target keywords
        _insert(
            connection,
            listing_id="L4",
            title="Appartement moderne",
            description="Appartement moderne au centre de Genève avec vue.",
            city_slug="geneva",
            canton="GE",
        )
        # L5: no matching terms at all, Bern
        _insert(
            connection,
            listing_id="L5",
            title="Plain flat",
            description="Comfortable family home, two bedrooms, quiet street.",
            city_slug="bern",
            canton="BE",
        )
        connection.commit()
        create_indexes(connection)
        connection.execute("INSERT INTO listings_fts(listings_fts) VALUES('rebuild')")
        connection.commit()
    return db_path


def _ids(rows: list[dict]) -> list[str]:
    return [row["listing_id"] for row in rows]


# ---------- index + MATCH sanity ----------

def test_fts_index_returns_rows_for_literal_term(fts_db: Path) -> None:
    rows = search_listings(
        fts_db, HardFilterParams(bm25_keywords=["Altbau"], limit=10)
    )
    assert "L1" in _ids(rows)
    l1 = next(r for r in rows if r["listing_id"] == "L1")
    assert l1["bm25_score"] < 0  # FTS bm25 is negative when matched


def test_accent_fold_query_zurich_matches_umlaut_rows(fts_db: Path) -> None:
    rows = search_listings(
        fts_db, HardFilterParams(bm25_keywords=["Zurich"], limit=10)
    )
    matched_ids = [r["listing_id"] for r in rows if r["bm25_score"] < 0]
    # L1 and L2 both mention Zürich/Zurich in title or description.
    assert "L2" in matched_ids


def test_multilingual_german_term_matches(fts_db: Path) -> None:
    rows = search_listings(
        fts_db, HardFilterParams(bm25_keywords=["Wohnung"], limit=10)
    )
    matched_ids = [r["listing_id"] for r in rows if r["bm25_score"] < 0]
    assert "L2" in matched_ids


def test_bm25_orders_by_relevance(fts_db: Path) -> None:
    # L1 mentions "Minergie" once, L2 doesn't. Order should be L1 first.
    rows = search_listings(
        fts_db, HardFilterParams(bm25_keywords=["Minergie"], limit=10)
    )
    matched_ids = [r["listing_id"] for r in rows if r["bm25_score"] < 0]
    assert matched_ids[0] == "L1"


def test_or_semantics_returns_either_match(fts_db: Path) -> None:
    # "Minergie" is only in L1; "moderne" is only in L4 (French).
    rows = search_listings(
        fts_db,
        HardFilterParams(bm25_keywords=["Minergie", "moderne"], limit=10),
    )
    matched_ids = {r["listing_id"] for r in rows if r["bm25_score"] < 0}
    assert "L1" in matched_ids
    assert "L4" in matched_ids


# ---------- gate intersection ----------

def test_hard_filter_plus_keywords_keeps_gate_semantics(fts_db: Path) -> None:
    # Gate by canton=ZH (L1, L2, L3). BM25 "balcony" matches L1, L2, L3 too.
    # Gate by canton=GE would drop L1/L2/L3 regardless of BM25 match.
    rows = search_listings(
        fts_db,
        HardFilterParams(
            canton="GE", bm25_keywords=["balcony", "Balkon"], limit=10
        ),
    )
    assert _ids(rows) == ["L4"]  # gate reduces to L4; BM25 score is 1e9 (no match)
    assert rows[0]["bm25_score"] >= 1e8


def test_keywords_only_still_returns_non_matching_gate_rows_last(fts_db: Path) -> None:
    # No hard filter; bm25 keyword hits only L1/L2/L3.
    # LEFT JOIN semantics: all 5 rows returned, matches first, non-matches after.
    rows = search_listings(
        fts_db, HardFilterParams(bm25_keywords=["balcony", "Balkon"], limit=10)
    )
    assert len(rows) == 5
    matched = [r["listing_id"] for r in rows if r["bm25_score"] < 0]
    unmatched = [r["listing_id"] for r in rows if r["bm25_score"] >= 1e8]
    assert set(matched) >= {"L1", "L2", "L3"}
    assert set(unmatched) <= {"L4", "L5"}
    # All matched rows should appear before any unmatched row.
    last_matched_idx = max(i for i, r in enumerate(rows) if r["bm25_score"] < 0)
    first_unmatched_idx = min(i for i, r in enumerate(rows) if r["bm25_score"] >= 1e8)
    assert last_matched_idx < first_unmatched_idx


# ---------- LEFT-join fallback ----------

def test_no_bm25_matches_still_returns_gate_rows(fts_db: Path) -> None:
    # A keyword that doesn't appear anywhere.
    rows = search_listings(
        fts_db,
        HardFilterParams(
            city=["Zurich"], bm25_keywords=["SomeNonexistentToken"], limit=10
        ),
    )
    ids = _ids(rows)
    assert "L1" in ids and "L2" in ids
    for row in rows:
        assert row["bm25_score"] >= 1e8


def test_empty_keywords_list_is_noop(fts_db: Path) -> None:
    # Empty list -> no FTS join, default sort by listing_id.
    rows = search_listings(fts_db, HardFilterParams(bm25_keywords=[], limit=10))
    assert _ids(rows) == ["L1", "L2", "L3", "L4", "L5"]
    # No bm25_score key in the rows when no FTS join.
    assert "bm25_score" not in rows[0]


def test_whitespace_only_keywords_are_dropped(fts_db: Path) -> None:
    rows = search_listings(
        fts_db, HardFilterParams(bm25_keywords=["   ", ""], limit=10)
    )
    # Behaves like no bm25_keywords at all.
    assert _ids(rows) == ["L1", "L2", "L3", "L4", "L5"]
    assert "bm25_score" not in rows[0]


def test_quotes_in_keyword_are_sanitized(fts_db: Path) -> None:
    # Double-quotes in keywords should be stripped so they don't break FTS syntax.
    rows = search_listings(
        fts_db,
        HardFilterParams(bm25_keywords=['Altbau"; DROP TABLE x'], limit=10),
    )
    assert "L1" in _ids(rows)
