"""Integration test for pass 2's negated-feature WRITE path.

Agent 2 flagged that `stats["feature_*_negated"]` / `_filled='0'` / `_raw='NEG:…'`
write path was not covered end-to-end. This test seeds a synthetic listing
with a clearly negated feature and runs pass 2 against the DB.
"""
from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest


def _connect(db_path: Path) -> sqlite3.Connection:
    c = sqlite3.connect(str(db_path))
    c.row_factory = sqlite3.Row
    return c


@pytest.fixture
def db_with_synthetic_sred_row(base_db, tmp_path) -> Path:
    """Copy base_db and append a synthetic SRED listing with 'ohne Balkon' in desc."""
    from enrichment.scripts.pass0_create_table import run as pass0_run

    dst = tmp_path / "listings.db"
    shutil.copy(str(base_db), str(dst))

    # Append a synthetic listing. Pass 0 will ingest it.
    synth_id = "__SYNTH_NEGATED_BALCONY__"
    with sqlite3.connect(str(dst)) as conn:
        # See what columns listings has so we can INSERT safely.
        cols = [r[1] for r in conn.execute("PRAGMA table_info(listings);").fetchall()]
        # Populate only the columns we need; leave the rest NULL.
        required_non_null = {"listing_id": synth_id, "title": "Synthetic",
                             "features_json": "[]", "raw_json": "{}"}
        placeholders = ",".join("?" * len(cols))
        values = []
        for c in cols:
            if c in required_non_null:
                values.append(required_non_null[c])
            elif c == "scrape_source":
                values.append("SRED")
            elif c == "description":
                values.append("Helle Wohnung ohne Balkon, dafür mit Terrasse — not-a-balcony at all")
            elif c == "latitude":
                values.append(47.3697)
            elif c == "longitude":
                values.append(8.5386)
            elif c == "price":
                values.append(2500)
            elif c == "rooms":
                values.append(3.0)
            else:
                values.append(None)
        conn.execute(f"INSERT OR REPLACE INTO listings ({','.join(cols)}) VALUES ({placeholders});", values)
        conn.commit()

    pass0_run(dst)
    return dst


def test_pass2_writes_negated_feature_as_zero_with_neg_raw(db_with_synthetic_sred_row: Path):
    """Description says 'ohne Balkon' — pass 2 must write feature_balcony_filled='0'
    with a text_regex_* source and raw starting with 'NEG:'.

    Note: there's also a positive 'Terrasse' mention in the same text. Per our
    patterns, 'Terrasse' is listed as a balcony synonym. The extractor's
    `find_first_match` returns the FIRST regex hit (which could be either
    'Balkon' or 'Terrasse'). If 'Balkon' hits first and is negated, the negated
    write path fires. We assert that at minimum the write happened.
    """
    from enrichment.scripts.pass2_text_extract import run as pass2_run

    pass2_run(db_with_synthetic_sred_row)

    synth_id = "__SYNTH_NEGATED_BALCONY__"
    with _connect(db_with_synthetic_sred_row) as conn:
        row = conn.execute(
            """SELECT feature_balcony_filled, feature_balcony_source,
                      feature_balcony_confidence, feature_balcony_raw
               FROM listings_enriched WHERE listing_id = ?;""",
            (synth_id,),
        ).fetchone()

    assert row is not None, "synthetic row wasn't inserted into listings_enriched"
    # Source must be text_regex_*
    assert row["feature_balcony_source"].startswith("text_regex_"), (
        f"expected text_regex_* source, got {row['feature_balcony_source']!r}"
    )
    # Either:
    #  (a) 'Balkon' hit first and was negated → filled='0', raw starts with 'NEG:'
    #  (b) 'Terrasse' hit first and was not negated → filled='1', raw='Terrasse'
    # Both are valid given the pattern order; we assert that the branch that DID
    # fire is self-consistent.
    if row["feature_balcony_filled"] == "0":
        assert row["feature_balcony_raw"].startswith("NEG:"), (
            f"feature_balcony_filled='0' requires raw to start with 'NEG:', got {row['feature_balcony_raw']!r}"
        )
        assert 0.0 < row["feature_balcony_confidence"] <= 0.5, (
            f"negated feature should have low but positive confidence, got {row['feature_balcony_confidence']}"
        )
    else:
        assert row["feature_balcony_filled"] == "1"
        assert not row["feature_balcony_raw"].startswith("NEG:")


def test_pass2_writes_unambiguously_negated_feature(base_db, tmp_path):
    """Pure negation test: use a description that ONLY mentions 'Lift' (no positive
    synonym). 'kein Lift' must trigger the negated write with filled='0'."""
    from enrichment.scripts.pass0_create_table import run as pass0_run
    from enrichment.scripts.pass2_text_extract import run as pass2_run

    dst = tmp_path / "listings.db"
    shutil.copy(str(base_db), str(dst))

    synth_id = "__SYNTH_NO_LIFT__"
    with sqlite3.connect(str(dst)) as conn:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(listings);").fetchall()]
        values = []
        for c in cols:
            if c == "listing_id":
                values.append(synth_id)
            elif c == "title":
                values.append("Charming walkup")
            elif c == "features_json":
                values.append("[]")
            elif c == "raw_json":
                values.append("{}")
            elif c == "scrape_source":
                values.append("SRED")
            elif c == "description":
                values.append("Charmante Altbauwohnung, leider kein Lift vorhanden.")
            elif c == "latitude":
                values.append(47.3697)
            elif c == "longitude":
                values.append(8.5386)
            elif c == "price":
                values.append(2500)
            elif c == "rooms":
                values.append(3.0)
            else:
                values.append(None)
        conn.execute(
            f"INSERT OR REPLACE INTO listings ({','.join(cols)}) VALUES ({','.join('?' * len(cols))});",
            values,
        )
        conn.commit()

    pass0_run(dst)
    pass2_run(dst)

    with _connect(dst) as conn:
        row = conn.execute(
            """SELECT feature_elevator_filled, feature_elevator_source,
                      feature_elevator_confidence, feature_elevator_raw
               FROM listings_enriched WHERE listing_id = ?;""",
            (synth_id,),
        ).fetchone()

    assert row["feature_elevator_filled"] == "0", (
        f"'kein Lift' must map to feature_elevator='0', got {row['feature_elevator_filled']!r}"
    )
    assert row["feature_elevator_source"].startswith("text_regex_")
    assert row["feature_elevator_raw"].startswith("NEG:"), (
        f"negated raw must start with 'NEG:', got {row['feature_elevator_raw']!r}"
    )
    assert row["feature_elevator_confidence"] > 0.0


def test_pass2_source_for_warns_on_unknown_lang(capsys):
    """CLAUDE.md §5: the defensive fallback in _source_for must log a [WARN]."""
    from enrichment.scripts.pass2_text_extract import _source_for

    result = _source_for("qq")
    assert result == "text_regex_en"
    captured = capsys.readouterr()
    assert "[WARN]" in captured.out
    assert "_source_for" in captured.out


def test_pass2_normalize_phone_warns_on_empty_groups(capsys):
    """The phone fallback path (capture groups empty) must [WARN]."""
    from enrichment.common.text_extract import ExtractionHit
    from enrichment.scripts.pass2_text_extract import _normalize_phone

    hit = ExtractionHit(
        value="044 not a full number",
        groups=("44", "", "", ""),   # three empty groups — falls back
        lang_used="de",
        pattern="<stub>",
        match_start=0,
        negated=False,
    )
    result = _normalize_phone(hit)
    assert result == "044 not a full number"
    captured = capsys.readouterr()
    assert "[WARN]" in captured.out
    assert "_normalize_phone" in captured.out


def test_drop_bad_rows_preserves_original_value_in_raw(base_db, tmp_path):
    """drop_bad_rows must store the pre-drop value in `_raw` for audit."""
    from enrichment.scripts.drop_bad_rows import run as drop_run
    from enrichment.scripts.pass0_create_table import run as pass0_run

    dst = tmp_path / "listings.db"
    shutil.copy(str(base_db), str(dst))
    pass0_run(dst)
    drop_run(dst)

    with _connect(dst) as conn:
        row = conn.execute("""
            SELECT le.price_raw, l.price
            FROM listings_enriched le
            JOIN listings l USING(listing_id)
            WHERE le.price_source='DROPPED_bad_data'
              AND le.price_raw LIKE 'price_below_200_chf%'
            LIMIT 1;
        """).fetchone()
    assert row is not None, "no price_below_200 drops in fixture"
    # raw must embed the original pre-drop value
    assert "original_was=" in row["price_raw"], (
        f"expected 'original_was=...' in raw, got {row['price_raw']!r}"
    )
    assert str(row["price"]) in row["price_raw"], (
        f"raw must contain the actual original price {row['price']}, got {row['price_raw']!r}"
    )
