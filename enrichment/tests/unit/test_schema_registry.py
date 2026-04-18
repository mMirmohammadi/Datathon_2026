"""Unit tests on the FIELDS registry — no DB required."""
from __future__ import annotations

import re

from enrichment.schema import FIELDS, INDEX_SQL, create_table_sql, validate_fields


def test_no_duplicate_field_names():
    names = [f.name for f in FIELDS]
    assert len(names) == len(set(names)), f"duplicate(s): {[n for n in names if names.count(n) > 1]}"


def test_validate_fields_accepts_current_registry():
    validate_fields()  # must not raise


def test_create_table_sql_mentions_every_field_four_times():
    sql = create_table_sql()
    for f in FIELDS:
        for suffix in ("filled", "source", "confidence", "raw"):
            col = f"{f.name}_{suffix}"
            assert col in sql, f"{col} missing from CREATE TABLE"


def test_create_table_uses_if_not_exists():
    assert "IF NOT EXISTS" in create_table_sql()


def test_indexes_only_reference_existing_columns():
    sql = create_table_sql()
    for idx in INDEX_SQL:
        m = re.search(r"ON listings_enriched\((\w+)\)", idx)
        assert m, f"index stmt has no ON listings_enriched(col): {idx}"
        col = m.group(1)
        assert col in sql, f"index refers to {col} but CREATE TABLE doesn't define it"


def test_all_listings_column_fields_have_that_attr_set():
    for f in FIELDS:
        if f.origin == "listings_column":
            assert f.listings_column, f"{f.name} missing listings_column"
        elif f.origin == "raw_json":
            assert f.raw_json_key, f"{f.name} missing raw_json_key"


def test_field_count_within_expected_range():
    # PLAN §Task says 48 fields; current registry has fewer because we defer
    # detected_lang and cross-source reconciliation to later passes.
    # Gate: between 30 and 60 — alert if someone adds/removes massively.
    assert 30 <= len(FIELDS) <= 60, f"FIELDS count {len(FIELDS)} outside expected band"
