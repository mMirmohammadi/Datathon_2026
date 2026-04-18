"""Unit tests for the Nominatim JSON cache (roundtrip + atomicity + graceful decoding)."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from enrichment.scripts import pass1b_nominatim as p1b


def test_cache_roundtrip(tmp_path, monkeypatch):
    cache_path = tmp_path / "cache.json"
    monkeypatch.setattr(p1b, "CACHE_PATH", cache_path)
    data = {"47.37,8.54": {"address": {"postcode": "8001"}}}
    p1b._save_cache(data)
    loaded = p1b._load_cache()
    assert loaded == data


def test_empty_load_when_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(p1b, "CACHE_PATH", tmp_path / "does_not_exist.json")
    assert p1b._load_cache() == {}


def test_save_is_atomic(tmp_path, monkeypatch):
    """Atomic rename: a crash during write must not leave a corrupt cache file.
    We simulate this by ensuring no .tmp file lingers after a successful save."""
    cache_path = tmp_path / "cache.json"
    monkeypatch.setattr(p1b, "CACHE_PATH", cache_path)
    p1b._save_cache({"k": "v"})
    assert cache_path.exists()
    assert not cache_path.with_suffix(".json.tmp").exists()


def test_load_invalid_json_quarantines_file_and_returns_empty(tmp_path, monkeypatch, capsys):
    """Corrupt JSON → quarantine original (to avoid overwrite) + return empty."""
    cache_path = tmp_path / "cache.json"
    cache_path.write_text("{ this is not json")
    monkeypatch.setattr(p1b, "CACHE_PATH", cache_path)
    result = p1b._load_cache()
    assert result == {}
    # The corrupt cache file must have been renamed to a .corrupt.<ts> sibling.
    assert not cache_path.exists(), (
        "corrupt cache must be quarantined (moved aside) — otherwise next _save_cache overwrites it"
    )
    corrupts = list(tmp_path.glob("cache.json.corrupt.*"))
    assert len(corrupts) == 1, f"expected 1 quarantined cache, got {corrupts}"
    captured = capsys.readouterr()
    assert "[WARN]" in captured.out
    assert "corrupt" in captured.out
    assert "json_decode_error" in captured.out


def test_load_non_dict_quarantines_file_and_returns_empty(tmp_path, monkeypatch, capsys):
    cache_path = tmp_path / "cache.json"
    cache_path.write_text('["list", "not", "dict"]')
    monkeypatch.setattr(p1b, "CACHE_PATH", cache_path)
    result = p1b._load_cache()
    assert result == {}
    assert not cache_path.exists(), "non-dict cache must be quarantined"
    corrupts = list(tmp_path.glob("cache.json.corrupt.*"))
    assert len(corrupts) == 1
    captured = capsys.readouterr()
    assert "[WARN]" in captured.out
    assert "not_a_dict" in captured.out


def test_cache_survives_unicode_roundtrip(tmp_path, monkeypatch):
    cache_path = tmp_path / "cache.json"
    monkeypatch.setattr(p1b, "CACHE_PATH", cache_path)
    data = {
        "47.37,8.54": {"address": {"city": "Zürich", "road": "Bahnhofstraße", "postcode": "8001"}},
    }
    p1b._save_cache(data)
    loaded = p1b._load_cache()
    assert loaded["47.37,8.54"]["address"]["city"] == "Zürich"
    assert loaded["47.37,8.54"]["address"]["road"] == "Bahnhofstraße"
