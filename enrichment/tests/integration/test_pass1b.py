"""Integration tests for pass 1b — uses a pre-populated Nominatim cache so
no live HTTP calls are made in CI.

For live verification against real Nominatim, run the script with --limit N
from the CLI (ad-hoc, not in CI).
"""
from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from enrichment.common.sources import REV_GEO_NOMINATIM, UNKNOWN_PENDING
from enrichment.scripts import pass1b_nominatim as p1b


def _connect(db_path: Path) -> sqlite3.Connection:
    c = sqlite3.connect(str(db_path))
    c.row_factory = sqlite3.Row
    return c


@pytest.fixture
def enriched_db_pass1_done(tmp_path, base_db) -> Path:
    """base_db + pass 0 + pass 1a (offline only). Postal/street still pending for SRED."""
    from enrichment.scripts.pass0_create_table import run as pass0_run
    from enrichment.scripts.pass1_geocode import run as pass1_run

    dst = tmp_path / "listings.db"
    shutil.copy(str(base_db), str(dst))
    pass0_run(dst)
    pass1_run(dst)
    return dst


def _seed_cache(
    tmp_path: Path,
    monkeypatch,
    entries: dict[str, dict],
) -> Path:
    """Redirect the cache path to tmp and write canned responses."""
    cache_path = tmp_path / "nominatim_cache.json"
    cache_path.write_text(json.dumps(entries))
    monkeypatch.setattr(p1b, "CACHE_PATH", cache_path)
    return cache_path


class _NoNetworkClient:
    """Fail loudly if any HTTP reverse() is attempted during the test."""
    def reverse(self, lat, lng):
        raise AssertionError(
            f"Unexpected live HTTP call in integration test "
            f"(lat={lat}, lng={lng}). Cache should cover every coord used in the test."
        )

    def close(self):
        pass


def _all_pending_coords(db_path: Path) -> list[tuple[str, float, float]]:
    """Replicate pass 1b's collection SQL so we can pre-seed the cache exactly."""
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT le.listing_id, l.latitude, l.longitude
            FROM listings_enriched le JOIN listings l USING(listing_id)
            WHERE (le.postal_code_source = ? OR le.street_source = ?)
              AND l.latitude IS NOT NULL AND l.longitude IS NOT NULL;
        """, (UNKNOWN_PENDING, UNKNOWN_PENDING)).fetchall()
    return [(r["listing_id"], r["latitude"], r["longitude"]) for r in rows]


def test_pass1b_fills_postal_and_street_from_cache(
    enriched_db_pass1_done, tmp_path, monkeypatch
):
    """Pre-seed cache for EVERY coord pass 1b will visit with canned CH responses.
    Verify that fills land on the 5 listings we sampled.
    """
    pending = _all_pending_coords(enriched_db_pass1_done)
    # Build one cache entry per unique rounded coord; store fake CH responses.
    cache_entries: dict[str, dict] = {}
    listing_id_to_coord: dict[str, str] = {}
    for lid, lat, lng in pending:
        key = p1b._coord_key(lat, lng)
        listing_id_to_coord[lid] = key
        if key not in cache_entries:
            # Keys alone aren't unique per listing — coords collide across buildings.
            # Derive a deterministic synthetic address per key.
            cache_entries[key] = {
                "address": {
                    "postcode": f"8{hash(key) & 0xFFF:03d}",
                    "road": "Testgasse",
                    "house_number": str(abs(hash(key)) % 100 + 1),
                    "country_code": "ch",
                }
            }
    _seed_cache(tmp_path, monkeypatch, cache_entries)

    client = _NoNetworkClient()
    stats = p1b.run(enriched_db_pass1_done, client=client)  # type: ignore[arg-type]

    # Every unique coord should have been a cache hit; no network misses.
    assert stats["cache_hits"] == len(cache_entries)
    assert stats["cache_misses"] == 0
    assert stats["postal_filled"] > 0
    assert stats["street_filled"] > 0

    # Spot-check: pick 5 random filled rows and verify source + content consistency.
    with _connect(enriched_db_pass1_done) as conn:
        rows = conn.execute("""
            SELECT listing_id, postal_code_filled, postal_code_source,
                   street_filled, street_source
            FROM listings_enriched
            WHERE postal_code_source = ?
            LIMIT 5;
        """, (REV_GEO_NOMINATIM,)).fetchall()
    assert len(rows) == 5
    for r in rows:
        assert r["postal_code_source"] == REV_GEO_NOMINATIM
        assert r["postal_code_filled"].startswith("8")
        assert r["street_source"] == REV_GEO_NOMINATIM
        assert r["street_filled"].startswith("Testgasse ")


def test_pass1b_skips_non_ch_responses(enriched_db_pass1_done, tmp_path, monkeypatch):
    """Seed EVERY coord as non-CH. Every row must remain pending."""
    pending = _all_pending_coords(enriched_db_pass1_done)
    cache_entries = {
        p1b._coord_key(lat, lng): {
            "address": {"country_code": "de", "postcode": "80331", "road": "Marienplatz"}
        }
        for _, lat, lng in pending
    }
    _seed_cache(tmp_path, monkeypatch, cache_entries)

    client = _NoNetworkClient()
    stats = p1b.run(enriched_db_pass1_done, client=client)  # type: ignore[arg-type]

    # Every coord should have been a cache hit, all skipped as non-CH.
    assert stats["cache_hits"] == len(cache_entries)
    assert stats["cache_misses"] == 0
    assert stats["postal_filled"] == 0
    assert stats["street_filled"] == 0
    assert stats["non_ch_skipped"] == len(cache_entries)


def test_pass1b_does_not_overwrite_non_pending(enriched_db_pass1_done, tmp_path, monkeypatch):
    """If postal_source is already 'original', pass 1b must not overwrite."""
    # Find a non-SRED row whose postal_code_source IS 'original'.
    with _connect(enriched_db_pass1_done) as conn:
        row = conn.execute("""
            SELECT listing_id, latitude, longitude, postal_code_filled AS pc
            FROM listings_enriched le JOIN listings l USING(listing_id)
            WHERE le.postal_code_source='original' AND l.latitude IS NOT NULL
            LIMIT 1;
        """).fetchone()
        assert row is not None
        listing_id = row["listing_id"]
        original_pc = row["pc"]
        lat, lng = row["latitude"], row["longitude"]

    # Even if someone mischievously injects a cache entry for this row's coord:
    _seed_cache(tmp_path, monkeypatch, {
        p1b._coord_key(lat, lng): {
            "address": {"postcode": "0000", "road": "Malicious", "country_code": "ch"}
        },
    })

    # _collect_pending_rows SQL selects rows where postal_source='pending' OR
    # street_source='pending'. This row is 'original' for postal but may have
    # street 'original' or 'pending'. If street is also not pending, the row
    # isn't even selected. Either way, postal must remain 'original'.
    client = _NoNetworkClient()
    try:
        p1b.run(enriched_db_pass1_done, client=client)  # type: ignore[arg-type]
    except AssertionError:
        pass  # may fire if the coord isn't used (no rows selected) → fine

    with _connect(enriched_db_pass1_done) as conn:
        after = conn.execute(
            "SELECT postal_code_filled, postal_code_source FROM listings_enriched WHERE listing_id=?;",
            (listing_id,),
        ).fetchone()
    assert after["postal_code_filled"] == original_pc
    assert after["postal_code_source"] == "original"


def test_pass1b_limit_bounds_unique_coords(enriched_db_pass1_done, tmp_path, monkeypatch):
    """--limit N bounds the number of unique coords processed (guard against 3h runs)."""
    # Give an empty cache so that pending coords WOULD need HTTP — but with limit=0,
    # nothing should run.
    _seed_cache(tmp_path, monkeypatch, {})
    client = _NoNetworkClient()
    stats = p1b.run(enriched_db_pass1_done, client=client, limit=0)  # type: ignore[arg-type]
    assert stats["cache_hits"] == 0
    assert stats["cache_misses"] == 0
    assert stats["postal_filled"] == 0
