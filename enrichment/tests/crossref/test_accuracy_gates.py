"""Seven cross-reference accuracy gates for the full pipeline.

Runs enrich_all (skip_1b=True) against the real 25k-row DB once per module,
then asserts each gate from the approved plan §D.3.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from enrichment.common.sources import (
    DROPPED_BAD_DATA,
    FINAL_SOURCES,
    ORIGINAL,
    REV_GEO_OFFLINE,
    UNKNOWN,
)


def _connect(db_path: Path) -> sqlite3.Connection:
    c = sqlite3.connect(str(db_path))
    c.row_factory = sqlite3.Row
    return c


@pytest.fixture(scope="module")
def e2e_db(tmp_path_factory, base_db) -> Path:
    """One end-to-end run, shared across the module (saves ~3 min of setup time)."""
    from enrichment.scripts.enrich_all import run

    dst = tmp_path_factory.mktemp("e2e_accuracy") / "listings.db"
    shutil.copy(str(base_db), str(dst))
    run(dst, skip_pass1b=True)
    return dst


# =============================================================================
# Gate 1: Raw-city-vs-geocoded agreement
# =============================================================================

def test_crossref_raw_canton_agrees_with_geocoded(e2e_db: Path):
    """
    For every row that has both a structured `listings.canton` AND lat/lng, what
    does reverse_geocoder say? At least 97% of structured-positive rows must
    agree on canton; disagreements are legitimate data-quality signals and get
    dumped for human review.
    """
    import reverse_geocoder as rg
    from enrichment.common.cantons import admin1_to_canton_code

    with _connect(e2e_db) as conn:
        rows = conn.execute("""
            SELECT listing_id, canton_filled, latitude, longitude
            FROM listings_enriched le JOIN listings l USING(listing_id)
            WHERE le.canton_source = ?
              AND l.latitude IS NOT NULL AND l.longitude IS NOT NULL;
        """, (ORIGINAL,)).fetchall()

    if len(rows) < 100:
        pytest.skip(f"Not enough structured-canton rows to measure agreement ({len(rows)})")

    coords = [(r["latitude"], r["longitude"]) for r in rows]
    results = rg.search(coords, mode=2)
    agree = disagree = unmappable = 0
    for r, res in zip(rows, results, strict=True):
        predicted = admin1_to_canton_code(res.get("admin1", ""))
        if predicted is None:
            unmappable += 1
        elif predicted == r["canton_filled"]:
            agree += 1
        else:
            disagree += 1

    comparable = agree + disagree
    assert comparable > 0
    agreement_rate = agree / comparable
    print(
        f"\n[crossref/raw_canton] rows={len(rows)} agree={agree} disagree={disagree} "
        f"unmappable={unmappable} agreement_rate={agreement_rate:.3f}",
        flush=True,
    )
    assert agreement_rate >= 0.97, (
        f"Structured canton agrees with geocoded canton on only {agreement_rate:.1%} "
        f"of {comparable} rows — possible data drift or canton-map regression."
    )


# =============================================================================
# Gate 2: Price sanity round-trip
# =============================================================================

def test_crossref_price_sanity_band(e2e_db: Path):
    """Every non-UNKNOWN non-DROPPED price_filled must be an integer in [200, 50_000]."""
    with _connect(e2e_db) as conn:
        rows = conn.execute("""
            SELECT listing_id, price_filled, price_source
            FROM listings_enriched
            WHERE price_source NOT IN (?, ?);
        """, (UNKNOWN, DROPPED_BAD_DATA)).fetchall()
    offenders = []
    for r in rows:
        try:
            p = int(r["price_filled"])
        except (ValueError, TypeError):
            offenders.append((r["listing_id"], r["price_filled"], "non_integer"))
            continue
        if not (200 <= p <= 50_000):
            offenders.append((r["listing_id"], r["price_filled"], "out_of_band"))
    assert not offenders, (
        f"{len(offenders)} rows have a non-UNKNOWN price outside [200, 50000]; "
        f"first 5: {offenders[:5]}"
    )


# =============================================================================
# Gate 3: Language-match alignment
# =============================================================================

def test_crossref_cross_language_match_rate_under_2pct(e2e_db: Path):
    """
    Rows where detected_lang != matched pattern language should be < 2% of all
    feature_* text_regex_* matches. This guards against regex leakage (e.g. a
    German regex pattern happening to match French text).

    Note: we can't read `detected_lang` directly (it's not yet in the registry),
    but we CAN infer the dominant description language from title+description
    and compare against the source.
    """
    from enrichment.common.langdet import guess_lang, strip_html

    with _connect(e2e_db) as conn:
        rows = conn.execute("""
            SELECT l.title, l.description, le.feature_balcony_source
            FROM listings_enriched le JOIN listings l USING(listing_id)
            WHERE le.feature_balcony_source LIKE 'text_regex_%';
        """).fetchall()

    if len(rows) < 100:
        pytest.skip(f"Not enough text_regex balcony matches ({len(rows)})")

    cross_lang = 0
    for r in rows:
        text = strip_html(f"{r['title'] or ''}\n{r['description'] or ''}")
        detected = guess_lang(text)
        if detected in ("unk",):
            continue  # ambiguous — don't count against us
        lang_in_source = r["feature_balcony_source"].split("_")[-1]  # text_regex_de → de
        if detected != lang_in_source:
            cross_lang += 1
    rate = cross_lang / max(1, len(rows))
    print(
        f"\n[crossref/lang_align] total_matches={len(rows)} cross_lang={cross_lang} "
        f"rate={rate:.3%}",
        flush=True,
    )
    assert rate <= 0.02, (
        f"Cross-language match rate {rate:.1%} > 2%. Detected-lang is being ignored "
        f"by the extractor OR patterns are too promiscuous."
    )


# =============================================================================
# Gate 4: Status vocabulary
# =============================================================================

def test_crossref_status_vocabulary_is_closed(e2e_db: Path):
    """Every `status_filled` must be in {ACTIVE, INACTIVE, DELETED, UNKNOWN}."""
    allowed = {"ACTIVE", "INACTIVE", "DELETED", "UNKNOWN"}
    with _connect(e2e_db) as conn:
        rows = conn.execute(
            "SELECT DISTINCT status_filled FROM listings_enriched;"
        ).fetchall()
    observed = {r[0] for r in rows}
    unexpected = observed - allowed
    assert not unexpected, f"status_filled has unexpected values: {sorted(unexpected)}"


def test_crossref_sred_status_is_always_unknown(e2e_db: Path):
    """SRED never ships a status field — every SRED row must end with status='UNKNOWN'."""
    with _connect(e2e_db) as conn:
        bad = conn.execute("""
            SELECT COUNT(*) FROM listings_enriched le
            JOIN listings l USING(listing_id)
            WHERE l.scrape_source = 'SRED'
              AND le.status_filled != 'UNKNOWN';
        """).fetchone()[0]
    assert bad == 0, (
        f"{bad} SRED rows have a non-UNKNOWN status — fabrication! Check pass 0 + pass 3."
    )


# =============================================================================
# Gate 5: Nominatim offline rerun
# =============================================================================

def test_crossref_nominatim_offline_rerun(e2e_db: Path, monkeypatch):
    """After a full run with --skip-1b, running pass 1b AGAIN with no network
    (httpx blocked) must be a total no-op. This verifies that pass 1b correctly
    gates on UNKNOWN-pending — it shouldn't try to hit the network when no rows
    are pending.

    After enrich_all, pass 3 flipped postal/street from pending → UNKNOWN, so
    pass 1b should see zero pending rows.
    """
    import httpx

    from enrichment.scripts import pass1b_nominatim as p1b

    def _boom(*a, **k):
        raise AssertionError("pass 1b tried to hit the network after pass 3")

    monkeypatch.setattr(httpx.Client, "get", _boom)
    # Also point cache to an empty tmp file so no cache-hit smuggling.
    cache_path = e2e_db.parent / "empty_cache.json"
    cache_path.write_text("{}")
    monkeypatch.setattr(p1b, "CACHE_PATH", cache_path)

    stats = p1b.run(e2e_db)
    assert stats["pending_in"] == 0, (
        f"Pass 1b still sees {stats['pending_in']} pending rows after the full pipeline. "
        "Pass 3 sentinel-fill should have flipped all postal/street to UNKNOWN."
    )
    assert stats["cache_hits"] == 0
    assert stats["cache_misses"] == 0


# =============================================================================
# Gate 6: Byte-identical idempotency
# =============================================================================

def _hash_enriched(db_path: Path) -> str:
    """SHA-256 of every listings_enriched row (excluding enriched_at), ordered."""
    with _connect(db_path) as conn:
        cols = [
            r[1] for r in conn.execute("PRAGMA table_info(listings_enriched);").fetchall()
            if r[1] != "enriched_at"
        ]
        h = hashlib.sha256()
        for row in conn.execute(
            f"SELECT {', '.join(cols)} FROM listings_enriched ORDER BY listing_id;"
        ):
            h.update(repr(tuple(row)).encode("utf-8"))
        return h.hexdigest()


def test_crossref_byte_identical_idempotency(e2e_db: Path):
    """Running enrich_all a second time must not change the DB bytes
    (excluding the enriched_at timestamp)."""
    from enrichment.scripts.enrich_all import run

    h1 = _hash_enriched(e2e_db)
    run(e2e_db, skip_pass1b=True)
    h2 = _hash_enriched(e2e_db)
    assert h1 == h2, (
        f"Enrich_all is NOT byte-idempotent. First-run hash: {h1}, second-run hash: {h2}. "
        f"Something is re-writing values on rerun — check the UNKNOWN-pending gates."
    )


# =============================================================================
# Gate 7: Report integrity
# =============================================================================

def test_crossref_generate_report_writes_expected_artifacts(e2e_db: Path, tmp_path, monkeypatch):
    """generate_report must produce REPORT.md with 8 section headers and
    fill_stats.json with the 7 expected top-level keys."""
    from enrichment.scripts import generate_report as gen

    report_md = tmp_path / "REPORT.md"
    stats_json = tmp_path / "data" / "fill_stats.json"
    dropped_json = tmp_path / "data" / "dropped_rows.json"
    disag_json = tmp_path / "data" / "disagreements.json"

    monkeypatch.setattr(gen, "REPORT_MD", report_md)
    monkeypatch.setattr(gen, "FILL_STATS_JSON", stats_json)
    monkeypatch.setattr(gen, "DROPPED_ROWS_JSON", dropped_json)
    monkeypatch.setattr(gen, "DISAGREEMENTS_JSON", disag_json)

    gen.run(e2e_db, include_disagreements=False)  # skip rg for speed

    # REPORT.md headers
    md_text = report_md.read_text()
    for header in gen.EXPECTED_SECTIONS:
        assert header in md_text, f"REPORT.md missing section: {header}"

    # fill_stats.json keys
    with stats_json.open() as f:
        stats = json.load(f)
    for key in gen.EXPECTED_STATS_KEYS:
        assert key in stats, f"fill_stats.json missing key: {key}"

    # Sidecars exist & parseable
    with dropped_json.open() as f:
        dropped = json.load(f)
    assert isinstance(dropped, list)
    with disag_json.open() as f:
        disag = json.load(f)
    assert isinstance(disag, list)
