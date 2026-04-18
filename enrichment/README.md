# `enrichment/` — Null-fill pipeline for Datathon 2026

Self-contained three-pass enrichment task (plus pass 0 + a bad-row filter +
pass 3 sentinel fill). Produces a `listings_enriched` side table in which
every field is either a recovered real value or an explicit `UNKNOWN`
sentinel — **no silent fallbacks, no fabrication**.

## What this folder guarantees

1. **Zero NULLs** in every `{field}_filled` column of `listings_enriched` for
   every listing in `listings`.
2. **Zero `UNKNOWN-pending`** sources left after the full pipeline runs.
3. Every `_filled` value carries a `_source`, `_confidence`, and `_raw` triple
   so the ranker can weight or reject low-confidence fills at query time.
4. Anything that couldn't be recovered is explicitly sentinel-filled — never
   defaulted to a plausible-looking value.

## Pipeline

```
listings  →  pass 0  (CREATE listings_enriched + backfill 'original' values from raw columns)
          →  drop_bad_rows  (mark price<200, price>50k, rooms=0 as DROPPED_bad_data)
          →  pass 1a  (reverse_geocoder offline → city + canton, 26-canton map)
          →  pass 1b  (Nominatim → postal_code + street, 1 req/s, disk-cached)
          →  pass 2   (multilingual regex → 12 feature flags, year_built, floor, area,
                       available_from, agency_phone, agency_email, agency_name)
          →  pass 3   (UNKNOWN-pending → UNKNOWN, registry-drift guard)
          →  assert_no_nulls post-condition
```

## Layout

```
enrichment/
  schema.py                  ← FIELDS registry + CREATE TABLE generator
  common/
    db.py                    ← sqlite3 connect helper
    sources.py               ← Source enum + VALID_SOURCES / FINAL_SOURCES sets
    provenance.py            ← write_field() + coerce_to_filled() with validation
    confidence.py            ← compute_confidence(base, lang_match, negated)
    text_extract.py          ← find_first_match() + is_negated() (3-token lookback)
    langdet.py               ← strip_html + guess_lang (self-contained, no matplotlib)
    cantons.py               ← reverse_geocoder admin1 → 2-letter ISO canton code
  patterns/                  ← YAML regex registry per field, per language
    features.yaml            ← 12 features × {de, fr, it, en}
    year_built.yaml
    floor.yaml               ← ground / basement / numeric sub-patterns
    area.yaml                ← m² with 10–500 validation
    available_from.yaml      ← immediate / ISO / European date sub-patterns
    agency_phone.yaml        ← Swiss +41 format
    agency_email.yaml        ← RFC-5322-lite + TLD allowlist
    agency_name.yaml         ← derived from agency_email
    negation.yaml            ← per-language negation tokens + 3-token lookback
  scripts/
    pass0_create_table.py    ← CREATE + backfill 'original'
    drop_bad_rows.py         ← price/rooms sanity drops
    pass1_geocode.py         ← offline reverse_geocoder
    pass1b_nominatim.py      ← rate-limited httpx + JSON cache + retries
    pass2_text_extract.py    ← multilingual regex over descriptions
    pass3_sentinel_fill.py   ← UNKNOWN-pending → UNKNOWN with drift guard
    enrich_all.py            ← orchestrator with assert_no_nulls post-condition
    generate_report.py       ← REPORT.md + fill_stats.json + dropped_rows.json + disagreements.json
  data/
    cache/nominatim.json     ← Nominatim response cache (write-through)
    fill_stats.json          ← generated: machine-readable stats
    dropped_rows.json        ← generated: every DROPPED_bad_data listing_id
    disagreements.json       ← generated: structured-vs-geocoded canton mismatches
  REPORT.md                  ← generated audit (mirrors analysis/REPORT.md shape)
  tests/
    conftest.py              ← session-scoped base_db + per-test enriched_db_pass0
    unit/                    ← ~160 unit tests (regex × 4 langs, negation, cache, schema, …)
    integration/             ← full-DB tests per pass (pass0 / pass1 / pass1b / pass2 / pass3 / orchestrator / drop_bad_rows)
    crossref/                ← accuracy gates (landmark truth, regex-vs-structured, …)
      fixtures/
        landmark_truths.yaml ← 26 hand-labeled (lat, lng) → canton pairs
```

## Running

All commands assume Docker is up (`docker compose up -d api`).

### Full pipeline

```bash
# Without Nominatim (~2 min, fast, no network)
docker compose exec api uv run python -m enrichment.scripts.enrich_all \
    --db /data/listings.db --skip-1b

# With Nominatim (production; slow because of the 1 req/s rate limit)
docker compose exec api uv run python -m enrichment.scripts.enrich_all \
    --db /data/listings.db

# With Nominatim, bounded (good for smoke testing pass 1b in CI-like conditions)
docker compose exec api uv run python -m enrichment.scripts.enrich_all \
    --db /data/listings.db --pass1b-limit 100
```

### Individual passes

```bash
docker compose exec api uv run python -m enrichment.scripts.pass0_create_table --db /data/listings.db
docker compose exec api uv run python -m enrichment.scripts.drop_bad_rows     --db /data/listings.db
docker compose exec api uv run python -m enrichment.scripts.pass1_geocode     --db /data/listings.db
docker compose exec api uv run python -m enrichment.scripts.pass1b_nominatim  --db /data/listings.db --limit 100
docker compose exec api uv run python -m enrichment.scripts.pass2_text_extract --db /data/listings.db
docker compose exec api uv run python -m enrichment.scripts.pass3_sentinel_fill --db /data/listings.db
```

### Report

```bash
docker compose exec api uv run python -m enrichment.scripts.generate_report --db /data/listings.db
# Faster (skips the live reverse_geocoder canton-disagreement scan):
docker compose exec api uv run python -m enrichment.scripts.generate_report --db /data/listings.db --no-disagreements
```

### Tests

```bash
# All tests (~2 min, offline)
docker compose exec api uv run pytest enrichment/tests/ -v

# Just unit tests (instant)
docker compose exec api uv run pytest enrichment/tests/unit/ -v

# Just crossref accuracy gates
docker compose exec api uv run pytest enrichment/tests/crossref/ -v
```

## Environment variables

| Var | Default | Purpose |
|---|---|---|
| `NOMINATIM_BASE_URL` | `https://nominatim.openstreetmap.org` | Point at a self-hosted instance to bypass the 1 req/s ToS limit. |
| `NOMINATIM_CONTACT_EMAIL` | `datathon2026-robin@example.invalid` | Required by Nominatim policy. Set to a real contact for production. |
| `NOMINATIM_RATE_SEC` | `1.0` | Seconds between requests. Clamped to ≥ 1.0 even if set lower (ToS). |

## Policy & safety rules

1. **CLAUDE.md §5 — no silent fallbacks.** Every fallback path emits a
   `[WARN] <context>: expected=... got=... fallback=...` line. No `except: pass`
   that eats errors.
2. **Never fabricate.** If a value can't be recovered, it becomes `UNKNOWN`
   with `_source='UNKNOWN'` and `_confidence=0.0`. The ranker must not
   surface UNKNOWN values as positive filter hits.
3. **Registry-driven schema.** Adding a field means editing `schema.FIELDS`
   once. Pass 3 `raise`s if the DB has `_filled` columns not in the
   registry — no silent sentinel-fills of unknown columns.
4. **Nominatim rate limit is non-negotiable.** The CLI clamps any rate below
   1 s to 1 s and logs a `[WARN]`. Going faster risks a ban for the whole IP.
5. **No downstream mutation to `listings`.** The enriched side table keeps
   the harness-owned `listings` schema untouched (`_schema_matches()` guard
   in `app/harness/bootstrap.py` would otherwise trip on schema drift).

## What's out of scope

The broader ranking-focused enrichment at the repo-root `Further Data Plan.md`
(SBB GTFS routing, OSM POIs, CLIP image scoring, Claude vision, embeddings,
SwissTopo DEM, etc.) is **not** part of this folder's responsibility. That's
for the ranker build, not the null-fill contract.
