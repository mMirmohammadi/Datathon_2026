# `enrichment/` — Layer 1: null-fill pipeline

Self-contained multi-pass pipeline that turns the raw ~97%-null feature columns in [`listings`](../data/listings.db) into a fully populated side table `listings_enriched` — in which **every field is either a recovered real value or an explicit `UNKNOWN` sentinel**. No silent fallbacks, no fabrication.

- **25,546** rows enriched (100%) · perfect 1:1 bijection with `listings`
- **41** fields × 4 provenance cols = **166** columns in `listings_enriched`
- **307** tests passing in 279 s
- Four external services used — deterministic offline where possible, rate-limited HTTP where necessary, GPT only for the residual hard cases.

---

## Guarantees

1. **Zero NULLs** in any `{field}_filled` column of `listings_enriched` for every row in `listings`.
2. **Zero `UNKNOWN-pending`** source values after the full pipeline runs.
3. Every `_filled` value carries a `_source`, `_confidence`, `_raw` triple so the ranker can weight or reject low-confidence fills at query time.
4. Anything that couldn't be recovered is **explicitly** sentinel-filled — never defaulted to a plausible-looking value.

---

## Pipeline

```mermaid
flowchart TB
    LST[("listings<br/>25,546 rows · ~97% null features")]

    subgraph P0["Pass 0 — bootstrap"]
        P0A["pass0_create_table.py<br/>CREATE listings_enriched<br/>backfill 'original' from raw"]
        P0B["drop_bad_rows.py<br/>mark price<200, price>50k,<br/>rooms=0 as DROPPED_bad_data"]
    end

    subgraph P1["Pass 1 — geo"]
        P1A["pass1_geocode.py<br/>reverse_geocoder (offline KD-tree)<br/>lat/lng → city + canton"]
        P1B["pass1b_nominatim.py<br/>Nominatim 1 req/s<br/>→ postal_code + street"]
        P1BX["pass1b_backfill_canton.py<br/>canton from cached Nominatim"]
        P1D["pass1d_canton_topup.py<br/>PLZ-majority-vote fallback"]
        P1E["pass1e_canton_gpt_nano.py<br/>gpt-5.4-nano residual ~64 rows"]
        P1EV["pass1e_verify.py<br/>audit GPT vs Nominatim"]
    end

    subgraph P2["Pass 2 — description extraction"]
        P2R["pass2_text_extract.py<br/>DE/FR/IT/EN regex + YAML patterns"]
        P2G["pass2_gpt_extract.py (default)<br/>gpt-5.4-mini Structured Outputs<br/>~$50 for 25k listings"]
        P2B["pass2b_bathroom_cellar_kitchen.py<br/>gpt-5.4-nano<br/>bathroom/cellar/shared-amenity"]
    end

    subgraph P4["Pass 4 — landmarks (cache-only)"]
        P4["pass4_landmark_mining.py<br/>gpt-5.4-nano mines mentions<br/>→ cache (feeds ranking/)"]
    end

    subgraph P3["Pass 3 — sentinel"]
        P3A["pass3_sentinel_fill.py<br/>UNKNOWN-pending → UNKNOWN"]
        P3B["assert_no_nulls<br/>post-condition check"]
    end

    LST --> P0A --> P0B
    P0B --> P1A --> P1B --> P1BX --> P1D --> P1E --> P1EV
    P1EV --> P2R & P2G --> P2B
    P2B --> P4 --> P3A --> P3B
    P3B --> ENR[("listings_enriched<br/>25,546 rows · 166 cols<br/>ZERO nulls")]

    classDef store fill:#f7f7ff,stroke:#446
    classDef done fill:#efe,stroke:#363
    class LST,ENR store
    class P3B done
```

---

## Passes in detail

All scripts live in [`enrichment/scripts/`](scripts/). Each is idempotent — safe to re-run.

| # | Script | Source | External API | Output columns |
| --- | --- | --- | --- | --- |
| 0a | [`pass0_create_table.py`](scripts/pass0_create_table.py) | `original` | — | all 41 fields backfilled from raw |
| 0b | [`drop_bad_rows.py`](scripts/drop_bad_rows.py) | `DROPPED_bad_data` | — | rows with price/rooms sentinels marked |
| 1a | [`pass1_geocode.py`](scripts/pass1_geocode.py) | `rev_geo_offline` | `reverse_geocoder` (offline GeoNames KD-tree) | `city_filled` (0.90), `canton_filled` (0.95) |
| 1b | [`pass1b_nominatim.py`](scripts/pass1b_nominatim.py) | `nominatim_reverse` | **Nominatim** HTTP, hard 1 req/s | `postal_code_filled` (0.85), `street_filled` (0.75) |
| 1b-bf | [`pass1b_backfill_canton.py`](scripts/pass1b_backfill_canton.py) | `nominatim_reverse` | — (cache only) | `canton_filled` from ISO3166-2-lvl4 in cached responses |
| 1d | [`pass1d_canton_topup.py`](scripts/pass1d_canton_topup.py) | `rev_geo_offline_plz_vote` | — | canton via PLZ-prefix majority vote |
| 1e | [`pass1e_canton_gpt_nano.py`](scripts/pass1e_canton_gpt_nano.py) | `gpt_5_4_nano` | **OpenAI gpt-5.4-nano** | canton for the last ~64 residual rows |
| 1e-v | [`pass1e_verify.py`](scripts/pass1e_verify.py) | audit only | Nominatim + OpenAI | cross-check — no DB writes |
| 2 (regex) | [`pass2_text_extract.py`](scripts/pass2_text_extract.py) | `text_regex_{lang}` | — | 12 features + year_built, floor, area, available_from, agency_* |
| 2 (GPT) | [`pass2_gpt_extract.py`](scripts/pass2_gpt_extract.py) | `text_gpt_5_4` | **OpenAI gpt-5.4-mini** Structured Outputs, 16 concurrent async | same 12 targets as regex pass |
| 2b | [`pass2b_bathroom_cellar_kitchen.py`](scripts/pass2b_bathroom_cellar_kitchen.py) | `text_gpt_5_4_nano_pass2b` | **OpenAI gpt-5.4-nano**, 10 concurrent async | `bathroom_count`, `bathroom_shared`, `has_cellar`, `kitchen_shared` |
| 4 | [`pass4_landmark_mining.py`](scripts/pass4_landmark_mining.py) | cache only | **OpenAI gpt-5.4-nano** | writes `enrichment/data/cache/gpt_landmark_mining.jsonl` — consumed by [`ranking/scripts/t1_landmarks_aggregate.py`](../ranking/scripts/t1_landmarks_aggregate.py) |
| 3 | [`pass3_sentinel_fill.py`](scripts/pass3_sentinel_fill.py) | `UNKNOWN` | — | every remaining `UNKNOWN-pending` promoted to `UNKNOWN` |

---

## Pass 2: regex vs GPT

Both passes target the same 12 features + 6 metadata fields. GPT is the **default** because it handles multilingual paraphrases the regex catalogue misses.

| | Pass 2 GPT (default) | Pass 2 regex (legacy) |
| --- | --- | --- |
| Model / logic | OpenAI gpt-5.4-mini, Structured Outputs | DE/FR/IT/EN regex + YAML patterns + ±5-token NegEx |
| Cost | ~$50 / 25k listings | free |
| Accuracy (500-row audit) | email 100%, phone 99%, year_built 94.7%, area 99% | varies by feature & language |
| Languages | native DE/FR/IT/EN | per-language pattern sets in [`patterns/`](patterns/) |
| Idempotent | yes (cache at [`data/cache/gpt_pass2.jsonl`](data/cache/)) | yes |

Both write to the same `{field}_filled` / `_source` / `_confidence` / `_raw` quartet — the `_source` value lets the ranker tell them apart.

---

## Schema — 41 fields × 4 provenance columns

From [`schema.py:33-85`](schema.py). Every field generates 4 columns:

```python
{name}_filled      TEXT NOT NULL   # real value or literal 'UNKNOWN'
{name}_source      TEXT NOT NULL   # enum: original | rev_geo_offline |
                                   #       text_regex_de | text_gpt_5_4 |
                                   #       UNKNOWN | DROPPED_bad_data | …
{name}_confidence  REAL NOT NULL   # ∈ [0.0, 1.0]
{name}_raw         TEXT            # nullable audit snippet
```

**Field categories:**

| Kind | Count | Examples |
| --- | ---: | --- |
| `listings_column` (mirrors raw source) | 31 | title, city, postal_code, price_chf, rooms, area_m2, latitude, … |
| `raw_json` (parsed from nested JSON) | 6 | agency_name, agency_phone, agency_email, … |
| `extraction_only` (not in raw, only derived) | 4 | bathroom_count, bathroom_shared, has_cellar, kitchen_shared |
| **Total** | **41** | → 41 × 4 + 2 (listing_id, enriched_at) = **166 columns** |

---

## Patterns

9 YAML files in [`enrichment/patterns/`](patterns/):

```text
features.yaml           floor.yaml            available_from.yaml
negation.yaml           year_built.yaml       agency_name.yaml
area.yaml               agency_phone.yaml     agency_email.yaml
```

Each YAML maps language → pattern → `(field, polarity, confidence)`. Negation is handled via a ±5-token NegEx window defined in `negation.yaml`.

---

## Tests — 307 passing

24 test files across [`enrichment/tests/`](tests/):

- **12 unit** — cantons, confidence, langdet, NegEx, Nominatim cache, pass1 guards, pass2 extensions, provenance, schema registry
- **8 integration** — drop_bad_rows, orchestrator, pass0, pass1, pass1b, pass2, pass2 negated writes, pass3
- **3 crossref / accuracy gates** — landmark geocoding, regex vs structured, accuracy gates
- `conftest.py`

Run: `uv run pytest enrichment/tests -q`

---

## Coverage (final)

From [`FINAL_REPORT.md`](FINAL_REPORT.md), line 28-39. Cell-level breakdown across all 25,546 rows × 37 historical fields (the 4 `extraction_only` fields were added post-report):

| Source | Cells | % |
| --- | ---: | ---: |
| `original` | 392,327 | 41.5 |
| `UNKNOWN` | 491,993 | 52.1 |
| `text_regex_de` | 27,274 | 2.9 |
| `rev_geo_offline` | 22,048 | 2.3 |
| `text_regex_fr` | 6,962 | 0.7 |
| `DROPPED_bad_data` | 3,630 | 0.4 |
| `text_regex_it` | 800 | 0.1 |
| `text_regex_en` | 168 | 0.0 |

The `UNKNOWN` share is intentional — fields like `agency_phone` are absent from most listings and we refuse to fabricate them. Ranker query-time logic treats `UNKNOWN` as "no signal", not "feature absent".

---

## Rebuild

```bash
# offline passes only (no API keys needed)
uv run python enrichment/scripts/pass0_create_table.py
uv run python enrichment/scripts/drop_bad_rows.py
uv run python enrichment/scripts/pass1_geocode.py
uv run python enrichment/scripts/pass2_text_extract.py    # regex legacy fallback

# HTTP + GPT passes (need OPENAI_API_KEY + NOMINATIM_CONTACT_EMAIL)
uv run python enrichment/scripts/pass1b_nominatim.py
uv run python enrichment/scripts/pass1b_backfill_canton.py
uv run python enrichment/scripts/pass1d_canton_topup.py
uv run python enrichment/scripts/pass1e_canton_gpt_nano.py
uv run python enrichment/scripts/pass2_gpt_extract.py     # recommended default
uv run python enrichment/scripts/pass2b_bathroom_cellar_kitchen.py
uv run python enrichment/scripts/pass4_landmark_mining.py # feeds ranking/

# finalize
uv run python enrichment/scripts/pass3_sentinel_fill.py
uv run python enrichment/scripts/assert_no_nulls.py
```

See [`docs/DEVELOPMENT.md`](../docs/DEVELOPMENT.md) for data-source prep.
