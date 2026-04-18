# Datathon 2026 — Robin Real-Estate Search: Implementation Plan

## Context

**The challenge (from `challenge.md` + `slides.pdf`):** Build a search-and-ranking system that takes a natural-language real-estate query in German/French/Italian/English — for example *"3-room bright apartment in Zurich under 2800 CHF with balcony, close to public transport"* — and returns a ranked list of Swiss RENT listings that (a) satisfy the **hard constraints** and (b) are ordered by **soft preferences** like *bright, modern, quiet, family-friendly, near ETH, cheap*. Must also gracefully handle vague, conflicting, and multi-lingual queries, and expose a public HTTPS API + live demo + presentation. Evaluation weighs hard-filter precision heaviest ("a system that regularly violates hard constraints by more than a margin is not a strong solution"), then ranking quality, technical depth, feature width, creativity, and failure analysis.

**Workspace convention:** All my work lives in a new folder `/Users/mahbod/Desktop/Datathon_2026/mahbod/`. The starter harness at the repo root stays untouched. At the start of Phase 0 I copy the harness into `mahbod/` and symlink `raw_data/` so we don't duplicate the 285MB CSV + image bundle. Every file path in this plan is relative to `mahbod/` unless explicitly prefixed with the repo root.

```
/Users/mahbod/Desktop/Datathon_2026/
├── app/            apps_sdk/       raw_data/       ...   ← starter harness (UNTOUCHED)
└── mahbod/         ← all my work
    ├── app/                         ← copied, participant stubs replaced
    ├── apps_sdk/                    ← copied, widget extended
    ├── scripts/                     ← copied + new enrichment/eval scripts
    ├── tests/                       ← copied + new failure-mode tests
    ├── eval/                        ← NEW: queries.jsonl, judgments/, reports/
    ├── data/                        ← NEW: embeddings.npy, landmarks.json, caches
    ├── raw_data -> ../raw_data      ← symlink, not a copy
    ├── pyproject.toml               ← copied + new dependencies
    ├── Dockerfile, docker-compose.yml, .dockerignore, .gitignore   ← copied
    └── README.md                    ← NEW: deployment runbook, eval instructions
```

**What exists today (starter harness at `/Users/mahbod/Desktop/Datathon_2026/`, copied into `mahbod/` verbatim at Phase 0):**
- FastAPI + SQLite harness; DB auto-bootstrapped from 4 CSVs (~22,819 RENT listings) via `app/harness/bootstrap.py` → `app/harness/csv_import.py` → `app/participant/listing_row_parser.py` (production-ready, leave alone).
- Working structured SQL hard-filter at `app/core/hard_filters.py:search_listings()` (city, canton, postal_code, price range, rooms range, lat/lng+Haversine radius, feature flags, object_category, sort — all production-quality).
- Orchestrator at `app/harness/search_service.py:query_from_text()` chains `extract_hard_facts → search_listings → extract_soft_facts → filter_soft_facts → rank_listings`.
- **Stubs to replace:** `app/participant/hard_fact_extraction.py` (returns empty `HardFilters()`), `app/participant/soft_fact_extraction.py` (returns `{"raw_query": q}`), `app/participant/soft_filtering.py` (pass-through), `app/participant/ranking.py` (all `score=1.0`).
- MCP server at `apps_sdk/server/main.py` exposing one `search_listings` tool; React+Vite widget at `apps_sdk/web/` with working list+map.
- `pyproject.toml` has only `fastapi, mcp, httpx, uvicorn, boto3, pytest`. No Anthropic SDK, no embeddings, no vision libs yet.

**Data reality (from profiling):** 4 disjoint CSVs, all RENT. Critical quirks:
1. SRED (11,105 rows, 48% of corpus) has `geo_lat/geo_lng` but **no city/canton/postal_code/street** — must reverse-geocode.
2. Feature flags (`prop_balcony`, `prop_elevator`, …) are **97% null** in structured/SRED — only robinreal has them populated. Must derive from free-text descriptions.
3. Descriptions are multi-lingual (DE 70%+, FR 20%+, IT/EN small) and contain HTML.
4. SRED has 1 local montage image per listing at `raw_data/sred_images/{id}.jpeg`; structured/robinreal have JSON lists of remote URLs.
5. Prices contain outliers/sentinels (0, 1, 1,111,111); `area` is stringly-typed with "nicht verfügbar".

**Intended outcome:** a jury-ready system that (1) extracts both hard filters and soft preferences from one Claude call; (2) uses **hybrid retrieval** (SQL filter + FTS5 BM25 + multilingual dense embeddings fused via RRF); (3) **reranks** on a transparent linear blend of 8 normalized signals (BM25, embedding cosine, feature match, price-fit, geo-fit, commute-fit, image-quality-fit, freshness − negative penalty); (4) ships **precomputed enrichments** (reverse-geocoded addresses, text-derived features, CLIP image scores, station distances, landmark gazetteer) without modifying the harness-owned schema; (5) supports session-level **personalization bonus**; (6) runs a **32-query eval** with LLM-as-judge; (7) deploys to **Fly.io** with Cloudflare Tunnel fallback.

---

## Architecture at a glance

```
query ──► extract_query_plan (1 Claude call, forced tool-use)
              │
              ├─► hard: HardFilters   ─► app/core/hard_filters.search_listings()  ┐
              │                                                                   │
              ├─► soft: SoftPreferences (keywords, negatives, landmarks,          │
              │        price_sentiment, quality_targets, commute)                 │
              │                                                                   ├─► RRF fusion
              └─► rewrites[] ──► FTS5 BM25 (listings_fts)                         │   (top 150)
                            ──► Dense embeddings (bge-m3, in-memory matrix)       ┘
                                                                                  │
                                                      relaxation ladder on 0 hits │
                                                                                  ▼
                                                   rerank: linear-blend score
                                                   (+ personalization boost if session)
                                                                                  │
                                                   render explanations (template, +optional LLM polish)
                                                                                  │
                                                                                  ▼
                                                          ListingsResponse + meta (extracted filters,
                                                                                   relaxations, clarification)
```

---

## Execution phases

### Phase 0 — Dev environment + folder setup (20 min)

```bash
cd /Users/mahbod/Desktop/Datathon_2026
mkdir mahbod
cp -R app apps_sdk scripts tests pyproject.toml Dockerfile docker-compose.yml .dockerignore .gitignore mahbod/
ln -s ../raw_data mahbod/raw_data
mkdir -p mahbod/{data/cache,data/image_cache,eval/judgments,eval/reports}
cd mahbod
```

- Add to `mahbod/pyproject.toml` dependencies: `anthropic>=0.39.0`, `sentence-transformers>=3.0.0`, `open_clip_torch>=2.24`, `torch` (CPU wheel), `numpy>=2.0`, `rapidfuzz>=3.9`, `pillow>=10.0`, `beautifulsoup4>=4.12`, `reverse_geocoder>=1.5.1`, `scikit-learn>=1.5`.
- `uv sync --dev`. Sanity: `uv run uvicorn app.main:app --reload` boots from inside `mahbod/`; DB builds into `mahbod/data/listings.db` (or whatever `LISTINGS_DB_PATH` resolves to — confirm via `app/config.py`).

### Phase 1 — Core pipeline (must ship; ~6h)

**1a. Query understanding** — NEW `app/participant/query_plan.py`:
- Single Claude Sonnet 4 call with forced tool `emit_query_plan`. System prompt cached (`cache_control: ephemeral`) with: feature-key vocabulary (12 keys matching `app/core/hard_filters.FEATURE_COLUMN_MAP`), canton codes, known-city aliases (Zürich/Zurich, Genève/Geneva…), rules ("3-room" → `min_rooms=max_rooms=3`, "under X CHF" → `max_price=X`, "near X" → `landmarks[]`, "ideally X" → soft-only).
- Output schema added to `app/models/schemas.py`: `Landmark`, `SoftPreferences`, `QueryPlan`. `SoftPreferences` = `{keywords, negatives, positive_features, required_features, price_sentiment: "cheap"|"moderate"|"premium"|None, landmarks: [Landmark], quality_targets: {brightness, modernity, view, …}, move_in_after, raw_query, rewrites[]}`.
- Post-process: promote `soft.required_features` into `hard.features`; resolve landmarks via gazetteer (below); attach rough `(lat,lng,radius_km)` prefilter when `max_travel_min ≤ 30`.
- 5s hard deadline with regex fallback (`_regex_fallback_extract()` parses rooms/price/city only).

Wire-up:
- `app/participant/hard_fact_extraction.py` → `return get_cached_plan(query).hard`
- `app/participant/soft_fact_extraction.py` → `return get_cached_plan(query).soft.model_dump()` (LRU cache keyed by query string so both calls share one API round-trip).

**1b. FTS5 + embeddings + retrieval** — NEW `app/participant/bootstrap_participant.py` (called from `app/main.py` `lifespan` after `bootstrap_database`):
- Create `listings_fts` virtual table (`tokenize='unicode61 remove_diacritics 2'`) over `title, description, street, city`, content-linked to `listings`. Rebuild on startup (`INSERT INTO listings_fts(listings_fts) VALUES('rebuild')`) — always in sync without touching harness schema.
- Load `data/embeddings.npy` (float16, 22k×1024 ≈ 46MB) + `data/embedding_ids.json` mapping row → listing_id into module-level numpy matrix.
- NEW `app/participant/retrieval.py`:
  - `bm25_candidates(plan, k=300)` — FTS5 `MATCH` with OR-joined tokens from `rewrites + keywords`, filtered to listings that also pass `hard`.
  - `embedding_candidates(plan, k=300)` — encode `raw_query + " " + rewrites.join(" ")` with bge-m3 query encoder; cosine against in-memory matrix; take top-k.
  - `structured_candidates(plan, k=500)` — reuse `app.core.hard_filters.search_listings()`.
  - `fuse(channels, k=150)` — Reciprocal Rank Fusion (`k_rrf=60`), dedupe by `listing_id`.

**1c. Ranking** — REWRITE `app/participant/ranking.py`:
```
score = 0.22·bm25_pct + 0.22·emb_cos_pct + 0.15·feature_match
      + 0.10·price_fit + 0.10·geo_fit + 0.08·commute_fit
      + 0.08·image_quality_fit + 0.05·freshness − 0.15·negative_penalty
```
Each component normalized to [0,1] as **percentile rank within the candidate pool** (no global calibration). Details:
- `price_fit` — triangle over candidate-pool price distribution, centered on `p25/median/p75` for `cheap/moderate/premium`.
- `geo_fit` — `exp(-d_km / 3.0)` to closest landmark; else neutral 0.5.
- `commute_fit` — if landmark + `max_travel_min`: `1 − dist_to_nearest_station / (mode_speed × max_travel_min)`; else copy `geo_fit`.
- `image_quality_fit` — dot product of `quality_targets` with `listings_enriched.img_*` scores; neutral 0.5 on miss.
- `negative_penalty` — keyword hit (via rapidfuzz + embedding sim > 0.55) OR violated structured flag ("no ground floor" → penalize `floor=0`).
- `freshness` — linear on `available_from` (60d→1, 365d→0).

Weights live in NEW `app/participant/scoring_config.py` for live tuning.

**1d. Explanations** — NEW `app/participant/explain.py`:
- `render_reason(candidate, plan, components) -> str`: deterministic template, e.g. *"Matches 3 rooms, Zurich, ≤ CHF 2,800, balcony (text-derived). Brightness 0.82. 12 min walk to ETH. Slight price premium vs candidate median."*
- Optional `?explain=llm` triggers one Claude call per top-5 with `max_tokens=120` for a fluent rewrite.

**1e. Soft filtering** — EDIT `app/participant/soft_filtering.py`:
- Drop candidates violating strict negatives only (e.g. `"kein Erdgeschoss"` with known `floor=0`). All other soft signals are handled at rank time.

### Phase 2 — Enrichment (high impact; ~5h; offline scripts)

All enrichment writes to a single auxiliary table **`listings_enriched`** (PK `listing_id`) created by `bootstrap_participant.py` with `CREATE TABLE IF NOT EXISTS` — no edits to the harness-owned `create_schema()` in `app/harness/csv_import.py` (which would trigger the `_schema_matches()` guard in `app/harness/bootstrap.py` and block DB reuse).

- **`scripts/enrich_geocode.py`** — `reverse_geocoder` package for 11,105 SRED rows (~30s, offline, returns city+canton). Optional Nominatim top-up with JSON cache at `data/cache/nominatim_reverse.json` for postal_code/street on eval-set cities only. Hard-filter queries will `COALESCE(listings.city, enriched.city_reverse)` — requires a view or minor change to hard-filter SQL.
- **`scripts/enrich_text_features.py`** — multilingual regex dictionaries in NEW `app/participant/text_feature_patterns.py` (DE/FR/IT/EN for 12 features), negation guard ("kein/ohne/pas de/sans/no/non" within 3 tokens). HTML strip via `BeautifulSoup`. Writes `feat_{name}_txt INT, feat_{name}_conf REAL` into `listings_enriched`. At rank time: `derived_feature = explicit_flag OR feat_*_txt`.
- **`scripts/build_embeddings.py`** — bge-m3 encoder, one-shot over all 22,819 listings. Document template: `"{title}\n{city}, {canton}\n{rooms} rooms, {area} m², CHF {price}\n{enabled_features_text}\n{desc_first_800_chars_HTML_stripped}"`. Saves `data/embeddings.npy` + `data/embedding_ids.json`. Runs once, ~20-60 min CPU.
- **`scripts/enrich_images.py`** — `open_clip` ViT-B/32 locally. For each listing: compute cosine of hero image against prompts `"bright sunlit room", "modern interior", "mountain/lake view", "spacious open floor plan", "family-friendly cozy home"`. Stores 5 floats per listing in `listings_enriched.img_*`. SRED montages (11k) used directly; structured/robinreal download 1 hero per listing to `data/image_cache/{listing_id}.jpg` (capped). Pillow-luminance fallback for CLIP failures.
- **`scripts/enrich_geo.py`** — Download SBB `stops.txt` (~2MB, ~20k stops), build scikit BallTree on lat/lng, compute `dist_station_m` per listing. City-center distances from `reverse_geocoder` city centroids. Writes `dist_station_m, dist_center_m, urbanity` to `listings_enriched`.
- **`data/cache/landmarks.json`** — Manually seeded ~200 entries (ETH, EPFL, universities, major stations, lakes) resolved via Nominatim forward-geocode once. Read by `app/participant/geo.py:resolve_landmark()`.

One-shot runner: `scripts/enrich_all.py` chains all enrichment scripts. All write-paths are idempotent (`INSERT OR REPLACE`).

### Phase 3 — Graceful degradation (~2h)

- NEW `app/participant/relaxation.py`: ordered ladder — price ±10% → drop city (keep canton) → drop canton → expand radius ×1.5 → drop features least-frequent-first. Annotate `meta.relaxations: [...]`.
- Unknown city: `rapidfuzz.process.extractOne(city, known_cities, threshold=85)` for typos/Zurich-vs-Zürich normalization.
- Claude extraction timeout/5xx: regex fallback in `app/participant/query_plan.py`.
- Vague query: if Claude sets `clarification` field, orchestrator short-circuits with empty listings + `meta.clarification` text.

### Phase 4 — Eval framework (~3h)

- **`eval/queries.jsonl`** — 32 queries covering: 8 clear-hard, 6 soft-heavy, 4 conflicting, 6 multilingual (DE/FR/IT), 4 landmark-relative, 4 adversarial (typos, impossibilities, non-RENT, huge feature lists). Schema: `{qid, lang, category, query, expected_filters, must_satisfy: [predicates], gold_topk_listing_ids?}`.
- **`scripts/evaluate.py`** — runs each query through `POST /listings`, computes:
  - **HF-P** (hard-filter precision): extracted-field vs expected.
  - **CSR** (constraint satisfaction rate): fraction of returned listings satisfying all `must_satisfy`.
  - **NDCG@10**: vs `gold_topk_listing_ids` where present, else vs Bradley–Terry ranks derived from Claude pairwise judgments cached in `eval/judgments/{qid}.json`.
  - **MRR**, **COV** (≥5 results), **LAT** (p50 ms).
- Writes markdown leaderboard to `eval/reports/YYYYMMDD-HHMM.md`. ~$4/full run.
- NEW `tests/test_failure_modes.py` — 8 proactive tests (unicode typos, impossible conflicts, empty filters, IT-language, sale attempt, misspelled landmark, cheap-vs-expensive conflict, adversarial feature set). Each asserts graceful fallback via `meta.warnings`.

### Phase 5 — Personalization bonus (~2h)

- NEW `app/participant/personalization.py`: in-memory `SESSIONS: dict[str, UserProfile]`, keyed by header `X-Session-Id`. Tracks `query_history, favorited, skipped, clicks`.
- Inferred profile (recomputed on each mutation): `preferred_city` (mode, ≥2 signals), `preferred_rooms_range (p25,p75)`, `preferred_feature_set` (≥60% overlap), `preferred_style_vector` (centroid of favorited listings' CLIP image embeddings + bge-m3 document embeddings).
- Ranking boost: `score_final = score_base + 0.1·cos(listing_vec, pref_centroid) + 0.05·feature_overlap`, capped at +0.2.
- NEW `app/api/feedback.py` router: `POST /users/{id}/feedback`, `GET /users/{id}/profile`, `DELETE /users/{id}`.
- Demo flow: run "bright modern flat in Zurich" 3x, favorite a high-floor loft each time, show reordering on 4th query without changing the query text.

### Phase 6 — Demo UI (~2h)

Extend `apps_sdk/web/src/App.tsx` via 4 new components under `apps_sdk/web/src/components/`:
- `FiltersChipRow` — renders `meta.extracted_filters`.
- `ExplanationBar` — shows per-listing `result.reason`.
- `ScoreBar` — colored gradient from score.
- `ClarificationChip` — renders `meta.clarification` + answer chips.
- Map: color markers by score.

Backend: `app/harness/search_service.py` populates `meta = {extracted_filters, relaxations, clarification, warnings}` (the existing pass-through `meta: {}` makes this non-breaking).

### Phase 7 — Deploy (~1h)

**Primary: Fly.io** (`fra` region, 1GB volume for SQLite + image cache):
```
flyctl launch --no-deploy --name datathon2026-robin --region fra
flyctl volumes create listings_data --size 1 --region fra
flyctl secrets set ANTHROPIC_API_KEY=... APPS_SDK_PUBLIC_BASE_URL=https://datathon2026-robin.fly.dev
flyctl deploy
```
Image bundle (~135MB) goes in the container image; DB on the volume; enrichment script runs once via `fly ssh console -C 'uv run python scripts/enrich_all.py'`.

**Fallback: Cloudflare Tunnel** — `npx cloudflared tunnel --url http://localhost:8000` + one for `:8001` (MCP). Documented in README runbook.

---

## Critical files

**New (in priority order):**
- `app/participant/query_plan.py` — Claude call, schema glue, cache, 5s timeout + regex fallback.
- `app/participant/retrieval.py` — BM25 + embeddings + RRF fusion.
- `app/participant/bootstrap_participant.py` — FTS5 rebuild + `listings_enriched` table + embeddings matrix loader (called from `app/main.py:lifespan`).
- `app/participant/scoring_config.py` — weights, normalization helpers.
- `app/participant/explain.py` — template + optional LLM polish.
- `app/participant/text_feature_patterns.py` — DE/FR/IT/EN regex dictionaries.
- `app/participant/geo.py` — landmark gazetteer + Haversine helpers.
- `app/participant/relaxation.py` — 0-candidate fallback ladder.
- `app/participant/personalization.py` — sessions + boost logic.
- `app/api/feedback.py` — session feedback endpoints.
- `scripts/enrich_geocode.py`, `enrich_text_features.py`, `build_embeddings.py`, `enrich_images.py`, `enrich_geo.py`, `enrich_all.py`, `evaluate.py`.
- `data/cache/landmarks.json`, `data/embeddings.npy`, `data/embedding_ids.json`.
- `eval/queries.jsonl`, `eval/judgments/`, `eval/reports/`.
- `tests/test_failure_modes.py`, `tests/test_query_plan.py`, `tests/test_retrieval.py`, `tests/test_scoring.py`.

**Edit:**
- `app/models/schemas.py` — add `Landmark`, `SoftPreferences`, `QueryPlan`.
- `app/participant/hard_fact_extraction.py` — delegate to `query_plan`.
- `app/participant/soft_fact_extraction.py` — delegate to `query_plan` (shared cache).
- `app/participant/soft_filtering.py` — enforce hard negatives only.
- `app/participant/ranking.py` — blended score + personalization boost + explanations.
- `app/harness/search_service.py` — populate `meta` (non-breaking: `meta` is already pass-through); route `clarification`.
- `app/main.py` — call `bootstrap_participant()` after `bootstrap_database()` in `lifespan`.
- `app/api/routes/listings.py` — accept optional `X-Session-Id` header for personalization.
- `apps_sdk/web/src/App.tsx` — add 4 components.
- `pyproject.toml` — add dependencies (see Phase 0).
- `README.md` — deployment runbook, env vars, eval run command.

**Reuse as-is (do NOT modify):**
- `app/participant/listing_row_parser.py` — already production-ready; reverse-geocoded city comes via side table, not via parser.
- `app/harness/csv_import.py`, `app/harness/bootstrap.py` — harness-owned, modifying would break `_schema_matches()` guard.
- `app/core/hard_filters.py:search_listings()` — full SQL filter, reused via `structured_candidates()`.
- `app/core/s3.py` — for remote image lookup.
- `apps_sdk/server/main.py` — MCP `search_listings` tool unchanged.

---

## Verification

**Unit (fast, in-process):**
```bash
uv run pytest tests -q                             # existing + 4 new test files
uv run pytest tests/test_query_plan.py -v          # verify Claude extraction on 10 canonical queries
uv run pytest tests/test_failure_modes.py -v       # 8 proactive failure tests
uv run pytest tests/test_scoring.py -v             # score monotonicity: more matching features → higher score
```

**Integration (live API):**
```bash
cd /Users/mahbod/Desktop/Datathon_2026/mahbod
uv run uvicorn app.main:app --reload --port 8000   # boots, builds DB, loads embeddings/FTS5, loads landmarks
# 1. Health
curl -s http://localhost:8000/health | jq
# 2. The four canonical slide queries
for q in \
  "3-room bright apartment in Zurich under 2800 CHF with balcony, close to public transport" \
  "Bright family-friendly flat in Winterthur, not too expensive, ideally with parking" \
  "Modern studio in Geneva for June move-in, quiet area, nice views if possible" \
  "affordable student accomodation, max half an hour door to door to ETH Zurich by public transport, i like modern kitchens"; do
  curl -s -X POST http://localhost:8000/listings -H 'content-type: application/json' \
    -d "{\"query\": \"$q\", \"limit\": 10}" | jq '{extracted: .meta.extracted_filters, top3: [.listings[0:3][] | {id: .listing_id, score, reason}]}'
done
# 3. Multilingual smoke
curl -s -X POST http://localhost:8000/listings -H 'content-type: application/json' \
  -d '{"query": "Helle 3.5-Zimmer-Wohnung in Zürich, nah am Bahnhof, max 2800 CHF", "limit": 5}' | jq
curl -s -X POST http://localhost:8000/listings -H 'content-type: application/json' \
  -d '{"query": "Cerco un bilocale a Lugano con balcone, max 2000 CHF", "limit": 5}' | jq
# 4. Failure-mode smokes (impossible, vague, typo)
curl -s -X POST http://localhost:8000/listings -H 'content-type: application/json' \
  -d '{"query": "5 rooms in Geneva under CHF 500", "limit": 5}' | jq '.meta'
curl -s -X POST http://localhost:8000/listings -H 'content-type: application/json' \
  -d '{"query": "nice flat", "limit": 5}' | jq '.meta'
# 5. Personalization
SESS=demo-1
curl -s -X POST -H "X-Session-Id: $SESS" http://localhost:8000/users/$SESS/feedback \
  -d '{"action":"favorite","listing_id":"<id-from-step-2>"}'
curl -s -X POST -H "X-Session-Id: $SESS" http://localhost:8000/listings \
  -d '{"query":"bright modern flat in Zurich"}' | jq '.listings[0:3]'
```

**Evaluation (end-to-end quality):**
```bash
cd /Users/mahbod/Desktop/Datathon_2026/mahbod
uv run python scripts/evaluate.py --out eval/reports/run-$(date +%Y%m%d-%H%M).md
# Expect: mean HF-P ≥ 0.90, mean CSR ≥ 0.90, mean NDCG@10 ≥ 0.70, COV ≥ 0.95, p50 latency < 2.5s.
```

**MCP smoke:**
```bash
cd /Users/mahbod/Desktop/Datathon_2026/mahbod
uv run uvicorn apps_sdk.server.main:app --reload --port 8001 &
uv run python scripts/mcp_smoke.py --url http://localhost:8001/mcp
# Confirms: initialize, tools/list (search_listings), resources/read (widget HTML).
```

**Manual demo run-through (before recording):**
1. Run each of the 4 canonical queries in the widget, confirm extracted-filter chips appear, explanations read well, map markers are color-coded by score.
2. Trigger the clarification chip on `"nice flat"`; select a chip answer and confirm requery.
3. Personalization demo: run vague query, favorite 2 modern listings, re-run the same query, show reordering.
4. Run eval, screenshot the leaderboard markdown — goes into presentation.

**Deployment sanity:**
```bash
cd /Users/mahbod/Desktop/Datathon_2026/mahbod
flyctl deploy
curl https://datathon2026-robin.fly.dev/health
curl -X POST https://datathon2026-robin.fly.dev/listings -d '{"query":"3 room flat in Zurich under 2800 CHF"}' -H 'content-type: application/json'
# Fallback:
npx cloudflared tunnel --url http://localhost:8000
```

## Time budget

| Phase | Hours | Ship-blocker? |
|---|---|---|
| 0. Env setup | 0.25 | yes |
| 1. Core pipeline (query plan + hybrid retrieval + ranking + explanations) | 6 | yes |
| 2. Enrichment (geocode, text features, embeddings, CLIP, stations, landmarks) | 5 | yes (embeddings + FTS5); rest high-impact |
| 3. Graceful degradation | 2 | yes |
| 4. Eval framework + failure tests | 3 | yes for credibility |
| 5. Personalization bonus | 2 | no (bonus) |
| 6. Demo UI polish | 2 | yes for presentation |
| 7. Deploy | 1 | yes |
| **Total** | **~21h** | |

**If time is tight, drop in this order:** Phase 5 (personalization bonus) → CLIP image scoring (substitute Pillow-luminance only) → Nominatim top-up in Phase 2 (`reverse_geocoder`-only is enough for city filter) → LLM-polish in explanations.
