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

## Data sources — organizer guidance (authoritative)

Transcribed from the organizer welcome message. Treat as the authoritative priority order when tradeoffs arise.

### CSV priority (best → worst)

| Rank | File | Rows | Features populated? | Images | Use it for |
|---|---|---|---|---|---|
| 1 | `raw_data/robinreal_data_withimages-1776461278845.csv` | 797 | yes (`prop_*` flags) | S3 URLs, multi per listing, **HQ** | ranking demos, image-based signals, feature-heavy queries |
| 2 | `raw_data/structured_data_withimages-1776412361239.csv` | 4,160 | partial (~11% child_friendly only) | S3 URLs, multi per listing, **HQ** | volume of HQ images, broad coverage |
| 3 | `raw_data/sred_data_withmontageimages_latlong.csv` | 11,105 | none | 1 local **low-quality** montage per listing at `raw_data/sred_images/{id}.jpeg` | geographic coverage (lat/lng 100%), fallback; organizer explicitly called these "low quality" |
| 4 | `raw_data/structured_data_withoutimages-1776412361239.csv` | 6,757 | none | none | text-only signal |

**Implication for ranking + demo:** prioritize listings from ranks 1-2 when showcasing the image-quality component (`img_brightness/modernity/view/spaciousness/family_friendly`). SRED remains essential for geographic breadth but its montages should not carry the demo. PLAN §2 `scripts/enrich_images.py` still uses SRED montages first because they're already on disk (no download), but image-score tuning should be validated on rank-1/2 data.

### S3 image trees (organizer-provided)

Bucket `s3://crawl-data-951752554117-eu-central-2-an`, region `eu-central-2`, prefix `prod/`:

- `prod/comparis/images/` — full-res images for the two `structured_*` CSVs (COMPARIS source).
- `prod/robinreal/images/` — full-res images for the `robinreal_*` CSV.

Credentials live in `/Users/mahbod/Desktop/Datathon_2026/.env` (gitignored, see `.gitignore:7`). Env vars required: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION=eu-central-2`.

**Optional bulk local copy** (organizer commands, one-time):
```bash
set -a; source .env; set +a
aws s3 cp "s3://crawl-data-951752554117-eu-central-2-an/prod/comparis/images/"  ./raw_data/structured_data_images  --recursive
aws s3 cp "s3://crawl-data-951752554117-eu-central-2-an/prod/robinreal/images/" ./raw_data/robinreal_images         --recursive
```

**Server-side per-listing lookup** is already implemented in `app/core/s3.py:get_image_urls_by_listing_id()` — reads the same env vars via the standard AWS credential chain. For SRED it returns the local montage; for COMPARIS/ROBINREAL it lists the S3 prefix `prod/<source>/images/platform_id={platform_id}/`. No code change needed to consume these images at query time.

### Other organizer pointers (verbatim)

- DB schemas: `app/models/schemas.py`.
- CSV → SQLite normalization (adjustable if we want more extraction at ingest): `app/harness/csv_import.py`.
- Where we plug filtering + ranking: `app/participant/` (stubs + `listing_row_parser.py`).
- README and challenge.md contain MCP App UI setup (already read by PLAN §7 Deploy).

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

### Phase 5 — Personalization bonus (~4h)

Designed to cover **every** signal listed in `challenge.md` lines 65-74 and **every** dimension in the bonus question at line 78 ("locations, price bands, listing styles, image types, building features"), with the explicit anti-overfitting constraint from `challenge.md` line 255 ("use user history without overfitting to one past action"). Each sub-phase below is independently droppable — see tail of plan for drop order.

**5a. Storage + endpoints**
- NEW `app/api/feedback.py` — router with three endpoints:
  - `POST /users/{session_id}/feedback` — body: `{action, listing_id, duration_ms?, query?, timestamp?}`.
  - `GET /users/{session_id}/profile` — returns the inferred profile + breakdown used by the demo's "why you saw this" panel.
  - `DELETE /users/{session_id}` — reset; also supports `GDPR`-style opt-out.
- NEW `app/participant/personalization.py` — profile I/O + inference + boost.
- Persistence: per-user JSON at `data/user_profiles/{session_id}.json` (gitignored). Chosen over in-memory so **"longer-term interaction or preference history"** (challenge.md L61) survives server restarts. Size is trivial (one small JSON per user). Profile TTL = 90 days since last interaction; older profiles are auto-purged on startup and `[INFO]` logged.
- Session id via header `X-Session-Id` (read in `app/api/routes/listings.py`). Literal `anonymous` disables tracking: `[INFO] personalization=opted_out`.

**5b. Signals — covers challenge.md L65-L74 one-by-one**

| Challenge bullet | Action name | Weight for profile centroid | Notes |
|---|---|---|---|
| favorites (L68) | `favorite` | +1.0 | strongest positive |
| clicks (L69) | `click` | +0.2 | card click without detail open |
| detail views (L70) | `view` | +0.3 base | carries `duration_ms` |
| dwell time (L71) | — | `view` weight becomes `0.3·log10(1 + duration_ms/1000)` clipped to `[0.3, 0.8]` | longer dwell → stronger signal, capped to prevent one-action dominance |
| hides or skips (L72) | `skip`, `hide` | `−0.5`, `−1.0` | negative signals |
| past searches (L66) | `search` | — | stored in query history, used by 5d |
| historical query patterns (L73) | — | — | derived from query history by 5d |
| inferred preference documents (L65) | — | — | LLM-generated summary by 5e |
| summaries (L67) | — | — | same as 5e |
| preference graph (L74) | — | — | feature co-occurrence by 5c |

Each interaction is stamped with `ts`. Weight decay: `w_decayed = w · exp(−Δdays / 30)` so stale actions fade — this is the primary guard against overfitting to one past action (challenge.md L255).

**5c. Profile inference — covers all 5 bonus-question dimensions (L78)**

Recomputed on every mutation. Each dimension only activates above a **min-sample threshold** (second overfitting guard):

| Bonus-question dimension (L78) | Inferred field | Min samples | Computation |
|---|---|---|---|
| **locations** | `preferred_cities: dict[city, weight]` | ≥3 positive interactions in that city | weighted count by action weight × decay |
| **price bands** | `preferred_price_band: (p25, p75)` | ≥3 favorites | weighted quantiles over favorited prices |
| **building features** | `preferred_feature_set: dict[feature, weight]` | ≥2 favorites sharing feature | features present in ≥60% of favorited listings, weighted |
| **listing styles** | `preferred_style_vector: np.ndarray[1024]` | ≥2 favorites | weighted centroid of favorited listings' bge-m3 doc embeddings |
| **image types** | `preferred_image_vector: np.ndarray[512]` | ≥2 favorites | weighted centroid of CLIP image embeddings (from `scripts/enrich_images.py` output) |

Plus three derived signals from the challenge bullets:
- `preferred_rooms_range: (p25, p75)` — min 3 favorites.
- `negative_feature_set` — features over-represented in skipped/hidden vs favorited (for the skip/hide bullet L72).
- `feature_cooccurrence_graph: dict[tuple[feature, feature], weight]` — pair counts in favorites. This is the **inferred preference graph** from L74. Lightweight dict, no graph library.

**5d. Query-history signal — covers L66 (past searches) + L73 (historical query patterns)**
- Keep last 20 queries with timestamps in profile.
- At query time: if `len(history) ≥ 3`, encode each historical query with bge-m3, time-decayed weighted mean → `query_centroid`. Blend with current query embedding for **retrieval only**: `q_personalized = 0.8·q_current + 0.2·q_centroid`, L2-normalized. Weight 0.2 is low to avoid drowning the explicit query.
- Also detect repeated hard filters in past searches: if the user submitted `city=Zurich` in ≥70% of the last 10 queries, attach a weak implicit city preference at retrieval time (NOT as a hard filter — logged `[INFO] implicit_filter_hint=Zurich`).

**5e. LLM preference summary — covers L65 (inferred preference documents) + L67 (summaries)**
- Every 10th interaction OR on demand via `GET /users/{id}/profile?regenerate=true`: one Claude call summarizes the user in ≤3 sentences from {last 10 favorites, last 10 skips, last 20 queries}.
- Stored as `inferred_summary: str` in profile. Cached until next 10 interactions.
- Used in two places:
  1. `render_reason()` in `explain.py` can include "Matches your past preference for X" when a listing scores high on personalization signals.
  2. Returned by `GET /users/{id}/profile` for the demo's transparency panel.
- Rate-limited to 1 summary / 5 minutes / user to control cost. Falls back to template summary on API failure with `[WARN]` per CLAUDE.md §5.

**5f. Ranking boost — linear, transparent, capped**

Applied after base ranking in `app/participant/ranking.py`:

```
boost = 0.0
  + min(0.08, 0.02 · Σ_w w_favorites_in(listing.city))          # locations, capped 0.08
  + 0.05 · triangle(listing.price, p25_fav, p75_fav)            # price bands
  + 0.04 · jaccard(listing.features, preferred_feature_set)      # building features (direct)
  + 0.04 · graph_match(listing.features, cooccurrence_graph)     # preference graph
  + 0.06 · cos(listing.doc_embedding, preferred_style_vector)   # listing styles
  + 0.06 · cos(listing.image_embedding, preferred_image_vector) # image types
  + 0.03 · cos(listing.doc_embedding, query_centroid)           # past searches
  − 0.10 · jaccard(listing.features, negative_feature_set)
  − 0.10 if listing_id in skipped (last 30 days)
  − 0.20 if listing_id in hidden (ever)
Each additive term gated by its own min-sample threshold (→ 0 if below).
Final boost clipped to [−0.25, +0.25]   # third overfitting guard
score_final = score_base + boost
```

Returned in `meta.personalization: {boost, components: {...}, summary}` for UI transparency. Exposes weights so jury/user can see *why* ranking shifted.

**5g. Cold start + opt-out + logging (CLAUDE.md §5 compliance)**
- `n_interactions ≤ 1`: `boost = 0`, log `[INFO] personalization=cold_start session=... interactions=N` once per request (not per listing).
- `session_id == "anonymous"` or header missing: `boost = 0`, log `[INFO] personalization=opted_out`.
- LLM summary failure: fall back to template, `[WARN] user_summary: expected=claude, got=error, fallback=template_summary`.
- Embedding missing for a listing (not yet in index): `[WARN] style_boost: expected=embedding, got=None listing_id=X, fallback=skip_component`.

**5h. Tests — `tests/test_personalization.py`**
1. `test_cold_start_boost_is_zero` — 0 or 1 interaction → boost == 0.
2. `test_monotonic_city_boost` — adding 3 Zurich favorites strictly increases Zurich-listing boost.
3. `test_boost_is_capped` — 100 favorites on one listing → total boost ≤ 0.25.
4. `test_time_decay` — an interaction 60 days ago has < 30% weight of one today (exp(−60/30) ≈ 0.135 < 0.3 ✓).
5. `test_opposite_signals_cancel` — favoriting and skipping the same listing → net boost ≈ 0.
6. `test_opt_out_anonymous_session` — `X-Session-Id: anonymous` → boost == 0, no file written.
7. `test_persistence_across_restart` — write profile, reload, ensure it re-applies.
8. `test_graph_cooccurrence_emerges_after_3_favorites` — 3 balcony+parking favorites → balcony+parking pair score > singletons.

**5i. Demo flow** (script: `scripts/demo_personalization.py`)
Scripted walkthrough shown during presentation:
1. Query `"bright modern flat in Zurich"` — baseline top 10.
2. `POST /users/demo/feedback` `favorite` 2 modern high-floor listings + `view` with 30s dwell on a similar one + `skip` one ground-floor listing.
3. Re-run same query — show the top 10 has reordered; `meta.personalization.boost` non-zero; `meta.personalization.summary` shows inferred "modern high-floor preference in Zurich" (generated by Claude).
4. Query `"apartment"` (intentionally vague) — show past-search centroid steers results toward Zurich modern even though city is absent.
5. `DELETE /users/demo` — repeat step 4, show results revert to neutral.

### Mapping to `challenge.md` — every bonus bullet covered

| `challenge.md` line | Bullet | Covered in |
|---|---|---|
| L65 | inferred preference documents | 5e |
| L66 | past searches | 5d |
| L67 | summaries | 5e |
| L68 | favorites | 5b action `favorite` |
| L69 | clicks | 5b action `click` |
| L70 | detail views | 5b action `view` |
| L71 | dwell time | 5b `view.duration_ms` |
| L72 | hides or skips | 5b actions `skip`, `hide` + negative-features derivation in 5c + skip penalty in 5f |
| L73 | historical query patterns | 5d |
| L74 | user preference graph | 5c `feature_cooccurrence_graph` |
| L78 locations | 5c `preferred_cities`, 5f term 1 |
| L78 price bands | 5c `preferred_price_band`, 5f term 2 |
| L78 listing styles | 5c `preferred_style_vector`, 5f term 5 |
| L78 image types | 5c `preferred_image_vector`, 5f term 6 |
| L78 building features | 5c `preferred_feature_set`, 5f terms 3-4 |
| L255 "without overfitting to one past action" | min-sample thresholds (5c) + decay (5b) + hard cap ±0.25 (5f) + cold-start zero-boost (5g) |

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
- `app/participant/personalization.py` — profile I/O, inference, 9-component boost formula, overfitting guards, CLAUDE.md §5 warn logs.
- `app/participant/user_summary.py` — Claude-generated ≤3-sentence preference summary (Phase 5e), 5-min rate limit, template fallback.
- `app/api/feedback.py` — session feedback endpoints (POST /users/{id}/feedback, GET /users/{id}/profile, DELETE /users/{id}).
- `data/user_profiles/` — per-user JSON store (gitignored; add `data/user_profiles/` to `.gitignore`).
- `scripts/demo_personalization.py` — scripted walkthrough for the bonus demo.
- `scripts/enrich_geocode.py`, `enrich_text_features.py`, `build_embeddings.py`, `enrich_images.py`, `enrich_geo.py`, `enrich_all.py`, `evaluate.py`.
- `data/cache/landmarks.json`, `data/embeddings.npy`, `data/embedding_ids.json`.
- `eval/queries.jsonl`, `eval/judgments/`, `eval/reports/`.
- `tests/test_failure_modes.py`, `tests/test_query_plan.py`, `tests/test_retrieval.py`, `tests/test_scoring.py`, `tests/test_personalization.py` (8 tests: cold-start zero-boost, monotonic city boost, cap, time decay, skip/favorite cancellation, opt-out, persistence across restart, co-occurrence graph emergence).

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
| 5. Personalization bonus (5a-5i) | 4 | no (bonus) |
| 6. Demo UI polish | 2 | yes for presentation |
| 7. Deploy | 1 | yes |
| **Total** | **~23h** | |

**If time is tight, drop in this order:** within Phase 5, drop **5e** (LLM preference summary; template summary still works) → **5d** (query-history blend; retrieval still sound) → the `feature_cooccurrence_graph` term in 5c/5f (keep direct feature overlap) → else drop all of Phase 5. Outside Phase 5: CLIP image scoring (substitute Pillow-luminance only) → Nominatim top-up in Phase 2 (`reverse_geocoder`-only is enough for city filter) → LLM-polish in explanations.
