# Baseline MVP — Roham + Mehrshad

*Ship a working end-to-end pipeline fast. Prove the skeleton. Measure a baseline. Then layer dense + rerank on top from [ARCHITECTURE.md](ARCHITECTURE.md).*

---

## 0. TL;DR

**Pipeline:** `query → Claude (QueryPlan) → SQL hard-filter GATE → BM25 (FTS5) over allowed set → 4-signal linear rank → relaxation ladder if empty`

**Out of scope for MVP** (deferred to v2): dense embeddings, cross-encoder rerank, image scoring, SBB/GTFS enrichment, reverse geocoding, landmark gazetteer, personalization.

**Realistic target** based on multilingual BM25 benchmarks [^bm25-beir] [^miracl]: **NDCG@10 ≈ 0.35–0.45** on our eval set. That's our floor; v2 (dense + rerank) lifts us to ~0.55–0.65.

**Timeline:** ~20 h split evenly between two people. Both start with a 30-min shared-contract commit; then parallel work.

---

## 1. Scope

### In scope (MVP ships these)

- ✅ Claude `QueryPlan` extraction (forced tool-use, `strict: true`, multilingual paraphrase rewrites)
- ✅ SQL hard-filter as a **GATE** (reuse existing [app/core/hard_filters.py](app/core/hard_filters.py))
- ✅ **FTS5 BM25** over the allowed set (new: `listings_fts` virtual table)
- ✅ **4-signal linear ranking** (BM25F + feature match + price fit + freshness, with negative penalty)
- ✅ **Relaxation ladder** on zero results
- ✅ Templated per-listing `reason` strings
- ✅ `meta.extracted_filters`, `meta.relaxations`, `meta.warnings` populated
- ✅ 15-query smoke eval
- ✅ Docker Compose + public HTTPS URL (cloudflared for MVP, Fly.io for prod)

### Out of scope (v2 / ARCHITECTURE.md)

- ❌ Arctic-Embed-L dense retrieval
- ❌ bge-reranker-v2-m3 cross-encoder
- ❌ SigLIP-2 image scoring
- ❌ SBB GTFS station / commute
- ❌ Reverse geocoding SRED (SRED rows simply fail city-based hard filters in MVP — that's fine for baseline)
- ❌ Wikidata landmark gazetteer
- ❌ Personalization (sessions, favorites)
- ❌ `listings_enriched` side-table (all MVP signals come from existing columns)
- ❌ LLM-polished explanations

**Why defer enrichment:** the organizer explicitly said *"no UI judging, just ranking output quality and breadth"* — MVP proves the skeleton and gives us a measurable baseline. Every v2 upgrade can then be quantified as ΔNDCG vs. the MVP, which is exactly what the jury wants to see.

---

## 2. The MVP pipeline

```
┌───────────────────────────────────────────────────────────────────────┐
│  Query (DE/FR/IT/EN)                                                  │
│         │                                                             │
│         ▼                                                             │
│  ┌──────────────────────────────────────────────────────────────┐     │
│  │ Claude Sonnet 4.6 — forced tool-use emit_query_plan          │     │
│  │  · strict: true                                               │     │
│  │  · cached system prompt (padded to >1024 tok for Sonnet 4.5  │     │
│  │    or >2048 for 4.6; if <threshold, cache silently off)      │     │
│  │  · 5 s timeout → regex fallback                               │     │
│  └────────────────────────────┬─────────────────────────────────┘     │
│                               │ QueryPlan { hard, soft, rewrites }     │
│                               ▼                                        │
│  ┌──────────────────────────────────────────────────────────────┐     │
│  │ SQL GATE — reuse app/core/hard_filters.search_listings()     │     │
│  │  · returns allowed set of listing_ids                         │     │
│  │  · violators CANNOT appear downstream                         │     │
│  └────────────────────────────┬─────────────────────────────────┘     │
│                               │ allowed_ids (≈100–500 typically)       │
│                               ▼                                        │
│  ┌──────────────────────────────────────────────────────────────┐     │
│  │ FTS5 BM25 on (title×3, description×1, street×0.5, city×0.5) │     │
│  │  · MATCH = OR of (bm25_keywords + 2–3 multilingual rewrites) │     │
│  │  · tokenize='unicode61 remove_diacritics 2'                   │     │
│  │  · filtered to allowed_ids                                    │     │
│  └────────────────────────────┬─────────────────────────────────┘     │
│                               │ top 100 candidates                     │
│                               ▼                                        │
│  ┌──────────────────────────────────────────────────────────────┐     │
│  │ Rank (4 positive + 1 negative signal, percentile-normalized) │     │
│  │  +0.40 BM25F percentile                                       │     │
│  │  +0.20 feature_match (explicit flags only in MVP)             │     │
│  │  +0.20 price_fit (triangle on candidate pool p25/median/p75)  │     │
│  │  +0.10 freshness (linear on available_from)                   │     │
│  │  −0.10 negative_penalty (negated keyword hit)                 │     │
│  └────────────────────────────┬─────────────────────────────────┘     │
│                               │                                        │
│                               ▼                                        │
│                 ┌───────────────────────────┐                          │
│                 │ count == 0?               │                          │
│                 └──────────┬────────────────┘                          │
│                  yes       │    no                                     │
│                  ▼         ▼                                           │
│        [relaxation]    [return top 10 + reasons + meta]                │
└───────────────────────────────────────────────────────────────────────┘
```

---

## 3. Ranking — 4 positive + 1 negative signal

Every signal **percentile-normalized within the candidate pool** (no global calibration, no tuning thresholds).

| Signal | Weight | Source | Why |
|---|---|---|---|
| **BM25F percentile** | +0.40 | `bm25(listings_fts, 3.0, 1.0, 0.5, 0.5)` over (title, description, street, city) | Title gets 3× weight — standard BM25F short-field practice [^bm25f]. |
| **Feature match** | +0.20 | Count of `required_features` found in `listings.feature_*` flags, normalized | MVP uses structured flags only; text-derived features are v2. |
| **Price fit** | +0.20 | Triangle centered on candidate pool's p25/median/p75, picked by `price_sentiment` ∈ {cheap, moderate, premium} | Handles "not too expensive" without extra logic. |
| **Freshness** | +0.10 | Linear on `available_from` (60 d → 1.0, 365 d → 0.0) | Stale listings demoted. |
| **Negative penalty** | **−0.10** | Keyword hit from `plan.soft.negatives` (fuzzy match) OR structured flag violation | "Kein Erdgeschoss" → demote floor=0. |

Weights live in `app/participant/scoring_config.py` and can be tuned live.

Each top-10 listing gets a templated `reason`:
> *"Matches 3 rooms, Zurich, ≤ 2,800 CHF, balcony. BM25 rank 2/50. Slight price premium vs. candidate median."*

---

## 4. FTS5 tokenizer — one table, `unicode61`

**Single virtual table:**

```sql
CREATE VIRTUAL TABLE listings_fts USING fts5(
  title, description, street, city,
  content='listings', content_rowid='rowid',
  tokenize='unicode61 remove_diacritics 2'
);
INSERT INTO listings_fts(listings_fts) VALUES('rebuild');
```

**Trade-off** (acknowledged, mitigated): `unicode61` does NOT split German compounds (*Zimmerwohnung* ≠ *Zimmer*) [^sqlite-fts5]. Two mitigations in MVP, both cheap:

1. **Claude rewrites** — `QueryPlan.rewrites` carries 2–3 multilingual paraphrases. A user query *"3-Zimmer Altbauwohnung"* gets expanded to `["3-Zimmer Altbau", "apartment old building", "appartement ancien", ...]` and OR-joined in the `MATCH`. Published +3–15% NDCG@10 on BM25 from exactly this pattern [^query2doc] [^enrichindex].
2. **HTML-strip + lowercase** on description before indexing — handled in `bootstrap_participant.py`.

**If eval shows German-compound misses**, we add a second `trigram` FTS5 table on `description` in v1.1 — 30 min of work. Not worth the complexity for day-1 MVP.

---

## 5. Relaxation ladder

Triggered when hard-filter result count is 0. Each rung tried in order until count ≥ 5. Every step annotated into `meta.relaxations[]` so the user sees *what changed*.

| Rung | Action | Rationale |
|---|---|---|
| 1 | Expand price window ±10% | Most common cause of zero hits; small change |
| 2 | Drop `city`, keep `canton` | Still in the right region |
| 3 | Drop `canton` entirely | Wider radius |
| 4 | Expand `radius_km` by ×1.5 | For lat/lng-based queries |
| 5 | Drop `required_features` least-frequent-first | Gives up on rare combos last |

After rung 5, return `meta.clarification_needed = true` + empty listings. The UI can then render a "refine your search" chip row.

---

## 6. Shared contract — commit this FIRST

**Both people must agree on `QueryPlan` before any parallel work.** Everything downstream depends on its shape. Minimum viable form for MVP — fewer fields = more reliable Claude output.

Add to [app/models/schemas.py](app/models/schemas.py):

```python
from typing import Literal
from pydantic import BaseModel, Field

class NumRange(BaseModel):
    min_value: float | None = None
    max_value: float | None = None

class Feature(BaseModel):
    name: str                          # canonical: "balcony", "elevator", ...
    required: bool = True              # True → goes to SQL gate; False → soft only

class SoftPreferences(BaseModel):
    keywords: list[str] = Field(default_factory=list)       # free-text soft terms
    negatives: list[str] = Field(default_factory=list)      # "no ground floor"
    price_sentiment: Literal["cheap","moderate","premium"] | None = None
    features: list[Feature] = Field(default_factory=list)   # soft feature prefs

class QueryPlan(BaseModel):
    # Hard filters (→ SQL GATE)
    city: list[str] | None = None
    postal_code: list[str] | None = None
    canton: str | None = None
    price: NumRange = Field(default_factory=NumRange)
    rooms: NumRange = Field(default_factory=NumRange)
    latitude: float | None = None
    longitude: float | None = None
    radius_km: float | None = None
    offer_type: str | None = None
    object_category: list[str] | None = None
    required_features: list[str] = Field(default_factory=list)

    # Soft / ranking
    soft: SoftPreferences = Field(default_factory=SoftPreferences)

    # Multilingual rewrites for BM25 (2–3 paraphrases, DE/FR/IT/EN mix)
    rewrites: list[str] = Field(default_factory=list, max_length=3)

    # Original query + confidence for replay / reason generation
    raw_query: str
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)

    # Claude-flagged ambiguity
    clarification_needed: bool = False
    clarification_question: str | None = None
```

**Why these fields and not others:**
- No `source_span` per field (nice-to-have, deferred to v2 — adds tokens, slows Claude).
- No `landmarks` / `commute` (v2 needs GTFS to be meaningful).
- No `image_quality_targets` (v2 needs SigLIP-2).
- `rewrites` IS kept — free BM25 boost per [^query2doc].

Adapter function that returns a `HardFilters` from a `QueryPlan` (for `search_listings()`) lives in `query_plan.py` — both halves call it, shape is stable.

---

## 7. Task split — Roham / Mehrshad

**~10 h each**, balanced in complexity. Boundary: Roham owns everything **before** the SQL gate; Mehrshad owns everything **after**.

### 🅡 Roham — FRONT (query → allowed set)

| # | File | What | Hours |
|---|---|---|---|
| 1 | [app/models/schemas.py](app/models/schemas.py) | Add `NumRange`, `Feature`, `SoftPreferences`, `QueryPlan` classes (the contract above) | 0.5 |
| 2 | [pyproject.toml](pyproject.toml) | Add `anthropic>=0.40`, `rapidfuzz>=3.9`, `beautifulsoup4>=4.12`, `python-dotenv>=1.0` | 0.25 |
| 3 | **NEW** `app/participant/query_plan.py` | Claude Sonnet 4.6, `tool_choice={"type":"tool","name":"emit_query_plan"}`, `strict: true`, cached system prompt (pad with 8–12 few-shot to cross 1024-tok cache threshold), 5 s timeout, regex fallback for rooms/price/city. Includes `queryplan_to_hard_filters()` adapter. | 3 |
| 4 | [app/participant/hard_fact_extraction.py](app/participant/hard_fact_extraction.py) | Delegate to `query_plan.get_plan(query)` then return `queryplan_to_hard_filters(plan)` | 0.5 |
| 5 | [app/participant/soft_fact_extraction.py](app/participant/soft_fact_extraction.py) | Delegate to `query_plan.get_plan(query)` then return `plan.soft.model_dump() + {"rewrites": plan.rewrites, "raw_query": plan.raw_query}` | 0.5 |
| 6 | **NEW** `app/participant/bootstrap_participant.py` | `CREATE VIRTUAL TABLE IF NOT EXISTS listings_fts`; `INSERT INTO listings_fts(listings_fts) VALUES('rebuild')`; idempotent, callable on every startup | 1 |
| 7 | [app/main.py](app/main.py) | Add `bootstrap_participant(db_path)` call in `lifespan` right after `bootstrap_database` | 0.25 |
| 8 | [app/participant/soft_filtering.py](app/participant/soft_filtering.py) | Enforce `plan.soft.negatives` only — keyword hit + structured flag violation → drop. All else passes through. | 0.5 |
| 9 | **NEW** `eval/queries_mvp.jsonl` + `scripts/eval_mvp.py` | 15 queries: 3 clear-hard, 3 soft-heavy, 3 multilingual (DE/FR/IT), 3 landmark-ish, 3 adversarial. Runner reports HF-P + CSR + coverage + p50 latency (no LLM-judge for MVP — gold top-k only, hand-picked) | 2.5 |
| 10 | `tests/test_query_plan.py` | 10 canonical queries: assert extracted `rooms`, `max_price`, `city_slug` + `rewrites` length 2–3. Mock Claude with fixture | 1 |
| **Total** | | | **~10 h** |

**Roham's deliverable:** a query goes in, a `QueryPlan` comes out, SQL gate resolves the allowed set. Tests pass. FTS table exists in the DB.

---

### 🅜 Mehrshad — BACK (allowed set → ranked response)

| # | File | What | Hours |
|---|---|---|---|
| 1 | **NEW** `app/participant/retrieval.py` | `bm25_candidates(plan, allowed_ids, k=100) -> list[dict]` — build `MATCH` string from `plan.rewrites + plan.soft.keywords` (OR-joined, quoted for phrase safety). Use `bm25(listings_fts, 3.0, 1.0, 0.5, 0.5)` for title-weighted scoring. Filter to `allowed_ids` via `JOIN listings USING (listing_id)` subquery. | 2.5 |
| 2 | [app/participant/ranking.py](app/participant/ranking.py) | 4 positive + 1 negative signal (§3). `scoring_config.py` constants. Template `_render_reason(candidate, components, plan)` returning one-sentence explanation. | 2.5 |
| 3 | **NEW** `app/participant/relaxation.py` | `relax(plan) -> Iterator[HardFilters]` — yields successively relaxed filters per §5 ladder. | 2 |
| 4 | [app/harness/search_service.py](app/harness/search_service.py) | Rewire `query_from_text`: extract → SQL gate → retrieval → rank. On zero results, loop over `relax(plan)` and retry. Populate `meta = {"extracted_filters": ..., "relaxations": [...], "warnings": [...]}`. | 1.5 |
| 5 | `tests/test_retrieval_mvp.py` + `tests/test_ranking_mvp.py` + `tests/test_relaxation.py` | BM25 returns only allowed listings · ranking is monotonic in feature-match · relaxation yields ≥1 rung in expected order | 1 |
| 6 | [docker-compose.yml](docker-compose.yml) validation + cloudflared tunnel + README HTTPS pin | `docker compose up` smoke test, `npx cloudflared tunnel --url http://localhost:8000`, pin resulting URL at top of README | 0.5 |
| **Total** | | | **~10 h** |

**Mehrshad's deliverable:** allowed set goes in, ranked `ListingsResponse` with `meta` comes out. Tests pass. HTTPS URL reachable.

---

## 8. Dependency graph

```
            ┌─────────────────────────────────────┐
            │  COMMIT 0 (both, ~30 min)           │
            │  · QueryPlan schema                 │
            │  · pyproject.toml deps              │
            │  · empty module stubs committed     │
            └──────────────┬──────────────────────┘
                           │
           ┌───────────────┴───────────────┐
           │                               │
           ▼                               ▼
  ┌─────────────────┐         ┌──────────────────────┐
  │ ROHAM           │         │ MEHRSHAD             │
  │                 │         │                      │
  │ query_plan.py   │         │ retrieval.py          │
  │ (owns Claude)   │         │ (uses FTS — works    │
  │                 │         │  off a stub plan     │
  │ bootstrap       │         │  until Roham ships)  │
  │ _participant.py │◀────────│                      │
  │ (owns FTS table)│  needs  │ ranking.py            │
  │                 │  FTS    │ (uses BM25 output)   │
  │ main.py lifespan│         │                      │
  │ hook            │         │ relaxation.py         │
  │                 │         │                      │
  │ hard_fact /     │         │ search_service.py    │
  │ soft_fact       │◀────────│ (calls both halves)  │
  │ delegators      │   ITF   │                      │
  │                 │         │ tests_retrieval,     │
  │ soft_filtering  │         │ tests_ranking,       │
  │                 │         │ tests_relaxation     │
  │ eval/queries_   │         │                      │
  │ mvp.jsonl       │         │ docker + cloudflared │
  │                 │         │                      │
  │ test_query_plan │         │                      │
  └─────────────────┘         └──────────────────────┘
           │                               │
           └───────────────┬───────────────┘
                           │
                           ▼
           ┌───────────────────────────────────────┐
           │  INTEGRATION (both, ~1 h)             │
           │  · run eval_mvp.py end-to-end         │
           │  · fix any contract mismatches        │
           │  · verify HTTPS URL pinned in README  │
           └───────────────────────────────────────┘
```

**Critical bottleneck:** Roham's `bootstrap_participant.py` (creates FTS table) must land before Mehrshad can integration-test `retrieval.py`. Solution: Mehrshad stubs `retrieval.py` against a hand-seeded FTS table in `tests/fixtures/` while waiting. Neither blocks the other for more than ~30 min.

---

## 9. First 30 minutes (both, before splitting)

Run these commands together to land the shared contract:

```bash
# 1. sync env
cd /home/rohamzn/ETH_Uni/Datathon_2026
uv sync

# 2. Roham adds deps (commit 1)
# Edit pyproject.toml dependencies section:
#   anthropic>=0.40
#   rapidfuzz>=3.9
#   beautifulsoup4>=4.12
#   python-dotenv>=1.0
uv sync

# 3. Roham writes QueryPlan schema in app/models/schemas.py (commit 2)
#    (paste §6 contract verbatim)

# 4. Both: create empty module stubs (commit 3)
touch app/participant/query_plan.py \
      app/participant/bootstrap_participant.py \
      app/participant/retrieval.py \
      app/participant/relaxation.py \
      app/participant/scoring_config.py

# 5. sanity: does it still boot?
uv run uvicorn app.main:app --reload --port 8000
# ctrl-c, then split off and start working.
```

Commit message convention: `feat(mvp): <what>` with co-authors where applicable.

---

## 10. Tests

| Test file | Asserts | Owner |
|---|---|---|
| `tests/test_query_plan.py` | 10 canonical queries, Claude mocked with fixtures, check rooms/price/city extraction + rewrites length 2–3 + `confidence` ≥ 0.5 | Roham |
| `tests/test_retrieval_mvp.py` | BM25 MATCH returns only listings in `allowed_ids`; empty allowed → empty result; multilingual keyword finds DE listing for an EN query via rewrites | Mehrshad |
| `tests/test_ranking_mvp.py` | Score monotonic in feature-match count; negative-penalty demotes; top-1 has ≥ (top-k median + 2σ) when signal is clean | Mehrshad |
| `tests/test_relaxation.py` | Zero-result input yields expected rung order; stops when count ≥ 5; `meta.relaxations` populated | Mehrshad |
| `tests/test_integration_mvp.py` | 15 eval queries → non-empty results, all respect hard filters, p50 latency < 2 s on a warm cache | Both |

No LLM-as-judge in MVP — gold top-k are hand-picked from eval set by inspection. (Full judge protocol lands with v2.)

---

## 11. Deploy

### Local smoke
```bash
docker compose up --build -d
curl -s -X POST http://localhost:8000/listings \
  -H 'content-type: application/json' \
  -d '{"query":"3 room bright apartment in Zurich under 2800 CHF with balcony","limit":10}' | jq '.meta, .listings[0:3] | map({score,reason,listing:{id,city,price_chf,rooms}})'
```

Expected: non-empty listings, all city=Zurich, all rooms≈3, all price≤2800, all balcony=1, `meta.extracted_filters` populated.

### Public HTTPS (MVP minimum)
```bash
npx cloudflared tunnel --url http://localhost:8000
# copy printed https URL
```

Pin the URL at the top of [README.md](README.md) with a line like:
> **Live demo (MVP): https://some-random.trycloudflare.com**
> `POST /listings {"query":"..."}`

### Prod (nice-to-have, drop if tight)
Fly.io Frankfurt; secrets via `flyctl secrets set ANTHROPIC_API_KEY=...`. Only if cloudflared tunnel proves unstable during judging.

---

## 12. Accept / reject gate for moving to v2

Run the 15-query eval. **Stop-and-layer v2** if ANY of the following:

- **HF-P_overall < 0.85** — query understanding is leaking, fix Claude prompt before adding more.
- **CSR (strict) < 0.70** — hard filter has bugs; do not layer noise on top.
- **NDCG@10 < 0.30** — something fundamental is wrong (probably BM25 or FTS indexing).

**Ship-and-iterate if all ≥ thresholds.** Expected MVP numbers [^bm25-beir] [^miracl]:

| Metric | Realistic MVP | v2 target (ARCHITECTURE.md) |
|---|---|---|
| HF-P_overall | 0.85–0.95 | 0.95+ |
| CSR (strict) | 0.70–0.85 | 0.85+ |
| NDCG@10 | 0.35–0.45 | 0.55–0.65 |
| Coverage (≥5 hits) | 0.90 | 0.95+ |
| p50 latency | < 2 s | < 2.5 s |

---

## 13. Known limitations — what BM25-only will NOT solve

Be honest with the jury. These are **v2 territory**, not MVP bugs:

| Failure | Why BM25 can't fix it | v2 fix |
|---|---|---|
| *"bright, cozy, modern"* | BM25 matches tokens, not concepts | Dense embeddings + SigLIP-2 |
| *"good for remote work"* | Requires inference from description | Cross-encoder rerank |
| German-compound miss (*Zimmerwohnung ≠ Zimmer*) | `unicode61` doesn't split compounds | Claude rewrites mitigate ~50%; trigram FTS closes rest |
| Cross-lingual synonym | *"near station"* ≠ *"nah Bahnhof"* | Claude rewrites mitigate ~70%; dense closes rest |
| SRED rows with no city | 11,105 rows fail any city-based filter | Reverse geocoding (deferred) |

---

## 14. References

Evidence backing every concrete number in this doc:

[^bm25-beir]: [BM25 retrieval — BEIR benchmark summary](https://www.emergentmind.com/topics/bm25-retrieval) · [BEIR paper — arXiv 2104.08663](https://arxiv.org/abs/2104.08663). BM25 NDCG@10 average ≈ 0.434; cross-encoder rerank lifts to > 0.526.
[^miracl]: [MIRACL multilingual retrieval — Pyserini baselines](https://github.com/castorini/pyserini/blob/master/docs/experiments-miracl-v1.0.md) · [MIRACL paper — arXiv 2210.09984](https://arxiv.org/pdf/2210.09984). Hybrid BM25+mDPR wins on multilingual NDCG@10; BM25 alone on MIRACL French: 0.183 NDCG@10 / 0.653 R@100.
[^bm25f]: [BM25F tutorial (Robertson & Zaragoza)](https://www.researchgate.net/publication/308991534_A_Tutorial_on_the_BM25F_Model) · [Practical BM25 — Elastic](https://www.elastic.co/blog/practical-bm25-part-2-the-bm25-algorithm-and-its-variables). Title weighting ~3× description is standard short-field practice.
[^sqlite-fts5]: [SQLite FTS5 documentation](https://www.sqlite.org/fts5.html) · [FTS5 tokenizer choices](https://audrey.feldroy.com/articles/2025-01-13-SQLite-FTS5-Tokenizers-unicode61-and-ascii). `unicode61 remove_diacritics 2` handles DE/FR/IT/EN accent folding but does NOT split German compounds.
[^query2doc]: [Query2doc: Query Expansion with LLMs — EMNLP 2023](https://aclanthology.org/2023.emnlp-main.585.pdf). +3–15% NDCG@10 on BM25 from LLM-generated paraphrases.
[^enrichindex]: [EnrichIndex: LLM-augmented BM25 — CWCR 2025](https://cogcomp.seas.upenn.edu/papers/CWCR25.pdf). +11.8 NDCG@10 / +14.6 R@100 on BM25 with LLM-paraphrased index.
