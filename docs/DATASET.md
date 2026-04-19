# Dataset bundle

Pre-built runtime artifacts — DB, embeddings, landmarks — so you don't have to rebuild from raw CSVs. Extract at the repo root; every path inside the zip is already repo-relative.

---

## Download

**🔗 [datathon2026_dataset.zip (~2.5 GB) — Google Drive](https://drive.google.com/file/d/1rxYd17eX7C99AgNVlJft1QIFGtwogxX8/view?usp=sharing)**

CLI (handles Drive's large-file confirm step automatically):

```bash
uv run --with gdown gdown 1rxYd17eX7C99AgNVlJft1QIFGtwogxX8 -O datathon2026_dataset.zip
```

---

## Install

```bash
cd /path/to/Datathon_2026
unzip -o datathon2026_dataset.zip

# 2. sanity-check expected files landed
test -f data/listings.db                                  && echo "OK listings.db"
test -f data/ranking/embeddings.fp16.npy                  && echo "OK arctic text emb"
test -f data/ranking/embeddings_ids.json                  && echo "OK arctic ids"
test -f data/ranking/landmarks.json                       && echo "OK landmarks"
test -f image_search/data/full/store/embeddings.fp32.npy  && echo "OK SigLIP emb"
test -f image_search/data/full/store/floorplans.fp32.npy  && echo "OK SigLIP floorplans"
test -f image_search/data/full/store/index.sqlite         && echo "OK SigLIP index"
test -f image_search/data/full/dinov2_store/main.fp32.npy        && echo "OK DINOv2 emb"
test -f image_search/data/full/dinov2_store/floorplans.fp32.npy  && echo "OK DINOv2 floorplans"
test -f image_search/data/full/dinov2_store/index.sqlite         && echo "OK DINOv2 index"

# 3. verify row counts (expected values below)
python -c "
import sqlite3
con = sqlite3.connect('data/listings.db')
for tbl, want in [('listings', 25546), ('listings_enriched', 25546),
                  ('listings_ranking_signals', 25546), ('listing_commute_times', 125396)]:
    got = con.execute(f'SELECT COUNT(*) FROM {tbl}').fetchone()[0]
    print(f'{tbl}: got={got} want={want} {\"OK\" if got == want else \"MISMATCH\"}')"
```

If any `test -f` line prints nothing — the zip was incomplete. Stop and re-download.

---

## What's in the zip (and where each file must live)

All paths below are **repo-relative** — they match the zip's internal structure exactly. `unzip` at the repo root will place them correctly.

### 1. `data/listings.db` (463 MB)

The single source of truth for the website. Contains 4 tables:

| table | rows | what's it for |
| --- | ---: | --- |
| `listings` | 25,546 | raw corpus (Comparis + SRED + ROBINREAL) |
| `listings_enriched` | 25,546 | 41 fields × 4 provenance cols (pass 0-3 + pass 2b) |
| `listings_ranking_signals` | 25,546 | 30 derived ranking signals |
| `listing_commute_times` | 125,396 | real r5py GTFS commute minutes for every (listing, landmark < 40 km) pair |

Consumed by: [`app/config.py:30`](../app/config.py#L30), all of `ranking/`, all of `enrichment/`.

### 2. Arctic-Embed text embeddings

```text
data/ranking/embeddings.fp16.npy     (50 MB, shape (25546, 1024), float16)
data/ranking/embeddings_ids.json     (260 KB — listing_id ↔ row-index map)
```

Consumed by [`ranking/runtime/embedding_search.py:28-29`](../ranking/runtime/embedding_search.py#L28-L29). Model: `Snowflake/snowflake-arctic-embed-l-v2.0`. Queries need the `"query: "` prefix; documents do not.

### 3. Landmarks gazetteer

```text
data/ranking/landmarks.json                    (24 KB — 45 curated Swiss landmarks: unis, HBs, lakes, employers)
data/ranking/landmarks_mined_candidates.json   (16 KB — GPT-5.4-nano proposals, kept for provenance)
```

Consumed by [`app/core/landmarks.py:24`](../app/core/landmarks.py#L24) and the r5py commute matrix builder.

### 4. SigLIP-2 Giant image embeddings (primary visual search)

```text
image_search/data/full/store/embeddings.fp32.npy   (414 MB — listing images)
image_search/data/full/store/floorplans.fp32.npy   (3.7 MB — floorplan images)
image_search/data/full/store/index.sqlite          (23 MB — image_id ↔ row-index map)
```

Consumed by [`app/core/visual_search.py:82-84`](../app/core/visual_search.py#L82-L84). Model: `google/siglip2-giant-opt-patch16-384`.

### 5. DINOv2-Giant image embeddings (re-ranker)

```text
image_search/data/full/dinov2_store/main.fp32.npy        (276 MB — 1024-d global descriptors)
image_search/data/full/dinov2_store/floorplans.fp32.npy  (2.5 MB)
image_search/data/full/dinov2_store/index.sqlite         (8.4 MB)
```

Consumed by [`app/core/dinov2_search.py:37-38`](../app/core/dinov2_search.py#L37-L38).

---

## What's NOT in the zip (and why)

- **Listing image thumbnails** (`data/image_cache/`, `image_search/data/full/samples/`) — user directive excluded these. SigLIP/DINOv2 embeddings already derived; the site renders image URLs from the source CSVs, not local thumbnails.
- **OSM PBF** (`data/ranking/osm/switzerland-latest.osm.pbf`, 506 MB) — only needed to *rebuild* the commute matrix. The matrix itself is already inside `data/listings.db → listing_commute_times`. If a colleague wants to regenerate: `curl -L -o data/ranking/osm/switzerland-latest.osm.pbf https://download.geofabrik.de/europe/switzerland-latest.osm.pbf`.
- **GTFS feed** (`data/ranking/gtfs/`, 1.2 GB) — same reason; re-derivable from the 2026 Swiss GTFS feed. Already materialized into `listing_commute_times`.
- **Enrichment GPT caches** (`enrichment/data/cache/*.jsonl`, ~60 MB) — only needed to *rerun* enrichment idempotently without paying OpenAI again. The enriched fields are already in `listings_enriched`.
- **`data/users.db`** — auth state, per-deployment; each colleague starts with a fresh one.

---

## After extraction — smoke test

```bash
# 1. launch the app
uv run uvicorn app.main:app --reload

# 2. health check
curl -s http://localhost:8000/health

# 3. natural-language query
curl -s -X POST http://localhost:8000/listings \
     -H content-type:application/json \
     -d '{"query":"modern bright apartment near HB Zurich","limit":5}'
```

If startup logs `[WARN] dinov2_load_failed` the DINOv2 store didn't land at [`image_search/data/full/dinov2_store/`](../image_search/data/full/dinov2_store/) — re-check step 2.

---

## Integrity checksums (optional)

```bash
sha256sum data/listings.db \
          data/ranking/embeddings.fp16.npy \
          image_search/data/full/store/embeddings.fp32.npy \
          image_search/data/full/dinov2_store/main.fp32.npy
```

Expected values are printed at the end of the upstream build in each script's `[INFO]` log line.

---

**Provenance.** This bundle is the exact set of files the repo consumed at commit `HEAD` on 2026-04-19. Regenerate with the per-layer build recipes documented in [`enrichment/README.md`](../enrichment/README.md) (Layer 1), [`ranking/README.md`](../ranking/README.md) (Layer 2 + embeddings + commute matrix), and [`image_search/README.md`](../image_search/README.md) (SigLIP + DINOv2 indexes). End-to-end rebuild walk-through in [`docs/DEVELOPMENT.md`](DEVELOPMENT.md).
