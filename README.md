# Datathon 2026 · Robin Hybrid Listing Search

> **🌍 Live API (MVP)** — `https://tries-predict-identical-employer.trycloudflare.com`
>
> ```bash
> curl -X POST https://tries-predict-identical-employer.trycloudflare.com/listings \
>   -H 'content-type: application/json' \
>   -d '{"query":"3-room bright apartment in Zurich under 2800 CHF with balcony","limit":5}'
> ```
>
> _Default endpoint: `POST /listings` · `GET /health` · `POST /listings/search/filter`. URL is a
> no-login Cloudflare quick tunnel — if down, run `npx cloudflared tunnel --url http://localhost:8000`
> after `docker compose up`._

## MVP overview

Natural-language query → Claude `QueryPlan` (forced tool-use, strict JSON, multilingual
rewrites) → **SQL hard-filter GATE** → **BM25 (FTS5) over the allowed set** →
**4+1 signal linear rank** → **relaxation ladder** if zero hits. Every fallback path
emits a `[WARN]` log; no silent degradation.

- **[ARCHITECTURE.md](ARCHITECTURE.md)** — full v2 architecture (dense + rerank + enrichment + eval methodology)
- **[baseline_mvp.md](baseline_mvp.md)** — scope of this MVP baseline
- **[docs/pipeline-flowchart.md](docs/pipeline-flowchart.md)** — rendered PNG flowcharts
- **[eval/report_mvp.md](eval/report_mvp.md)** — latest 15-query eval output

### Quick start

```bash
# Conda env (matches environment.yml)
conda env create -f environment.yml
conda activate datathon2026

# .env (needs ANTHROPIC_API_KEY at minimum; AWS creds for S3 images)
cp .env.example .env  # then edit

# DB + FTS bootstrap happens automatically on first request
uvicorn app.main:app --host 0.0.0.0 --port 8000

# Eval
python scripts/eval_mvp.py --out eval/report_mvp.md
```

Or with docker:

```bash
docker compose up --build -d
curl http://localhost:8000/health
```

### Latest eval headline (15 stratified queries)

| Metric                                | Value     | Target                                           |
| ------------------------------------- | --------- | ------------------------------------------------ |
| HF-P (hard-filter precision, overall) | **1.000** | ≥ 0.85                                           |
| CSR (constraint satisfaction, strict) | **1.000** | ≥ 0.70                                           |
| Coverage (≥5 hits)                    | 0.867     | ≥ 0.90                                           |
| p50 latency                           | 6.3 s     | < 2.5 s (bottleneck: Claude call; v2 caches)     |

## Starter harness context

This repository is a minimal starter harness for participants building listing search and ranking systems.

Using this harness is optional. You are free to build your submission with it, adapt only parts of it, or implement your own solution independently.

It gives you:

- a FastAPI server
- a minimal Apps SDK / MCP app
- a Vite + React widget app
- committed listings CSVs under `raw_data/`
- optional raw source bundles under `raw_data/` that can be normalized into harness CSVs
- automatic CSV -> SQLite bootstrap on startup
- a simple hard-filter search module
- stub extraction, soft filtering, and ranking flow
- Docker and Docker Compose setup

The important point: this is a starter harness, not a reference solution and not the required submission format. Use it only if it helps your team move faster.

## The Data

Download the challenge data bundle from the organizer-provided link, then extract `raw_data.zip` into the root of this repository. The starter harness expects that layout.

There are data with and without images.
With images:

- robinreal
- sred (montage) <- these are missing addresses, but have lat long, you may use geo reverse search as e.g. https://nominatim.org/ has
- structured

## Where To Edit

If you choose to use the starter harness, the main participant-owned extension points are under `app/participant/`:

- `hard_fact_extraction.py`
- `soft_fact_extraction.py`
- `soft_filtering.py`
- `ranking.py`
- `listing_row_parser.py`

Starter-harness glue code lives under `app/harness/`:

- `search_service.py`
- `bootstrap.py`
- `csv_import.py`

Those files handle orchestration, startup wiring, and import flow.

## Quick Start

### Run locally

Install dependencies:

```bash
uv sync --dev
```

Start the API:

```bash
uv run uvicorn app.main:app --reload
```

The API will be available at:

```text
http://localhost:8000
```

### Run with Docker

```bash
docker compose up --build
```

This starts the API on port `8000`.

The SQLite database is built automatically from the committed CSVs in `raw_data/` on first startup and stored in the mounted `/data` volume.

If the SRED raw bundle is present under `raw_data/SRED_data(1)`, the harness also generates `raw_data/sred_data.csv` from the `*_with_text.csv` files, flattens the needed montage images into `raw_data/sred_images/`, and serves them locally under `/raw-data-images/<platform_id>.jpeg`.

## Apps SDK MCP App

This repository also includes a minimal split MCP app for ChatGPT and other MCP Apps-compatible clients such as Claude Desktop / Web.

Shape:

- FastAPI harness: data service
- `apps_sdk/server`: MCP bridge
- `apps_sdk/web`: Vite + React widget

The MCP app is intentionally thin:

- one tool only: `search_listings`
- no authentication
- no write actions
- one combined UI with ranked list + map

You may extend it, but you do not need to. The challenge focus is search quality, not Apps SDK integration.

### Build the widget

```bash
cd apps_sdk/web
npm install
npm run build
```

### Run the MCP app

In one shell, run the FastAPI harness:

```bash
uv run uvicorn app.main:app --reload --port 8000
```

In another shell, run the MCP server:

```bash
uv run uvicorn apps_sdk.server.main:app --reload --port 8001
```

The MCP endpoint is:

```text
http://localhost:8001/mcp
```

For tunnel testing, the minimal env setup is:

```bash
export APPS_SDK_LISTINGS_API_BASE_URL=http://localhost:8000
export APPS_SDK_PUBLIC_BASE_URL=https://your-public-url
```

The widget HTML uses `APPS_SDK_PUBLIC_BASE_URL` to build its JS and CSS asset URLs. If this stays at the default `http://localhost:8001`, remote MCP hosts can reach the server but still fail to load the widget assets.

These are the MCP-related env vars used by the split app:

```bash
export APPS_SDK_LISTINGS_API_BASE_URL=http://localhost:8000
export APPS_SDK_PUBLIC_BASE_URL=https://your-public-url
export MCP_ALLOWED_HOSTS=your-public-host
export MCP_ALLOWED_ORIGINS=https://your-public-url
```

Meaning:

- `APPS_SDK_LISTINGS_API_BASE_URL`: where the MCP server calls the local FastAPI harness
- `APPS_SDK_PUBLIC_BASE_URL`: the public origin used for widget JS/CSS asset URLs
- `MCP_ALLOWED_HOSTS`: optional public hostname allowlist for MCP transport protection
- `MCP_ALLOWED_ORIGINS`: optional public HTTPS origin allowlist for MCP transport protection

For local development and simple Cloudflare tunnel testing, leave `MCP_ALLOWED_HOSTS` and `MCP_ALLOWED_ORIGINS` unset. If you set them incorrectly, the MCP server can reject requests with `421 Misdirected Request`.

### Testing in ChatGPT or other MCP Apps clients

https://developers.openai.com/apps-sdk/deploy/testing (requires active subscription)
https://modelcontextprotocol.io/extensions/apps/build#testing-with-claude

For local testing in either client, expose the MCP server with a tunnel and point the client to:

```text
https://your-public-url/mcp
```

#### `cloudflared` example

Start the FastAPI harness and MCP server locally first:

```bash
uv run uvicorn app.main:app --reload --port 8000
uv run uvicorn apps_sdk.server.main:app --reload --port 8001
```

In another shell, open a tunnel to the MCP server:

```bash
npx cloudflared tunnel --url http://localhost:8001
```

`cloudflared` will print a public URL like:

```text
https://random-name.trycloudflare.com
```

Then export:

```bash
export APPS_SDK_LISTINGS_API_BASE_URL=http://localhost:8000
export APPS_SDK_PUBLIC_BASE_URL=https://random-name.trycloudflare.com
```

Then restart the MCP server so it picks up the env vars:

```bash
uv run uvicorn apps_sdk.server.main:app --reload --port 8001
```

Register this MCP URL in ChatGPT or another MCP Apps client:

```text
https://random-name.trycloudflare.com/mcp
```

For pure local development and simple tunnel testing, the server accepts requests when those variables are unset. Only add them if you specifically want stricter host/origin enforcement and know the exact values you need.

### Smoke test the MCP server

You can run a small protocol-level smoke test before connecting a real host. It checks:

- `initialize`
- `tools/list`
- `resources/list`
- `resources/read`

First build the widget and start the MCP server, then run:

```bash
uv run python scripts/mcp_smoke.py --url http://localhost:8001/mcp
```

If it passes, you know the MCP server is serving the `search_listings` tool and the widget resource with the expected metadata shape.

## API

### `GET /health`

Simple health check.

Example:

```bash
curl http://localhost:8000/health
```

### `POST /listings`

High-level challenge entrypoint.

This endpoint accepts only the natural-language user query and sends it through the full harness flow:

```text
query
-> extract_hard_facts
-> extract_soft_facts
-> filter_hard_facts
-> filter_soft_facts
-> rank_listings
```

Important:

- by default, `extract_hard_facts` is a stub and does not interpret the query
- by default, soft filtering and ranking are placeholders
- this endpoint exists to show the intended flow, not to provide a real baseline

Example request:

```bash
curl -X POST http://localhost:8000/listings \
  -H "content-type: application/json" \
  -d '{
    "query": "3 room bright apartment in Zurich under 2800 CHF",
    "limit": 25,
    "offset": 0
  }'
```

If you omit `limit`, the harness defaults to returning the top `25` listings. Since query understanding is stubbed by default, this makes the endpoint immediately usable for UI and Apps SDK testing.

### `POST /listings/search/filter`

Low-level search entrypoint.

This endpoint accepts only explicit hard filters. It is useful if you want to call the structured search directly, for example from your own app, service, or MCP tool.

Example request:

```bash
curl -X POST http://localhost:8000/listings/search/filter \
  -H "content-type: application/json" \
  -d '{
    "hard_filters": {
      "city": ["Winterthur"],
      "features": ["child_friendly"],
      "latitude": 47.4988,
      "longitude": 8.7237,
      "radius_km": 5,
      "min_price": 1000,
      "max_price": 3000,
      "min_rooms": 2.0,
      "max_rooms": 4.5,
      "limit": 5,
      "offset": 0,
      "sort_by": "price_asc"
    }
  }'
```

### Response format

Both endpoints in the starter harness currently return a wrapper object in this shape:

```json
{
  "listings": [
    {
      "listing_id": "123",
      "score": 1.0,
      "reason": "Matched hard filters; soft ranking stub.",
      "listing": {
        "id": "123",
        "title": "Example listing",
        "city": "Zurich",
        "latitude": 47.37,
        "longitude": 8.54,
        "price_chf": 2500,
        "rooms": 3.0
      }
    }
  ],
  "meta": {}
}
```

The `listings` key contains the ranked results. The `meta` key is intentionally left open so teams can add extracted filters, debug info, or other useful response metadata later.

## Supported Hard Filters

The default hard-filter implementation supports simple structured filters over the SQLite database:

- `city`
- `postal_code`
- `canton`
- `min_price`
- `max_price`
- `min_rooms`
- `max_rooms`
- `latitude`
- `longitude`
- `radius_km`
- `features`
- `offer_type`
- `object_category`
- `limit`
- `offset`
- `sort_by`

This logic is intentionally simple and isolated so teams can replace it easily.

## Where To Customize

If you want to build your submission on top of this starter, the main extension points are:

- [app/participant/hard_fact_extraction.py](app/participant/hard_fact_extraction.py)
  Stub for natural-language hard fact extraction.
- [app/participant/soft_fact_extraction.py](app/participant/soft_fact_extraction.py)
  Stub for extracting softer preferences from the query.
- [app/participant/soft_filtering.py](app/participant/soft_filtering.py)
  Stub for post-filtering candidates after hard filtering.
- [app/participant/ranking.py](app/participant/ranking.py)
  Stub ranking logic and result shaping.
- [app/participant/listing_row_parser.py](app/participant/listing_row_parser.py)
  CSV-row parsing and feature extraction logic.
- [app/core/hard_filters.py](app/core/hard_filters.py)
  The current structured filter implementation over SQLite.
- [app/harness/search_service.py](app/harness/search_service.py)
  High-level orchestration between extraction, filtering, and ranking.
- [app/harness/bootstrap.py](app/harness/bootstrap.py)
  Database bootstrap lifecycle.
- [app/harness/csv_import.py](app/harness/csv_import.py)
  CSV import and schema/index creation.
- [app/core/s3.py](app/core/s3.py)
  Helper functions for loading listing image URLs from S3 by `listing_id`.
- [apps_sdk/server/main.py](apps_sdk/server/main.py)
  Minimal MCP Apps bridge exposing the single `search_listings` tool.
- [apps_sdk/web/src/App.tsx](apps_sdk/web/src/App.tsx)
  Combined ranked-list plus map widget.
- [app/api/routes/listings.py](app/api/routes/listings.py)
  API surface for the two listing endpoints.

## Project Structure

```text
app/
  api/routes/listings.py     API endpoints
  core/hard_filters.py       hard-filter search logic
  core/s3.py                 S3 image helper functions
  harness/bootstrap.py       database bootstrap lifecycle
  harness/csv_import.py      CSV -> SQLite import helpers
  harness/search_service.py  high-level orchestration
  models/schemas.py          request/response models
  participant/               participant-editable logic
apps_sdk/
  server/                    MCP Apps bridge
  web/                       Vite React widget app
raw_data.zip                 GET THIS FROM S3
tests/                       basic harness tests
docker-compose.yml           local container runtime
```

## Development

Run the tests:

```bash
uv run pytest tests -q
```

If you want to rebuild the SQLite database from scratch, remove the generated database file or clear the mounted Docker volume and restart the service.

## AWS Credentials

The S3 helper in `app/core/s3.py` uses `boto3` and the standard AWS credential chain. For example:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=eu-central-2
```

Optional S3 config:

```bash
export LISTINGS_S3_BUCKET=crawl-data-951752554117-eu-central-2-an
export LISTINGS_S3_REGION=eu-central-2
export LISTINGS_S3_PREFIX=prod
```

## Download All Images From S3

If you want a full local copy of the listing images, the simplest option is to copy the whole `prod/` prefix with the AWS CLI. The image files are stored under paths like `prod/<source>/images/...`, so starting from the `prod/` root will include every image tree.

Example:

```bash
export AWS_DEFAULT_REGION=eu-central-2
export LISTINGS_S3_BUCKET=crawl-data-951752554117-eu-central-2-an
export LISTINGS_S3_PREFIX=prod

aws s3 cp \
  "s3://${LISTINGS_S3_BUCKET}/${LISTINGS_S3_PREFIX}/" \
  ./downloads/prod \
  --recursive
```
