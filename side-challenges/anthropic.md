# How We Used Claude

Two distinct roles: Claude as the **builder** of this project, and Claude as a first-class **runtime client** via MCP.

P.S. As a cherry on top, this report itself was also written by Claude given the data that we provided it!

## 1. Claude as the builder (design + implementation)

We drove nearly the entire build loop through Claude (Claude Code in the CLI + Claude in Cursor).

- **Behavioral contract.** `CLAUDE.md` at the repo root is a small rules file we wrote for Claude: think-before-coding, simplicity first, surgical edits, goal-driven execution, and the project-wide *no silent fallbacks* rule (every fallback must emit a `[WARN]` line). Claude followed it consistently across sessions — grep `CLAUDE.md §5` across the code to see how that rule shows up in `app/config.py`, `app/harness/search_service.py`, `app/memory/rankings.py`, etc.
- **Design docs written with Claude.** `ARCHITECTURE.md`, `baseline_mvp.md`, `Further Data Plan.md`, `mahbod/PLAN.md`, the pipeline flowchart under `docs/`, and the per-session `_context/` reports (STATUS, NEXT_STEPS, SESSION_SUMMARY, BUNDLE_REPORT) were all drafted collaboratively with Claude and then edited by hand.
- **Scaffolding + features.** Claude produced first-pass implementations of the FastAPI harness extensions, the hard-filter SQLite layer, the enrichment passes (`enrichment/scripts/pass0…pass4`), the ranking pipeline (`ranking/scripts/t1…t4`), the visual search pieces under `image_search/`, and the MCP widget glue under `apps_sdk/`.
- **Review + refactor loop.** For larger changes we used Claude to propose a plan, critique it, implement, then re-read diffs and fix linter/test feedback. `tests/` was grown the same way.
- **Reports and the presentation.** `analysis/REPORT.md`, `enrichment/FINAL_REPORT.md`, `QUERY_EVALUATION_2026-04-19.md`, and the Typst source for `presentation/deck.typ` were drafted with Claude from our raw notes and evaluation outputs.

## 2. Claude as a runtime client (MCP Apps SDK)

The project ships a minimal **MCP app** (`apps_sdk/server/` + `apps_sdk/web/`) that exposes a single `search_listings` tool and a combined ranked-list + map widget. It is registerable from Claude Desktop / Claude Web, not just ChatGPT.

- A user types a natural-language query in Claude.
- Claude calls the `search_listings` tool over MCP.
- Our FastAPI harness runs the `query → hard facts → hard filter → soft facts → soft filter → rank` flow and returns ranked listings + metadata.
- Claude renders the widget inline, with list + map on Swiss coordinates.

Local testing path is the standard one: run the FastAPI harness on `:8000`, the MCP server on `:8001`, tunnel with `cloudflared`, and register the public `/mcp` URL inside Claude.

## What we did *not* do (honesty note)

`Further Data Plan.md` scopes out an Anthropic-API path for runtime query planning and Claude Vision reranking on hero images (brightness / modernity / view / kitchen quality). That plan is documented but **not** wired into the runtime — `anthropic` is not a dependency, and there are no direct API calls in `app/`, `enrichment/`, or `ranking/`. All current runtime Claude usage is through MCP, not through the API SDK.
