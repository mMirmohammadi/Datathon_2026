"""Pass 4 — mine landmark mentions from listing descriptions via gpt-5.4-nano.

Output is a read-only JSONL cache (`enrichment/data/cache/gpt_landmark_mining.jsonl`).
It does NOT touch the DB — a downstream aggregator
(`ranking/scripts/t1_landmarks_aggregate.py`) consumes the cache to decide which
landmarks to geocode and add to `data/ranking/landmarks.json`.

Why mine instead of hand-curating:
  * Our 30-entry gazetteer reflects OUR guess at what users want. Listing
    descriptions reflect what LANDLORDS advertise proximity to — which is
    exactly what users will query for.
  * gpt-5.4-nano is ~4× cheaper than mini and plenty for named-entity
    recognition from German/French/Italian/English descriptions.

Cost bound (25,546 listings × ~150 input / 100 output tokens @ gpt-5.4-nano
pricing): input 25,546 × 150 × $0.20 / 1M = $0.77; output 25,546 × 100 ×
$1.25 / 1M = $3.19; total **~$4** per full run. Cache keeps re-runs free.

Hard rules (CLAUDE.md §5 — NO silent fallbacks):
  * Every transient-error retry, every non-200, every empty parse emits
    `[WARN]` with context / expected / got / fallback.
  * No listing is written to cache unless GPT returned a valid ListingLandmarks
    payload (even if `mentions=[]` — that's a valid answer and must be cached
    so we don't re-call).

Usage:
    python -m enrichment.scripts.pass4_landmark_mining --db data/listings.db
    python -m enrichment.scripts.pass4_landmark_mining --db data/listings.db --limit 50    # smoke

Env:
    OPENAI_API_KEY              required
    OPENAI_LANDMARK_MODEL       default: gpt-5.4-nano-2026-03-17 (pinned snapshot)
    GPT_PASS4_CONCURRENCY       default: 16
    GPT_PASS4_MAX_CALLS         default: 26000  (budget hard-cap per run)
    GPT_PASS4_TIMEOUT_S         default: 60
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Literal, Optional

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field

from enrichment.common.db import connect
from enrichment.common.langdet import strip_html

load_dotenv()

# --- configuration -----------------------------------------------------------

MODEL = os.getenv("OPENAI_LANDMARK_MODEL", "gpt-5.4-nano-2026-03-17")
CONCURRENCY = int(os.getenv("GPT_PASS4_CONCURRENCY", "16"))
MAX_CALLS = int(os.getenv("GPT_PASS4_MAX_CALLS", "26000"))
TIMEOUT_S = float(os.getenv("GPT_PASS4_TIMEOUT_S", "60"))

CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "cache"
CACHE_PATH = CACHE_DIR / "gpt_landmark_mining.jsonl"

# The vocabulary we ask the model to pick from. These map 1:1 to the
# `kind` column of `data/ranking/landmarks.json` + a few extras that surface
# in descriptions but weren't in our hand-curated set.
LandmarkKind = Literal[
    "transit",        # Bahnhof, HB, station, gare, stazione
    "university",     # ETH, EPFL, UZH, HSG, FHNW, etc.
    "school",         # Gymnasium, Primarschule, Kantonsschule
    "employer",       # Roche, Novartis, Google, UBS, CS, Nestle, major industrial sites
    "shopping",       # Sihlcity, Letzipark, Glattzentrum, named malls
    "park",           # Named parks, Volksgarten, Bürkliplatz, Englischer Garten
    "hospital",       # Named hospitals, Kantonsspital, USZ, Inselspital
    "neighborhood",   # Oerlikon, Seefeld, Plainpalais, Kreis 4, Paradeplatz
    "cultural",       # Opernhaus, KKL, Kunsthaus, named museums, landmarks
    "lake",           # Zürichsee, Genfersee, Bodensee, Luganersee
    "other",          # airport, stadium, etc. — model decides; aggregator filters
]

# --- Pydantic output schema --------------------------------------------------

class LandmarkMention(BaseModel):
    """A single named landmark mentioned in a listing description.

    We ask the model for `canonical` explicitly so downstream dedupe doesn't
    have to re-normalise 25,546 free-form strings. The model has a much better
    sense of "ETH Zürich" vs "ETHZ" vs "ETH Zentrum" being the same entity
    than rapidfuzz does.
    """
    model_config = ConfigDict(extra="forbid")

    name: str = Field(description="The exact surface form the listing used "
                                  "(preserve capitalisation / diacritics).")
    kind: LandmarkKind
    canonical: str = Field(description="Lowercase snake_case canonical form "
                                       "(e.g. 'eth_zurich', 'stadelhofen_bahnhof', "
                                       "'lake_zurich', 'kreis_4_zurich').")


class ListingLandmarks(BaseModel):
    """Per-listing output. Empty `mentions` is valid and common (e.g. SRED
    listings that describe only the apartment's interior with no external
    context)."""
    model_config = ConfigDict(extra="forbid")

    mentions: list[LandmarkMention]


# --- system prompt -----------------------------------------------------------

_SYSTEM_PROMPT = """You extract named physical LANDMARKS from Swiss real-estate listing descriptions.

A landmark is a SPECIFIC NAMED PLACE a person could locate on a map, that the listing uses to signal proximity. The listing text is in German, French, Italian, or English.

WHAT COUNTS (examples — these are illustrative, not exhaustive):
- transit: Hauptbahnhof, HB Zürich, Stadelhofen, Bahnhof Bern, gare Cornavin, stazione Lugano, SBB stations, tram/bus stops if named specifically, airports (Zürich Flughafen, Genève Aéroport)
- university: ETH, ETH Hönggerberg, EPFL, UZH, Universität Basel, HSG, USI Lugano, FHNW
- school: Gymnasium Rämibühl, Kantonsschule, Primarschule X, named schools
- employer: Roche, Novartis, Google (Zürich), UBS, Credit Suisse, CS, Nestlé, ABB, Swisscom, Siemens, Lonza, named industrial parks
- shopping: Sihlcity, Letzipark, Glattzentrum, Stücki Park, Marktplatz (if acting as a named shopping district), Bahnhofstrasse (shopping)
- park: named parks (Englischer Garten, Bürkliplatz, Volksgarten, Parc des Bastions), named lake promenades
- hospital: USZ (Unispital Zürich), Inselspital, Kantonsspital X, named clinics
- neighborhood: Oerlikon, Altstetten, Schlieren, Seefeld, Paradeplatz, Plainpalais, Carouge, Kreis 3/4/5, Quartier X
- cultural: Opernhaus, Kunsthaus, KKL, Schauspielhaus, named museums, named theaters, Old Town / Altstadt as a neighborhood
- lake: Zürichsee, Genfersee, Lac Léman, Bodensee, Luganersee, Zugersee, Thunersee, Vierwaldstättersee
- other: airports (if you didn't catch them under transit), stadiums, convention centers

WHAT DOES NOT COUNT:
- Generic descriptors: "Stadtzentrum" / "city centre" / "near the lake" (without naming it) / "close to the forest"
- Cities themselves (Zürich, Bern, Basel) — UNLESS they are a named sub-district that functions as a landmark (e.g. "Zürich Altstadt" = cultural landmark; "Zürich" alone = not a landmark)
- The listing's own street address
- Distance / travel-time phrases: "5 min zu Fuss", "gute Verbindung" (we extract landmarks, not distances)
- Adjectives: "ruhig", "hell", "modern", "familienfreundlich"

CANONICAL FORM RULES:
- Lowercase snake_case.
- Strip language-specific connectors: "ETH Zürich" -> "eth_zurich" (not "eth_zuerich")
- Keep the city qualifier when needed for disambiguation: "hb_zurich", "hb_geneve", "kantonsspital_zuerich"
- Diacritics: drop (ASCII fold) — "zürichsee" -> "zurichsee"
- Variants of the same entity MUST get the same canonical:
    "ETH", "ETH Zentrum", "ETHZ", "ETH Zürich" -> all "eth_zentrum"
    "ETH Hönggerberg", "ETH Höngg" -> "eth_hoenggerberg"
    "Zürich HB", "HB Zürich", "Hauptbahnhof Zürich" -> "hb_zurich"
    "Zürichsee", "Lake Zurich", "Lac de Zurich" -> "lake_zurich"
    "Kreis 3" in Zürich context -> "kreis_3_zurich"
- Return "other" as kind if the category is unclear; the aggregator will filter.

OUTPUT RULES:
- Return a JSON object with exactly one key: `mentions`, value is a list.
- Empty list is valid and common — if the description has no named landmarks, return `{"mentions":[]}`.
- Deduplicate WITHIN one listing: if "ETH" is mentioned 3 times, emit it ONCE.
- Maximum 12 mentions per listing (long descriptions sometimes list many — pick the most prominent).
- Do not invent landmarks that aren't in the text.
"""


# --- cache helpers (mirror pass2) -------------------------------------------

def _cache_key(listing_id: str, model: str) -> str:
    return f"{listing_id}|{model}"


def _load_cache() -> dict[str, dict[str, Any]]:
    """JSONL append-only cache; last write for a given key wins."""
    cache: dict[str, dict[str, Any]] = {}
    if not CACHE_PATH.exists():
        return cache
    try:
        with CACHE_PATH.open("r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    cache[_cache_key(rec["listing_id"], rec["model"])] = rec
                except (json.JSONDecodeError, KeyError) as exc:
                    print(
                        f"[WARN] pass4_landmark_mining._load_cache: expected=valid jsonl, "
                        f"got={type(exc).__name__} at line {line_no}, fallback=skip line",
                        flush=True,
                    )
    except OSError as exc:
        print(
            f"[WARN] pass4_landmark_mining._load_cache: expected=readable {CACHE_PATH}, "
            f"got={exc!r}, fallback=empty cache",
            flush=True,
        )
    return cache


def _append_cache_line(rec: dict[str, Any]) -> None:
    """Append one cache record. Uses O_APPEND (atomic for <PIPE_BUF on POSIX)."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    line = (json.dumps(rec, ensure_ascii=False, default=str) + "\n").encode("utf-8")
    fd = os.open(str(CACHE_PATH), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, line)
    finally:
        os.close(fd)


# --- OpenAI client ----------------------------------------------------------

def _make_client():
    try:
        from openai import AsyncOpenAI
    except ImportError as exc:
        raise RuntimeError(
            f"openai package not importable: {exc!r}. Install in the conda env."
        )
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY missing from env. Set it in .env or export it."
        )
    return AsyncOpenAI(api_key=api_key, timeout=TIMEOUT_S)


# --- single listing call ----------------------------------------------------

async def _mine_one(
    client, listing_id: str, title: str, description: str,
) -> dict[str, Any] | None:
    """Call GPT on one listing; return cached-shape dict or None on terminal fail.

    3-try exponential backoff on transient OpenAI errors. Other exceptions
    (auth, bad schema, refusal) fail fast.
    """
    try:
        from openai import (
            APIConnectionError, APIStatusError, APITimeoutError, RateLimitError,
        )
    except ImportError:
        RateLimitError = APIStatusError = APITimeoutError = APIConnectionError = Exception  # type: ignore

    user_msg = (
        f"TITLE: {title or '(no title)'}\n"
        f"DESCRIPTION: {description or '(no description)'}"
    )

    t0 = time.monotonic()
    resp = None
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            resp = await client.responses.parse(
                model=MODEL,
                input=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                text_format=ListingLandmarks,
                reasoning={"effort": "none"},
                max_output_tokens=800,
            )
            break
        except (RateLimitError, APITimeoutError, APIConnectionError) as exc:
            last_exc = exc
            sleep_s = 2.0 ** (attempt + 1)  # 2, 4, 8
            print(
                f"[WARN] pass4_landmark_mining._mine_one: expected=gpt_response, "
                f"got={type(exc).__name__} (retrying in {sleep_s:.0f}s attempt={attempt + 1}/3), "
                f"listing_id={listing_id}",
                flush=True,
            )
            await asyncio.sleep(sleep_s)
        except APIStatusError as exc:
            print(
                f"[WARN] pass4_landmark_mining._mine_one: expected=gpt_response, "
                f"got=APIStatusError status={getattr(exc, 'status_code', '?')}, "
                f"fallback=skip listing_id={listing_id} exc={exc!r}",
                flush=True,
            )
            return None
        except Exception as exc:
            last_exc = exc
            print(
                f"[WARN] pass4_landmark_mining._mine_one: expected=gpt_response, "
                f"got={type(exc).__name__}: {exc!r}, fallback=skip listing_id={listing_id}",
                flush=True,
            )
            return None
    if resp is None:
        print(
            f"[WARN] pass4_landmark_mining._mine_one: exhausted 3 retries, "
            f"fallback=skip listing_id={listing_id} last_exc={last_exc!r}",
            flush=True,
        )
        return None
    elapsed = time.monotonic() - t0

    if getattr(resp, "status", None) == "incomplete":
        print(
            f"[WARN] pass4_landmark_mining._mine_one: expected=completed, "
            f"got=incomplete ({getattr(resp, 'incomplete_details', None)}), "
            f"fallback=skip listing_id={listing_id}",
            flush=True,
        )
        return None

    parsed = getattr(resp, "output_parsed", None)
    if parsed is None:
        # Check for a refusal
        for item in getattr(resp, "output", []) or []:
            for content in getattr(item, "content", []) or []:
                if getattr(content, "type", "") == "refusal":
                    print(
                        f"[WARN] pass4_landmark_mining._mine_one: expected=output_parsed, "
                        f"got=refusal ({getattr(content, 'refusal', '')!r}), "
                        f"fallback=skip listing_id={listing_id}",
                        flush=True,
                    )
                    return None
        print(
            f"[WARN] pass4_landmark_mining._mine_one: expected=output_parsed, "
            f"got=None, fallback=skip listing_id={listing_id}",
            flush=True,
        )
        return None

    usage = getattr(resp, "usage", None)
    return {
        "listing_id": listing_id,
        "model": MODEL,
        "elapsed_s": round(elapsed, 3),
        "usage": {
            "input_tokens":  getattr(usage, "input_tokens", None),
            "output_tokens": getattr(usage, "output_tokens", None),
            "cached_input":  getattr(getattr(usage, "input_tokens_details", None),
                                    "cached_tokens", None),
        },
        "mentions": [m.model_dump(mode="json") for m in parsed.mentions],
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


# --- rows fetch -------------------------------------------------------------

def _fetch_listings(
    conn: sqlite3.Connection,
    *,
    limit: int | None,
    stratify_canton_n: int | None,
) -> list[tuple[str, str, str]]:
    """Return (listing_id, title, description) for rows with non-empty text.

    Listings whose description is empty or shorter than 30 chars after
    HTML-stripping are excluded — not worth the API call (nothing to mine).

    `stratify_canton_n`: if set, return at most N listings per canton
    (based on `listings_enriched.canton_filled`). Rows with
    `canton_filled='UNKNOWN'` are excluded from the stratified set —
    we want LANDMARK coverage per canton, and UNKNOWN-canton listings
    are typically too ambiguous to contribute geographic-specific
    landmarks anyway.
    """
    if stratify_canton_n is not None:
        # Window-function SQL: rank by listing_id within each canton, keep
        # first N. Deterministic ordering means the same listings come back
        # run after run — essential for the cache to work across reruns.
        sql = """
            WITH ranked AS (
                SELECT l.listing_id, l.title, l.description,
                       le.canton_filled AS canton,
                       ROW_NUMBER() OVER (PARTITION BY le.canton_filled
                                          ORDER BY l.listing_id) AS rk
                FROM listings l
                JOIN listings_enriched le USING(listing_id)
                WHERE l.description IS NOT NULL AND l.description != ''
                  AND le.canton_filled != 'UNKNOWN'
            )
            SELECT listing_id, title, description FROM ranked
            WHERE rk <= ?
            ORDER BY listing_id
        """
        cur = conn.execute(sql, (int(stratify_canton_n),))
    else:
        cur = conn.execute(
            "SELECT listing_id, title, description FROM listings "
            "WHERE description IS NOT NULL AND description != '' "
            "ORDER BY listing_id"
        )
    rows = cur.fetchall()
    out: list[tuple[str, str, str]] = []
    for r in rows:
        listing_id = r["listing_id"] if isinstance(r, sqlite3.Row) else r[0]
        title = (r["title"] if isinstance(r, sqlite3.Row) else r[1]) or ""
        raw_desc = (r["description"] if isinstance(r, sqlite3.Row) else r[2]) or ""
        desc_plain = strip_html(raw_desc)
        if len(desc_plain.strip()) < 30:
            continue
        out.append((listing_id, title, desc_plain))
    if limit is not None:
        out = out[:limit]
    return out


# --- async worker + driver --------------------------------------------------

async def _worker(
    sem: asyncio.Semaphore, client, item,
    stats: Counter, cache_lock: asyncio.Lock,
) -> dict | None:
    listing_id, title, desc = item
    async with sem:
        result = await _mine_one(client, listing_id, title, desc)
        if result is not None:
            stats["gpt_calls_ok"] += 1
            stats["mentions_total"] += len(result["mentions"])
            async with cache_lock:
                _append_cache_line(result)
        else:
            stats["gpt_calls_failed"] += 1
        return result


async def _run_async(rows, client, stats: Counter) -> None:
    sem = asyncio.Semaphore(CONCURRENCY)
    cache_lock = asyncio.Lock()
    tasks = [_worker(sem, client, item, stats, cache_lock) for item in rows]
    completed = 0
    t0 = time.monotonic()
    for coro in asyncio.as_completed(tasks):
        _ = await coro
        completed += 1
        if completed % 50 == 0 or completed == len(tasks):
            elapsed = time.monotonic() - t0
            rate = completed / elapsed if elapsed > 0 else 0
            remaining = (len(tasks) - completed) / rate if rate > 0 else 0
            print(
                f"[INFO] pass4_landmark_mining: {completed}/{len(tasks)} "
                f"elapsed_s={elapsed:.0f} rate={rate:.1f}/s eta_s={remaining:.0f} "
                f"mentions={stats['mentions_total']}",
                flush=True,
            )


# --- main ------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, type=Path, help="path to listings.db")
    parser.add_argument("--limit", type=int, default=None,
                        help="smoke-test: max listings to process")
    parser.add_argument("--stratify-canton-n", type=int, default=None,
                        help="Per-canton cap: only extract N listings per "
                             "canton_filled (UNKNOWN canton excluded). Gives "
                             "balanced geographic coverage of landmarks.")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"[ERROR] pass4_landmark_mining: db not found at {args.db}", flush=True)
        return 1

    t_start = time.monotonic()
    print(
        f"[INFO] pass4_landmark_mining: model={MODEL} concurrency={CONCURRENCY} "
        f"max_calls={MAX_CALLS} db={args.db} limit={args.limit}",
        flush=True,
    )

    cache = _load_cache()
    print(f"[INFO] pass4_landmark_mining: cache loaded entries={len(cache)}", flush=True)

    with connect(args.db) as conn:
        conn.row_factory = sqlite3.Row
        rows = _fetch_listings(
            conn,
            limit=args.limit,
            stratify_canton_n=args.stratify_canton_n,
        )
        print(
            f"[INFO] pass4_landmark_mining: candidate_rows={len(rows)} "
            f"stratify_canton_n={args.stratify_canton_n}",
            flush=True,
        )

        # Partition: cached vs uncached.
        uncached: list[tuple[str, str, str]] = []
        cache_hits = 0
        for item in rows:
            lid, _, _ = item
            if _cache_key(lid, MODEL) in cache:
                cache_hits += 1
            else:
                uncached.append(item)
        print(
            f"[INFO] pass4_landmark_mining: cache_hits={cache_hits} "
            f"uncached={len(uncached)}",
            flush=True,
        )

        # Budget guard
        if len(uncached) > MAX_CALLS:
            print(
                f"[WARN] pass4_landmark_mining: expected={MAX_CALLS} max calls, "
                f"got={len(uncached)} uncached rows, fallback=truncating to MAX_CALLS "
                f"(remainder will be mined on the next run from the cache)",
                flush=True,
            )
            uncached = uncached[:MAX_CALLS]

        if not uncached:
            print("[INFO] pass4_landmark_mining: no uncached rows — done.", flush=True)
        else:
            try:
                client = _make_client()
            except RuntimeError as exc:
                print(f"[ERROR] pass4_landmark_mining: {exc}", flush=True)
                return 2

            stats: Counter = Counter()
            try:
                asyncio.run(_run_async(uncached, client, stats))
            except KeyboardInterrupt:
                print(
                    "[WARN] pass4_landmark_mining: interrupted; partial results are cached",
                    flush=True,
                )
                return 130

            print(
                f"[INFO] pass4_landmark_mining: gpt_calls_ok={stats.get('gpt_calls_ok', 0)} "
                f"failed={stats.get('gpt_calls_failed', 0)} "
                f"mentions_extracted={stats.get('mentions_total', 0)}",
                flush=True,
            )

    elapsed = time.monotonic() - t_start
    print(f"[INFO] pass4_landmark_mining DONE elapsed_s={elapsed:.1f}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
