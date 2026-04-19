"""Pass 2b — bathroom + cellar + shared-amenity extraction via gpt-5.4-nano.

Four new fields, written with `canton_source=text_gpt_5_4_nano_pass2b`:
  * bathroom_count   — int 1..10 | null
  * bathroom_shared  — "true" | "false" | null
  * has_cellar       — "true" | "false" | null
  * kitchen_shared   — "true" | "false" | null

Prompt + field contracts: `_context/PASS2B_PLAN.md` §1.

Filter — run only on rows whose description mentions any of:
  * bathroom keywords (no raw "Bad" — too many false positives)
  * cellar keywords
  * shared-living keywords
  OR whose object_category is in the residential whitelist (to pick up
  "infer-default false" cases for full-apartment listings).

Cost bound at 20-21k rows × (~600 in + ~200 out) @ nano pricing ≈ ~$3.

Cache: append-only `enrichment/data/cache/gpt_pass2b.jsonl`, keyed on
(listing_id, model, prompt_version). Re-runs are cache-only.

Validation (auditor §8.3):
  * `raw_snippet` MUST be a verbatim substring of the description IF the
    write is explicit (confidence ≥ 0.85 OR value is `true` / non-null count).
  * `raw_snippet = null` is ALLOWED for inferred-default writes
    (value=false for *_shared at conf ≤ 0.80, object_category residential).
    Validation pairing is checked before the write.
  * Anything else is treated as hallucination → drop + [WARN].

CLAUDE.md §5: every fallback path emits [WARN] with context/expected/got/fallback.

Idempotent: gated on `_source='UNKNOWN-pending'` in every UPDATE.

Usage:
    python -m enrichment.scripts.pass2b_bathroom_cellar_kitchen --db data/listings.db
    python -m enrichment.scripts.pass2b_bathroom_cellar_kitchen --db data/listings.db --limit 10
    python -m enrichment.scripts.pass2b_bathroom_cellar_kitchen --db data/listings.db --dry-run

Env:
    OPENAI_API_KEY                required
    OPENAI_PASS2B_MODEL           default: gpt-5.4-nano-2026-03-17
    GPT_PASS2B_CONCURRENCY        default: 16
    GPT_PASS2B_TIMEOUT_S          default: 30
    GPT_PASS2B_DESC_MAX_CHARS     default: 2500
    GPT_PASS2B_MAX_CALLS          default: 30000 (safety cap)
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Optional

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict

from enrichment.common.db import connect
from enrichment.common.langdet import strip_html
from enrichment.common.sources import TEXT_GPT_5_4_NANO_PASS2B, UNKNOWN_PENDING

load_dotenv()

# --- configuration -----------------------------------------------------------

MODEL = os.getenv("OPENAI_PASS2B_MODEL", "gpt-5.4-nano-2026-03-17")
# Lowered from 16 after first run hit 13.5k rate-limit 429s.
# OpenAI tier-1 gpt-5.4-nano: 500 RPM + 200k TPM. At ~900 tokens/call,
# 200k TPM / 900 = 222 calls/min = 3.7 call/s sustained.
# 6 workers × 300ms median round-trip ≈ 20 rps, smoothed by SDK retry.
CONCURRENCY = int(os.getenv("GPT_PASS2B_CONCURRENCY", "6"))
TIMEOUT_S = float(os.getenv("GPT_PASS2B_TIMEOUT_S", "60"))
# SDK-level retry on 429/5xx with exponential backoff.
# First run's 429s propagated because max_retries defaulted to 2.
MAX_RETRIES = int(os.getenv("GPT_PASS2B_MAX_RETRIES", "10"))
DESC_MAX_CHARS = int(os.getenv("GPT_PASS2B_DESC_MAX_CHARS", "2500"))
MAX_CALLS = int(os.getenv("GPT_PASS2B_MAX_CALLS", "30000"))

PROMPT_VERSION = "v1"  # bump when prompt changes; invalidates cache

CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "cache"
CACHE_PATH = CACHE_DIR / "gpt_pass2b.jsonl"
AUDIT_PATH = Path(__file__).resolve().parents[1] / "data" / "pass2b_audit.json"

# --- residential category whitelist (auditor §8.2) ---------------------------

RESIDENTIAL_CATEGORIES: frozenset[str] = frozenset({
    "Wohnung", "Möblierte Wohnung", "Haus", "Einzelzimmer", "WG-Zimmer",
    "Dachwohnung", "Maisonette", "Studio", "Attika", "Loft", "Villa",
    "Terrassenwohnung", "Doppeleinfamilienhaus", "Einfamilienhaus",
    "Reiheneinfamilienhaus", "Duplex", "Penthouse", "Atelier",
})

# Rooms clearly private to renter (default *_shared = false at 0.75).
PRIVATE_UNIT_CATEGORIES: frozenset[str] = RESIDENTIAL_CATEGORIES - {"Einzelzimmer", "WG-Zimmer"}
# Rooms inside a shared unit (default *_shared = true at 0.75 unless contradicted).
SHARED_UNIT_CATEGORIES: frozenset[str] = frozenset({"Einzelzimmer", "WG-Zimmer"})
# object_category=NULL means "not labelled", not "not residential". GPT infers
# full-apartment vs shared-room from the description itself, so NULL category
# is treated as "whichever GPT decided" (respecting the confidence cap).

# --- keyword filter (auditor §8.4 — no raw "Bad") ----------------------------

BATHROOM_KEYWORDS = (
    "Badezimmer", "Bäder", "Dusche", "Nasszelle", "Badewanne",
    "salle de bain", "salles de bain", "salle de bains",
    "bagno", "bagni", "doccia",
    "bathroom", "shower", "restroom",
)
CELLAR_KEYWORDS = (
    "Keller", "Kellerabteil", "Kellerraum",
    "cave", "caves", "cantina", "cantine",
    "cellar", "basement", "sous-sol", "sous sol",
)
SHARED_KEYWORDS = (
    "WG", "Wohngemeinschaft", "Einzelzimmer", "Mitbewohner", "Mitbewohnerin",
    "Gemeinschaft", "gemeinsam", "geteilt", "Mitbenützung",
    "colocation", "flatshare", "flatmate", "shared",
    "cuisine commune", "cuisine partagée", "salle de bain commune",
    "cucina in comune", "cucina condivisa", "bagno in comune",
    "Gemeinschaftsküche", "Gemeinschaftsbad",
)
ALL_KEYWORDS = BATHROOM_KEYWORDS + CELLAR_KEYWORDS + SHARED_KEYWORDS

# --- Pydantic output schema --------------------------------------------------


class FieldValue(BaseModel):
    """One field's extraction: value / confidence / optional raw_snippet."""
    model_config = ConfigDict(extra="forbid")

    value: Optional[str]
    confidence: float
    raw_snippet: Optional[str]


class Pass2bExtraction(BaseModel):
    """Four fields — pass 2b target surface."""
    model_config = ConfigDict(extra="forbid")

    bathroom_count:  FieldValue
    bathroom_shared: FieldValue
    has_cellar:      FieldValue
    kitchen_shared:  FieldValue


# --- system prompt -----------------------------------------------------------

_SYSTEM_PROMPT = """You extract 4 structured fields about bathrooms, cellars, and shared \
amenities from Swiss real-estate rental-listing text.

Languages: German, French, Italian, English. HTML-stripped plain text.
Do NOT translate. raw_snippet MUST be a verbatim substring of the description \
(with exception rules below for inferred defaults).

Return {value, confidence, raw_snippet} for each of the 4 fields.

## bathroom_count  (int string "1"-"10" | null)

- Count ROOMS with a shower OR bathtub. Separate WC/toilet does NOT count.
- "Badezimmer" / "Bad" / "Bäder" / "Nasszelle" / "Duschraum" → +1 each.
- "salle de bain" / "bagno" / "bathroom" → +1 each.
- "WC separé" / "separate WC" / "Gäste-WC" → +0 (toilet only).
- "1 Badezimmer mit Dusche" → "1". "2 Bäder" → "2". "Badezimmer + WC" → "1".
- "2 bagni completi" → "2". "salle de bain et WC indépendant" → "1".
- null if not mentioned.
- Range: 1-10; reject outside.

## bathroom_shared  ("true" / "false" / null)

- "true": bathroom is shared with tenants OUTSIDE the renter's household.
  Explicit keywords: "Gemeinschaftsbad", "salle de bain commune", "bagno in comune",
  "shared bathroom", "Badezimmer zur Mitbenützung".
  Inferred: object_category is a SHARED room (Einzelzimmer, WG-Zimmer), confidence 0.75.
- "false": bathroom is private to the unit.
  Explicit: "eigenes Bad", "private bathroom", "salle de bain privée".
  Inferred: object_category is a FULL UNIT (Wohnung, Haus, …), confidence 0.75.
- null: genuinely unclear (e.g., short teaser, non-residential category).

## has_cellar  ("true" / "false" / null)

- "true": rental includes cellar access. "Kellerabteil", "mit Keller", "eigener Keller",
  "Keller zur Mitbenützung" (shared cellar use still counts as has_cellar=true),
  "avec cave", "con cantina", "with cellar/basement".
  CONTEXT CLUE: "Waschmaschine im Keller" → has_cellar=true @ 0.70
  (the building has a cellar the renter can access).
- "false": "kein Keller", "sans cave", "no cellar".
- null: not mentioned at all.

## kitchen_shared  ("true" / "false" / null)

- "true": kitchen shared with tenants OUTSIDE the renter's household.
  Explicit: "Gemeinschaftsküche", "cuisine commune/partagée", "cucina in comune",
  "shared kitchen".
  Inferred: shared room (Einzelzimmer/WG-Zimmer) context, confidence 0.75.
- "false": private kitchen. Inferred: full apartment/house, confidence 0.75.
  Open-plan "offene Küche" is still PRIVATE (not shared).
- null: unclear.

## Confidence

Calibrate 0.0-1.0:
  - 0.95+ explicit, unambiguous phrase in detected language
  - 0.85  clear text mention
  - 0.75  inferred from object_category + absence of contradiction
  - 0.60  weak inference
  - <0.60 prefer null

## raw_snippet rules

- For EXPLICIT writes (confidence ≥ 0.85 OR bathroom_count has a value):
  raw_snippet MUST be a verbatim substring of the description.
- For INFERRED DEFAULTS (value="false" for *_shared at conf ≤ 0.80 on a full unit,
  OR value="true" for *_shared at conf ≤ 0.80 on a WG/Zimmer):
  raw_snippet = null is OK. Write the cue in the confidence field.
- For null values: raw_snippet is null.
- NEVER fabricate a snippet. If you can't find one, use null.

## Critical rules

1. ONLY extract what's truly supported by the description or object_category.
2. Do not confuse rooms with bathrooms: "3.5 Zimmer" is rooms, not bathrooms.
3. A separate WC is NOT a bathroom.
4. "Keller zur Mitbenützung" = has_cellar=true (they use it, even if shared).
5. Full-apartment listings default *_shared=false @ 0.75, not null.
6. Shared-room listings (WG-Zimmer/Einzelzimmer) default *_shared=true @ 0.75.
7. All 4 fields must be in every response, nulls allowed.
"""

# --- cache helpers ----------------------------------------------------------


def _prompt_hash() -> str:
    h = hashlib.sha256()
    h.update(PROMPT_VERSION.encode())
    h.update(_SYSTEM_PROMPT.encode("utf-8"))
    return h.hexdigest()[:16]


def _cache_key(listing_id: str) -> str:
    return f"{listing_id}|{MODEL}|{_prompt_hash()}"


def _load_cache() -> dict[str, dict]:
    if not CACHE_PATH.exists():
        return {}
    out: dict[str, dict] = {}
    try:
        with CACHE_PATH.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    out[_cache_key(rec["listing_id"])] = rec
                except (json.JSONDecodeError, KeyError) as exc:
                    print(f"[WARN] pass2b._load_cache: expected=valid jsonl, "
                          f"got={type(exc).__name__} at line {line_no}, fallback=skip",
                          flush=True)
    except OSError as exc:
        print(f"[WARN] pass2b._load_cache: expected=readable {CACHE_PATH}, "
              f"got={exc!r}, fallback=empty cache", flush=True)
    return out


def _append_cache(rec: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    line = (json.dumps(rec, ensure_ascii=False, default=str) + "\n").encode("utf-8")
    fd = os.open(str(CACHE_PATH), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, line)
    finally:
        os.close(fd)


# --- filter -----------------------------------------------------------------


def _row_is_in_scope(description: str, object_category: str | None) -> bool:
    """Filter: run GPT on this row? True if any keyword OR residential category."""
    if description and any(kw in description for kw in ALL_KEYWORDS):
        return True
    if object_category and object_category in RESIDENTIAL_CATEGORIES:
        return True
    return False


# --- post-validation --------------------------------------------------------


_BOOL_VALUES: frozenset[str] = frozenset({"true", "false"})


def _validate_bathroom_count(value: str) -> tuple[bool, str | None, str]:
    try:
        n = int(value)
    except (TypeError, ValueError):
        return False, None, f"bathroom_count not int: {value!r}"
    if 1 <= n <= 10:
        return True, str(n), ""
    return False, None, f"bathroom_count out of [1,10]: {n}"


def _validate_bool(value: str) -> tuple[bool, str | None, str]:
    v = (value or "").strip().lower()
    if v in _BOOL_VALUES:
        return True, v, ""
    return False, None, f"bool value not in {{true,false}}: {value!r}"


_WHITESPACE_RE = re.compile(r"\s+")


def _normalize_whitespace(s: str) -> str:
    """Collapse any Unicode whitespace run (regular space, nbsp, tab, newline)
    into a single ASCII space. Keeps the "verbatim" intent while tolerating
    the \xa0 vs regular-space variation we see in Comparis descriptions.
    """
    return _WHITESPACE_RE.sub(" ", s)


def _validate_raw_snippet(
    raw_snippet: Optional[str],
    description: str,
    value: Optional[str],
    confidence: float,
    field_name: str,
    object_category: str | None,
) -> tuple[bool, str]:
    """Hallucination guard: snippet must be substring of desc (whitespace-normalised),
    UNLESS inferred-default.

    Returns (ok, reason_if_rejected).
    """
    # Allowed: null snippet if this is an inferred-default write.
    if raw_snippet is None:
        # null value → no snippet needed
        if value is None:
            return True, ""
        # Inferred *_shared default. Acceptance logic:
        #   * object_category in PRIVATE_UNIT_CATEGORIES → expect false@≤0.80
        #   * object_category in SHARED_UNIT_CATEGORIES → expect true@≤0.80
        #   * object_category IS NULL → GPT inferred from description text;
        #     trust with confidence cap ≤0.80 (whichever direction it chose)
        if field_name in ("bathroom_shared", "kitchen_shared"):
            # Explicit residential category — require matching direction
            if object_category in PRIVATE_UNIT_CATEGORIES:
                if value == "false" and confidence <= 0.80:
                    return True, ""
                # true@low-conf on a private unit is a contradiction — reject.
            elif object_category in SHARED_UNIT_CATEGORIES:
                if value == "true" and confidence <= 0.80:
                    return True, ""
                # false@low-conf on a shared unit is a contradiction — reject.
            elif object_category is None:
                # Category not labelled in raw CSV — GPT used description as signal.
                # Accept either direction at conf ≤ 0.80.
                if value in ("true", "false") and confidence <= 0.80:
                    return True, ""
            # Anything else (e.g. object_category='Gewerbeobjekt') — we're out of scope.
        # inferred has_cellar from context clue (e.g. "Waschmaschine im Keller") is OK at <=0.70
        if field_name == "has_cellar" and value in ("true", "false") and confidence <= 0.70:
            return True, ""
        # Explicit write needs a snippet. Reject.
        return False, (
            f"{field_name}={value!r} conf={confidence:.2f} has no raw_snippet "
            f"but isn't an inferred-default case"
        )
    # Snippet present → must be a verbatim substring of the (stripped) description.
    # Normalise whitespace (nbsp / tab / newline → single space) on both sides
    # so "2 Bäder" matches "2\xa0Bäder" — a common Comparis artefact.
    if raw_snippet.strip() == "":
        return False, f"{field_name} raw_snippet is empty string"
    norm_desc = _normalize_whitespace(description)
    norm_snippet = _normalize_whitespace(raw_snippet)
    if norm_snippet in norm_desc:
        return True, ""
    return False, (
        f"{field_name} raw_snippet not a substring of description "
        f"(snippet={raw_snippet[:60]!r})"
    )


def _validate_extraction(
    ext: Pass2bExtraction,
    description: str,
    object_category: str | None,
) -> tuple[dict[str, tuple[str, float, str | None]], list[str]]:
    """Return (accepted_writes, warnings).

    accepted_writes[field_name] = (value, confidence, raw_snippet_or_None)
    """
    writes: dict[str, tuple[str, float, str | None]] = {}
    warnings: list[str] = []

    per_field = {
        "bathroom_count": ext.bathroom_count,
        "bathroom_shared": ext.bathroom_shared,
        "has_cellar": ext.has_cellar,
        "kitchen_shared": ext.kitchen_shared,
    }

    for name, fv in per_field.items():
        if fv.value is None:
            # null value → no write (row stays UNKNOWN-pending for pass 3)
            continue

        # Type validation
        if name == "bathroom_count":
            ok, norm, why = _validate_bathroom_count(fv.value)
        else:
            ok, norm, why = _validate_bool(fv.value)
        if not ok:
            warnings.append(f"{name}: {why}")
            continue

        # Confidence bounds
        if not (0.0 <= fv.confidence <= 1.0):
            warnings.append(f"{name}: confidence out of [0,1]: {fv.confidence}")
            continue

        # Snippet / hallucination guard
        snippet_ok, why = _validate_raw_snippet(
            fv.raw_snippet, description, norm, fv.confidence, name, object_category
        )
        if not snippet_ok:
            warnings.append(f"{name}: {why}")
            continue

        writes[name] = (norm, fv.confidence, fv.raw_snippet)

    return writes, warnings


# --- prompt building --------------------------------------------------------


def _build_user_prompt(description: str, object_category: str | None) -> str:
    cat_line = (f"object_category: {object_category!r}\n"
                if object_category else "object_category: (null)\n")
    return (
        f"{cat_line}"
        f"description (HTML-stripped, truncated):\n"
        f"---\n{description}\n---\n"
        f"Return JSON with bathroom_count, bathroom_shared, has_cellar, kitchen_shared."
    )


# --- GPT call ---------------------------------------------------------------


async def _extract_one(
    client, listing_id: str, description: str, object_category: str | None,
    sem: asyncio.Semaphore,
) -> tuple[str, Optional[Pass2bExtraction], str | None]:
    """(listing_id, parsed_extraction_or_None, error_reason)."""
    async with sem:
        try:
            resp = await asyncio.wait_for(
                client.responses.parse(
                    model=MODEL,
                    instructions=_SYSTEM_PROMPT,
                    input=_build_user_prompt(description, object_category),
                    text_format=Pass2bExtraction,
                ),
                timeout=TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            return listing_id, None, f"timeout>{TIMEOUT_S}s"
        except Exception as exc:
            return listing_id, None, f"{type(exc).__name__}: {exc}"
    if resp.output_parsed is None:
        return listing_id, None, "output_parsed=None"
    return listing_id, resp.output_parsed, None


# --- main runner ------------------------------------------------------------


def _collect_rows(conn: sqlite3.Connection, limit: int | None) -> list[dict]:
    """Pull rows needing pass 2b (any of the 4 fields still UNKNOWN-pending)."""
    q = """
        SELECT le.listing_id,
               COALESCE(l.description, '') AS description_raw,
               l.object_category,
               le.bathroom_count_source,
               le.bathroom_shared_source,
               le.has_cellar_source,
               le.kitchen_shared_source
          FROM listings_enriched le
          JOIN listings l USING (listing_id)
         WHERE le.bathroom_count_source = ?
            OR le.bathroom_shared_source = ?
            OR le.has_cellar_source = ?
            OR le.kitchen_shared_source = ?
    """
    params: tuple = (UNKNOWN_PENDING,) * 4
    if limit:
        q += " LIMIT ?"
        params = (*params, limit)
    return [dict(r) for r in conn.execute(q, params).fetchall()]


async def _run(db_path: Path, limit: int | None, dry_run: bool) -> dict:
    from openai import AsyncOpenAI
    client = AsyncOpenAI(max_retries=MAX_RETRIES)

    conn = connect(db_path)
    try:
        rows = _collect_rows(conn, limit)
        stats = {
            "rows_in_table": 0,
            "rows_out_of_scope": 0,
            "rows_in_scope": 0,
            "cache_hits": 0,
            "gpt_calls": 0,
            "gpt_failed": 0,
            "write_attempts": 0,
            "writes_applied": 0,
            "validation_dropped": 0,
            "by_field": {
                "bathroom_count":  {"true_values": 0, "false_values": 0, "count_values": 0, "snippet_null": 0, "snippet_literal": 0},
                "bathroom_shared": {"true_values": 0, "false_values": 0, "count_values": 0, "snippet_null": 0, "snippet_literal": 0},
                "has_cellar":      {"true_values": 0, "false_values": 0, "count_values": 0, "snippet_null": 0, "snippet_literal": 0},
                "kitchen_shared":  {"true_values": 0, "false_values": 0, "count_values": 0, "snippet_null": 0, "snippet_literal": 0},
            },
        }
        stats["rows_in_table"] = len(rows)

        # In-scope filter
        in_scope: list[dict] = []
        for r in rows:
            desc = strip_html(r["description_raw"])[:DESC_MAX_CHARS]
            r["description_stripped"] = desc
            if _row_is_in_scope(desc, r["object_category"]):
                in_scope.append(r)
            else:
                stats["rows_out_of_scope"] += 1
        stats["rows_in_scope"] = len(in_scope)

        cache = _load_cache()
        need_call = [r for r in in_scope if _cache_key(r["listing_id"]) not in cache]
        already = len(in_scope) - len(need_call)
        stats["cache_hits"] = already

        # Rate-limit / max-call cap
        if len(need_call) > MAX_CALLS:
            print(f"[WARN] pass2b._run: expected<=MAX_CALLS={MAX_CALLS}, "
                  f"got={len(need_call)}, fallback=truncate", flush=True)
            need_call = need_call[:MAX_CALLS]

        # Do the GPT calls
        sem = asyncio.Semaphore(CONCURRENCY)

        async def _fanout():
            tasks = [
                _extract_one(client, r["listing_id"],
                             r["description_stripped"], r["object_category"], sem)
                for r in need_call
            ]
            return await asyncio.gather(*tasks, return_exceptions=False)

        results = await _fanout() if need_call else []

        # Index results back by listing_id and append to cache
        by_id: dict[str, Pass2bExtraction] = {}
        for (lid, ext, err) in results:
            stats["gpt_calls"] += 1
            if ext is None:
                stats["gpt_failed"] += 1
                print(f"[WARN] pass2b._extract_one: expected=parsed, "
                      f"got={err}, listing_id={lid}, fallback=skip", flush=True)
                continue
            by_id[lid] = ext
            rec = {
                "listing_id": lid,
                "model": MODEL,
                "prompt_version": PROMPT_VERSION,
                "inferred_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "extraction": ext.model_dump(),
            }
            _append_cache(rec)
            cache[_cache_key(lid)] = rec

        # Apply to DB (both cached hits and fresh calls)
        for r in in_scope:
            lid = r["listing_id"]
            desc = r["description_stripped"]
            obj_cat = r["object_category"]
            key = _cache_key(lid)
            if key not in cache:
                continue  # gpt_failed earlier
            rec = cache[key]
            try:
                ext = Pass2bExtraction.model_validate(rec["extraction"])
            except Exception as exc:
                print(f"[WARN] pass2b.apply: expected=valid Pass2bExtraction, "
                      f"got={type(exc).__name__}: {exc}, listing_id={lid}, "
                      f"fallback=skip", flush=True)
                stats["validation_dropped"] += 1
                continue

            writes, warnings = _validate_extraction(ext, desc, obj_cat)
            for w in warnings:
                print(f"[WARN] pass2b.validate: listing_id={lid}: {w}", flush=True)
                stats["validation_dropped"] += 1

            for field_name, (value, conf, raw_snippet) in writes.items():
                stats["write_attempts"] += 1
                # Per-field source check to preserve idempotency (only write UNKNOWN-pending).
                current_src = r.get(f"{field_name}_source")
                if current_src != UNKNOWN_PENDING:
                    # Already written by a prior run — skip (no-op on repeat).
                    continue

                if dry_run:
                    snippet_preview = (raw_snippet[:50] if raw_snippet else None)
                    print(f"  [dry-run] {lid:<14s} {field_name:<20s} "
                          f"= {value!r} conf={conf:.2f} snippet={snippet_preview!r}")
                else:
                    conn.execute(
                        f"UPDATE listings_enriched "
                        f"SET {field_name}_filled=?, {field_name}_source=?, "
                        f"    {field_name}_confidence=?, {field_name}_raw=? "
                        f"WHERE listing_id=? AND {field_name}_source=?",
                        (value, TEXT_GPT_5_4_NANO_PASS2B, conf, raw_snippet,
                         lid, UNKNOWN_PENDING),
                    )
                stats["writes_applied"] += 1
                # Per-field tally
                t = stats["by_field"][field_name]
                if value == "true":
                    t["true_values"] += 1
                elif value == "false":
                    t["false_values"] += 1
                else:
                    t["count_values"] += 1
                if raw_snippet is None:
                    t["snippet_null"] += 1
                else:
                    t["snippet_literal"] += 1

        if not dry_run:
            conn.commit()

        # Post-state residual
        stats["residual_unknown_pending"] = {
            name: conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched WHERE {name}_source=?",
                (UNKNOWN_PENDING,),
            ).fetchone()[0]
            for name in ("bathroom_count", "bathroom_shared", "has_cellar", "kitchen_shared")
        }

        AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
        AUDIT_PATH.write_text(json.dumps({
            "run_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "model": MODEL,
            "prompt_version": PROMPT_VERSION,
            "prompt_hash": _prompt_hash(),
            "desc_max_chars": DESC_MAX_CHARS,
            "stats": stats,
        }, indent=2, ensure_ascii=False))
        return stats
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=None,
                        help="Process at most N rows (smoke test).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Extract but do not write to DB.")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"[ERROR] DB not found at {args.db}", file=sys.stderr)
        return 2
    if not os.getenv("OPENAI_API_KEY"):
        print("[ERROR] OPENAI_API_KEY not set", file=sys.stderr)
        return 3

    stats = asyncio.run(_run(args.db, args.limit, args.dry_run))
    print("Pass 2b complete:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    print(f"\nAudit: {AUDIT_PATH}")
    print(f"Cache: {CACHE_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
