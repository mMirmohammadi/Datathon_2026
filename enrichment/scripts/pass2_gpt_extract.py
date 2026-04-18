"""Pass 2 — OpenAI GPT-5.4-mini structured extraction from `description` + `title`.

A drop-in replacement for `pass2_text_extract.py` (the multilingual regex pass).
Produces the IDENTICAL output contract so pass 3 + tests keep working, just
with a different `_source` tag (`text_gpt_5_4`).

Why GPT over regex:
  * Handles DE/FR/IT/EN natively — no per-language pattern catalogue to maintain.
  * Reads context, not just tokens — `"der Balkon wurde entfernt"` ("balcony was
    removed") is handled cleanly, which the regex NegEx window often misses.
  * One call per listing, no pattern combinatorics.

Cost bound (25,546 listings × ~1,000 input / 350 output tokens @ gpt-5.4-mini
pricing): ~$50 full run. Aggressive per-listing cache keeps re-runs free.

Hard rules (CLAUDE.md §5 compliant — NO silent fallbacks):
  * Never overwrite a field whose source is not 'UNKNOWN-pending'.
  * Every fallback path (no key, timeout, refusal, bad JSON, post-validation
    fail) emits a [WARN] line with context/expected/got/fallback.
  * If we can't extract a value or it fails validation, we DON'T write and the
    field stays `UNKNOWN-pending` → pass 3 sentinel-fills it to `UNKNOWN`.

Usage:
    python -m enrichment.scripts.pass2_gpt_extract --db data/listings.db
    python -m enrichment.scripts.pass2_gpt_extract --db data/listings.db --limit 20    # smoke test
    python -m enrichment.scripts.pass2_gpt_extract --db data/listings.db --dry-run     # no writes

Env:
    OPENAI_API_KEY              required
    OPENAI_EXTRACT_MODEL        default: gpt-5.4-mini-2026-03-17  (pinned snapshot)
    GPT_PASS2_CONCURRENCY       default: 16
    GPT_PASS2_MAX_CALLS         default: 26000  (hard cap on live API calls per run)
    GPT_PASS2_TIMEOUT_S         default: 60
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import re
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Literal, Optional

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, ValidationError

from enrichment.common.db import connect
from enrichment.common.langdet import strip_html
from enrichment.common.provenance import write_field
from enrichment.common.sources import TEXT_GPT_5_4, UNKNOWN_PENDING

load_dotenv()

# --- configuration -----------------------------------------------------------

MODEL = os.getenv("OPENAI_EXTRACT_MODEL", "gpt-5.4-mini-2026-03-17")
CONCURRENCY = int(os.getenv("GPT_PASS2_CONCURRENCY", "16"))
MAX_CALLS = int(os.getenv("GPT_PASS2_MAX_CALLS", "26000"))
TIMEOUT_S = float(os.getenv("GPT_PASS2_TIMEOUT_S", "60"))

CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "cache"
CACHE_PATH = CACHE_DIR / "gpt_pass2.jsonl"
LOG_PATH = Path(__file__).resolve().parents[1] / "data" / "gpt_pass2_run.log"

# --- field registry (MUST match schema.FIELDS 19 target fields) --------------

FEATURE_FIELDS: tuple[str, ...] = (
    "feature_balcony", "feature_elevator", "feature_parking", "feature_garage",
    "feature_fireplace", "feature_child_friendly", "feature_pets_allowed",
    "feature_temporary", "feature_new_build", "feature_wheelchair_accessible",
    "feature_private_laundry", "feature_minergie_certified",
)

# Base confidences mirror enrichment/patterns/*.yaml so tests stay comparable.
BASE_CONFIDENCE: dict[str, float] = {
    "feature_balcony":               0.80,
    "feature_elevator":              0.85,
    "feature_parking":               0.75,
    "feature_garage":                0.80,
    "feature_fireplace":             0.85,
    "feature_child_friendly":        0.70,
    "feature_pets_allowed":          0.75,
    "feature_temporary":             0.80,
    "feature_new_build":             0.85,
    "feature_wheelchair_accessible": 0.85,
    "feature_private_laundry":       0.75,
    "feature_minergie_certified":    0.95,
    "year_built":                    0.90,
    "floor":                         0.85,   # avg of ground(0.90) / basement(0.90) / numeric(0.80)
    "area":                          0.85,
    "available_from":                0.88,   # avg across immediate / iso / european
    "agency_phone":                  0.85,
    "agency_email":                  0.95,
    "agency_name":                   0.70,
}

GENERIC_EMAIL_DOMAINS: frozenset[str] = frozenset({
    "gmail", "bluewin", "hotmail", "yahoo", "outlook", "gmx", "icloud",
    "protonmail", "mail", "proton", "swissmail",
})

# --- Pydantic output schema --------------------------------------------------


class FieldValue(BaseModel):
    """GPT output per field. Value is nullable; if null, we don't write."""
    # `extra='forbid'` is required so the OpenAI Structured-Outputs JSON Schema
    # carries `additionalProperties: false` (strict-mode prerequisite).
    model_config = ConfigDict(extra="forbid")

    value: Optional[str]
    confidence: float
    raw_snippet: Optional[str]


class ListingExtraction(BaseModel):
    """19 fields — the complete pass-2 target surface."""
    model_config = ConfigDict(extra="forbid")

    feature_balcony: FieldValue
    feature_elevator: FieldValue
    feature_parking: FieldValue
    feature_garage: FieldValue
    feature_fireplace: FieldValue
    feature_child_friendly: FieldValue
    feature_pets_allowed: FieldValue
    feature_temporary: FieldValue
    feature_new_build: FieldValue
    feature_wheelchair_accessible: FieldValue
    feature_private_laundry: FieldValue
    feature_minergie_certified: FieldValue
    year_built: FieldValue
    floor: FieldValue
    area: FieldValue
    available_from: FieldValue
    agency_phone: FieldValue
    agency_email: FieldValue
    agency_name: FieldValue


# --- system prompt -----------------------------------------------------------

_SYSTEM_PROMPT = """You extract 19 structured fields from Swiss real-estate rental-listing text.

The text may be in German, French, Italian, or English; treat HTML-stripped content \
as plain text. Do NOT translate and do NOT paraphrase — `raw_snippet` must be a \
verbatim substring of the description where possible.

For each of the 19 fields produce {value, confidence, raw_snippet} with these rules:

## Value conventions

Feature flags (12 fields, all named `feature_*`):
  - "1"  = clearly mentioned as present
  - "0"  = explicitly negated (e.g. "kein Balkon", "ohne Lift", "pas de balcon", "senza ascensore")
  - null = not mentioned
  Write raw_snippet as the matched clause. For negations, PREFIX with "NEG:" — e.g. "NEG:Balkon".

`year_built` (construction year):
  - string in the form "YYYY" (1800–2030 only — reject implausible years)
  - null if not explicitly stated ("Baujahr 1920", "built in 1985", "anno di costruzione 1990")

`floor` (which floor the unit is on):
  - "0"  for ground floor (EG, Erdgeschoss, rez-de-chaussée, pianterreno, ground floor)
  - "-1" for basement (UG, Untergeschoss, sous-sol, seminterrato, basement)
  - "1", "2", "3", ... for numeric floors ("3. Stock", "3. OG", "2nd floor", "2ème étage")
  - null otherwise

`area` (living area in m²):
  - integer string "10"–"500"
  - null if unit is not clearly m² or if value is out of range

`available_from` (move-in date):
  - ISO "YYYY-MM-DD"
  - "sofort", "immediately", "subito", "immédiatement" → today's date (provided in user message)
  - European "DD.MM.YYYY" → convert to ISO
  - null if not stated or nonsensical (past >90d, future >730d)

`agency_phone` (Swiss phone):
  - Normalize to "+41 AA BBB CC DD" (with single spaces). Accept any Swiss format.
  - null if non-Swiss or the format is unclear

`agency_email`:
  - lowercased, single email
  - REJECT generic providers (null): gmail, bluewin, hotmail, yahoo, outlook, gmx, icloud, protonmail, mail, proton, swissmail

`agency_name`:
  - derive from the DOMAIN of agency_email (second-to-last dotted label, Title Case).
    Examples: "info@robinreal.ch" → "Robinreal"; "do@a-zimmodienste.ch" → "A-zimmodienste"
  - null if agency_email is null or from a generic provider

## Confidence

0.0 – 1.0 calibrated to YOUR certainty. Anchor points:
  - 0.95+ = explicit, unambiguous mention in the detected language
  - 0.80  = clear mention but ambiguous wording or cross-language
  - 0.60  = plausible but indirect
  - 0.40  = weak inference
  - <0.40 prefer to return null

## Critical rules

1. NEVER invent. Only extract what's truly in the description.
2. Negation window is ~3 tokens before the target noun — "kein/ohne/pas de/sans/senza/non/no/not".
3. "Balkon möglich" (balcony possible) is NOT a positive — it's a future state; return null.
4. "Garage optional" / "upon request" → null (not a present feature).
5. Do not confuse rooms count with floor number (e.g. "3 Zimmer" is rooms, "3. Stock" is floor).
6. Return `null` for fields you cannot verify. Null is honest; a guess is a bug.
7. Ignore contact info that clearly belongs to a third-party (management company URLs, etc.)
   if multiple candidates — prefer the first agency-looking email/phone in the text.

Emit ALL 19 fields on every response, each with {value, confidence, raw_snippet}.
If a field doesn't apply, return {"value": null, "confidence": 0.0, "raw_snippet": null}.
"""

# ---- cache helpers ---------------------------------------------------------


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
                        f"[WARN] pass2_gpt_extract._load_cache: expected=valid jsonl, "
                        f"got={type(exc).__name__} at line {line_no}, fallback=skip line",
                        flush=True,
                    )
    except OSError as exc:
        print(
            f"[WARN] pass2_gpt_extract._load_cache: expected=readable {CACHE_PATH}, "
            f"got={exc!r}, fallback=empty cache",
            flush=True,
        )
    return cache


_CACHE_LOCK: asyncio.Lock | None = None


def _append_cache_line(rec: dict[str, Any]) -> None:
    """Append one cache record. Uses O_APPEND (atomic for < PIPE_BUF on POSIX)."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    line = (json.dumps(rec, ensure_ascii=False, default=str) + "\n").encode("utf-8")
    # os.write on O_APPEND fd is atomic for writes smaller than PIPE_BUF (4 KB).
    # Our lines are ~1-3 KB so this holds. For larger, the asyncio.Lock below
    # in the worker serialises access within the process.
    fd = os.open(str(CACHE_PATH), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, line)
    finally:
        os.close(fd)


# ---- client -----------------------------------------------------------------


def _make_client():
    try:
        from openai import AsyncOpenAI
    except ImportError as exc:  # pragma: no cover - dependency is project-pinned
        raise RuntimeError(
            "openai SDK not installed; add openai to the conda env"
        ) from exc
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY missing — set it in .env before running pass 2 GPT"
        )
    return AsyncOpenAI(api_key=api_key, timeout=TIMEOUT_S)


# ---- post-validation (identical rules to regex pass) ---------------------


# Already-normalized Swiss E.164
_PHONE_E164_RE = re.compile(r"^\+41\s\d{2}\s\d{3}\s\d{2}\s\d{2}$")
# Swiss local / international variants — same as regex pass agency_phone.yaml
_SWISS_PHONE_RE = re.compile(
    r"(?:\+41|0041|0)\s*\(?0?([1-9]\d)\)?[\s.\-\u00a0]?(\d{3})[\s.\-\u00a0]?(\d{2})[\s.\-\u00a0]?(\d{2})"
)


def _validate_year(v: str) -> tuple[bool, str, str]:
    try:
        y = int(v)
    except (TypeError, ValueError):
        return False, v, f"year not int: {v!r}"
    now = dt.date.today().year
    if 1800 <= y <= now + 5:
        return True, str(y), ""
    return False, v, f"year out of [1800,{now + 5}]: {y}"


def _validate_area(v: str) -> tuple[bool, str, str]:
    try:
        a = int(float(v))
    except (TypeError, ValueError):
        return False, v, f"area not int: {v!r}"
    if 10 <= a <= 500:
        return True, str(a), ""
    return False, v, f"area out of [10,500]: {a}"


def _validate_floor(v: str) -> tuple[bool, str, str]:
    try:
        f = int(v)
    except (TypeError, ValueError):
        return False, v, f"floor not int: {v!r}"
    if -1 <= f <= 99:
        return True, str(f), ""
    return False, v, f"floor out of [-1,99]: {f}"


def _validate_available_from(v: str) -> tuple[bool, str, str]:
    try:
        d = dt.date.fromisoformat(v[:10])
    except (TypeError, ValueError):
        return False, v, f"date not ISO: {v!r}"
    today = dt.date.today()
    lo = today - dt.timedelta(days=90)
    hi = today + dt.timedelta(days=730)
    if lo <= d <= hi:
        return True, d.isoformat(), ""
    return False, v, f"date outside [today-90d, today+730d]: {d.isoformat()}"


def _validate_phone(v: str) -> tuple[bool, str, str]:
    """Accept Swiss phone formats and normalize to E.164 '+41 AA BBB CC DD'.

    Strict on inputs to avoid extracting a phone embedded in a larger free-text
    blob that GPT might hallucinate (e.g. "call +41 44 123 45 67 or email …").
    We use fullmatch so the entire string must BE the phone — if not, we try to
    fullmatch the Swiss-local variants via `_SWISS_PHONE_RE`.
    """
    if not v:
        return False, v, "phone empty"
    s = str(v).strip()
    if _PHONE_E164_RE.fullmatch(s):
        return True, s, ""
    m = _SWISS_PHONE_RE.fullmatch(s)
    if m is None:
        # Permissive fallback: allow GPT to have included a bounded amount of
        # noise (e.g. "Tel. 044 123 45 67") by taking the FIRST Swiss-pattern
        # match within the first 30 chars. Any longer and we reject.
        if len(s) <= 30:
            m = _SWISS_PHONE_RE.search(s)
        if m is None:
            return False, v, f"phone doesn't match Swiss pattern: {v!r}"
    normalized = f"+41 {m.group(1)} {m.group(2)} {m.group(3)} {m.group(4)}"
    return True, normalized, ""


def _validate_email(v: str) -> tuple[bool, str, str]:
    if not v or "@" not in v:
        return False, v, f"email missing @: {v!r}"
    v_low = v.strip().lower()
    domain = v_low.rsplit("@", 1)[-1]
    parts = domain.split(".")
    if len(parts) < 2:
        return False, v, f"email domain invalid: {domain!r}"
    # reject generic providers (matches regex-pass agency_name rules)
    if parts[-2] in GENERIC_EMAIL_DOMAINS:
        return False, v, f"generic provider domain: {parts[-2]}"
    return True, v_low, ""


def _validate_agency_name(v: str) -> tuple[bool, str, str]:
    s = (v or "").strip()
    if not s:
        return False, v, "agency_name empty"
    if s.lower() in GENERIC_EMAIL_DOMAINS:
        return False, v, f"agency_name is generic provider: {s!r}"
    return True, s, ""


def _derive_agency_name_from_email(email: str) -> str | None:
    """Contract parity with regex pass: derive from email domain second label,
    Title Case, reject generic providers. Returns None if not derivable.
    """
    if not email or "@" not in email:
        return None
    domain = email.strip().lower().rsplit("@", 1)[-1]
    parts = domain.split(".")
    if len(parts) < 2:
        return None
    label = parts[-2]
    if label in GENERIC_EMAIL_DOMAINS:
        return None
    if not label:
        return None
    return label.capitalize()


def _validate_feature(v: str) -> tuple[bool, str, str]:
    if v in ("0", "1"):
        return True, v, ""
    return False, v, f"feature value must be '0' or '1', got {v!r}"


VALIDATORS: dict[str, Any] = {
    **{f: _validate_feature for f in FEATURE_FIELDS},
    "year_built":     _validate_year,
    "area":           _validate_area,
    "floor":          _validate_floor,
    "available_from": _validate_available_from,
    "agency_phone":   _validate_phone,
    "agency_email":   _validate_email,
    "agency_name":    _validate_agency_name,
}


def _clamp_confidence(c: float) -> float:
    """GPT sometimes emits 1.0001 / -0.0 due to numeric rendering; bound it."""
    try:
        cf = float(c)
    except (TypeError, ValueError):
        return 0.0
    if cf < 0.0:
        return 0.0
    if cf > 1.0:
        return 1.0
    return cf


# ---- single listing call ----------------------------------------------------


async def _extract_one(client, listing_id: str, title: str, description: str) -> dict[str, Any] | None:
    """Call GPT on one listing; return cached-shape dict or None on terminal failure.

    Retries on OpenAI rate-limit / transient errors with exponential backoff (3 tries).
    Other exceptions (bad auth, 400, etc.) fail fast — no point retrying.
    """
    # Lazy-import for error-class catching; client may be AsyncOpenAI
    try:
        from openai import APIStatusError, RateLimitError, APITimeoutError, APIConnectionError
    except ImportError:
        RateLimitError = APIStatusError = APITimeoutError = APIConnectionError = Exception  # type: ignore

    today_iso = dt.date.today().isoformat()
    user_msg = (
        f"TODAY: {today_iso}\n"
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
                text_format=ListingExtraction,
                reasoning={"effort": "none"},
                max_output_tokens=1500,
            )
            break
        except (RateLimitError, APITimeoutError, APIConnectionError) as exc:
            last_exc = exc
            sleep_s = 2.0 ** (attempt + 1)  # 2, 4, 8
            print(
                f"[WARN] pass2_gpt_extract._extract_one: expected=gpt_response, "
                f"got={type(exc).__name__} (retrying in {sleep_s:.0f}s attempt={attempt + 1}/3), "
                f"listing_id={listing_id}",
                flush=True,
            )
            await asyncio.sleep(sleep_s)
        except APIStatusError as exc:
            # 4xx (non-429) — don't retry, these are prompt/schema problems
            print(
                f"[WARN] pass2_gpt_extract._extract_one: expected=gpt_response, "
                f"got=APIStatusError status={getattr(exc, 'status_code', '?')}, "
                f"fallback=skip listing_id={listing_id} exc={exc!r}",
                flush=True,
            )
            return None
        except Exception as exc:
            last_exc = exc
            print(
                f"[WARN] pass2_gpt_extract._extract_one: expected=gpt_response, "
                f"got={type(exc).__name__}: {exc!r}, fallback=skip listing_id={listing_id}",
                flush=True,
            )
            return None
    if resp is None:
        print(
            f"[WARN] pass2_gpt_extract._extract_one: exhausted 3 retries, "
            f"fallback=skip listing_id={listing_id} last_exc={last_exc!r}",
            flush=True,
        )
        return None
    elapsed = time.monotonic() - t0

    if getattr(resp, "status", None) == "incomplete":
        print(
            f"[WARN] pass2_gpt_extract._extract_one: expected=completed, "
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
                        f"[WARN] pass2_gpt_extract._extract_one: expected=output_parsed, "
                        f"got=refusal ({getattr(content, 'refusal', '')!r}), "
                        f"fallback=skip listing_id={listing_id}",
                        flush=True,
                    )
                    return None
        print(
            f"[WARN] pass2_gpt_extract._extract_one: expected=output_parsed, "
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
            "cached_input":  getattr(getattr(usage, "input_tokens_details", None), "cached_tokens", None),
        },
        "extraction": parsed.model_dump(mode="json"),
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


# ---- apply extraction to DB (respects non-overwrite invariant) --------------


def _apply_extraction(
    conn: sqlite3.Connection,
    listing_id: str,
    current_sources: dict[str, str] | None,  # may be None if we should re-read from DB
    extraction: dict[str, Any],
    stats: Counter,
) -> int:
    """Write pending fields. Returns number of fields updated.

    If `current_sources` is None, we re-read `_source` columns fresh from the
    DB — this defeats stale caches after concurrent pass 1b writes on the same
    rows, at the cost of one SELECT per row. Pass the snapshot when we KNOW
    nothing else has touched the row in between (e.g. cache-apply loop).
    """
    # Agency flow: agency_email validated first, then agency_name re-derived
    # from the validated email to preserve contract parity with the regex pass.
    ordered_fields = FEATURE_FIELDS + (
        "year_built", "floor", "area", "available_from",
        "agency_phone", "agency_email",   # email BEFORE name so derivation works
        "agency_name",
    )

    if current_sources is None:
        source_cols = [f"{f}_source" for f in ordered_fields]
        row = conn.execute(
            f"SELECT {', '.join(source_cols)} FROM listings_enriched WHERE listing_id=?",
            (listing_id,),
        ).fetchone()
        if row is None:
            stats["no_enriched_row"] += 1
            return 0
        current_sources = {col: row[col] for col in source_cols}

    validated_email: str | None = None
    n_written = 0
    for field in ordered_fields:
        source_col = f"{field}_source"
        if current_sources.get(source_col) != UNKNOWN_PENDING:
            continue  # invariant: never overwrite a non-pending source

        fv = extraction.get(field)
        if not isinstance(fv, dict):
            stats[f"{field}:bad_shape"] += 1
            continue
        value = fv.get("value")
        if value is None or (isinstance(value, str) and not value.strip()):
            stats[f"{field}:gpt_null"] += 1
            continue
        value = str(value).strip()

        # Agency name parity: always derive from validated email, never trust GPT.
        if field == "agency_name" and validated_email:
            derived = _derive_agency_name_from_email(validated_email)
            if not derived:
                stats[f"{field}:email_not_derivable"] += 1
                continue
            value = derived
        elif field == "agency_name" and not validated_email:
            # No validated email this row → reject GPT-supplied name (contract parity)
            stats[f"{field}:no_email"] += 1
            continue

        validator = VALIDATORS.get(field)
        if validator is None:
            stats[f"{field}:no_validator"] += 1
            continue
        ok, normalized, reason = validator(value)
        if not ok:
            print(
                f"[WARN] pass2_gpt_extract._apply_extraction: field={field} "
                f"validation_failed={reason} listing_id={listing_id}, fallback=drop",
                flush=True,
            )
            stats[f"{field}:validation_fail"] += 1
            continue

        if field == "agency_email":
            validated_email = normalized

        raw_snippet = fv.get("raw_snippet")
        if raw_snippet is not None:
            raw_snippet = str(raw_snippet)[:500]

        base = BASE_CONFIDENCE.get(field, 0.70)
        gpt_conf = _clamp_confidence(fv.get("confidence", 0.0))
        # Blend: keep the LOWER of (GPT's self-rated conf) and (base confidence)
        # so we don't let a confident GPT override our calibrated priors.
        final_conf = round(min(base, gpt_conf), 3)

        # Negation cue — any feature_* with value "0" is by definition a
        # negation (features are present or absent, nothing in between).
        # Cap confidence at 0.5 (regex-pass parity).
        if field.startswith("feature_") and normalized == "0":
            final_conf = round(min(0.5, base * 0.6), 3)

        write_field(
            conn,
            listing_id=listing_id,
            field=field,
            filled=normalized,
            source=TEXT_GPT_5_4,
            confidence=final_conf,
            raw=raw_snippet,
        )
        n_written += 1
        stats[f"{field}:written"] += 1
    return n_written


# ---- orchestration ---------------------------------------------------------


def _fetch_pending_rows(conn: sqlite3.Connection, limit: int | None) -> list[tuple[str, str, str, dict[str, str]]]:
    """Return [(listing_id, title, description, current_sources_dict), ...] for rows
    that have at least one pass-2 target field still UNKNOWN-pending."""
    pass2_target_sources = [f"le.{f}_source" for f in (
        *FEATURE_FIELDS,
        "year_built", "floor", "area", "available_from",
        "agency_phone", "agency_email", "agency_name",
    )]
    any_pending_clause = " OR ".join(f"{c}=?" for c in pass2_target_sources)
    sql = f"""
        SELECT l.listing_id, l.title, l.description,
               {", ".join(pass2_target_sources)}
        FROM listings l
        JOIN listings_enriched le USING(listing_id)
        WHERE {any_pending_clause}
    """
    params = tuple([UNKNOWN_PENDING] * len(pass2_target_sources))
    if limit is not None:
        sql += " LIMIT ?"
        params = params + (int(limit),)
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    # Build sources dict for each row — preserves the exact column naming
    source_cols = [
        *[f"{f}_source" for f in FEATURE_FIELDS],
        "year_built_source", "floor_source", "area_source", "available_from_source",
        "agency_phone_source", "agency_email_source", "agency_name_source",
    ]
    out: list[tuple[str, str, str, dict[str, str]]] = []
    for r in rows:
        listing_id = r["listing_id"]
        title = r["title"] or ""
        raw_desc = r["description"] or ""
        desc_plain = strip_html(raw_desc)
        sources = {col: r[col] for col in source_cols}
        out.append((listing_id, title, desc_plain, sources))
    return out


async def _worker(sem: asyncio.Semaphore, client, item, stats: Counter, cache_lock: asyncio.Lock) -> dict | None:
    listing_id, title, desc = item
    async with sem:
        result = await _extract_one(client, listing_id, title, desc)
        if result is not None:
            stats["gpt_calls_ok"] += 1
            async with cache_lock:
                _append_cache_line(result)
        else:
            stats["gpt_calls_failed"] += 1
        return result


async def _run_async(
    rows,
    client,
    stats: Counter,
    *,
    conn: sqlite3.Connection | None = None,
    db_lock: asyncio.Lock | None = None,
) -> dict[str, dict]:
    """Concurrently extract. If `conn` + `db_lock` are given, each result is
    immediately applied to the DB (durable against crashes). Returns any
    results that could NOT be applied inline (should always be empty in the
    normal path; only used for dry-run).
    """
    sem = asyncio.Semaphore(CONCURRENCY)
    cache_lock = asyncio.Lock()
    tasks = [
        _worker(sem, client, (lid, t, d), stats, cache_lock)
        for (lid, t, d, _) in rows
    ]
    results: dict[str, dict] = {}
    completed = 0
    applied_since_commit = 0
    t0 = time.monotonic()
    for coro in asyncio.as_completed(tasks):
        rec = await coro
        completed += 1
        if rec is None:
            pass  # failure already logged inside _extract_one
        elif conn is not None and db_lock is not None:
            async with db_lock:
                # sqlite3 isn't threadsafe across coroutines without
                # serialization; db_lock guarantees only one coroutine ever
                # calls UPDATE at a time. Pass current_sources=None so the
                # write-path re-reads _source columns fresh (defeats races
                # with concurrent pass 1b on the same rows).
                _apply_extraction(conn, rec["listing_id"], None, rec["extraction"], stats)
                # Commit after every write. At ~1.67 writes/s (TPM-limited),
                # per-commit overhead is ~5 ms × 1.67/s = 8 ms/s — negligible.
                # Holding the tx open across multiple writes blocks pass 1b
                # until it times out (30s busy_timeout).
                conn.commit()
                applied_since_commit += 1
        else:
            # dry-run path: accumulate + return without writing
            results[rec["listing_id"]] = rec
        if completed % 50 == 0 or completed == len(tasks):
            elapsed = time.monotonic() - t0
            rate = completed / elapsed if elapsed > 0 else 0
            remaining = (len(tasks) - completed) / rate if rate > 0 else 0
            print(
                f"[INFO] pass2_gpt_extract: {completed}/{len(tasks)} "
                f"elapsed_s={elapsed:.0f} rate={rate:.1f}/s eta_s={remaining:.0f}",
                flush=True,
            )
    if conn is not None and applied_since_commit > 0:
        conn.commit()
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, type=Path, help="path to listings.db")
    parser.add_argument("--limit", type=int, default=None, help="smoke-test: max listings to process")
    parser.add_argument("--dry-run", action="store_true", help="don't write to DB")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"[ERROR] pass2_gpt_extract: db not found at {args.db}", flush=True)
        return 1

    t_start = time.monotonic()
    print(
        f"[INFO] pass2_gpt_extract: model={MODEL} concurrency={CONCURRENCY} "
        f"max_calls={MAX_CALLS} db={args.db} limit={args.limit} dry_run={args.dry_run}",
        flush=True,
    )

    # Load cache up front so we can short-circuit already-extracted rows
    cache = _load_cache()
    print(f"[INFO] pass2_gpt_extract: cache loaded entries={len(cache)}", flush=True)

    with connect(args.db) as conn:
        conn.row_factory = sqlite3.Row
        rows = _fetch_pending_rows(conn, args.limit)
        print(f"[INFO] pass2_gpt_extract: pending_rows={len(rows)}", flush=True)

        # Partition: cached vs uncached. Apply cached extractions immediately.
        uncached: list[tuple[str, str, str, dict[str, str]]] = []
        stats = Counter()
        cache_hits = 0
        applied_since_commit = 0
        for (lid, title, desc, srcs) in rows:
            key = _cache_key(lid, MODEL)
            if key in cache:
                cache_hits += 1
                extraction = cache[key].get("extraction", {})
                if not args.dry_run:
                    _apply_extraction(conn, lid, srcs, extraction, stats)
                    applied_since_commit += 1
                    if applied_since_commit >= 25:
                        conn.commit()
                        applied_since_commit = 0
            else:
                uncached.append((lid, title, desc, srcs))
        if cache_hits:
            print(f"[INFO] pass2_gpt_extract: applied cached extractions n={cache_hits}", flush=True)
        conn.commit()

        # Budget guard
        if len(uncached) > MAX_CALLS:
            print(
                f"[WARN] pass2_gpt_extract: expected={MAX_CALLS} max calls, "
                f"got={len(uncached)} uncached rows, fallback=truncating to MAX_CALLS "
                f"(remainder stays UNKNOWN-pending for next run)",
                flush=True,
            )
            uncached = uncached[:MAX_CALLS]

        if not uncached:
            print("[INFO] pass2_gpt_extract: no uncached rows — done.", flush=True)
        else:
            try:
                client = _make_client()
            except RuntimeError as exc:
                print(f"[ERROR] pass2_gpt_extract: {exc}", flush=True)
                return 2

            try:
                # Apply results inline as each task completes — durable against
                # SIGKILL. db_lock + cache_lock independently serialize DB vs
                # cache-file access across concurrent coroutines.
                async def _go():
                    db_lock = asyncio.Lock()
                    return await _run_async(
                        uncached, client, stats,
                        conn=None if args.dry_run else conn,
                        db_lock=None if args.dry_run else db_lock,
                    )
                unapplied = asyncio.run(_go())
                if unapplied:
                    # Only hit in dry-run; drop on the floor.
                    print(
                        f"[INFO] pass2_gpt_extract: dry_run discarded {len(unapplied)} results",
                        flush=True,
                    )
            except KeyboardInterrupt:
                print("[WARN] pass2_gpt_extract: interrupted; partial results are cached + committed", flush=True)
                conn.commit()
                return 130

    elapsed = time.monotonic() - t_start
    print(f"\n[INFO] pass2_gpt_extract DONE elapsed_s={elapsed:.1f}", flush=True)
    # Print stats summary
    wrote = sum(v for k, v in stats.items() if k.endswith(":written"))
    failed = stats.get("gpt_calls_failed", 0)
    print(f"[INFO] pass2_gpt_extract: gpt_calls_ok={stats.get('gpt_calls_ok', 0)} gpt_calls_failed={failed} fields_written={wrote}", flush=True)
    per_field = {
        k.split(":")[0]: stats[k] for k in stats if k.endswith(":written")
    }
    if per_field:
        print("[INFO] pass2_gpt_extract: per-field writes:", flush=True)
        for field in sorted(per_field, key=lambda f: -per_field[f]):
            print(f"    {field:32s} {per_field[field]:>6}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
