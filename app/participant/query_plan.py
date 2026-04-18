"""Query understanding: natural-language query → QueryPlan via Claude forced tool-use.

Single Claude call with:
  - forced tool-use (`tool_choice={"type":"tool","name":"emit_query_plan"}`)
  - strict structured output (constrained decoding)
  - cached system prompt (5-min ephemeral TTL; 2048+ tokens to cross Sonnet 4.6 cache threshold)
  - 5 s timeout

On any failure (missing key, network, timeout, schema-validation), we fall back to a
regex-based extractor that pulls rooms/price/city best-effort. Every fallback emits
a `[WARN]` log line per CLAUDE.md §5 — no silent degradation.

LRU-cached on raw query so `extract_hard_facts()` and `extract_soft_facts()` share
one API round-trip.
"""
from __future__ import annotations

import json
import os
import re
import time
from functools import lru_cache

import anthropic
from dotenv import load_dotenv
from pydantic import ValidationError

from app.models.schemas import (
    Feature,
    HardFilters,
    NumRange,
    QueryPlan,
    SoftPreferences,
)

# Load .env once at import
load_dotenv()

# --- configuration ----------------------------------------------------------

CLAUDE_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-5")
# Forced tool-use + long cached system prompt measures ~6-7s on Sonnet 4.5 in our env.
# 15s gives headroom for cache-miss + slow network while still failing loudly on real problems.
API_TIMEOUT_SECONDS = float(os.getenv("ANTHROPIC_TIMEOUT_S", "15"))
MAX_QUERY_LEN = 1000
MAX_TOKENS = 1024

_API_KEY = os.getenv("ANTHROPIC_API_KEY")
_client: anthropic.Anthropic | None = None

if _API_KEY:
    _client = anthropic.Anthropic(api_key=_API_KEY, timeout=API_TIMEOUT_SECONDS)
    print(
        f"[INFO] query_plan: anthropic client initialized model={CLAUDE_MODEL} timeout={API_TIMEOUT_SECONDS}s",
        flush=True,
    )
else:
    print(
        "[WARN] query_plan: expected=ANTHROPIC_API_KEY, got=unset, "
        "fallback=regex-only (Claude-based extraction disabled until key is set)",
        flush=True,
    )

# --- canonical vocabulary ---------------------------------------------------

FEATURE_VOCAB = {
    "balcony",
    "elevator",
    "parking",
    "garage",
    "fireplace",
    "child_friendly",
    "pets_allowed",
    "temporary",
    "new_build",
    "wheelchair_accessible",
    "private_laundry",
    "minergie_certified",
}

CITY_ALIASES: dict[str, str] = {
    # canonical english → variants
    "zurich": "Zürich",
    "zürich": "Zürich",
    "zurigo": "Zürich",
    "geneva": "Genève",
    "genève": "Genève",
    "genf": "Genève",
    "ginevra": "Genève",
    "bern": "Bern",
    "berna": "Bern",
    "berne": "Bern",
    "basel": "Basel",
    "bâle": "Basel",
    "basilea": "Basel",
    "lausanne": "Lausanne",
    "lugano": "Lugano",
    "luzern": "Luzern",
    "lucerne": "Luzern",
    "winterthur": "Winterthur",
    "st. gallen": "St. Gallen",
    "st.gallen": "St. Gallen",
    "sankt gallen": "St. Gallen",
}

CANTON_CODES = {
    "ZH", "GE", "VD", "TI", "BS", "BE", "BL", "LU", "AG", "SG", "SO", "VS",
    "ZG", "SH", "FR", "TG", "JU", "GR", "NE", "GL", "OW", "NW", "UR", "SZ",
    "AR", "AI",
}

# Canonical object_category values that appear in the DB (German). Claude must
# use these verbatim or leave null — ANY other string here is silently dropped
# at adapter time (with a [WARN]) because it would never match the SQL gate.
OBJECT_CATEGORY_VOCAB = {
    "Wohnung",         # apartment (most common)
    "Möblierte Wohnung",
    "Studio",          # studio
    "Maisonette",
    "Attika",          # attic/penthouse
    "Dachwohnung",
    "Terrassenwohnung",
    "Loft",
    "Haus",            # house
    "Reihenhaus",
    "Doppeleinfamilienhaus",
    "Mehrfamilienhaus",
    "Villa",
    "Bauernhaus",
    "Ferienwohnung",
    "Einzelzimmer",
    "WG-Zimmer",
}

# --- tool schema (hand-maintained to match QueryPlan, trimmed for strict mode) ---
# Anthropic strict mode does not support min/max/regex-backreferences etc.

_TOOL_SCHEMA = {
    "name": "emit_query_plan",
    "description": (
        "Emit the structured query plan extracted from the user's real-estate "
        "search query. Set any unknown field to null (do NOT guess). Only populate "
        "a field if the user's text clearly supports it."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "city": {
                "type": ["array", "null"],
                "items": {"type": "string"},
                "description": "Canonical city names (e.g. 'Zürich'), or null if not specified.",
            },
            "postal_code": {
                "type": ["array", "null"],
                "items": {"type": "string"},
            },
            "canton": {
                "type": ["string", "null"],
                "description": "2-letter canton code (e.g. 'ZH') or null.",
            },
            "price": {
                "type": "object",
                "properties": {
                    "min_value": {"type": ["number", "null"]},
                    "max_value": {"type": ["number", "null"]},
                },
                "required": ["min_value", "max_value"],
                "additionalProperties": False,
            },
            "rooms": {
                "type": "object",
                "properties": {
                    "min_value": {"type": ["number", "null"]},
                    "max_value": {"type": ["number", "null"]},
                },
                "required": ["min_value", "max_value"],
                "additionalProperties": False,
            },
            "latitude": {"type": ["number", "null"]},
            "longitude": {"type": ["number", "null"]},
            "radius_km": {"type": ["number", "null"]},
            "offer_type": {
                "type": ["string", "null"],
                "description": "Almost always 'RENT' for this system; null if unclear.",
            },
            "object_category": {
                "type": ["array", "null"],
                "items": {"type": "string"},
            },
            "required_features": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Canonical feature keys the user HARD-REQUIRES. Choose from: "
                    "balcony, elevator, parking, garage, fireplace, child_friendly, "
                    "pets_allowed, temporary, new_build, wheelchair_accessible, "
                    "private_laundry, minergie_certified."
                ),
            },
            "soft": {
                "type": "object",
                "properties": {
                    "keywords": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Free-text soft-preference terms that should boost ranking "
                            "(e.g. 'bright', 'modern', 'quiet', 'view', 'central')."
                        ),
                    },
                    "negatives": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Terms the user wants to AVOID (e.g. 'ground floor', "
                            "'basement', 'noisy', 'near highway')."
                        ),
                    },
                    "price_sentiment": {
                        "type": ["string", "null"],
                        "enum": ["cheap", "moderate", "premium", None],
                    },
                    "features": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "required": {"type": "boolean"},
                            },
                            "required": ["name", "required"],
                            "additionalProperties": False,
                        },
                    },
                },
                "required": ["keywords", "negatives", "price_sentiment", "features"],
                "additionalProperties": False,
            },
            "rewrites": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "2-3 short paraphrases of the user's query in a MIX of languages "
                    "(DE + FR + EN or DE + IT + EN) to boost BM25 recall. Preserve "
                    "literal domain tokens like 'Attika', 'Minergie', 'Maisonette', "
                    "'Altbau'. Do NOT add constraints the user did not express."
                ),
            },
            "confidence": {
                "type": "number",
                "description": "Self-rated confidence 0.0-1.0 in your extraction.",
            },
            "clarification_needed": {"type": "boolean"},
            "clarification_question": {"type": ["string", "null"]},
        },
        "required": [
            "city",
            "postal_code",
            "canton",
            "price",
            "rooms",
            "latitude",
            "longitude",
            "radius_km",
            "offer_type",
            "object_category",
            "required_features",
            "soft",
            "rewrites",
            "confidence",
            "clarification_needed",
            "clarification_question",
        ],
        "additionalProperties": False,
    },
}

# --- system prompt (cached; must exceed ~2048 tokens for Sonnet 4.6 cache hit) ---

_SYSTEM_PROMPT = """You extract structured search intent from Swiss real-estate rental search queries.

Input may be in German, French, Italian, or English — or mixed. The output is consumed \
by a downstream hybrid search system (SQL filter + BM25). Accuracy and calibration matter \
more than verbosity; hard filters cannot be violated.

## Classification rules

HARD (goes to SQL gate; violation disqualifies a listing):
- Explicit numeric operators: "unter 2800", "max 3000", "bis 3500", "under 2800 CHF", \
  "sous 2500", "fino a 2000", "at most", "no more than", "mindestens 3", "at least", "from 60m²".
- Explicit required features: "muss Balkon haben", "must have parking", "with elevator".
- Explicit city/canton/postal.
- Number of rooms when stated as exact or range ("3 Zimmer", "3.5 rooms", "2-3 rooms").

SOFT (goes to BM25 / ranking; never disqualifies):
- Hedged language: "ideally", "gerne", "if possible", "nice to have", "plutôt", "magari", \
  "nicht zu teuer", "not too expensive", "eher", "preferably", "bevorzugt".
- Sensory / qualitative adjectives: "bright", "hell", "modern", "cozy", "quiet", \
  "ruhig", "family-friendly", "kinderfreundlich", "nice view".
- Vague locality: "near ETH", "close to station", "nah am Bahnhof", "central", \
  "good transport connections".

## Rules you MUST follow

1. NEVER invent constraints the user did not express. If the user says "apartment" \
   without giving rooms, leave `rooms.min_value` and `rooms.max_value` as null.
2. A single number ("3 Zimmer", "3 rooms") → set BOTH `rooms.min_value=3` AND \
   `rooms.max_value=3`. A range ("2-3 rooms") → min=2, max=3. "at least 3" → min=3, max=null.
3. "unter/under/sous/fino a N" → `price.max_value=N`, `price.min_value=null`.
4. City names: output the canonical German/French/Italian spelling the user used, \
   preferring the locally-used form. Aliases: Zurich→Zürich, Geneva→Genève, Genf→Genève, \
   Bâle→Basel, Lucerne→Luzern.
5. `canton` is a 2-letter ISO code (ZH, GE, VD, TI, BS, BE, LU, ...). Set null if uncertain.
6. `required_features`: only canonical keys from this list: balcony, elevator, parking, \
   garage, fireplace, child_friendly, pets_allowed, temporary, new_build, \
   wheelchair_accessible, private_laundry, minergie_certified. If the user's phrasing is \
   soft ("ideally with parking"), put it in `soft.features` with `required=false`, not here.
7. `object_category`: ONLY emit if the user uses a specific property term. Allowed \
   values (German, case-sensitive, use EXACTLY as written): Wohnung, Möblierte Wohnung, \
   Studio, Maisonette, Attika, Dachwohnung, Terrassenwohnung, Loft, Haus, Reihenhaus, \
   Doppeleinfamilienhaus, Mehrfamilienhaus, Villa, Bauernhaus, Ferienwohnung, \
   Einzelzimmer, WG-Zimmer. For generic words like "apartment", "flat", "apartement", \
   "appartement" → LEAVE object_category AS NULL (the user is not narrowing the category). \
   Only narrow via this field for terms that genuinely constrain (Studio, Loft, Villa, \
   Maisonette, Attika, Dachwohnung, Terrassenwohnung).
8. `offer_type` is 'RENT' by default for this system. Only set if clearly indicated.
8. `rewrites`: emit 2-3 short paraphrases that MIX languages to boost cross-lingual BM25 \
   recall. Preserve literal domain tokens (Attika, Altbau, Maisonette, Minergie). Do NOT \
   alter or add constraints.
9. `soft.keywords`: every qualitative term the user cares about, in their language. \
   `soft.negatives`: every term they want to avoid, with the negation stripped \
   ('no ground floor' → negatives=['ground floor']).
10. `price_sentiment`: 'cheap' if user says affordable/günstig/bon marché, 'premium' if \
    luxurious/exklusiv/haut de gamme, 'moderate' if "not too expensive"/"moderate", null \
    otherwise.
11. `confidence` ∈ [0.0, 1.0]: your calibrated certainty. Use 0.9+ for \
    clear queries, 0.5-0.7 for ambiguous, 0.3- for vague/uninterpretable.
12. `clarification_needed=true` ONLY when the query is too vague to serve (e.g. "nice flat") — \
    in which case return an empty-ish plan plus a natural-language \
    `clarification_question` in the user's language. For everything else, do your best \
    and leave `clarification_needed=false`.
13. Always emit via the `emit_query_plan` tool. Never respond with plain text.

## Few-shot examples

### Example 1 — English, clear hard
Query: "3-room bright apartment in Zurich under 2800 CHF with balcony, close to public transport"
Plan:
  city=["Zürich"], canton="ZH", rooms={min:3,max:3}, price={min:null,max:2800},
  required_features=["balcony"],
  soft.keywords=["bright","close to public transport"], soft.negatives=[],
  soft.price_sentiment=null, soft.features=[],
  rewrites=[
    "helle 3-Zimmer-Wohnung Zürich mit Balkon unter 2800",
    "appartement 3 pièces lumineux Zurich balcon proche transports",
    "3 room flat Zurich balcony near station"
  ],
  confidence=0.95, clarification_needed=false.

### Example 2 — German, soft-heavy, landmark
Query: "Helle 3.5-Zimmer-Wohnung in Zürich, nah am Bahnhof, max 2800 CHF"
Plan:
  city=["Zürich"], canton="ZH", rooms={min:3.5,max:3.5}, price={min:null,max:2800},
  required_features=[],
  soft.keywords=["hell","nah am Bahnhof"], soft.negatives=[],
  rewrites=[
    "bright 3.5 room apartment Zurich near station",
    "appartement 3.5 pièces lumineux Zurich proche gare"
  ],
  confidence=0.92.

### Example 3 — French
Query: "Cherche studio moderne à Genève, plutôt calme, autour de 1500 CHF"
Plan:
  city=["Genève"], canton="GE", object_category=["Studio"],
  price={min:null,max:null},  // "autour de 1500" is ambiguous, leave open
  rooms={min:null,max:null},
  soft.keywords=["moderne","calme"], soft.price_sentiment="moderate",
  soft.features=[],
  rewrites=[
    "moderne Studio Genf ruhig um 1500",
    "modern studio Geneva quiet around 1500"
  ],
  confidence=0.75.

### Example 4 — Italian
Query: "Cerco un bilocale a Lugano con balcone, max 2000 CHF"
Plan:
  city=["Lugano"], canton="TI", rooms={min:2,max:2},
  price={min:null,max:2000}, required_features=["balcony"],
  soft.keywords=[], soft.negatives=[],
  rewrites=[
    "2-Zimmer-Wohnung Lugano mit Balkon unter 2000",
    "2 room apartment Lugano balcony under 2000 CHF"
  ],
  confidence=0.93.

### Example 5 — Soft-only, negation
Query: "Modern studio in Geneva for June move-in, quiet area, nice views if possible, no ground floor"
Plan:
  city=["Genève"], canton="GE", object_category=["Studio"],
  rooms={min:null,max:null}, price={min:null,max:null},
  required_features=[],
  soft.keywords=["modern","quiet","nice views"],
  soft.negatives=["ground floor"],
  rewrites=[
    "modernes Studio Genf ruhig schöne Aussicht kein Erdgeschoss",
    "studio moderne Genève calme belle vue pas rez-de-chaussée"
  ],
  confidence=0.82.

### Example 6 — Vague, ask for clarification
Query: "nice flat"
Plan: empty-ish (everything null/empty), confidence=0.2,
  clarification_needed=true,
  clarification_question="Where are you looking (city or canton), and what's your rough budget?".

### Example 7 — Adversarial / impossible constraint
Query: "5 rooms in Geneva under CHF 500"
Plan:
  city=["Genève"], canton="GE", rooms={min:5,max:5}, price={min:null,max:500},
  confidence=0.95, clarification_needed=false.
  // Do NOT refuse; just extract. Downstream relaxation will handle zero-result case.

### Example 8 — Student / landmark-relative
Query: "affordable student accomodation, max half an hour door to door to ETH Zurich by public transport, i like modern kitchens"
Plan:
  city=["Zürich"], canton="ZH",
  rooms={min:null,max:null}, price={min:null,max:null},
  soft.keywords=["modern kitchens","student","half hour ETH"],
  soft.price_sentiment="cheap",
  rewrites=[
    "günstige Studentenwohnung Zürich ETH 30 Minuten moderne Küche",
    "logement étudiant abordable Zurich ETH 30 min cuisine moderne"
  ],
  confidence=0.75.

### Example 9 — "Not too expensive" → soft moderate
Query: "Bright family-friendly flat in Winterthur, not too expensive, ideally with parking"
Plan:
  city=["Winterthur"], canton="ZH",
  required_features=["child_friendly"],
  soft.keywords=["bright","family-friendly"],
  soft.price_sentiment="moderate",
  soft.features=[{name:"parking",required:false}],
  confidence=0.9.

### Example 10 — Rooms range + feature exclusion via negation
Query: "2 to 3.5 room apartment in Basel, no basement, with elevator"
Plan:
  city=["Basel"], canton="BS", rooms={min:2,max:3.5},
  required_features=["elevator"],
  soft.negatives=["basement"],
  confidence=0.95.

## End of rules. Emit the tool now."""

# --- primary API ------------------------------------------------------------


@lru_cache(maxsize=256)
def get_plan(query: str) -> QueryPlan:
    """Extract a QueryPlan from a natural-language query.

    LRU-cached so that the hard_fact + soft_fact delegators share one call.
    Never raises on known failure modes — falls back to regex with a [WARN] log.
    Does raise for invalid inputs (empty, too long).
    """
    if not isinstance(query, str):
        raise TypeError(f"query must be str, got {type(query).__name__}")
    query = query.strip()
    if not query:
        raise ValueError("query must not be empty")
    if len(query) > MAX_QUERY_LEN:
        raise ValueError(
            f"query too long: {len(query)} chars (max {MAX_QUERY_LEN})"
        )

    if _client is None:
        print(
            f"[WARN] query_plan.get_plan: expected=claude_api, got=no_api_key, "
            f"fallback=regex query={query!r}",
            flush=True,
        )
        return _regex_fallback(query)

    t0 = time.monotonic()
    try:
        response = _client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=MAX_TOKENS,
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            tools=[_TOOL_SCHEMA],
            tool_choice={"type": "tool", "name": "emit_query_plan"},
            messages=[{"role": "user", "content": query}],
        )
    except anthropic.APITimeoutError as exc:
        print(
            f"[WARN] query_plan.get_plan: expected=claude_response, got=timeout "
            f"after {API_TIMEOUT_SECONDS}s, fallback=regex query={query!r} exc={exc!r}",
            flush=True,
        )
        return _regex_fallback(query)
    except anthropic.APIError as exc:
        print(
            f"[WARN] query_plan.get_plan: expected=claude_response, got=api_error, "
            f"fallback=regex query={query!r} status={getattr(exc, 'status_code', 'NA')} "
            f"exc={exc!r}",
            flush=True,
        )
        return _regex_fallback(query)
    elapsed = time.monotonic() - t0

    tool_use_block = next(
        (b for b in response.content if getattr(b, "type", None) == "tool_use"),
        None,
    )
    if tool_use_block is None:
        print(
            f"[WARN] query_plan.get_plan: expected=tool_use_block, "
            f"got=stop_reason={response.stop_reason!r}, fallback=regex "
            f"query={query!r}",
            flush=True,
        )
        return _regex_fallback(query)

    raw = dict(tool_use_block.input)
    try:
        plan = _build_plan(raw, query)
    except (ValidationError, KeyError, TypeError, ValueError) as exc:
        print(
            f"[WARN] query_plan.get_plan: expected=valid_QueryPlan, got=schema_error, "
            f"fallback=regex query={query!r} raw={json.dumps(raw)[:300]!r} exc={exc!r}",
            flush=True,
        )
        return _regex_fallback(query)

    usage = getattr(response, "usage", None)
    cache_hit = getattr(usage, "cache_read_input_tokens", 0) if usage else 0
    cache_write = getattr(usage, "cache_creation_input_tokens", 0) if usage else 0
    print(
        f"[INFO] query_plan.get_plan: elapsed_s={elapsed:.2f} confidence={plan.confidence:.2f} "
        f"clarify={plan.clarification_needed} cache_read={cache_hit} cache_write={cache_write} "
        f"hard_fields={_hard_field_count(plan)} rewrites={len(plan.rewrites)}",
        flush=True,
    )
    return plan


def _build_plan(raw: dict, query: str) -> QueryPlan:
    """Coerce Claude's tool output into a validated QueryPlan."""
    soft_raw = raw.get("soft") or {}
    soft = SoftPreferences(
        keywords=list(soft_raw.get("keywords") or []),
        negatives=list(soft_raw.get("negatives") or []),
        price_sentiment=_coerce_sentiment(soft_raw.get("price_sentiment")),
        features=[
            Feature(name=str(f.get("name", "")).strip(), required=bool(f.get("required", False)))
            for f in (soft_raw.get("features") or [])
            if str(f.get("name", "")).strip()
        ],
    )

    required_features = _canonicalize_features(raw.get("required_features") or [])
    object_category = _canonicalize_object_category(raw.get("object_category") or [])

    return QueryPlan(
        city=_nonempty_list(raw.get("city")),
        postal_code=_nonempty_list(raw.get("postal_code")),
        canton=_canonicalize_canton(raw.get("canton")),
        price=NumRange(
            min_value=raw.get("price", {}).get("min_value"),
            max_value=raw.get("price", {}).get("max_value"),
        ),
        rooms=NumRange(
            min_value=raw.get("rooms", {}).get("min_value"),
            max_value=raw.get("rooms", {}).get("max_value"),
        ),
        latitude=raw.get("latitude"),
        longitude=raw.get("longitude"),
        radius_km=raw.get("radius_km"),
        offer_type=raw.get("offer_type"),
        object_category=object_category,
        required_features=required_features,
        soft=soft,
        rewrites=[r.strip() for r in (raw.get("rewrites") or []) if r and r.strip()][:3],
        raw_query=query,
        confidence=float(raw.get("confidence", 0.5)),
        clarification_needed=bool(raw.get("clarification_needed", False)),
        clarification_question=raw.get("clarification_question"),
    )


# --- helpers ----------------------------------------------------------------


def _nonempty_list(v) -> list[str] | None:
    if not v:
        return None
    out = [str(x).strip() for x in v if str(x).strip()]
    return out or None


def _coerce_sentiment(v):
    if v in ("cheap", "moderate", "premium"):
        return v
    return None


def _canonicalize_canton(v) -> str | None:
    if not v or not isinstance(v, str):
        return None
    code = v.strip().upper()[:2]
    if code in CANTON_CODES:
        return code
    print(
        f"[WARN] query_plan._canonicalize_canton: expected=2-letter canton, got={v!r}, fallback=None",
        flush=True,
    )
    return None


def _canonicalize_features(v) -> list[str]:
    out: list[str] = []
    for f in v or []:
        key = str(f).strip().lower().replace(" ", "_").replace("-", "_")
        if key in FEATURE_VOCAB:
            out.append(key)
        else:
            print(
                f"[WARN] query_plan._canonicalize_features: expected=known_feature, "
                f"got={f!r}, fallback=dropped",
                flush=True,
            )
    # dedupe, preserve order
    return list(dict.fromkeys(out))


# Rough synonym map for the most common cross-lingual object_category mistakes,
# so a Claude slip doesn't sink the hard filter. Verified values only.
_OBJECT_CATEGORY_SYNONYMS: dict[str, str] = {
    "apartment": "Wohnung",
    "appartement": "Wohnung",
    "appartamento": "Wohnung",
    "flat": "Wohnung",
    "house": "Haus",
    "maison": "Haus",
    "casa": "Haus",
    "studio flat": "Studio",
    "monolocale": "Studio",
    "bilocale": None,  # "2-room" — should be rooms count, not category
    "trilocale": None,
    "quadrilocale": None,
    "townhouse": "Reihenhaus",
    "penthouse": "Attika",
    "attic": "Attika",
    "duplex": "Maisonette",
}


def _canonicalize_object_category(v) -> list[str] | None:
    """Keep only canonical DB values; map common English synonyms. Drop+log the rest.

    Returns None if list becomes empty after canonicalization (matches the schema
    shape used by HardFilters / the SQL gate).
    """
    out: list[str] = []
    for f in v or []:
        s = str(f).strip()
        if not s:
            continue
        if s in OBJECT_CATEGORY_VOCAB:
            out.append(s)
            continue
        low = s.lower()
        mapped = _OBJECT_CATEGORY_SYNONYMS.get(low)
        if mapped is None and low in _OBJECT_CATEGORY_SYNONYMS:
            # Explicit drop: synonym says "this isn't a category"
            print(
                f"[WARN] query_plan._canonicalize_object_category: "
                f"got={s!r}, interpretation=rooms_count_not_category, fallback=dropped",
                flush=True,
            )
            continue
        if mapped is not None:
            print(
                f"[INFO] query_plan._canonicalize_object_category: got={s!r} → {mapped!r}",
                flush=True,
            )
            out.append(mapped)
            continue
        print(
            f"[WARN] query_plan._canonicalize_object_category: expected={sorted(OBJECT_CATEGORY_VOCAB)[:3]}..., "
            f"got={s!r}, fallback=dropped (would never match SQL gate)",
            flush=True,
        )
    if not out:
        return None
    return list(dict.fromkeys(out))


def _hard_field_count(plan: QueryPlan) -> int:
    """Diagnostic: how many hard fields were populated."""
    n = 0
    for field in ("city", "canton", "postal_code", "latitude", "object_category"):
        if getattr(plan, field):
            n += 1
    if plan.price.min_value is not None or plan.price.max_value is not None:
        n += 1
    if plan.rooms.min_value is not None or plan.rooms.max_value is not None:
        n += 1
    if plan.required_features:
        n += 1
    return n


# --- regex fallback --------------------------------------------------------

# Matches "3 rooms", "3-room", "3.5 Zimmer", "3.5-Zi.", "2 to 4 rooms", "2 bis 4 Zimmer"
_ROOMS_RE = re.compile(
    r"\b(\d(?:[.,]5)?)\s*"
    r"(?:(?:-|bis|to|à)\s*(\d(?:[.,]5)?))?"
    r"[\s-]*"
    r"(?:Zimmer|Zi\.?|pi[èe]ces?|locali|rooms?|room)\b",
    re.IGNORECASE,
)
_PRICE_RE = re.compile(
    r"(?:unter|under|max(?:imum)?|bis|sous|fino a|no more than|at most|<=?|bis zu)\s*"
    r"(\d[\d'\s]*)\s*(?:CHF|Fr\.?|chf|francs?)?",
    re.IGNORECASE,
)


def _regex_fallback(query: str) -> QueryPlan:
    """Best-effort regex extraction when Claude is unavailable.

    Extracts: rooms (single or range), max_price, canonical city.
    Everything else left null. Confidence fixed at 0.3 to signal "fallback path".
    """
    t0 = time.monotonic()
    plan = QueryPlan(raw_query=query, confidence=0.3)

    m = _ROOMS_RE.search(query)
    if m:
        lo = float(m.group(1).replace(",", "."))
        hi = float(m.group(2).replace(",", ".")) if m.group(2) else lo
        plan.rooms.min_value = lo
        plan.rooms.max_value = hi

    m = _PRICE_RE.search(query)
    if m:
        try:
            v = int(re.sub(r"[\s']", "", m.group(1)))
            if 100 <= v <= 50000:  # sanity bound
                plan.price.max_value = float(v)
        except ValueError:
            pass

    low = query.lower()
    for alias, canonical in CITY_ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", low):
            plan.city = [canonical]
            break

    elapsed = time.monotonic() - t0
    print(
        f"[INFO] query_plan._regex_fallback: elapsed_s={elapsed:.4f} "
        f"rooms=({plan.rooms.min_value},{plan.rooms.max_value}) "
        f"max_price={plan.price.max_value} city={plan.city}",
        flush=True,
    )
    return plan


# --- adapter: QueryPlan → HardFilters --------------------------------------


def queryplan_to_hard_filters(
    plan: QueryPlan,
    *,
    limit: int = 500,
    offset: int = 0,
) -> HardFilters:
    """Translate a QueryPlan into the HardFilters shape expected by the SQL gate.

    `limit=500` is deliberately high here — the SQL gate is the ALLOWED SET for
    BM25/ranking, not the final paginated output. Final pagination happens later
    in `search_service.query_from_text`.
    """
    def _to_int(x):
        return int(x) if x is not None else None

    return HardFilters(
        city=plan.city,
        postal_code=plan.postal_code,
        canton=plan.canton,
        min_price=_to_int(plan.price.min_value),
        max_price=_to_int(plan.price.max_value),
        min_rooms=plan.rooms.min_value,
        max_rooms=plan.rooms.max_value,
        latitude=plan.latitude,
        longitude=plan.longitude,
        radius_km=plan.radius_km,
        features=plan.required_features or None,
        offer_type=plan.offer_type,
        object_category=plan.object_category,
        limit=limit,
        offset=offset,
        sort_by=None,
    )
