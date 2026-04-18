from __future__ import annotations

import json
import os

from openai import OpenAI

from app.core.hard_filters import FEATURE_COLUMN_MAP
from app.core.normalize import OBJECT_CATEGORY_ENGLISH
from app.models.schemas import HardFilters


_MODEL = "gpt-4o-mini"

_FEATURE_KEYS = sorted(FEATURE_COLUMN_MAP.keys())

_CANTONS = [
    "AG", "AI", "AR", "BE", "BL", "BS", "FR", "GE", "GL", "GR", "JU", "LU", "NE",
    "NW", "OW", "SG", "SH", "SO", "SZ", "TG", "TI", "UR", "VD", "VS", "ZG", "ZH",
]

SYSTEM_PROMPT = f"""\
You extract hard constraints from a Swiss real-estate search query in German,
French, Italian, or English. Emit a single JSON object conforming to the schema.
Every field is optional and must be null when the user does not state it.
Do not invent constraints. Prices are CHF. Rooms use the Swiss convention
(e.g. "3.5 Zimmer" -> 3.5).

CANTON: use ISO 2-letter codes ({", ".join(_CANTONS)}). Never put a city in the
canton field.

CITY: emit ASCII-folded lowercase English names. Canonical aliases:
zurich, geneva, bern, basel, lucerne, biel, neuchatel, fribourg, st-gallen,
lausanne, lugano, sion, winterthur. For any other Swiss city, use its ASCII
fold (Zuerich -> zurich, Genf -> geneva, Delemont -> delemont).

OBJECT_CATEGORY: choose from the English canonical enum. Map DE/FR/IT terms:
Wohnung/appartement/appartamento -> apartment; Moeblierte Wohnung/meuble ->
furnished_apartment; Haus/maison/casa -> house; Studio -> studio;
Loft -> loft; Villa -> villa; Reihenhaus -> terraced_house; Attika ->
penthouse; Maisonette -> maisonette; Dachwohnung -> attic_apartment;
Gewerbeobjekt -> commercial; Parkplatz -> parking.

DATES: `available_from_after` is ISO `YYYY-MM-DD`. For "June move-in" without a
year, use the next occurrence from today.

EMISSION RULES (populate when the user is explicit):
1. "Zurich Kreis N" / "Kreis 3" -> postal_code ["80NN"] zero-padded.
   Kreis 3 -> "8003", Kreis 5 -> "8005", Kreis 3, 4 oder 5 -> ["8003","8004","8005"].
2. "moebliert" / "furnished" / "meuble" -> object_category ["furnished_apartment"].
3. "N Schlafzimmer" / "N bedrooms" / "N chambres" -> bedrooms-to-Swiss-rooms:
   min_rooms = N + 0.5 (err generous). "2 bedrooms" -> min_rooms 2.5.
4. Sub-city neighborhoods (Oerlikon, Altstetten, Schlieren, Plainpalais, etc.)
   -> include BOTH the neighborhood and the parent city in the `city` list.
5. Disjunctions or ranges ("3.5 oder 4 Zimmer", "between 2 and 3") -> set
   both min_rooms and max_rooms inclusive.

NON-EMISSION RULES (soft signals, leave out of hard filters):
6. Commute / landmark-proximity phrases ("max 25 Min zum HB", "30 min to
   Zurich HB", "near ETH", "nah am See", "close to EPFL", "walking distance
   to shops") -> DO NOT set radius_km, latitude, or longitude.
7. Vague quality adjectives ("ruhig", "hell", "modern", "sicher",
   "family-friendly", "bright", "cozy", "angenehm") -> DO NOT infer features
   from these. "family-friendly" does NOT imply features=["child_friendly"];
   "modern" does NOT imply features=["new_build"]. Only emit a feature when
   the query explicitly mentions its concept.

FEATURES_EXCLUDED: only populate when the user explicitly negates ("without
fireplace", "no garage", "ohne Kamin", "kein Erdgeschoss is handled via
min_floor not features_excluded").

BM25_KEYWORDS: emit short literal terms the user mentioned that help lexical
text matching against listing descriptions. Include:
- Domain nouns: "Minergie", "Altbau", "Attika", "Dachwohnung", "Loft",
  "Keller", "Terrasse", "Lift", "Waschturm".
- Named places / landmarks: "ETH", "EPFL", "HB", "Hauptbahnhof", "Stadelhofen",
  "See", "Plainpalais".
- Soft quality adjectives IF the user stated them verbatim: "modern", "hell",
  "ruhig", "bright", "quiet". These are BM25 signal here; they are still
  forbidden from turning into `features`.
Skip generic words already covered by the hard schema (apartment, Wohnung,
house, city names, numbers, dates) and stopwords. Keep it short (<= 8 terms).
If nothing useful remains, emit an empty list.

EXAMPLES:

Query: "3-room bright apartment in Zurich under 2800 CHF with balcony, close to public transport"
Output: {{"city":["zurich"],"min_rooms":3.0,"max_rooms":3.0,"max_price":2800,"features":["balcony"],"bm25_keywords":["bright"]}}

Query: "Wohnung im Raum Zuerich oder Duebendorf, 2.5 bis 3.5 Zimmer, ab 70 m2, bis 3100 CHF, max 25 Min zu Stadelhofen, gern mit Balkon und Waschturm"
Output: {{"city":["zurich","dubendorf"],"min_rooms":2.5,"max_rooms":3.5,"min_area":70,"max_price":3100,"features":["balcony","private_laundry"],"bm25_keywords":["Balkon","Waschturm","Stadelhofen"]}}
"""


def _nullable(type_: str | list[str], **extra) -> dict:
    t = type_ if isinstance(type_, list) else [type_, "null"]
    return {"type": t, **extra}


_HARD_FILTERS_SCHEMA = {
    "name": "hard_filters",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "city": _nullable(["array", "null"], items={"type": "string"}),
            "postal_code": _nullable(["array", "null"], items={"type": "string"}),
            "canton": _nullable(["string", "null"], enum=[*_CANTONS, None]),
            "min_price": _nullable(["integer", "null"], minimum=0),
            "max_price": _nullable(["integer", "null"], minimum=0),
            "min_rooms": _nullable(["number", "null"], minimum=0),
            "max_rooms": _nullable(["number", "null"], minimum=0),
            "min_area": _nullable(["integer", "null"], minimum=0),
            "max_area": _nullable(["integer", "null"], minimum=0),
            "min_floor": _nullable(["integer", "null"]),
            "max_floor": _nullable(["integer", "null"]),
            "min_year_built": _nullable(["integer", "null"], minimum=0),
            "max_year_built": _nullable(["integer", "null"], minimum=0),
            "available_from_after": _nullable(["string", "null"]),
            "features": _nullable(
                ["array", "null"],
                items={"type": "string", "enum": _FEATURE_KEYS},
            ),
            "features_excluded": _nullable(
                ["array", "null"],
                items={"type": "string", "enum": _FEATURE_KEYS},
            ),
            "object_category": _nullable(
                ["array", "null"],
                items={"type": "string", "enum": OBJECT_CATEGORY_ENGLISH},
            ),
            "bm25_keywords": _nullable(["array", "null"], items={"type": "string"}),
        },
        "required": [
            "city", "postal_code", "canton",
            "min_price", "max_price", "min_rooms", "max_rooms",
            "min_area", "max_area", "min_floor", "max_floor",
            "min_year_built", "max_year_built", "available_from_after",
            "features", "features_excluded", "object_category",
            "bm25_keywords",
        ],
    },
}


def extract_hard_facts(query: str) -> HardFilters:
    try:
        if not os.environ.get("OPENAI_API_KEY"):
            raise RuntimeError("OPENAI_API_KEY not set")

        client = OpenAI()
        response = client.chat.completions.create(
            model=_MODEL,
            temperature=0,
            response_format={"type": "json_schema", "json_schema": _HARD_FILTERS_SCHEMA},
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": query},
            ],
        )
        content = response.choices[0].message.content or "{}"
        payload = json.loads(content)
        return HardFilters(**payload)
    except Exception as exc:
        print(
            f"[WARN] extract_hard_facts failed: "
            f"expected=HardFilters JSON from {_MODEL}, "
            f"got={type(exc).__name__}: {exc}, "
            f"fallback=raise",
            flush=True,
        )
        raise
