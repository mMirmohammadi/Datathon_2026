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

_PRICE_SENTIMENTS = ["cheap", "moderate", "premium"]

_COMMUTE_TARGETS = [
    "zurich_hb", "bern_hb", "basel_hb", "geneve_hb",
    "lausanne_hb", "lugano_hb", "winterthur_hb", "st_gallen_hb",
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

BATHROOM / CELLAR / KITCHEN (pass-2b extracted fields):
- "2 Badezimmer" / "two bathrooms" / "2 salles de bain" / "2 bagni"
  -> min_bathrooms=2, max_bathrooms=2.
- "mindestens 2 Badezimmer" / "at least 2 bathrooms" -> min_bathrooms=2.
- "1 oder 2 Badezimmer" / "1-2 bathrooms" -> min_bathrooms=1, max_bathrooms=2.
- "mit Keller" / "with cellar" / "avec cave" / "con cantina" -> has_cellar=true.
- "ohne Keller" / "without cellar" -> has_cellar=false.
- "eigene Küche" / "private kitchen" / "cuisine privee" -> kitchen_shared=false.
- "Gemeinschaftsküche" / "shared kitchen" / "cuisine commune" / "cucina
  condivisa" / "WG-Küche" -> kitchen_shared=true.
- "eigenes Bad" / "private bathroom" / "salle de bain privee" ->
  bathroom_shared=false.
- "geteiltes Bad" / "shared bathroom" / "Gemeinschaftsbad" / "salle de bain
  partagee" -> bathroom_shared=true.
- "WG-Zimmer" / "shared flat room" / "chambre en coloc" -> bathroom_shared=true
  AND kitchen_shared=true (canonical shared-living defaults).
- "Einzelzimmer" / "private studio" where shared status is explicitly negated
  -> bathroom_shared=false, kitchen_shared=false.

These map to hard filters on the enriched DB columns. Do NOT emit them as
bm25_keywords (dedicated filter fields exist).

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

SOFT_PREFERENCES: a structured object that activates ranking channels. Emit
only keys explicitly hinted by the query. Cue table (cross-lingual):

- "guenstig" / "nicht zu teuer" / "affordable" / "pas cher" / "economico" ->
  soft_preferences.price_sentiment = "cheap"
- "gehoben" / "luxurious" / "premium" / "haut de gamme" / "lusso" ->
  soft_preferences.price_sentiment = "premium"
- "ruhig" / "quiet" / "calme" / "tranquillo" / "nicht zu laut" ->
  soft_preferences.quiet = true
- "gut angebunden" / "nahe OeV" / "S-Bahn" / "near public transport" ->
  soft_preferences.near_public_transport = true
- "gute Schulen" / "near schools" / "ecoles" / "kinderfreundlich" ->
  soft_preferences.near_schools = true
- "familie" / "family" / "famille" / "famiglia" + cue ->
  soft_preferences.family_friendly = true
- "nahe Supermarkt" / "near supermarket" -> soft_preferences.near_supermarket = true
- "nahe Park" / "near park" / "park nearby" -> soft_preferences.near_park = true
- "max N min zum HB Zurich / Bern / Basel / Geneve / Lausanne / Luzern /
   Winterthur / St. Gallen" -> soft_preferences.commute_target = "<city>_hb".
   Also set near_public_transport = true (the commute relies on transit).
- "nahe ETH" / "near EPFL" / "am Zuerichsee" / "nahe HB" ->
  soft_preferences.near_landmark = ["<alias>"] (free text; the Python side
  resolves aliases to canonical landmark keys).

Do not activate a preference if the query does not explicitly hint at it.

EXAMPLES:

Query: "3-room bright apartment in Zurich under 2800 CHF with balcony, close to public transport"
Output: {{"city":["zurich"],"min_rooms":3.0,"max_rooms":3.0,"max_price":2800,"features":["balcony"],"bm25_keywords":["bright"],"soft_preferences":{{"near_public_transport":true}}}}

Query: "Wohnung im Raum Zuerich oder Duebendorf, 2.5 bis 3.5 Zimmer, ab 70 m2, bis 3100 CHF, max 25 Min zum HB, guenstig und ruhig, nahe ETH"
Output: {{"city":["zurich","dubendorf"],"min_rooms":2.5,"max_rooms":3.5,"min_area":70,"max_price":3100,"bm25_keywords":["Balkon","Waschturm"],"soft_preferences":{{"price_sentiment":"cheap","quiet":true,"near_public_transport":true,"commute_target":"zurich_hb","near_landmark":["ETH"]}}}}

Query: "4.5 Zimmer Wohnung in Zuerich mit 2 Badezimmern und Keller, unter 4000 CHF"
Output: {{"city":["zurich"],"min_rooms":4.5,"max_rooms":4.5,"min_bathrooms":2,"max_bathrooms":2,"has_cellar":true,"max_price":4000}}

Query: "WG-Zimmer in Bern, bis 900 CHF"
Output: {{"city":["bern"],"max_price":900,"object_category":["shared_room"],"bathroom_shared":true,"kitchen_shared":true}}
"""


def _nullable(type_: str | list[str], **extra) -> dict:
    t = type_ if isinstance(type_, list) else [type_, "null"]
    return {"type": t, **extra}


_SOFT_PREFERENCES_SCHEMA = {
    "type": ["object", "null"],
    "additionalProperties": False,
    "properties": {
        "price_sentiment": _nullable(
            ["string", "null"], enum=[*_PRICE_SENTIMENTS, None]
        ),
        "quiet": {"type": "boolean"},
        "near_public_transport": {"type": "boolean"},
        "near_schools": {"type": "boolean"},
        "near_supermarket": {"type": "boolean"},
        "near_park": {"type": "boolean"},
        "family_friendly": {"type": "boolean"},
        "commute_target": _nullable(
            ["string", "null"], enum=[*_COMMUTE_TARGETS, None]
        ),
        "near_landmark": {"type": "array", "items": {"type": "string"}},
    },
    "required": [
        "price_sentiment", "quiet", "near_public_transport", "near_schools",
        "near_supermarket", "near_park", "family_friendly",
        "commute_target", "near_landmark",
    ],
}


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
            "min_bathrooms": _nullable(["integer", "null"], minimum=0),
            "max_bathrooms": _nullable(["integer", "null"], minimum=0),
            "bathroom_shared": _nullable(["boolean", "null"]),
            "has_cellar": _nullable(["boolean", "null"]),
            "kitchen_shared": _nullable(["boolean", "null"]),
            "bm25_keywords": _nullable(["array", "null"], items={"type": "string"}),
            "soft_preferences": _SOFT_PREFERENCES_SCHEMA,
        },
        "required": [
            "city", "postal_code", "canton",
            "min_price", "max_price", "min_rooms", "max_rooms",
            "min_area", "max_area", "min_floor", "max_floor",
            "min_year_built", "max_year_built", "available_from_after",
            "features", "features_excluded", "object_category",
            "min_bathrooms", "max_bathrooms", "bathroom_shared",
            "has_cellar", "kitchen_shared",
            "bm25_keywords", "soft_preferences",
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
