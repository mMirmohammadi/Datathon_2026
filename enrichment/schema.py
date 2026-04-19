"""Single source of truth for `listings_enriched` fields.

Every pass iterates `FIELDS`. Pass 3 refuses to run if the DB has
`*_filled` columns not in this registry (prevents silent sentinel-fill
of unexpected columns).

For each field we generate four columns in listings_enriched:
    {name}_filled       TEXT NOT NULL   -- real value or literal 'UNKNOWN'
    {name}_source       TEXT NOT NULL   -- see common.sources.Source
    {name}_confidence   REAL NOT NULL   -- [0.0, 1.0]
    {name}_raw          TEXT            -- matched snippet / debug, nullable
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

FieldOrigin = Literal["listings_column", "raw_json", "extraction_only"]


@dataclass(frozen=True, slots=True)
class EnrichedField:
    name: str
    origin: FieldOrigin
    listings_column: str | None = None   # set when origin == "listings_column"
    raw_json_key: str | None = None      # set when origin == "raw_json"
    # origin == "extraction_only" means the field has no source in listings or
    # raw_json — it is populated exclusively by a later extraction pass
    # (e.g. pass 2b GPT on the description). Pass 0 seeds these as
    # UNKNOWN-pending; the extraction pass overwrites where it finds signal.


FIELDS: list[EnrichedField] = [
    # --- Location (filled by pass 0 from listings, pass 1 fills pending) ---
    EnrichedField("city",        "listings_column", listings_column="city"),
    EnrichedField("canton",      "listings_column", listings_column="canton"),
    EnrichedField("postal_code", "listings_column", listings_column="postal_code"),
    EnrichedField("street",      "listings_column", listings_column="street"),
    # --- Numeric core ---
    EnrichedField("price",          "listings_column", listings_column="price"),
    EnrichedField("rooms",          "listings_column", listings_column="rooms"),
    EnrichedField("area",           "listings_column", listings_column="area"),
    EnrichedField("available_from", "listings_column", listings_column="available_from"),
    # --- Geo ---
    EnrichedField("latitude",  "listings_column", listings_column="latitude"),
    EnrichedField("longitude", "listings_column", listings_column="longitude"),
    # --- Distances ---
    EnrichedField("distance_public_transport", "listings_column", listings_column="distance_public_transport"),
    EnrichedField("distance_shop",             "listings_column", listings_column="distance_shop"),
    EnrichedField("distance_kindergarten",     "listings_column", listings_column="distance_kindergarten"),
    EnrichedField("distance_school_1",         "listings_column", listings_column="distance_school_1"),
    EnrichedField("distance_school_2",         "listings_column", listings_column="distance_school_2"),
    # --- 12 feature flags ---
    EnrichedField("feature_balcony",               "listings_column", listings_column="feature_balcony"),
    EnrichedField("feature_elevator",              "listings_column", listings_column="feature_elevator"),
    EnrichedField("feature_parking",               "listings_column", listings_column="feature_parking"),
    EnrichedField("feature_garage",                "listings_column", listings_column="feature_garage"),
    EnrichedField("feature_fireplace",             "listings_column", listings_column="feature_fireplace"),
    EnrichedField("feature_child_friendly",        "listings_column", listings_column="feature_child_friendly"),
    EnrichedField("feature_pets_allowed",          "listings_column", listings_column="feature_pets_allowed"),
    EnrichedField("feature_temporary",             "listings_column", listings_column="feature_temporary"),
    EnrichedField("feature_new_build",             "listings_column", listings_column="feature_new_build"),
    EnrichedField("feature_wheelchair_accessible", "listings_column", listings_column="feature_wheelchair_accessible"),
    EnrichedField("feature_private_laundry",       "listings_column", listings_column="feature_private_laundry"),
    EnrichedField("feature_minergie_certified",    "listings_column", listings_column="feature_minergie_certified"),
    # --- Categorical ---
    EnrichedField("offer_type",      "listings_column", listings_column="offer_type"),
    EnrichedField("object_category", "listings_column", listings_column="object_category"),
    EnrichedField("object_type",     "listings_column", listings_column="object_type"),
    EnrichedField("original_url",    "listings_column", listings_column="original_url"),
    # --- Surfaced from raw_json (original CSV row) ---
    EnrichedField("floor",        "raw_json", raw_json_key="floor"),
    EnrichedField("year_built",   "raw_json", raw_json_key="year_built"),
    EnrichedField("status",       "raw_json", raw_json_key="status"),
    EnrichedField("agency_name",  "raw_json", raw_json_key="agency_name"),
    EnrichedField("agency_phone", "raw_json", raw_json_key="agency_phone"),
    EnrichedField("agency_email", "raw_json", raw_json_key="agency_email"),
    # --- Pass 2b: bathroom + cellar + shared-amenity (GPT-5.4-nano from description) ---
    # Origin "extraction_only": pass 0 seeds UNKNOWN-pending for every row; pass 2b
    # fills where signal is present; pass 3 sentinel-fills remainder to UNKNOWN.
    EnrichedField("bathroom_count",   "extraction_only"),
    EnrichedField("bathroom_shared",  "extraction_only"),
    EnrichedField("has_cellar",       "extraction_only"),
    EnrichedField("kitchen_shared",   "extraction_only"),
]


def validate_fields() -> None:
    names = [f.name for f in FIELDS]
    if len(names) != len(set(names)):
        dupes = [n for n in names if names.count(n) > 1]
        raise RuntimeError(f"Duplicate field names in FIELDS: {sorted(set(dupes))}")
    for f in FIELDS:
        if f.origin == "listings_column" and not f.listings_column:
            raise RuntimeError(f"{f.name}: origin=listings_column but listings_column is empty")
        if f.origin == "raw_json" and not f.raw_json_key:
            raise RuntimeError(f"{f.name}: origin=raw_json but raw_json_key is empty")
        if f.origin == "extraction_only" and (f.listings_column or f.raw_json_key):
            raise RuntimeError(
                f"{f.name}: origin=extraction_only must have no listings_column "
                f"or raw_json_key (got listings_column={f.listings_column!r}, "
                f"raw_json_key={f.raw_json_key!r})"
            )


validate_fields()


def create_table_sql() -> str:
    cols: list[str] = [
        "listing_id TEXT PRIMARY KEY",
        "enriched_at TEXT NOT NULL",
    ]
    for f in FIELDS:
        cols.append(f"{f.name}_filled TEXT NOT NULL")
        cols.append(f"{f.name}_source TEXT NOT NULL")
        cols.append(f"{f.name}_confidence REAL NOT NULL")
        cols.append(f"{f.name}_raw TEXT")
    cols.append("FOREIGN KEY (listing_id) REFERENCES listings(listing_id)")
    body = ",\n    ".join(cols)
    return f"CREATE TABLE IF NOT EXISTS listings_enriched (\n    {body}\n);"


INDEX_SQL: list[str] = [
    "CREATE INDEX IF NOT EXISTS idx_le_canton        ON listings_enriched(canton_filled);",
    "CREATE INDEX IF NOT EXISTS idx_le_city          ON listings_enriched(city_filled);",
    "CREATE INDEX IF NOT EXISTS idx_le_status        ON listings_enriched(status_filled);",
    "CREATE INDEX IF NOT EXISTS idx_le_canton_source ON listings_enriched(canton_source);",
]
