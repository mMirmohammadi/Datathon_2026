"""Export a subset of enriched listings for downstream / backend testing.

Reads `listings` JOIN `listings_enriched` and emits:
  * `enrichment/exports/sample_enriched_500.json`  — small hand-picked
    subset (500 rows, highest enrichment coverage, offer_type=RENT, has city).
  * `enrichment/exports/sample_enriched_500.csv`   — same, flat tabular.
  * `enrichment/exports/enriched_all_gpt.json`     — every row that has at
    least one `text_gpt_5_4` source (full corrected set so far).

Each record is coalesced: the `_filled` value wins if the harness column is
NULL, otherwise the original wins. A parallel `sources` dict on the same
record exposes provenance for every field so the backend can weight or filter
by confidence.

The script is safe to run while the background pipeline writes — SQLite WAL
lets readers see a consistent snapshot without blocking writers.
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

from enrichment.common.db import connect
from enrichment.schema import FIELDS

EXPORT_DIR = Path(__file__).resolve().parents[1] / "exports"

# Fields we surface in the export — keep the payload small and useful.
CORE_FIELDS = [
    "city", "canton", "postal_code", "street",
    "price", "rooms", "area", "available_from",
    "latitude", "longitude",
    "feature_balcony", "feature_elevator", "feature_parking", "feature_garage",
    "feature_fireplace", "feature_child_friendly", "feature_pets_allowed",
    "feature_temporary", "feature_new_build", "feature_wheelchair_accessible",
    "feature_private_laundry", "feature_minergie_certified",
    "offer_type", "object_category", "object_type",
    "floor", "year_built", "status",
    "agency_name", "agency_phone", "agency_email",
    "original_url",
]


def _flatten_one(row: sqlite3.Row) -> dict[str, Any]:
    """Build a coalesced record from a listings ⋈ listings_enriched row."""
    # Harness listings columns come through with their original names.
    out: dict[str, Any] = {
        "listing_id": row["listing_id"],
        "scrape_source": row["scrape_source"],
        "title": row["title"],
        "description": (row["description"] or "")[:800],  # truncate for payload size
        "hero_image_url": None,  # ranker code will fill; not part of null-fill scope
    }
    sources: dict[str, dict[str, Any]] = {}
    n_real = 0
    n_fields = 0
    for f in CORE_FIELDS:
        filled = row[f"{f}_filled"]
        src = row[f"{f}_source"]
        conf = row[f"{f}_confidence"]
        raw_snippet = row[f"{f}_raw"]
        n_fields += 1
        if src not in ("UNKNOWN", "UNKNOWN-pending", "DROPPED_bad_data"):
            n_real += 1
        # Coalesce: if filled is the literal "UNKNOWN", emit None in the flat value
        value: Any = filled if src not in ("UNKNOWN", "UNKNOWN-pending") else None
        # Cast numeric-looking fields back to numbers for usability
        if value is not None:
            if f in ("price", "rooms", "area", "latitude", "longitude", "year_built", "floor"):
                try:
                    value = float(value) if "." in value or f in ("rooms", "latitude", "longitude") else int(value)
                except (TypeError, ValueError):
                    pass
            elif f.startswith("feature_"):
                try:
                    value = int(value)
                except (TypeError, ValueError):
                    pass
        out[f] = value
        sources[f] = {"source": src, "confidence": round(conf, 3), "raw": raw_snippet}
    out["_sources"] = sources
    out["_enrichment_coverage_pct"] = round(100 * n_real / n_fields, 1)
    return out


def _query_rows(conn: sqlite3.Connection, *, where: str, params: tuple, limit: int | None) -> list[dict[str, Any]]:
    col_exprs: list[str] = [
        "l.listing_id", "l.scrape_source", "l.title", "l.description",
    ]
    for f in CORE_FIELDS:
        col_exprs.extend([
            f"le.{f}_filled",
            f"le.{f}_source",
            f"le.{f}_confidence",
            f"le.{f}_raw",
        ])
    sql = f"""
        SELECT {', '.join(col_exprs)}
        FROM listings l
        JOIN listings_enriched le USING(listing_id)
        WHERE {where}
    """
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return [_flatten_one(r) for r in conn.execute(sql, params).fetchall()]


def _csv_row(rec: dict[str, Any]) -> dict[str, Any]:
    """Flatten for CSV — drop nested sources, keep coalesced values + coverage."""
    flat = {k: v for k, v in rec.items() if not k.startswith("_") and k != "description"}
    flat["description_head"] = (rec.get("description") or "")[:300]
    flat["enrichment_coverage_pct"] = rec["_enrichment_coverage_pct"]
    return flat


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("data/listings.db"))
    parser.add_argument("--sample-size", type=int, default=500)
    args = parser.parse_args()

    if not args.db.exists():
        print(f"[ERROR] export_subset: db not found at {args.db}", file=sys.stderr)
        return 2

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    with connect(args.db) as conn:
        # --- 1. Full set: every listing that has ANY field sourced from text_gpt_5_4 ----
        gpt_where_any = " OR ".join(
            f"le.{f}_source='text_gpt_5_4'" for f in CORE_FIELDS
        )
        print(f"[INFO] export_subset: querying listings with ≥1 GPT-sourced field …", flush=True)
        all_gpt = _query_rows(conn, where=gpt_where_any, params=(), limit=None)
        all_gpt_path = EXPORT_DIR / "enriched_all_gpt.json"
        with all_gpt_path.open("w", encoding="utf-8") as fh:
            json.dump(all_gpt, fh, ensure_ascii=False, indent=2)
        print(f"[INFO] export_subset: wrote {len(all_gpt):,} rows → {all_gpt_path}", flush=True)

        # --- 2. Hand-picked sample: highest-coverage subset, RENT only, has city ----
        # Rank by "enrichment coverage" across our CORE_FIELDS, preferring the
        # rows where almost everything is known. These are the easiest for a
        # backend dev to sanity-check against.
        coverage_score = " + ".join([
            f"(CASE WHEN le.{f}_source NOT IN ('UNKNOWN', 'UNKNOWN-pending', 'DROPPED_bad_data') "
            f"THEN 1 ELSE 0 END)"
            for f in CORE_FIELDS
        ])
        sample_sql = f"""
            SELECT l.listing_id, ({coverage_score}) AS coverage
            FROM listings l
            JOIN listings_enriched le USING(listing_id)
            WHERE UPPER(COALESCE(le.offer_type_filled, '')) = 'RENT'
              AND le.city_source   != 'UNKNOWN'
              AND le.canton_source != 'UNKNOWN'
              AND le.price_source  != 'UNKNOWN'
              AND le.price_source  != 'DROPPED_bad_data'
              AND le.rooms_source  != 'UNKNOWN'
              AND le.rooms_source  != 'DROPPED_bad_data'
            ORDER BY coverage DESC, l.listing_id
            LIMIT {int(args.sample_size)}
        """
        top_ids = [r[0] for r in conn.execute(sample_sql).fetchall()]
        if not top_ids:
            print("[WARN] export_subset: sample set empty — pipeline hasn't produced enough RENT+located rows yet", flush=True)
            return 0
        placeholders = ",".join(["?"] * len(top_ids))
        sample_rows = _query_rows(
            conn,
            where=f"l.listing_id IN ({placeholders})",
            params=tuple(top_ids),
            limit=None,
        )
        # Preserve the ranked order
        by_id = {r["listing_id"]: r for r in sample_rows}
        sample_rows_sorted = [by_id[lid] for lid in top_ids if lid in by_id]

        sample_json = EXPORT_DIR / f"sample_enriched_{len(sample_rows_sorted)}.json"
        sample_csv = EXPORT_DIR / f"sample_enriched_{len(sample_rows_sorted)}.csv"

        with sample_json.open("w", encoding="utf-8") as fh:
            json.dump(sample_rows_sorted, fh, ensure_ascii=False, indent=2)
        print(f"[INFO] export_subset: wrote {len(sample_rows_sorted):,} rows → {sample_json}", flush=True)

        # CSV
        csv_rows = [_csv_row(r) for r in sample_rows_sorted]
        if csv_rows:
            with sample_csv.open("w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=list(csv_rows[0].keys()))
                writer.writeheader()
                writer.writerows(csv_rows)
            print(f"[INFO] export_subset: wrote {len(csv_rows):,} rows → {sample_csv}", flush=True)

        # --- 3. README for the exports folder so the colleague knows what each is ----
        readme = EXPORT_DIR / "README.md"
        readme.write_text(
            "# enrichment exports\n\n"
            "Snapshots of `listings_enriched` produced while the null-fill pipeline\n"
            "is running. Regenerate any time with:\n\n"
            "```bash\n"
            "python -m enrichment.scripts.export_subset --db data/listings.db\n"
            "```\n\n"
            "## Files\n\n"
            f"| file | size | rows |\n"
            f"|---|---|---|\n"
            f"| `sample_enriched_{len(sample_rows_sorted)}.json` | hand-picked highest-coverage subset, JSON array | {len(sample_rows_sorted):,} |\n"
            f"| `sample_enriched_{len(sample_rows_sorted)}.csv`  | same as above, flat CSV (no nested `_sources`) | {len(sample_rows_sorted):,} |\n"
            f"| `enriched_all_gpt.json` | every listing with at least one `text_gpt_5_4` field — the full *corrected-so-far* set | {len(all_gpt):,} |\n\n"
            "## Record shape\n\n"
            "```jsonc\n"
            "{\n"
            '  \"listing_id\": \"...\",\n'
            '  \"scrape_source\": \"COMPARIS|ROBINREAL|SRED\",\n'
            '  \"title\": \"...\",\n'
            '  \"description\": \"... (first 800 chars)\",\n'
            '  \"city\": \"Zürich\",            // coalesced value, or null if UNKNOWN\n'
            '  \"canton\": \"ZH\",\n'
            '  \"price\": 2500,\n'
            '  \"rooms\": 3.5,\n'
            '  \"feature_balcony\": 1,         // 0 = explicit \"no balcony\", 1 = yes, null = unknown\n'
            '  \"...\": \"...\",\n'
            '  \"_sources\": {                 // provenance for every field\n'
            '    \"city\":   {\"source\": \"original\",          \"confidence\": 1.0,  \"raw\": null},\n'
            '    \"canton\": {\"source\": \"rev_geo_offline\",   \"confidence\": 0.95, \"raw\": \"Zurich\"},\n'
            '    \"feature_balcony\": {\"source\": \"text_gpt_5_4\", \"confidence\": 0.8, \"raw\": \"Balkon\"}\n'
            '  },\n'
            '  \"_enrichment_coverage_pct\": 82.3\n'
            "}\n"
            "```\n\n"
            "Source tags you'll see:\n\n"
            "- `original` — was non-null in the raw CSV. Confidence 1.0.\n"
            "- `rev_geo_offline` — reverse_geocoder KDTree. Confidence 0.90–0.95.\n"
            "- `rev_geo_nominatim` — OpenStreetMap Nominatim HTTP. Confidence 0.75–0.85.\n"
            "- `text_gpt_5_4` — OpenAI gpt-5.4-mini Structured-Outputs extraction.\n"
            "- `text_regex_{de,fr,it,en}` — legacy multilingual regex pass.\n"
            "- `DROPPED_bad_data` — known-bogus value (`price<200`, `price>50k`, `rooms=0`). Field value is `null`; original is preserved in `_sources[f].raw` as `\"<reason>:original_was=<value>\"`.\n"
            "- `UNKNOWN` / `UNKNOWN-pending` — truly unrecoverable (e.g. SRED has no `status` column at all). Value is `null`, confidence 0.0.\n",
            encoding="utf-8",
        )
        print(f"[INFO] export_subset: wrote {readme}", flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
