"""Cross-check the 5 listings-query results against source CSV rows.

For each query listing (from sample_enriched_500) and each top-3 hit:
 - Look up the hit's platform_id in the matching source CSV
    (structured_data_withimages_updated.csv or robinreal_data_withimages-*.csv)
 - Emit a side-by-side table: city / canton / rooms / area / price / object_type / title / description snippet
 - Flag mismatches (different city, rooms diff >1.5, price diff >2x) vs the query listing

Output:
  image_search/data/full/listings_samples/crosscheck.md
"""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path


RESULTS = Path("image_search/data/full/listings_query_results.json")
OUT = Path("image_search/data/full/listings_samples/crosscheck.md")

STRUCTURED_CSV  = Path("raw_data/structured_data_withimages_updated.csv")
ROBINREAL_CSV   = Path("raw_data/robinreal_data_withimages-1776461278845.csv")
SRED_CSV        = Path("raw_data/sred_data_withmontageimages_latlong.csv")


def _load_index(csv_path: Path) -> dict[str, dict]:
    out: dict[str, dict] = {}
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = (row.get("platform_id") or "").strip().strip('"')
            if pid:
                out[pid] = row
    return out


def _parse_location_address(raw: str) -> dict:
    """location_address column is a JSON blob — parse best-effort."""
    if not raw:
        return {}
    try:
        if raw.startswith("{"):
            return json.loads(raw)
    except Exception:
        pass
    return {}


def _attrs_from_row(row: dict, csv_kind: str) -> dict:
    loc = _parse_location_address(row.get("location_address", ""))
    city = loc.get("city") or loc.get("object_city") or ""
    canton = (loc.get("object_state") or loc.get("state") or "").upper()
    zipc = loc.get("object_zip") or loc.get("zip") or ""
    return {
        "source": csv_kind,
        "platform_id": row.get("platform_id", "").strip('"'),
        "title": (row.get("title") or "").strip()[:120],
        "city": city,
        "canton": canton,
        "zip": zipc,
        "rooms": row.get("number_of_rooms", "").strip(),
        "area": row.get("area", "").strip(),
        "price": row.get("price", "").strip(),
        "object_type": row.get("object_type_text", "").strip() or row.get("object_type", ""),
        "object_category": row.get("object_category", "").strip(),
        "year_built": row.get("year_built", "").strip(),
        "floor": row.get("floor", "").strip(),
        "status": row.get("status", "").strip(),
        "desc_snippet": (row.get("object_description", "") or "").replace("\n", " ").strip()[:240],
    }


def _float_or_none(s: str) -> float | None:
    if not s:
        return None
    try:
        return float(str(s).replace(",", "."))
    except (ValueError, TypeError):
        return None


def _flags(query: dict, hit: dict) -> list[str]:
    out: list[str] = []
    qc = (query.get("city") or "").lower()
    hc = (hit.get("city") or "").lower()
    if qc and hc and qc != hc:
        out.append(f"city: {query.get('city')!r} ≠ {hit.get('city')!r}")
    qr = _float_or_none(query.get("rooms"))
    hr = _float_or_none(hit.get("rooms"))
    if qr is not None and hr is not None and abs(qr - hr) >= 1.5:
        out.append(f"rooms: {qr} vs {hr}")
    qp = _float_or_none(query.get("price"))
    hp = _float_or_none(hit.get("price"))
    if qp and hp and (hp > qp * 2 or qp > hp * 2):
        out.append(f"price: CHF {qp:.0f} vs CHF {hp:.0f}")
    return out


def main() -> None:
    results = json.loads(RESULTS.read_text())
    idx_structured = _load_index(STRUCTURED_CSV) if STRUCTURED_CSV.exists() else {}
    idx_robinreal = _load_index(ROBINREAL_CSV) if ROBINREAL_CSV.exists() else {}

    lines = [
        "# Cross-check: do the top-1/top-3 images' listing data match the queried listing?",
        "",
        "For each of the 5 test queries we look up the top-3 retrieved images' "
        "platform_ids in the matching source CSV and compare listing attributes "
        "(city, rooms, price, object_type, description) against the listing we "
        "queried from. Mismatches are called out.",
        "",
        "**Source CSVs used for look-up:**",
        "- structured: `raw_data/structured_data_withimages_updated.csv`",
        "- robinreal:  `raw_data/robinreal_data_withimages-1776461278845.csv`",
        "",
    ]

    for i, r in enumerate(results, 1):
        L = r["listing"]
        lines.append(f"## Query {i}: `{L['title']}`")
        lines.append("")
        lines.append(
            f"**Queried listing** (from sample_enriched_500): "
            f"city={L.get('city')!r} canton={L.get('canton')!r} "
            f"rooms={L.get('rooms')!r} area={L.get('area')!r} "
            f"price=CHF {L.get('price')}  source=`{L['source']}` "
            f"platform_id=`{L['platform_id']}`"
        )
        lines.append("")
        lines.append("| rank | sim | src | pid | city | canton | rooms | area | price | obj_type | title | flags |")
        lines.append("|---|---|---|---|---|---|---:|---:|---:|---|---|---|")
        query_attrs = {
            "city": L.get("city"),
            "rooms": L.get("rooms"),
            "price": L.get("price"),
        }
        for rnk, hit in enumerate(r["top3_overall"], 1):
            pid = hit["platform_id"]
            if hit["source"] == "structured":
                row = idx_structured.get(pid)
                csv_kind = "structured"
            elif hit["source"] == "robinreal":
                row = idx_robinreal.get(pid)
                csv_kind = "robinreal"
            else:
                row = None
                csv_kind = hit["source"]
            if row is None:
                lines.append(
                    f"| {rnk} | {hit['sim']:.4f} | {hit['source']} | `{pid}` | "
                    f"(not in CSV) |  |  |  |  |  |  | — |"
                )
                continue
            at = _attrs_from_row(row, csv_kind)
            flags = _flags(query_attrs, at)
            flag_txt = "; ".join(flags) if flags else "match"
            lines.append(
                f"| {rnk} | {hit['sim']:.4f} | {at['source']} | `{at['platform_id']}` | "
                f"{at['city']} | {at['canton']} | {at['rooms']} | {at['area']} | "
                f"{at['price']} | {at['object_type']} | {at['title'][:40]}… | {flag_txt} |"
            )
        # own-listing best image
        tops = r.get("top_in_same_listing")
        if tops:
            lines.append("")
            lines.append(f"**Best-in-own-listing** image: `{tops['image_id']}`  "
                         f"sim={tops['sim']:.4f}  overall rank #{tops['rank_overall']:,}  "
                         f"label=`{tops['relevance_label']}`")
        lines.append("")
        lines.append("**Description snippets of the top-3 hits:**")
        lines.append("")
        for rnk, hit in enumerate(r["top3_overall"], 1):
            if hit["source"] == "structured":
                row = idx_structured.get(hit["platform_id"])
            elif hit["source"] == "robinreal":
                row = idx_robinreal.get(hit["platform_id"])
            else:
                row = None
            if row is None:
                lines.append(f"- #{rnk}: (no CSV match)")
                continue
            at = _attrs_from_row(row, hit["source"])
            snippet = re.sub(r"\s+", " ", at["desc_snippet"])
            lines.append(f"- **#{rnk}** `{at['title']}`  → {snippet or '(empty description)'}")
        lines.append("")

    OUT.write_text("\n".join(lines) + "\n")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
