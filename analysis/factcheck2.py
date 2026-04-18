"""Follow-up investigation of A17 and A25 discrepancies."""
from __future__ import annotations

import math
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.participant.listing_row_parser import prepare_listing_row

ROOT = Path("/home/rohamzn/ETH_Uni/Datathon_2026")
RAW = ROOT / "raw_data"

FILES = {
    "robinreal": RAW / "robinreal_data_withimages-1776461278845.csv",
    "struct_img": RAW / "structured_data_withimages-1776412361239.csv",
    "struct_noi": RAW / "structured_data_withoutimages-1776412361239.csv",
    "sred": RAW / "sred_data_withmontageimages_latlong.csv",
}

def parsed_columns():
    return [
        "id","platform_id","scrape_source","title","description","street","city",
        "postal_code","canton","price","rooms","area","available_from","lat","lng",
        "dist_pt","dist_shop","dist_kg","dist_sc1","dist_sc2",
        "feature_balcony","feature_elevator","feature_parking","feature_garage",
        "feature_fireplace","feature_child_friendly","feature_pets_allowed",
        "feature_temporary","feature_new_build","feature_wheelchair_accessible",
        "feature_private_laundry","feature_minergie_certified",
        "enabled_features","offer_type","object_category","object_type","platform_url",
        "images","location_address_json","orig_data_json","raw_row",
    ]


def main():
    rows = []
    sources = []
    for name, path in FILES.items():
        df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
        for r in df.to_dict(orient="records"):
            cleaned = {k: (v if not (isinstance(v, float) and math.isnan(v)) else None) for k, v in r.items()}
            rows.append(prepare_listing_row(cleaned))
            sources.append(name)
    p = pd.DataFrame(rows, columns=parsed_columns())
    p["__source__"] = sources

    resid = {"Wohnung","Möblierte Wohnung","Haus","Einfamilienhaus","Maisonette",
             "Terrassenwohnung","Loft","Attikawohnung","Studio","Duplex"}
    r = p[p["object_category"].isin(resid)].copy()
    r["price_num"] = pd.to_numeric(r["price"], errors="coerce")

    # Variant 1: with offer_type=RENT
    r1 = r[r["offer_type"] == "RENT"]
    r1b = r1[r1["price_num"].between(200, 20000)]
    print(f"With offer_type=RENT filter: n={len(r1b)}")
    print(f"  p10={r1b['price_num'].quantile(0.10)}, median={r1b['price_num'].median()}, p90={r1b['price_num'].quantile(0.90)}")

    # Variant 2: without offer_type filter (report didn't explicitly say RENT)
    r2 = r[r["price_num"].between(200, 20000)]
    print(f"Without offer_type=RENT filter: n={len(r2)}")
    print(f"  p10={r2['price_num'].quantile(0.10)}, median={r2['price_num'].median()}, p90={r2['price_num'].quantile(0.90)}")

    # Variant 3: offer_type=RENT or NULL
    r3 = r[(r["offer_type"] == "RENT") | r["offer_type"].isna()]
    r3b = r3[r3["price_num"].between(200, 20000)]
    print(f"offer_type RENT or NULL: n={len(r3b)}")
    print(f"  p10={r3b['price_num'].quantile(0.10)}, median={r3b['price_num'].median()}, p90={r3b['price_num'].quantile(0.90)}")

    # Area outliers per source
    area_all = pd.to_numeric(p["area"], errors="coerce")
    print(f"\nArea > 2000: {int((area_all > 2000).sum())}")
    print(f"Area >= 2000: {int((area_all >= 2000).sum())}")
    print(f"Area <= 5: {int((area_all <= 5).sum())}")
    print(f"Area < 5: {int((area_all < 5).sum())}")
    print(f"Area > 2000 breakdown by source:")
    print(p[area_all > 2000]["__source__"].value_counts().to_dict())


if __name__ == "__main__":
    main()
