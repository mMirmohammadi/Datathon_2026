"""Independent fact-check of REPORT.md §2-§7."""
from __future__ import annotations

import json
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


def load_raw():
    dfs = {}
    for name, path in FILES.items():
        df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
        df["__source__"] = name
        dfs[name] = df
    return dfs


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


def parse_all(dfs):
    rows = []
    sources = []
    for name, df in dfs.items():
        for r in df.to_dict(orient="records"):
            cleaned = {k: (v if not (isinstance(v, float) and math.isnan(v)) else None) for k, v in r.items()}
            rows.append(prepare_listing_row(cleaned))
            sources.append(name)
    cols = parsed_columns()
    p = pd.DataFrame(rows, columns=cols)
    p["__source__"] = sources
    return p


def main():
    dfs = load_raw()
    raw_all = pd.concat(dfs.values(), ignore_index=True)
    p = parse_all(dfs)

    results = {}

    # A1
    counts = {k: len(v) for k, v in dfs.items()}
    results["A1"] = {"counts": counts, "total": sum(counts.values())}

    # A2-A4: status splits
    for claim_id, src in [("A2", "robinreal"), ("A3", "struct_img"), ("A4", "struct_noi")]:
        df = dfs[src]
        status = df["status"].fillna("__NULL__")
        vc = status.value_counts().to_dict()
        active = int(vc.get("ACTIVE", 0))
        inactive = int(vc.get("INACTIVE", 0))
        n = len(df)
        results[claim_id] = {"n": n, "active": active, "inactive": inactive,
                             "active_pct": round(100 * active / n, 2),
                             "inactive_pct": round(100 * inactive / n, 2),
                             "vc": vc}

    # A5: SRED 100% null
    sred = dfs["sred"]
    feature_flag_cols = [c for c in sred.columns if c.startswith("prop_") or c.startswith("feature_")]
    cols = ["object_city","object_zip","object_state","object_street","available_from","location_address"] + feature_flag_cols
    null_check = {}
    for c in cols:
        if c in sred.columns:
            null_check[c] = int(sred[c].isna().sum())
    # Also check canton via coalesce
    sred_parsed = p[p["__source__"] == "sred"]
    null_check["parsed_city"] = int(sred_parsed["city"].isna().sum())
    null_check["parsed_postal_code"] = int(sred_parsed["postal_code"].isna().sum())
    null_check["parsed_canton"] = int(sred_parsed["canton"].isna().sum())
    null_check["parsed_street"] = int(sred_parsed["street"].isna().sum())
    null_check["parsed_available_from"] = int(sred_parsed["available_from"].isna().sum())
    results["A5"] = null_check

    # A6: struct_noi canton null 54.4%, lat null 22.9%
    sn = p[p["__source__"] == "struct_noi"]
    n_sn = len(sn)
    canton_null = int(sn["canton"].isna().sum())
    lat_null = int(sn["lat"].isna().sum())
    results["A6"] = {"n": n_sn,
                     "canton_null": canton_null, "canton_null_pct": round(100*canton_null/n_sn, 2),
                     "lat_null": lat_null, "lat_null_pct": round(100*lat_null/n_sn, 2)}

    # A7: unlocatable: no canton AND no (lat,lng inside Swiss bbox)
    def in_ch(lat, lng):
        if lat is None or lng is None:
            return False
        try:
            lat = float(lat); lng = float(lng)
        except (TypeError, ValueError):
            return False
        if math.isnan(lat) or math.isnan(lng):
            return False
        return 45.7 <= lat <= 47.9 and 5.8 <= lng <= 10.6

    p["in_ch"] = p.apply(lambda r: in_ch(r["lat"], r["lng"]), axis=1)
    unlocatable = int(((p["canton"].isna()) & (~p["in_ch"])).sum())
    results["A7"] = {"unlocatable": unlocatable}

    # A8: Status totals (raw status col across all)
    raw_status = raw_all["status"].fillna("__NULL__")
    vc_all = raw_status.value_counts().to_dict()
    results["A8"] = {"vc": vc_all, "null": int(raw_all["status"].isna().sum())}

    # A9
    total = len(p)
    active_total = int(vc_all.get("ACTIVE", 0))
    results["A9"] = {"total": total, "active": active_total,
                     "pct": round(100*active_total/total, 2)}

    # A10
    inactive_total = int(vc_all.get("INACTIVE", 0))
    deleted_total = int(vc_all.get("DELETED", 0))
    results["A10"] = {"total": total, "inactive": inactive_total, "deleted": deleted_total,
                      "kept": total - inactive_total - deleted_total}

    # A11: lat/lng in Swiss bbox
    in_ch_count = int(p["in_ch"].sum())
    results["A11"] = {"in_ch": in_ch_count, "pct": round(100*in_ch_count/total, 2)}

    # A12: no lat/lng at all per source
    nogeo = {}
    for src in ["robinreal", "struct_img", "struct_noi", "sred"]:
        sub = p[p["__source__"] == src]
        n = int(((sub["lat"].isna()) & (sub["lng"].isna())).sum())
        nogeo[src] = n
    results["A12"] = {"per_src": nogeo, "total": sum(nogeo.values())}

    # A13: rows with coordinates outside CH, of which 8 at (0,0) in struct_img
    has_geo = p["lat"].notna() & p["lng"].notna()
    outside = p[has_geo & (~p["in_ch"])]
    n_outside = len(outside)
    zero_island = outside[(outside["lat"] == 0) & (outside["lng"] == 0)]
    zero_island_img = zero_island[zero_island["__source__"] == "struct_img"]
    results["A13"] = {"outside": n_outside, "zero_island_total": len(zero_island),
                      "zero_island_struct_img": len(zero_island_img)}

    # A14: no canton AFTER coalesce but DO have lat/lng in CH
    no_canton_in_ch = int(((p["canton"].isna()) & (p["in_ch"])).sum())
    results["A14"] = {"count": no_canton_in_ch}

    # A15: Median price per m² per canton in residential rent with price_per_m2 in [5, 100]
    resid = {"Wohnung","Möblierte Wohnung","Haus","Einfamilienhaus","Maisonette",
             "Terrassenwohnung","Loft","Attikawohnung","Studio","Duplex"}
    resi = p[p["object_category"].isin(resid)].copy()
    # rent offer type
    resi = resi[resi["offer_type"] == "RENT"]
    resi["price_num"] = pd.to_numeric(resi["price"], errors="coerce")
    resi["area_num"] = pd.to_numeric(resi["area"], errors="coerce")
    resi = resi[resi["price_num"].notna() & resi["area_num"].notna() & (resi["area_num"] > 0)]
    resi["ppm2"] = resi["price_num"] / resi["area_num"]
    resi = resi[(resi["ppm2"] >= 5) & (resi["ppm2"] <= 100)]
    medians = {}
    for cc in ["ZH", "GE", "VD", "BS", "TI", "NE"]:
        sub = resi[resi["canton"] == cc]
        if len(sub) > 0:
            medians[cc] = round(float(sub["ppm2"].median()), 1)
        else:
            medians[cc] = None
    results["A15"] = {"medians": medians}

    # A16: Haversine near ETH
    def hav(lat1, lng1, lat2, lng2):
        R = 6371.0
        phi1 = math.radians(lat1); phi2 = math.radians(lat2)
        dphi = math.radians(lat2-lat1); dlam = math.radians(lng2-lng1)
        a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
        c = 2*math.asin(math.sqrt(a))
        return R*c
    eth_lat, eth_lng = 47.3769, 8.5417
    geo = p[p["lat"].notna() & p["lng"].notna()].copy()
    geo["lat_f"] = pd.to_numeric(geo["lat"], errors="coerce")
    geo["lng_f"] = pd.to_numeric(geo["lng"], errors="coerce")
    geo = geo[geo["lat_f"].notna() & geo["lng_f"].notna()]
    geo["dist_km"] = geo.apply(lambda r: hav(r["lat_f"], r["lng_f"], eth_lat, eth_lng), axis=1)
    results["A16"] = {
        "within_5": int((geo["dist_km"] <= 5).sum()),
        "within_2": int((geo["dist_km"] <= 2).sum()),
        "within_10": int((geo["dist_km"] <= 10).sum()),
    }

    # A17: residential category rent, price in [200, 20000]
    r2 = p[p["object_category"].isin(resid)].copy()
    r2 = r2[r2["offer_type"] == "RENT"]
    r2["price_num"] = pd.to_numeric(r2["price"], errors="coerce")
    r2 = r2[r2["price_num"].between(200, 20000, inclusive="both")]
    prices = r2["price_num"].dropna()
    results["A17"] = {
        "n": len(r2),
        "p10": round(float(prices.quantile(0.10)), 0) if len(prices) else None,
        "median": round(float(prices.median()), 0) if len(prices) else None,
        "p90": round(float(prices.quantile(0.90)), 0) if len(prices) else None,
    }

    # A18: Price sentinels
    price_all = pd.to_numeric(p["price"], errors="coerce")
    sentinels = {}
    for v in [1, 100, 1000, 1111111]:
        sentinels[v] = int((price_all == v).sum())
    results["A18"] = sentinels

    # A19: price < 200
    results["A19"] = {"n": int((price_all < 200).sum())}

    # A20: price > 50k and > 100k
    results["A20"] = {"gt50k": int((price_all > 50000).sum()), "gt100k": int((price_all > 100000).sum())}

    # A21: rooms == 0
    rooms_all = pd.to_numeric(p["rooms"], errors="coerce")
    zero_rooms = p[rooms_all == 0]
    results["A21"] = {"n": len(zero_rooms),
                      "all_struct_noi": bool((zero_rooms["__source__"] == "struct_noi").all()),
                      "by_source": zero_rooms["__source__"].value_counts().to_dict()}

    # A22: rooms > 15
    results["A22"] = {"n": int((rooms_all > 15).sum())}

    # A23: "nicht verfügbar" and "<missing area>" in raw area
    area_raw = raw_all["area"].astype(str)
    results["A23"] = {
        "nicht_verfuegbar": int((area_raw == "nicht verfügbar").sum()),
        "missing_area": int((area_raw == "<missing area>").sum()),
    }

    # A24: area numeric coercion not null
    results["A24"] = {"n": int(pd.to_numeric(p["area"], errors="coerce").notna().sum())}

    # A25: area > 2000, area <= 5
    area_all = pd.to_numeric(p["area"], errors="coerce")
    results["A25"] = {"gt2000": int((area_all > 2000).sum()),
                      "le5": int((area_all <= 5).sum())}

    # A26: raw prop_* columns nullness per source
    prop_cols = ["prop_balcony", "prop_elevator", "prop_parking", "prop_garage",
                 "prop_fireplace", "prop_child_friendly"]
    a26 = {}
    for src in ["struct_img", "struct_noi", "sred"]:
        df = dfs[src]
        n = len(df)
        out = {}
        for c in prop_cols:
            if c in df.columns:
                out[c] = {"null": int(df[c].isna().sum()),
                          "pct_null": round(100*df[c].isna().sum()/n, 2)}
            else:
                out[c] = "MISSING"
        a26[src] = out
    results["A26"] = a26

    # A27: parsed feature known 100% for struct_img & struct_noi
    feat_cols = ["feature_balcony","feature_elevator","feature_parking","feature_garage",
                 "feature_fireplace","feature_pets_allowed"]
    a27 = {}
    for src in ["struct_img", "struct_noi"]:
        sub = p[p["__source__"] == src]
        n = len(sub)
        out = {}
        for c in feat_cols:
            known = int(sub[c].notna().sum())
            out[c] = {"known": known, "pct_known": round(100*known/n, 2)}
        a27[src] = out
    results["A27"] = a27

    # A28: SRED all 12 parsed feature columns 100% null
    all_feat = ["feature_balcony","feature_elevator","feature_parking","feature_garage",
                "feature_fireplace","feature_child_friendly","feature_pets_allowed",
                "feature_temporary","feature_new_build","feature_wheelchair_accessible",
                "feature_private_laundry","feature_minergie_certified"]
    sred_p = p[p["__source__"] == "sred"]
    a28 = {}
    for c in all_feat:
        known = int(sred_p[c].notna().sum())
        a28[c] = known
    results["A28"] = a28

    # A29: feature_child_friendly known and TRUE rate
    a29 = {}
    for src in ["struct_img", "struct_noi"]:
        sub = p[p["__source__"] == src]
        n = len(sub)
        cf = sub["feature_child_friendly"]
        known = int(cf.notna().sum())
        true_ct = int((cf == 1).sum())
        a29[src] = {"n": n, "known": known, "pct_known": round(100*known/n, 2),
                    "true": true_ct, "true_rate_among_known": round(100*true_ct/known, 2) if known else None}
    results["A29"] = a29

    print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()
