"""Stakeholder-grade profile of the Datathon-2026 listings corpus.

Runs a single unified pass over the 4 CSVs, normalises each row via the
same logic the harness uses (prepare_listing_row), then emits:

  - analysis/data/stats.json      machine-readable summary of every finding
  - analysis/data/unified.parquet Normalised dataframe (one row per listing)
  - analysis/plots/*.png          Plots referenced in the report

Every stat printed here is also written to stats.json so the report and
validator agents reference the same numbers.
"""
from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter
from html.parser import HTMLParser
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
csv.field_size_limit(sys.maxsize)

from app.participant.listing_row_parser import prepare_listing_row  # noqa: E402

OUT_DATA = ROOT / "analysis" / "data"
OUT_PLOTS = ROOT / "analysis" / "plots"
OUT_DATA.mkdir(parents=True, exist_ok=True)
OUT_PLOTS.mkdir(parents=True, exist_ok=True)

SOURCES = {
    "robinreal":   "raw_data/robinreal_data_withimages-1776461278845.csv",
    "sred":        "raw_data/sred_data_withmontageimages_latlong.csv",
    "struct_img":  "raw_data/structured_data_withimages-1776412361239.csv",
    "struct_noi":  "raw_data/structured_data_withoutimages-1776412361239.csv",
}

# Swiss bbox (approximate).
CH_LAT = (45.7, 47.9)
CH_LNG = (5.8, 10.6)

COLUMNS = [
    "listing_id","platform_id","scrape_source","title","description","street","city",
    "postal_code","canton","price","rooms","area","available_from","latitude","longitude",
    "distance_public_transport","distance_shop","distance_kindergarten","distance_school_1",
    "distance_school_2","feature_balcony","feature_elevator","feature_parking","feature_garage",
    "feature_fireplace","feature_child_friendly","feature_pets_allowed","feature_temporary",
    "feature_new_build","feature_wheelchair_accessible","feature_private_laundry",
    "feature_minergie_certified","features_json","offer_type","object_category","object_type",
    "original_url","images_json","location_address_json","orig_data_json","raw_json",
]

FEATURE_COLS = [c for c in COLUMNS if c.startswith("feature_")]

sns.set_theme(style="whitegrid", context="talk", font_scale=0.85)


class _HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, data):
        self.parts.append(data)


def strip_html(s: str) -> str:
    if not s:
        return ""
    if "<" not in s:
        return s
    p = _HTMLStripper()
    try:
        p.feed(s)
    except Exception as exc:  # pragma: no cover
        print(f"[WARN] strip_html: expected=valid_html, got={exc!r}, fallback=raw_string", flush=True)
        return s
    return "".join(p.parts)


DE_TOKENS = {"und","die","der","das","ist","mit","wohnung","zimmer","nicht","für","schön",
             "balkon","küche","neu","helle","hell","stock","bahnhof","sehr","grosse","grosser"}
FR_TOKENS = {"et","le","la","les","une","avec","chambre","cuisine","appartement","pour",
             "située","belle","studio","meublé","proche","gare","balcon"}
IT_TOKENS = {"e","il","la","con","camera","cucina","appartamento","per","bellissimo",
             "luminoso","stanza","bagno","vicino","stazione","balcone"}
EN_TOKENS = {"and","the","with","room","kitchen","apartment","flat","bright","modern",
             "near","station","for","rent","studio"}


def guess_lang(text: str) -> str:
    t = text.lower()
    scores = {
        "de": sum(w in t for w in DE_TOKENS),
        "fr": sum(w in t for w in FR_TOKENS),
        "it": sum(w in t for w in IT_TOKENS),
        "en": sum(w in t for w in EN_TOKENS),
    }
    best = max(scores, key=scores.get)
    return best if scores[best] >= 2 else "unk"


def load_frame() -> pd.DataFrame:
    all_rows = []
    for source, rel_path in SOURCES.items():
        csv_path = ROOT / rel_path
        if not csv_path.exists():
            raise FileNotFoundError(csv_path)
        with csv_path.open(newline="", encoding="utf-8") as f:
            for raw in csv.DictReader(f):
                parsed = prepare_listing_row(raw)
                rec = dict(zip(COLUMNS, parsed))
                rec["source"] = source
                rec["raw_status"] = (raw.get("status") or "").strip().upper() or None
                rec["raw_price_type"] = (raw.get("price_type") or "").strip() or None
                rec["raw_available_from"] = (raw.get("available_from") or "").strip() or None
                rec["raw_floor"] = raw.get("floor")
                rec["raw_year_built"] = raw.get("year_built")
                rec["raw_time_of_creation"] = (raw.get("time_of_creation") or "").strip() or None
                rec["raw_last_scraped"] = (raw.get("last_scraped") or "").strip() or None
                rec["raw_agency_name"] = (raw.get("agency_name") or "").strip() or None
                rec["raw_partner_name"] = (raw.get("partner_name") or "").strip() or None
                rec["raw_url"] = (raw.get("platform_url") or "").strip() or None
                all_rows.append(rec)
    df = pd.DataFrame(all_rows)
    print(f"Loaded {len(df):,} rows across {len(SOURCES)} sources.")
    return df


def ensure_types(df: pd.DataFrame) -> pd.DataFrame:
    # Numeric coercion
    for col in ("price","rooms","area","latitude","longitude",
                "distance_public_transport","distance_shop","distance_kindergarten",
                "distance_school_1","distance_school_2"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["floor"] = pd.to_numeric(df["raw_floor"], errors="coerce")
    df["year_built"] = pd.to_numeric(df["raw_year_built"], errors="coerce")
    # Dates (strings already ISO from parser for available_from; raw ones parsed below)
    df["available_from"] = pd.to_datetime(df["available_from"], errors="coerce")
    df["time_of_creation"] = pd.to_datetime(df["raw_time_of_creation"], errors="coerce")
    df["last_scraped"] = pd.to_datetime(df["raw_last_scraped"], errors="coerce")
    return df


def derive_helpers(df: pd.DataFrame) -> pd.DataFrame:
    df["has_price"] = df["price"].notna() & (df["price"] >= 200)
    df["has_rooms"] = df["rooms"].notna() & (df["rooms"] > 0) & (df["rooms"] <= 15)
    df["has_area"] = df["area"].notna() & (df["area"] > 5) & (df["area"] < 2000)
    df["has_city"] = df["city"].notna() & df["city"].str.strip().astype(bool)
    df["has_postal"] = df["postal_code"].notna() & df["postal_code"].str.strip().astype(bool)
    df["has_canton"] = df["canton"].notna() & df["canton"].str.strip().astype(bool)
    df["has_geo_in_ch"] = (
        df["latitude"].between(*CH_LAT) & df["longitude"].between(*CH_LNG)
    )
    df["has_any_location"] = df["has_city"] | df["has_geo_in_ch"]

    df["n_feature_flags_known"] = df[FEATURE_COLS].notna().sum(axis=1)
    df["n_feature_flags_true"] = (df[FEATURE_COLS] == 1).sum(axis=1)

    df["is_active"] = df["raw_status"].fillna("").eq("ACTIVE")
    df["is_inactive_or_deleted"] = df["raw_status"].fillna("").isin(["INACTIVE","DELETED"])

    df["offer_type_clean"] = df["offer_type"].fillna("__NULL__")
    df["object_category_clean"] = df["object_category"].fillna("__NULL__")

    # Residential vs not
    residential_cats = {"Wohnung","Möblierte Wohnung","Haus","Einfamilienhaus",
                        "Maisonette","Terrassenwohnung","Loft","Attikawohnung","Studio","Duplex"}
    df["is_residential"] = df["object_category_clean"].isin(residential_cats)
    df["is_non_residential"] = df["object_category_clean"].isin({
        "Parkplatz","Parkplatz, Garage","Tiefgarage","Garage","Aussenparkplatz",
        "Gewerbeobjekt","Industrieobjekt","Gastgewerbe","Lager","Bueroraum","Büroraum","Büro",
        "Wohnnebenraeume","Wohnnebenräume","Hobbyraum","Reklamefläche","Landwirtschaft",
    })

    # Description + language
    df["desc_raw_len"] = df["description"].fillna("").str.len()
    df["desc_has_html"] = df["description"].fillna("").str.contains("<", regex=False)
    df["desc_text"] = df["description"].fillna("").apply(strip_html)
    df["desc_len_text"] = df["desc_text"].str.len()
    df["lang_guess"] = df["desc_text"].str.slice(0, 800).apply(guess_lang)

    # Images: json wrapper → count
    def _count_images(s):
        if not s:
            return 0
        try:
            v = json.loads(s)
        except Exception:
            return 0
        if isinstance(v, dict):
            inner = v.get("images", v)
            if isinstance(inner, list):
                return len(inner)
            return 0
        if isinstance(v, list):
            return len(v)
        return 0

    df["n_images"] = df["images_json"].fillna("").apply(_count_images)

    # Price per m²
    df["price_per_m2"] = df["price"] / df["area"].where(df["has_area"])
    df["price_per_room"] = df["price"] / df["rooms"].where(df["has_rooms"])

    return df


# --------------------------------------------------------------------------
# Plotting helpers
# --------------------------------------------------------------------------
def savefig(name: str):
    path = OUT_PLOTS / name
    plt.tight_layout()
    plt.savefig(path, dpi=160, bbox_inches="tight")
    plt.close()
    print(f"  wrote {path}")


def plot_row_counts(df: pd.DataFrame, stats: dict):
    counts = df.groupby("source").size().rename("rows").reset_index()
    plt.figure(figsize=(8, 4))
    ax = sns.barplot(data=counts, x="source", y="rows",
                     order=["robinreal","struct_img","struct_noi","sred"], color="#3b82f6")
    for p in ax.patches:
        ax.annotate(f"{int(p.get_height()):,}",
                    (p.get_x() + p.get_width()/2., p.get_height()),
                    ha="center", va="bottom", fontsize=11)
    plt.title(f"Rows per source (total = {len(df):,})")
    plt.ylabel("rows")
    savefig("01_row_counts.png")
    stats["row_counts"] = counts.set_index("source")["rows"].to_dict()


def plot_null_heatmap(df: pd.DataFrame, stats: dict):
    fields = ["city","postal_code","canton","street","price","rooms","area","available_from",
              "latitude","longitude","feature_balcony","feature_elevator","feature_parking",
              "feature_child_friendly","feature_pets_allowed","feature_new_build"]
    mat = []
    for src in ["robinreal","struct_img","struct_noi","sred"]:
        sub = df[df["source"] == src]
        row = [sub[f].isna().mean() if f in sub.columns else 1.0 for f in fields]
        mat.append(row)
    arr = np.array(mat)
    plt.figure(figsize=(13, 5))
    sns.heatmap(arr, annot=True, fmt=".0%",
                xticklabels=fields, yticklabels=["robinreal","struct_img","struct_noi","sred"],
                cmap="rocket_r", vmin=0, vmax=1, cbar_kws={"label": "null rate"})
    plt.title("Null rate by (source, field) — darker = more missing")
    plt.xticks(rotation=35, ha="right")
    savefig("02_null_heatmap.png")
    stats["null_heatmap"] = {src: dict(zip(fields, row.tolist()))
                             for src, row in zip(["robinreal","struct_img","struct_noi","sred"], arr)}


def plot_status(df: pd.DataFrame, stats: dict):
    cross = pd.crosstab(df["source"], df["raw_status"].fillna("__NULL__"))
    cross = cross.loc[["robinreal","struct_img","struct_noi","sred"]]
    cross.plot(kind="barh", stacked=True, figsize=(10, 4), colormap="Set2")
    plt.title("Listing status by source")
    plt.xlabel("rows")
    plt.legend(title="status", bbox_to_anchor=(1.02, 1), loc="upper left")
    savefig("03_status_by_source.png")
    stats["status_counts_by_source"] = cross.to_dict()


def plot_price_distribution(df: pd.DataFrame, stats: dict):
    sub = df[df["has_price"] & df["is_residential"] & df["price"].between(200, 20000)]
    plt.figure(figsize=(10, 5))
    for src, g in sub.groupby("source"):
        sns.kdeplot(g["price"], label=f"{src} (n={len(g):,})",
                    log_scale=(True, False), common_norm=False, bw_adjust=0.8)
    plt.legend()
    plt.xlabel("monthly price (CHF, log scale)")
    plt.title("Residential-rent price density — residential categories only, clipped 200–20,000 CHF")
    savefig("04_price_density_residential.png")

    # All prices — show sentinels / outliers
    plt.figure(figsize=(10, 4))
    hist_src = df[df["price"].notna() & (df["price"] > 0)]
    sns.histplot(hist_src, x="price", log_scale=(True, False), bins=80,
                 hue="source", multiple="stack", palette="Set2")
    plt.title("Raw price distribution (log x) — note placeholder spikes at 1 / 100 / 1000")
    plt.xlabel("price (CHF, log)")
    savefig("04b_price_raw_log.png")

    stats["price"] = {
        "residential_rent_count": int(sub.shape[0]),
        "residential_rent_p10": float(sub["price"].quantile(0.1)),
        "residential_rent_median": float(sub["price"].median()),
        "residential_rent_p90": float(sub["price"].quantile(0.9)),
        "count_lt_200": int((df["price"] < 200).sum()),
        "count_gt_20k": int((df["price"] > 20000).sum()),
        "count_eq_1": int((df["price"] == 1).sum()),
        "count_eq_100": int((df["price"] == 100).sum()),
        "count_eq_1000": int((df["price"] == 1000).sum()),
        "count_eq_1111111": int((df["price"] == 1111111).sum()),
    }


def plot_rooms_area(df: pd.DataFrame, stats: dict):
    sub = df[df["has_price"] & df["is_residential"] & df["price"].between(200, 15000) &
             df["has_rooms"] & df["has_area"]]
    plt.figure(figsize=(9, 6))
    sns.scatterplot(data=sub.sample(min(4000, len(sub)), random_state=1),
                    x="area", y="price", hue="source", alpha=0.4, s=18, edgecolor="none")
    plt.xlim(0, 400); plt.ylim(200, 15000)
    plt.title("Price vs living area (residential rent, n sampled)")
    plt.xlabel("area (m²)"); plt.ylabel("price (CHF/month)")
    savefig("05_price_vs_area.png")

    plt.figure(figsize=(10, 4))
    sns.boxplot(data=df[df["has_rooms"] & df["rooms"].between(1, 8) & df["has_price"]
                        & df["price"].between(300, 12000) & df["is_residential"]],
                x="rooms", y="price", palette="viridis")
    plt.title("Price by room count (residential rent)")
    savefig("06_price_by_rooms.png")

    stats["rooms"] = {
        "rooms_value_counts_top": df["rooms"].round(1).value_counts().head(15).to_dict(),
        "rooms_eq_0": int((df["rooms"] == 0).sum()),
        "rooms_gt_15": int((df["rooms"] > 15).sum()),
    }
    stats["area"] = {
        "with_numeric_area": int(df["area"].notna().sum()),
        "area_le_5": int((df["area"] <= 5).sum()),
        "area_ge_2000": int((df["area"] >= 2000).sum()),
    }


def plot_price_per_m2(df: pd.DataFrame, stats: dict):
    sub = df[df["has_price"] & df["has_area"] & df["is_residential"]
             & df["price_per_m2"].between(5, 100) & df["has_canton"]]
    top_cantons = sub["canton"].value_counts().head(14).index.tolist()
    sub = sub[sub["canton"].isin(top_cantons)]
    plt.figure(figsize=(12, 5))
    order = sub.groupby("canton")["price_per_m2"].median().sort_values().index.tolist()
    sns.boxplot(data=sub, x="canton", y="price_per_m2", order=order, color="#60a5fa", showfliers=False)
    plt.title("Price per m² by canton — residential rent, 5–100 CHF/m² window")
    plt.ylabel("CHF / m² / month"); plt.xlabel("")
    savefig("07_price_per_m2_by_canton.png")
    stats["price_per_m2"] = {
        "median_by_canton": sub.groupby("canton")["price_per_m2"].median().round(1).to_dict(),
        "n_by_canton": sub.groupby("canton").size().to_dict(),
    }


def plot_geo(df: pd.DataFrame, stats: dict):
    geo = df[df["has_geo_in_ch"]].copy()
    plt.figure(figsize=(9, 7))
    sns.scatterplot(data=geo.sample(min(8000, len(geo)), random_state=2),
                    x="longitude", y="latitude", hue="source",
                    s=6, alpha=0.5, edgecolor="none", palette="Set2")
    plt.title(f"Geographic distribution (n_with_geo={len(geo):,} / {len(df):,}) — Swiss bbox")
    plt.xlabel("lon"); plt.ylabel("lat")
    savefig("08_geo_scatter.png")

    # Density heatmap
    plt.figure(figsize=(9, 7))
    plt.hexbin(geo["longitude"], geo["latitude"], gridsize=60, mincnt=1, cmap="rocket_r")
    plt.colorbar(label="listings per hex")
    plt.title("Listing density heatmap")
    plt.xlabel("lon"); plt.ylabel("lat")
    savefig("09_geo_hex.png")

    outside = df[df["latitude"].notna() & df["longitude"].notna() & ~df["has_geo_in_ch"]]
    stats["geo"] = {
        "has_geo_in_ch": int(df["has_geo_in_ch"].sum()),
        "has_lat_lng_outside_ch": int(len(outside)),
        "has_no_lat_lng": int((df["latitude"].isna() | df["longitude"].isna()).sum()),
    }


def plot_canton_coverage(df: pd.DataFrame, stats: dict):
    vc = df.loc[df["has_canton"], "canton"].value_counts().head(20)
    plt.figure(figsize=(11, 5))
    sns.barplot(x=vc.values, y=vc.index, color="#10b981", orient="h")
    plt.title(f"Listings per canton (top 20; {int(df['has_canton'].sum()):,} rows have canton field)")
    plt.xlabel("rows"); plt.ylabel("")
    savefig("10_canton_counts.png")
    stats["canton_top20"] = vc.to_dict()

    # Compare to actually observable by geo reverse-geocode potential
    stats["rows_with_no_canton_but_geo"] = int(
        (~df["has_canton"] & df["has_geo_in_ch"]).sum()
    )
    stats["rows_with_no_canton_no_geo"] = int(
        (~df["has_canton"] & ~df["has_geo_in_ch"]).sum()
    )


def plot_object_category(df: pd.DataFrame, stats: dict):
    vc = df["object_category_clean"].value_counts().head(15)
    plt.figure(figsize=(11, 5))
    sns.barplot(x=vc.values, y=vc.index, color="#f59e0b", orient="h")
    plt.title("Object category distribution (top 15)")
    plt.xlabel("rows"); plt.ylabel("")
    savefig("11_object_category.png")
    stats["object_category_top"] = vc.to_dict()
    stats["non_residential_rows"] = int(df["is_non_residential"].sum())
    stats["residential_rows"] = int(df["is_residential"].sum())
    stats["object_category_null"] = int((df["object_category_clean"] == "__NULL__").sum())


def plot_offer_type(df: pd.DataFrame, stats: dict):
    cross = pd.crosstab(df["source"], df["offer_type_clean"])
    cross.plot(kind="barh", stacked=True, figsize=(10, 3.5), colormap="tab10")
    plt.title("offer_type distribution by source")
    plt.xlabel("rows")
    plt.legend(title="offer_type", bbox_to_anchor=(1.02, 1), loc="upper left")
    savefig("12_offer_type.png")
    stats["offer_type_by_source"] = cross.to_dict()


def plot_feature_flags(df: pd.DataFrame, stats: dict):
    feat_known = (
        df.groupby("source")[FEATURE_COLS].apply(lambda g: g.notna().mean()).T
    )
    feat_known = feat_known[["robinreal","struct_img","struct_noi","sred"]]
    plt.figure(figsize=(12, 6))
    sns.heatmap(feat_known, annot=True, fmt=".0%", cmap="BuGn", vmin=0, vmax=1,
                cbar_kws={"label": "% rows where flag is known (0 or 1, not NULL)"})
    plt.title("Feature-flag coverage by source (known-value rate, higher = better)")
    plt.ylabel(""); plt.xlabel("")
    savefig("13_feature_flag_coverage.png")
    stats["feature_flag_known_rate_by_source"] = feat_known.to_dict()

    # True rates among known
    feat_true = df.groupby("source").apply(
        lambda g: (g[FEATURE_COLS] == 1).sum() / g[FEATURE_COLS].notna().sum().replace(0, np.nan)
    ).T
    feat_true = feat_true[["robinreal","struct_img","struct_noi","sred"]]
    plt.figure(figsize=(12, 6))
    sns.heatmap(feat_true, annot=True, fmt=".0%", cmap="Purples", vmin=0, vmax=1,
                cbar_kws={"label": "% TRUE among known"})
    plt.title("Feature-flag positive rate (among rows where flag is known)")
    plt.ylabel(""); plt.xlabel("")
    savefig("14_feature_flag_true_rate.png")
    stats["feature_flag_true_rate_by_source"] = feat_true.to_dict()


def plot_language(df: pd.DataFrame, stats: dict):
    cross = pd.crosstab(df["source"], df["lang_guess"])
    cross = cross.loc[["robinreal","struct_img","struct_noi","sred"]]
    cross.plot(kind="bar", stacked=True, figsize=(10, 4.5), colormap="Set3")
    plt.title("Description language mix by source (token-heuristic)")
    plt.ylabel("rows"); plt.xticks(rotation=0)
    plt.legend(title="lang", bbox_to_anchor=(1.02, 1), loc="upper left")
    savefig("15_language_mix.png")
    stats["language_by_source"] = cross.to_dict()
    stats["language_total"] = df["lang_guess"].value_counts().to_dict()


def plot_images(df: pd.DataFrame, stats: dict):
    plt.figure(figsize=(10, 4))
    sns.histplot(data=df, x="n_images", hue="source", multiple="stack",
                 bins=range(0, df["n_images"].max() + 2), palette="Set2")
    plt.title("Number of images per listing")
    plt.xlabel("n images"); plt.ylabel("rows")
    savefig("16_image_counts.png")
    stats["image_counts"] = {
        "zero_images": int((df["n_images"] == 0).sum()),
        "mean_images": float(df["n_images"].mean()),
        "median_images": float(df["n_images"].median()),
        "max_images": int(df["n_images"].max()),
    }
    stats["image_counts_by_source"] = df.groupby("source")["n_images"].describe().to_dict()


def plot_desc_length(df: pd.DataFrame, stats: dict):
    plt.figure(figsize=(10, 4))
    sns.histplot(df, x="desc_len_text", hue="source", log_scale=(True, False),
                 bins=60, multiple="layer", element="step", common_norm=False)
    plt.title("Description length (HTML-stripped, log x)")
    plt.xlabel("chars")
    savefig("17_desc_length.png")
    stats["desc_len_text"] = {
        "median": int(df["desc_len_text"].median()),
        "p90": int(df["desc_len_text"].quantile(0.9)),
        "has_html_rows": int(df["desc_has_html"].sum()),
        "empty_desc_rows": int((df["desc_len_text"] == 0).sum()),
        "short_desc_lt_50": int((df["desc_len_text"] < 50).sum()),
    }


def plot_temporal(df: pd.DataFrame, stats: dict):
    # last_scraped
    if df["last_scraped"].notna().any():
        tmp = df[df["last_scraped"].notna()].copy()
        tmp["last_scraped_month"] = tmp["last_scraped"].dt.to_period("M").dt.to_timestamp()
        counts = tmp.groupby(["last_scraped_month","source"]).size().unstack(fill_value=0)
        counts.plot(kind="area", figsize=(11, 4.5), colormap="Set2", alpha=0.7)
        plt.title("Listings by last_scraped month, stacked by source")
        plt.ylabel("rows"); plt.xlabel("")
        savefig("18_last_scraped_timeline.png")
        stats["last_scraped_range"] = [
            str(df["last_scraped"].min()), str(df["last_scraped"].max())
        ]
    # time_of_creation
    if df["time_of_creation"].notna().any():
        tmp = df[df["time_of_creation"].notna()].copy()
        tmp["created_month"] = tmp["time_of_creation"].dt.to_period("M").dt.to_timestamp()
        counts = tmp.groupby(["created_month","source"]).size().unstack(fill_value=0)
        counts.plot(kind="area", figsize=(11, 4.5), colormap="Set2", alpha=0.7)
        plt.title("Listings by time_of_creation month")
        plt.ylabel("rows"); plt.xlabel("")
        savefig("19_creation_timeline.png")
        stats["time_of_creation_range"] = [
            str(df["time_of_creation"].min()), str(df["time_of_creation"].max())
        ]
    stats["available_from_nulls"] = {
        src: int(df[df["source"] == src]["available_from"].isna().sum())
        for src in df["source"].unique()
    }


def plot_usability_funnel(df: pd.DataFrame, stats: dict):
    total = len(df)
    steps = [
        ("Total rows", total),
        ("has_price (≥200)", int(df["has_price"].sum())),
        ("+ has_rooms", int((df["has_price"] & df["has_rooms"]).sum())),
        ("+ any location", int((df["has_price"] & df["has_rooms"] & df["has_any_location"]).sum())),
        ("+ residential", int((df["has_price"] & df["has_rooms"] & df["has_any_location"] & df["is_residential"]).sum())),
        ("+ ACTIVE status", int((df["has_price"] & df["has_rooms"] & df["has_any_location"]
                                  & df["is_residential"] & df["is_active"]).sum())),
    ]
    labels = [s[0] for s in steps]; vals = [s[1] for s in steps]
    plt.figure(figsize=(11, 5))
    bars = plt.barh(range(len(labels)), vals, color=sns.color_palette("viridis", len(labels)))
    for i, v in enumerate(vals):
        plt.text(v + total*0.005, i, f"{v:,}  ({v/total:.1%})", va="center", fontsize=11)
    plt.yticks(range(len(labels)), labels)
    plt.gca().invert_yaxis()
    plt.xlim(0, total * 1.15)
    plt.title("Usability funnel — cumulative rows surviving each filter")
    savefig("20_usability_funnel.png")
    stats["usability_funnel"] = dict(steps)

    # Version without the ACTIVE requirement (since SRED has no status field)
    soft_steps = [
        ("Total", total),
        ("Price ≥ 200", int(df["has_price"].sum())),
        ("+ rooms ∈ (0,15]", int((df["has_price"] & df["has_rooms"]).sum())),
        ("+ locatable (city OR geo)", int((df["has_price"] & df["has_rooms"] & df["has_any_location"]).sum())),
        ("+ RENT offer_type", int((df["has_price"] & df["has_rooms"] & df["has_any_location"]
                                     & (df["offer_type_clean"] == "RENT")).sum())),
        ("+ residential category (or NULL)", int((df["has_price"] & df["has_rooms"] & df["has_any_location"]
                                                    & (df["offer_type_clean"] == "RENT")
                                                    & (df["is_residential"] | (df["object_category_clean"] == "__NULL__"))).sum())),
        ("+ NOT INACTIVE/DELETED", int((df["has_price"] & df["has_rooms"] & df["has_any_location"]
                                         & (df["offer_type_clean"] == "RENT")
                                         & (df["is_residential"] | (df["object_category_clean"] == "__NULL__"))
                                         & ~df["is_inactive_or_deleted"]).sum())),
    ]
    labels = [s[0] for s in soft_steps]; vals = [s[1] for s in soft_steps]
    plt.figure(figsize=(11, 5))
    bars = plt.barh(range(len(labels)), vals, color=sns.color_palette("mako", len(labels)))
    for i, v in enumerate(vals):
        plt.text(v + total*0.005, i, f"{v:,}  ({v/total:.1%})", va="center", fontsize=11)
    plt.yticks(range(len(labels)), labels)
    plt.gca().invert_yaxis()
    plt.xlim(0, total * 1.15)
    plt.title("Realistic funnel — keeping NULL status (SRED has no status)")
    savefig("21_funnel_realistic.png")
    stats["funnel_realistic"] = dict(soft_steps)


def plot_filter_answerability(df: pd.DataFrame, stats: dict):
    """For common query constraints, how many rows CAN be evaluated."""
    q = {
        "city = Zurich":         (df["city"].fillna("").str.lower().eq("zürich")
                                   | df["city"].fillna("").str.lower().eq("zurich")),
        "canton = ZH":           df["canton"].fillna("") == "ZH",
        "city in top-5":         df["city"].fillna("").str.lower().isin(
                                    [c.lower() for c in
                                     df.loc[df["has_city"],"city"].value_counts().head(5).index]),
        "price in [1500, 3500]": df["price"].between(1500, 3500),
        "rooms ∈ [2.5, 4.5]":    df["rooms"].between(2.5, 4.5),
        "balcony known":         df["feature_balcony"].notna(),
        "balcony = TRUE":        df["feature_balcony"] == 1,
        "parking known":         df["feature_parking"].notna(),
        "parking = TRUE":        df["feature_parking"] == 1,
        "within 5 km of ETH":    (
            (df["latitude"].notna() & df["longitude"].notna()) &
            ((df["latitude"] - 47.3769)**2 + (df["longitude"] - 8.5417)**2 < (5/111)**2)
        ),
        "available_from set":    df["available_from"].notna(),
        "has any image URL":     df["n_images"] > 0,
    }
    rows = [(k, int(m.sum()), float(m.mean())) for k, m in q.items()]
    tb = pd.DataFrame(rows, columns=["query_constraint","rows_matching","share_of_corpus"])
    plt.figure(figsize=(11, 5.5))
    sns.barplot(data=tb, y="query_constraint", x="rows_matching", color="#0ea5e9", orient="h")
    for i, r in tb.iterrows():
        plt.text(r["rows_matching"] + 50, i, f"{r['rows_matching']:,}  ({r['share_of_corpus']:.1%})",
                 va="center", fontsize=10)
    plt.xlim(0, len(df) * 1.1)
    plt.title(f"How many rows can answer common query constraints? (n={len(df):,})")
    plt.xlabel("rows matching"); plt.ylabel("")
    savefig("22_filter_answerability.png")
    stats["filter_answerability"] = tb.to_dict(orient="records")


def plot_duplicates(df: pd.DataFrame, stats: dict):
    # Potential dup signals: same URL, same platform_id across sources, same (title, city, price, rooms)
    url_dup = df[df["raw_url"].notna()].duplicated(subset=["raw_url"], keep=False).sum()
    pid_dup = df[df["platform_id"].notna()].duplicated(subset=["platform_id"], keep=False).sum()
    fuzzy = (
        df.dropna(subset=["title","city","price","rooms"])
          .duplicated(subset=["title","city","price","rooms"], keep=False)
          .sum()
    )
    # Cross-source title+price+rooms match
    xs = (
        df.dropna(subset=["title","price","rooms"])
          .groupby(["title","price","rooms"])["source"].nunique()
    )
    xsource = int((xs > 1).sum())
    stats["duplicates"] = {
        "dup_by_url": int(url_dup),
        "dup_by_platform_id": int(pid_dup),
        "dup_by_title_city_price_rooms": int(fuzzy),
        "cross_source_title_price_rooms_groups": xsource,
    }
    # Bar chart
    plt.figure(figsize=(9, 3.5))
    labels = ["same URL", "same platform_id", "(title,city,price,rooms)", "xsrc (title,price,rooms)"]
    values = [url_dup, pid_dup, fuzzy, xsource]
    sns.barplot(x=labels, y=values, color="#ef4444")
    for i, v in enumerate(values):
        plt.text(i, v, f"{v:,}", ha="center", va="bottom", fontsize=11)
    plt.title("Duplicate signals across the corpus")
    savefig("23_duplicates.png")


def plot_price_type(df: pd.DataFrame, stats: dict):
    cross = pd.crosstab(df["source"], df["raw_price_type"].fillna("__NULL__"))
    cross.plot(kind="bar", stacked=True, figsize=(10, 4), colormap="tab20")
    plt.title("price_type code distribution by source (enum meaning undocumented)")
    plt.ylabel("rows"); plt.xticks(rotation=0)
    plt.legend(title="price_type", bbox_to_anchor=(1.02, 1), loc="upper left")
    savefig("24_price_type.png")
    stats["price_type_by_source"] = cross.to_dict()


def plot_distance_coverage(df: pd.DataFrame, stats: dict):
    dist_cols = ["distance_public_transport","distance_shop","distance_kindergarten",
                 "distance_school_1","distance_school_2"]
    known = df.groupby("source")[dist_cols].apply(lambda g: g.notna().mean())
    known = known.loc[["robinreal","struct_img","struct_noi","sred"]]
    plt.figure(figsize=(11, 4))
    sns.heatmap(known, annot=True, fmt=".0%", cmap="YlGnBu", vmin=0, vmax=1,
                cbar_kws={"label": "known rate"})
    plt.title("Coverage of distance_* fields by source")
    savefig("25_distance_coverage.png")
    stats["distance_known_rate"] = known.to_dict()


def main() -> None:
    df = load_frame()
    df = ensure_types(df)
    df = derive_helpers(df)

    # Persist normalised frame for downstream validators (parquet + csv fallback)
    try:
        df.drop(columns=["raw_json","orig_data_json","images_json","location_address_json",
                         "features_json","desc_text"]).to_parquet(OUT_DATA / "unified.parquet",
                                                                  index=False)
        print(f"  wrote {OUT_DATA / 'unified.parquet'}")
    except Exception as e:  # pragma: no cover
        print(f"[WARN] parquet_write: expected=ok, got={e!r}, fallback=csv", flush=True)
        df.drop(columns=["raw_json","orig_data_json","images_json","location_address_json",
                         "features_json","desc_text"]).to_csv(OUT_DATA / "unified.csv", index=False)

    stats: dict = {"total_rows": int(len(df))}

    plot_row_counts(df, stats)
    plot_null_heatmap(df, stats)
    plot_status(df, stats)
    plot_price_distribution(df, stats)
    plot_rooms_area(df, stats)
    plot_price_per_m2(df, stats)
    plot_geo(df, stats)
    plot_canton_coverage(df, stats)
    plot_object_category(df, stats)
    plot_offer_type(df, stats)
    plot_feature_flags(df, stats)
    plot_language(df, stats)
    plot_images(df, stats)
    plot_desc_length(df, stats)
    plot_temporal(df, stats)
    plot_usability_funnel(df, stats)
    plot_filter_answerability(df, stats)
    plot_duplicates(df, stats)
    plot_price_type(df, stats)
    plot_distance_coverage(df, stats)

    (OUT_DATA / "stats.json").write_text(json.dumps(stats, indent=2, default=str))
    print(f"\nwrote {OUT_DATA / 'stats.json'}  (keys: {len(stats)})")


if __name__ == "__main__":
    main()
