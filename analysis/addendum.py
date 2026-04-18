"""Addendum stats + plots triggered by parallel-validator findings.

Writes:
  analysis/data/addendum.json
  analysis/plots/26_year_built_floor_coverage.png
  analysis/plots/27_pii_in_descriptions.png
  analysis/plots/28_url_hygiene.png
  analysis/plots/29_discriminatory_phrases.png
  analysis/plots/30_price_sanity_edges.png
"""
from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter
from html.parser import HTMLParser
from math import asin, cos, radians, sin, sqrt
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

SOURCES = {
    "robinreal":   "raw_data/robinreal_data_withimages-1776461278845.csv",
    "sred":        "raw_data/sred_data_withmontageimages_latlong.csv",
    "struct_img":  "raw_data/structured_data_withimages-1776412361239.csv",
    "struct_noi":  "raw_data/structured_data_withoutimages-1776412361239.csv",
}

ETH_LAT, ETH_LON = 47.3769, 8.5417
sns.set_theme(style="whitegrid", context="talk", font_scale=0.85)


def haversine_km(lat1, lon1, lat2, lon2):
    lat1r, lat2r = radians(lat1), radians(lat2)
    dlat = lat2r - lat1r
    dlon = radians(lon2) - radians(lon1)
    a = sin(dlat / 2) ** 2 + cos(lat1r) * cos(lat2r) * sin(dlon / 2) ** 2
    return 2 * 6371.0 * asin(sqrt(a))


class _HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, data):
        self.parts.append(data)


def strip_html(s):
    if not s or "<" not in s:
        return s or ""
    p = _HTMLStripper()
    try:
        p.feed(s)
    except Exception as e:
        print(f"[WARN] strip_html: expected=valid_html, got={e!r}, fallback=raw", flush=True)
        return s
    return "".join(p.parts)


HTML_TAG_RE = re.compile(r"<[a-zA-Z/]")
EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
# Swiss phone-style: exclude obvious dates like "1. April 2026" by requiring 7+ digits in cluster.
PHONE_RE = re.compile(r"(?<!\d)(?:\+?41|0)\s?(?:\d[\s./-]?){8,12}\d(?!\d)")
URL_RE = re.compile(r"https?://[^\s<>\"]+", re.IGNORECASE)
# Tight pattern: require the event handler to start at a word boundary AFTER a tag-like context,
# not in the middle of a word like "Monat =". Match literal event-handler names only.
_EVENT_NAMES = (r"onclick|onload|onerror|onmouseover|onmouseout|onfocus|onblur|onchange|onsubmit|"
                r"oninput|onkeydown|onkeyup|onkeypress|ontoggle|onanimation\w*|ontransition\w*")
SCRIPT_RE = re.compile(
    r"<\s*(?:script|iframe|object|embed)\b|javascript:|(?:(?<=<)|\s)(?:" + _EVENT_NAMES + r")\s*=",
    re.IGNORECASE,
)
ANCHOR_RE = re.compile(r"<\s*a\b", re.IGNORECASE)
IMG_RE = re.compile(r"<\s*img\b", re.IGNORECASE)

# Discriminatory / exclusionary phrases in DE/FR/IT/EN. Deliberately narrow: these are phrases that
# would surface badly to unrelated users in a ranking UI.
DISCRIM_PATTERNS = {
    "pets_forbidden": re.compile(r"\b(keine tiere|keine haustiere|pas d'animaux|no pets|niente animali)\b", re.IGNORECASE),
    "smokers_forbidden": re.compile(r"\b(nichtraucher|non[-\s]?smoker|no smokers|non\s?fumatore|non\s?fumeur)\b", re.IGNORECASE),
    "singles_only": re.compile(r"\b(single[-\s]?wohnung|für einzelperson|ideal für singles|per single|pour célibataire|studio pour une personne)\b", re.IGNORECASE),
    "adults_only": re.compile(r"\b(erwachsene|adult[-\s]?only|adultes seulement|solo adulti)\b", re.IGNORECASE),
    "no_wg": re.compile(r"\b(keine wg|pas de colocation|no sharing)\b", re.IGNORECASE),
    "children_restriction": re.compile(r"\b(keine kinder|pas d'enfants|no children)\b", re.IGNORECASE),
}

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


def load_frame() -> pd.DataFrame:
    rows = []
    for source, rel in SOURCES.items():
        with (ROOT / rel).open(newline="", encoding="utf-8") as f:
            for raw in csv.DictReader(f):
                parsed = prepare_listing_row(raw)
                rec = dict(zip(COLUMNS, parsed))
                rec["source"] = source
                rec["raw_status"] = (raw.get("status") or "").strip().upper() or None
                rec["raw_year_built"] = raw.get("year_built")
                rec["raw_floor"] = raw.get("floor")
                rec["raw_agency_email"] = (raw.get("agency_email") or "").strip() or None
                rec["raw_agency_phone"] = (raw.get("agency_phone") or "").strip() or None
                rec["raw_agency_name"] = (raw.get("agency_name") or "").strip() or None
                rec["raw_url"] = (raw.get("platform_url") or "").strip() or None
                rec["raw_rent_net"] = raw.get("rent_net")
                rec["raw_rent_gross"] = raw.get("rent_gross")
                rows.append(rec)
    df = pd.DataFrame(rows)
    for c in ("price","rooms","area","latitude","longitude"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["year_built"] = pd.to_numeric(df["raw_year_built"], errors="coerce")
    df["floor"] = pd.to_numeric(df["raw_floor"], errors="coerce")
    df["desc_text"] = df["description"].fillna("").apply(strip_html)
    return df


def savefig(name):
    path = OUT_PLOTS / name
    plt.tight_layout()
    plt.savefig(path, dpi=160, bbox_inches="tight")
    plt.close()
    print(f"  wrote {path}")


def plot_year_built_floor(df, stats):
    cov = pd.DataFrame({
        "year_built_known": df.groupby("source")["year_built"].apply(lambda s: s.notna().mean()),
        "floor_known": df.groupby("source")["floor"].apply(lambda s: s.notna().mean()),
    }).reindex(["robinreal","struct_img","struct_noi","sred"])
    plt.figure(figsize=(9, 3.8))
    sns.heatmap(cov.T, annot=True, fmt=".0%", cmap="BuGn", vmin=0, vmax=1,
                cbar_kws={"label": "known rate"})
    plt.title("year_built and floor — fraction of rows where the field is populated")
    savefig("26_year_built_floor_coverage.png")
    stats["year_built_floor_coverage"] = cov.to_dict()

    # Year-built histogram for the rows that have it.
    sub = df[df["year_built"].between(1800, 2030)]
    if len(sub):
        plt.figure(figsize=(10, 3.8))
        sns.histplot(sub, x="year_built", bins=40, color="#10b981")
        plt.title(f"year_built distribution (n_with_value={len(sub):,} / {len(df):,})")
        savefig("26b_year_built_hist.png")
        stats["year_built_range"] = [float(sub["year_built"].min()), float(sub["year_built"].max())]


def plot_pii(df, stats):
    rows = []
    samples = {}
    for src, g in df.groupby("source"):
        n = len(g)
        texts = g["desc_text"].fillna("")
        e_mask = texts.str.contains(EMAIL_RE, na=False)
        p_mask = texts.str.contains(PHONE_RE, na=False)
        u_mask = texts.str.contains(URL_RE, na=False)
        rows.append((src, n, int(e_mask.sum()), int(p_mask.sum()), int(u_mask.sum())))
        # redacted samples
        hit_e = g.loc[e_mask, "desc_text"].head(1).tolist()
        if hit_e:
            raw = hit_e[0]
            samples[f"{src}_email"] = EMAIL_RE.sub("<EMAIL>", raw[:200])
    tb = pd.DataFrame(rows, columns=["source","n","email_hits","phone_hits","url_hits"])
    tb["email_pct"] = tb["email_hits"] / tb["n"]
    tb["phone_pct"] = tb["phone_hits"] / tb["n"]
    tb["url_pct"] = tb["url_hits"] / tb["n"]
    stats["pii_in_descriptions"] = tb.to_dict(orient="records")
    stats["pii_samples_redacted"] = samples
    stats["agency_field_populated"] = {
        c: int(df[c].notna().sum())
        for c in ("raw_agency_email","raw_agency_phone","raw_agency_name")
    }

    plt.figure(figsize=(10, 4.2))
    mx = tb.melt(id_vars=["source"], value_vars=["email_hits","phone_hits","url_hits"],
                 var_name="kind", value_name="rows")
    sns.barplot(data=mx, x="source", y="rows", hue="kind",
                order=["robinreal","struct_img","struct_noi","sred"])
    plt.title("PII / URL mentions in free-text descriptions (agency_* fields are 100% NULL everywhere)")
    plt.ylabel("rows")
    plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left")
    savefig("27_pii_in_descriptions.png")


def plot_url_hygiene(df, stats):
    def image_urls(s):
        if not s:
            return []
        try:
            v = json.loads(s)
        except Exception:
            return []
        if isinstance(v, dict):
            v = v.get("images", [])
        if not isinstance(v, list):
            return []
        return [x.get("url") if isinstance(x, dict) else x for x in v if x]

    # Compute per-row image URL mix
    def classify(urls):
        if not urls:
            return ("none", "none")
        has_http = any(isinstance(u, str) and u.startswith("http") for u in urls)
        has_local = any(isinstance(u, str) and u.startswith("/") for u in urls)
        if has_http and has_local: return ("mixed", "mixed")
        if has_http: return ("https", "remote")
        if has_local: return ("local", "local")
        return ("other", "other")

    from collections import defaultdict
    per_source = defaultdict(lambda: Counter())
    platform_url_empty = Counter()
    for src, g in df.groupby("source"):
        for _, row in g.iterrows():
            urls = image_urls(row.get("images_json"))
            cat = classify(urls)[0]
            per_source[src][cat] += 1
            if not row.get("raw_url"):
                platform_url_empty[src] += 1
    stats["image_url_classification"] = {src: dict(c) for src, c in per_source.items()}
    stats["platform_url_empty_by_source"] = dict(platform_url_empty)

    tb = (pd.DataFrame(per_source).fillna(0).astype(int).T
          .reindex(["robinreal","struct_img","struct_noi","sred"]))
    tb.plot(kind="bar", stacked=True, figsize=(10, 4), colormap="tab20")
    plt.title("Image URL hygiene per source — 'mixed' rows have both HTTPS and local paths")
    plt.ylabel("rows"); plt.xticks(rotation=0)
    plt.legend(title="url kind", bbox_to_anchor=(1.02, 1), loc="upper left")
    savefig("28_url_hygiene.png")


def plot_discrimination(df, stats):
    rows = []
    for label, rx in DISCRIM_PATTERNS.items():
        per_source = {}
        total = 0
        for src, g in df.groupby("source"):
            m = g["desc_text"].str.contains(rx, na=False)
            per_source[src] = int(m.sum())
            total += int(m.sum())
        rows.append((label, total, per_source))
    tb = pd.DataFrame({"phrase": [r[0] for r in rows], "total_rows": [r[1] for r in rows]})
    stats["discriminatory_phrases"] = {r[0]: {"total": r[1], **r[2]} for r in rows}

    plt.figure(figsize=(10, 4.5))
    sns.barplot(data=tb, y="phrase", x="total_rows", orient="h", color="#ef4444")
    for i, v in enumerate(tb["total_rows"]):
        plt.text(v + 3, i, f"{v:,}", va="center")
    plt.title("Exclusionary / discriminatory phrases in descriptions (DE/FR/IT/EN)")
    plt.xlabel("rows"); plt.ylabel("")
    savefig("29_discriminatory_phrases.png")


def plot_price_sanity(df, stats):
    # rent_net > rent_gross impossibility + null-island lat/lng + price outliers in [1,10]
    rent_net = pd.to_numeric(df["raw_rent_net"].astype(str).str.replace("'","").str.replace(",","."), errors="coerce")
    rent_gross = pd.to_numeric(df["raw_rent_gross"].astype(str).str.replace("'","").str.replace(",","."), errors="coerce")
    impossible = int(((rent_net > rent_gross) & rent_net.notna() & rent_gross.notna()).sum())
    null_island = int(((df["latitude"] == 0) & (df["longitude"] == 0)).sum())
    low_1_to_10 = int(df["price"].between(1, 10, inclusive="both").sum())
    very_high = int((df["price"] > 50000).sum())
    huge_outlier = int((df["price"] > 100000).sum())
    # coord precision: SRED rows with ≤2 decimals (anonymisation grid)
    sred_low_precision = 0
    for _, r in df[df["source"] == "sred"].iterrows():
        lat = r["latitude"]; lng = r["longitude"]
        if pd.isna(lat) or pd.isna(lng):
            continue
        # check raw string representation's decimals
        if abs(lat - round(lat, 2)) < 1e-9 and abs(lng - round(lng, 2)) < 1e-9:
            sred_low_precision += 1
    stats["price_and_geo_sanity"] = {
        "rent_net_gt_rent_gross": impossible,
        "lat_lng_null_island": null_island,
        "price_in_1_to_10": low_1_to_10,
        "price_gt_50k": very_high,
        "price_gt_100k": huge_outlier,
        "sred_coords_2dp_rounded": sred_low_precision,
    }

    # Haversine 5 km from ETH for the *correct* count
    mask = df["latitude"].notna() & df["longitude"].notna()
    dists = df.loc[mask].apply(lambda r: haversine_km(r["latitude"], r["longitude"], ETH_LAT, ETH_LON), axis=1)
    within_5km = int((dists <= 5).sum())
    within_2km = int((dists <= 2).sum())
    within_10km = int((dists <= 10).sum())
    stats["near_eth_zurich_counts_haversine"] = {
        "within_2km": within_2km,
        "within_5km": within_5km,
        "within_10km": within_10km,
    }

    # HTML tag (tight regex) count
    html_hits = int(df["description"].fillna("").str.contains(HTML_TAG_RE, regex=True).sum())
    script_hits = int(df["description"].fillna("").str.contains(SCRIPT_RE, regex=True).sum())
    anchor_hits = int(df["description"].fillna("").str.contains(ANCHOR_RE, regex=True).sum())
    img_hits = int(df["description"].fillna("").str.contains(IMG_RE, regex=True).sum())
    stats["html_tight"] = {
        "rows_with_html_tag": html_hits,
        "rows_with_script_or_event": script_hits,
        "rows_with_anchor_tag": anchor_hits,
        "rows_with_img_tag": img_hits,
    }

    labels = ["price ∈ [1,10]", "price > 50k", "price > 100k", "rent_net>rent_gross",
              "lat/lng = (0,0)", "SRED coord 2dp", "desc has <a>", "desc has <img>", "desc has script/evt"]
    values = [low_1_to_10, very_high, huge_outlier, impossible, null_island, sred_low_precision,
              anchor_hits, img_hits, script_hits]
    plt.figure(figsize=(11, 4.8))
    sns.barplot(x=labels, y=values, color="#f97316")
    for i, v in enumerate(values):
        plt.text(i, v + max(values)*0.01, f"{v:,}", ha="center", va="bottom", fontsize=10)
    plt.xticks(rotation=20, ha="right")
    plt.title("Safety / integrity edge cases")
    plt.ylabel("rows")
    savefig("30_price_geo_safety.png")

    # Status split pie
    plt.figure(figsize=(6, 6))
    s = df["raw_status"].fillna("__NULL__").value_counts()
    plt.pie(s.values, labels=[f"{k}\n{v:,}" for k, v in s.items()], autopct="%.1f%%",
            colors=sns.color_palette("Set2", len(s)))
    plt.title("Listing status — corpus-wide")
    savefig("31_status_pie.png")
    stats["status_totals"] = s.to_dict()


def main():
    df = load_frame()
    print(f"Loaded {len(df):,} rows.")
    stats = {}
    plot_year_built_floor(df, stats)
    plot_pii(df, stats)
    plot_url_hygiene(df, stats)
    plot_discrimination(df, stats)
    plot_price_sanity(df, stats)
    (OUT_DATA / "addendum.json").write_text(json.dumps(stats, indent=2, default=str))
    print(f"\nwrote {OUT_DATA / 'addendum.json'}")


if __name__ == "__main__":
    main()
