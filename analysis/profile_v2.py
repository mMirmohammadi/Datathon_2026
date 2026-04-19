"""Stakeholder-grade profile of the Datathon-2026 corpus — post-enrichment.

The v1 `profile.py` audited the RAW 4 CSVs (22,819 rows, mostly-null fields).
This v2 script audits everything we built on top of that:

  Layer 1 — `listings_enriched` (37 null-filled fields × 4 provenance cols)
  Layer 2 — `listings_ranking_signals` (31 derived signals)
  Layer 2 — `listing_commute_times` (real transit minutes, 125k rows)
  Layer 2 — `data/ranking/embeddings.fp16.npy` (25,546 × 1024 semantic vectors)
  Landmarks — `data/ranking/landmarks.json` (45 curated + mined)

Outputs:
  analysis/data/stats_v2.json           Machine-readable numbers.
  analysis/plots_v2/*.png               ~20 production-grade plots.

Every number in analysis/REPORT_v2.md traces back to stats_v2.json.
Every plot is deterministic: same DB + same script → identical PNG bits.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "listings.db"
OUT_DATA = ROOT / "analysis" / "data"
OUT_PLOTS = ROOT / "analysis" / "plots_v2"
OUT_DATA.mkdir(parents=True, exist_ok=True)
OUT_PLOTS.mkdir(parents=True, exist_ok=True)

# Visual theme — match v1's sns "talk" context for pitch-deck legibility.
sns.set_theme(style="whitegrid", context="talk", font_scale=0.85)
BRAND = {
    "original":          "#2C3E50",
    "rev_geo_offline":   "#27AE60",
    "rev_geo_nominatim": "#16A085",
    "text_gpt_5_4":      "#E67E22",
    "text_gpt_5_4_nano": "#D35400",
    "plz_majority_vote": "#8E44AD",
    "DROPPED_bad_data":  "#C0392B",
    "UNKNOWN":           "#95A5A6",
    "UNKNOWN-pending":   "#BDC3C7",
    "default_constant":  "#7F8C8D",
    "cross_ref":         "#F39C12",
}

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _con():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB not found at {DB_PATH}")
    c = sqlite3.connect(str(DB_PATH))
    c.row_factory = sqlite3.Row
    return c


def _enriched_fields(con) -> list[str]:
    cols = {r[1] for r in con.execute("PRAGMA table_info(listings_enriched)").fetchall()}
    return sorted(c[:-7] for c in cols if c.endswith("_filled"))


def _signal_cols(con) -> list[str]:
    return [r[1] for r in con.execute("PRAGMA table_info(listings_ranking_signals)").fetchall()
            if r[1] != "listing_id"]


# ---------------------------------------------------------------------------
# §A — corpus at a glance
# ---------------------------------------------------------------------------

def section_A_corpus(stats: dict) -> None:
    con = _con()
    stats["section_A_corpus"] = {}

    # A.1 rowcounts across the 4 tables
    rows = {}
    for tbl in ("listings", "listings_enriched", "listings_ranking_signals",
                "listing_commute_times"):
        rows[tbl] = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    stats["section_A_corpus"]["table_rowcounts"] = rows

    # A.2 by scrape_source (comparis / sred / robinreal)
    src_counts = {r[0]: r[1] for r in con.execute(
        "SELECT scrape_source, COUNT(*) FROM listings GROUP BY scrape_source").fetchall()}
    stats["section_A_corpus"]["by_scrape_source"] = src_counts

    # Plot: rowcounts bar (new table count)
    fig, ax = plt.subplots(figsize=(10, 5))
    labels = list(rows.keys())
    vals = list(rows.values())
    bars = ax.barh(labels, vals, color=["#2C3E50", "#27AE60", "#16A085", "#E67E22"])
    for bar, val in zip(bars, vals):
        ax.text(val + max(vals) * 0.01, bar.get_y() + bar.get_height() / 2,
                f"{val:,}", va="center", fontsize=11)
    ax.set_xlabel("rows")
    ax.set_title("DB tables — the four pillars of the dataset")
    ax.set_xlim(0, max(vals) * 1.15)
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "40_table_rowcounts.png", dpi=130)
    plt.close()

    # Plot: by source
    fig, ax = plt.subplots(figsize=(8, 4.5))
    order = sorted(src_counts, key=lambda k: -src_counts[k])
    bars = ax.barh(order, [src_counts[k] for k in order],
                   color=["#2C3E50", "#27AE60", "#E67E22"])
    for bar, k in zip(bars, order):
        v = src_counts[k]
        pct = 100 * v / sum(src_counts.values())
        ax.text(v + max(src_counts.values()) * 0.01, bar.get_y() + bar.get_height() / 2,
                f"{v:,}  ({pct:.1f}%)", va="center", fontsize=11)
    ax.set_xlabel("listings")
    ax.set_title(f"Corpus by scrape_source  (total {sum(src_counts.values()):,})")
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "41_by_scrape_source.png", dpi=130)
    plt.close()

    con.close()


# ---------------------------------------------------------------------------
# §B — Layer 1 enrichment: source composition per field
# ---------------------------------------------------------------------------

def section_B_layer1(stats: dict) -> None:
    con = _con()
    stats["section_B_layer1"] = {}

    fields = _enriched_fields(con)

    # Per field: {source: count}
    per_field: dict[str, dict[str, int]] = {}
    for f in fields:
        rows = con.execute(
            f"SELECT {f}_source AS src, COUNT(*) n FROM listings_enriched GROUP BY src"
        ).fetchall()
        per_field[f] = {r["src"]: r["n"] for r in rows}
    stats["section_B_layer1"]["per_field_sources"] = per_field

    # B.1 pending invariant — MUST be zero everywhere
    pending = {f: per_field[f].get("UNKNOWN-pending", 0) for f in fields}
    total_pending = sum(pending.values())
    stats["section_B_layer1"]["total_pending"] = total_pending
    stats["section_B_layer1"]["pending_per_field"] = pending

    # B.2 coverage % per field (not-UNKNOWN, not-DROPPED)
    coverage = {}
    for f in fields:
        src = per_field[f]
        real = sum(v for k, v in src.items()
                   if k not in ("UNKNOWN", "UNKNOWN-pending", "DROPPED_bad_data"))
        coverage[f] = real / sum(src.values())
    stats["section_B_layer1"]["coverage_per_field"] = coverage

    # PLOT B-a: stacked-bar source distribution, all 37 fields
    fig, ax = plt.subplots(figsize=(13, 11))
    # source order — consistent colouring
    SRC_ORDER = ["original", "rev_geo_offline", "rev_geo_nominatim",
                 "plz_majority_vote",
                 "text_gpt_5_4", "text_gpt_5_4_nano",
                 "text_regex_de", "text_regex_fr", "text_regex_it", "text_regex_en",
                 "default_constant", "cross_ref",
                 "DROPPED_bad_data", "UNKNOWN", "UNKNOWN-pending"]
    # Sort fields by real-coverage descending
    fields_sorted = sorted(fields, key=lambda f: coverage[f], reverse=True)
    bottom = np.zeros(len(fields_sorted))
    y = np.arange(len(fields_sorted))
    for src in SRC_ORDER:
        vals = np.array([per_field[f].get(src, 0) for f in fields_sorted], dtype=float)
        if vals.sum() == 0:
            continue
        colour = BRAND.get(src, "#888")
        ax.barh(y, vals, left=bottom, color=colour, label=src, height=0.82)
        bottom += vals
    ax.set_yticks(y)
    ax.set_yticklabels(fields_sorted, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlim(0, 25546)
    ax.set_xlabel("listings")
    ax.set_title("Layer 1 enrichment — source composition per field\n"
                 "(each row = one field; bar segments = source of the value)")
    ax.legend(loc="lower right", fontsize=8, ncol=2, framealpha=0.9)
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "42_layer1_source_per_field.png", dpi=130)
    plt.close()

    # PLOT B-b: coverage percentages sorted
    fig, ax = plt.subplots(figsize=(12, 11))
    vals = [coverage[f] * 100 for f in fields_sorted]
    colours = ["#27AE60" if v >= 90 else "#F39C12" if v >= 50 else "#C0392B" for v in vals]
    bars = ax.barh(y, vals, color=colours)
    for bar, v in zip(bars, vals):
        ax.text(v + 1, bar.get_y() + bar.get_height() / 2,
                f"{v:.1f}%", va="center", fontsize=9)
    ax.set_yticks(y)
    ax.set_yticklabels(fields_sorted, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlim(0, 108)
    ax.set_xlabel("% of listings with a real value (not UNKNOWN / DROPPED)")
    ax.set_title("Layer 1 enrichment — real-value coverage per field")
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "43_layer1_coverage_per_field.png", dpi=130)
    plt.close()

    # PLOT B-c: canton coverage evolution
    # raw CSV: canton column NULL or empty for ~65%
    # listings.canton might differ from listings_enriched.canton_filled
    raw_canton_null = con.execute(
        "SELECT COUNT(*) FROM listings WHERE canton IS NULL OR canton=''"
    ).fetchone()[0]
    raw_canton_have = 25546 - raw_canton_null
    offline_n = per_field["canton"].get("rev_geo_offline", 0)
    nominatim_n = per_field["canton"].get("rev_geo_nominatim", 0)
    plz_vote_n = per_field["canton"].get("plz_majority_vote", 0)
    unknown_n = per_field["canton"].get("UNKNOWN", 0)
    dropped_n = per_field["canton"].get("DROPPED_bad_data", 0)

    stages = ["Raw CSV\n(listings.canton)",
              "+ offline rev_geo\n(pass 1a)",
              "+ Nominatim\n(pass 1b-backfill)",
              "+ PLZ majority vote\n(pass 1d)",
              "= Final\n(zero pending)"]
    # Cumulative coverage
    s1 = raw_canton_have
    s2 = s1 + offline_n
    s3 = s2 + nominatim_n
    s4 = s3 + plz_vote_n
    s5 = s4  # final = s4, remainder is UNKNOWN/DROPPED
    total = 25546
    vals = [s1, s2, s3, s4, s5]
    pcts = [v / total * 100 for v in vals]

    fig, ax = plt.subplots(figsize=(11, 6))
    bars = ax.bar(stages, pcts, color=["#95A5A6", "#27AE60", "#16A085",
                                        "#8E44AD", "#2C3E50"], alpha=0.9)
    for bar, p, v in zip(bars, pcts, vals):
        ax.text(bar.get_x() + bar.get_width() / 2, p + 1,
                f"{p:.1f}%\n({v:,})", ha="center", fontsize=10)
    ax.set_ylim(0, 108)
    ax.set_ylabel("% of 25,546 listings with a real canton")
    ax.set_title("Canton coverage — from raw CSV to final enriched state\n"
                 f"Raw → Final: {raw_canton_have/total*100:.1f}% → {s5/total*100:.2f}%")
    ax.axhline(99, linestyle="--", color="#27AE60", alpha=0.4, lw=1)
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "44_canton_coverage_evolution.png", dpi=130)
    plt.close()

    # PLOT B-d: feature-flag before/after (raw CSV vs GPT-enriched)
    feature_cols = [f for f in fields if f.startswith("feature_")]
    rows = con.execute(
        "SELECT " + ", ".join([f"SUM(CASE WHEN {c} IN (0,1) THEN 1 ELSE 0 END) raw_known_{c}"
                                for c in feature_cols]) + " FROM listings"
    ).fetchone()
    raw_known = {c: rows[f"raw_known_{c}"] or 0 for c in feature_cols}
    # After enrichment: not UNKNOWN
    enriched_known = {}
    for c in feature_cols:
        r = con.execute(
            f"SELECT COUNT(*) FROM listings_enriched WHERE {c}_source != 'UNKNOWN'"
        ).fetchone()[0]
        enriched_known[c] = r

    df = pd.DataFrame({
        "field": feature_cols,
        "raw": [raw_known[c] for c in feature_cols],
        "enriched": [enriched_known[c] for c in feature_cols],
    })
    df["lift"] = df["enriched"] - df["raw"]
    df = df.sort_values("enriched", ascending=True)

    fig, ax = plt.subplots(figsize=(13, 7.5))
    y = np.arange(len(df))
    ax.barh(y, df["raw"], color="#95A5A6", label="raw CSV (original)", height=0.35)
    ax.barh(y + 0.4, df["enriched"], color="#E67E22", label="after Pass 2 GPT", height=0.35)
    ax.set_yticks(y + 0.2)
    ax.set_yticklabels([c.replace("feature_", "") for c in df["field"]], fontsize=10)
    ax.set_xlim(0, 25546)
    ax.set_xlabel("listings with known value (not UNKNOWN)")
    ax.axvline(25546, ls=":", color="gray", alpha=0.4)
    ax.text(25546, -0.5, "25,546\n(all)", ha="center", fontsize=9, color="gray")
    ax.set_title("Feature-flag coverage — raw CSV vs after GPT-5.4-mini extraction")
    ax.legend(loc="lower right", fontsize=11)
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "45_feature_flag_lift.png", dpi=130)
    plt.close()

    stats["section_B_layer1"]["feature_lift"] = {
        c: {"raw": raw_known[c], "enriched": enriched_known[c],
            "lift": enriched_known[c] - raw_known[c]}
        for c in feature_cols
    }

    con.close()


# ---------------------------------------------------------------------------
# §C — Layer 2 ranking signals
# ---------------------------------------------------------------------------

def section_C_layer2_signals(stats: dict) -> None:
    con = _con()
    stats["section_C_layer2_signals"] = {}

    signal_cols = _signal_cols(con)
    # Populated-count per signal
    pop = {}
    for c in signal_cols:
        pop[c] = con.execute(
            f"SELECT COUNT(*) FROM listings_ranking_signals WHERE {c} IS NOT NULL"
        ).fetchone()[0]
    stats["section_C_layer2_signals"]["populated_per_signal"] = pop

    # Classify signal by kind prefix
    def _kind(c):
        if c.startswith("price_"): return "price"
        if c.startswith("dist_nearest_stop_m") or c.startswith("nearest_stop_"): return "gtfs"
        if c.startswith("poi_"): return "poi"
        if c.startswith("dist_") and not c.startswith("dist_landmark"): return "noise"
        if c.startswith("embedding_"): return "embedding"
        if c == "last_updated_utc": return "meta"
        return "other"

    # PLOT C-a: coverage bar grouped by kind
    df = pd.DataFrame({"signal": signal_cols,
                       "populated": [pop[c] for c in signal_cols],
                       "kind": [_kind(c) for c in signal_cols]})
    df["pct"] = 100 * df["populated"] / 25546
    df = df.sort_values(["kind", "pct"], ascending=[True, False])

    fig, ax = plt.subplots(figsize=(13, 11))
    kind_colour = {"price": "#2C3E50", "gtfs": "#27AE60", "poi": "#E67E22",
                   "noise": "#C0392B", "embedding": "#8E44AD", "meta": "#95A5A6",
                   "other": "#7F8C8D"}
    y = np.arange(len(df))
    bars = ax.barh(y, df["pct"], color=[kind_colour[k] for k in df["kind"]])
    for bar, v, n in zip(bars, df["pct"], df["populated"]):
        ax.text(v + 1, bar.get_y() + bar.get_height() / 2,
                f"{v:.1f}% ({n:,})", va="center", fontsize=9)
    ax.set_yticks(y)
    ax.set_yticklabels(df["signal"], fontsize=9)
    ax.invert_yaxis()
    ax.set_xlim(0, 115)
    ax.set_xlabel("% of 25,546 listings with a non-NULL value")
    ax.set_title("Layer 2 ranking signals — coverage (grouped by category, colored)")
    # Legend for kinds
    import matplotlib.patches as mpatches
    handles = [mpatches.Patch(color=c, label=k) for k, c in kind_colour.items()]
    ax.legend(handles=handles, loc="lower right", fontsize=10, title="signal kind")
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "46_layer2_signal_coverage.png", dpi=130)
    plt.close()

    # PLOT C-b: price-delta distribution (canton × rooms)
    rows = con.execute(
        "SELECT price_delta_pct_canton_rooms AS d "
        "FROM listings_ranking_signals WHERE price_delta_pct_canton_rooms IS NOT NULL"
    ).fetchall()
    deltas = np.array([r["d"] for r in rows], dtype=float)
    deltas_clipped = np.clip(deltas, -1.0, 1.5)  # clip for viz; outliers in text

    fig, ax = plt.subplots(figsize=(11, 5.5))
    ax.hist(deltas_clipped * 100, bins=80, color="#2C3E50", alpha=0.85)
    ax.axvline(0, color="#27AE60", lw=2, label="bucket median (0%)")
    ax.set_xlabel("price_delta_pct_canton_rooms (capped at −100% / +150% for viz)")
    ax.set_ylabel("listings")
    ax.set_title(
        f"Price delta vs (canton × rooms) bucket median\n"
        f"{len(deltas):,} listings with a baseline  |  "
        f"outliers with |delta| > 300%: {int(np.sum(np.abs(deltas) > 3))}")
    ax.legend()
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "47_price_delta_distribution.png", dpi=130)
    plt.close()

    # PLOT C-c: dist_nearest_stop_m distribution (log)
    rows = con.execute(
        "SELECT dist_nearest_stop_m AS d FROM listings_ranking_signals "
        "WHERE dist_nearest_stop_m IS NOT NULL"
    ).fetchall()
    dists = np.array([r["d"] for r in rows], dtype=float)
    fig, ax = plt.subplots(figsize=(11, 5.5))
    ax.hist(dists, bins=np.logspace(0, 4, 60), color="#27AE60", alpha=0.85)
    ax.set_xscale("log")
    ax.axvline(300, color="#F39C12", lw=2, ls="--", label="300 m (walking)")
    ax.axvline(1000, color="#C0392B", lw=2, ls="--", label="1 km (bike)")
    ax.set_xlabel("distance to nearest public-transport stop (metres, log scale)")
    ax.set_ylabel("listings")
    p50 = np.percentile(dists, 50)
    p90 = np.percentile(dists, 90)
    ax.set_title(f"How close are listings to public transport?\n"
                 f"p50 = {p50:.0f} m  |  p90 = {p90:.0f} m  |  {len(dists):,} listings scored")
    ax.legend()
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "48_transit_distance_distribution.png", dpi=130)
    plt.close()
    stats["section_C_layer2_signals"]["dist_nearest_stop_m_p50"] = float(p50)
    stats["section_C_layer2_signals"]["dist_nearest_stop_m_p90"] = float(p90)

    # PLOT C-d: 4 POI panels
    poi_pairs = [
        ("poi_supermarket_300m", "supermarkets", 300),
        ("poi_school_1km",       "schools",      1000),
        ("poi_restaurant_300m",  "restaurants",  300),
        ("poi_playground_500m",  "playgrounds",  500),
    ]
    fig, axs = plt.subplots(2, 2, figsize=(12, 9))
    for (col, label, r), ax in zip(poi_pairs, axs.flat):
        arr = np.array([r[0] for r in con.execute(
            f"SELECT {col} FROM listings_ranking_signals WHERE {col} IS NOT NULL").fetchall()])
        ax.hist(arr, bins=max(10, min(50, int(arr.max()))), color="#E67E22", alpha=0.85)
        ax.set_title(f"{label} within {r} m\n(p50 = {np.percentile(arr, 50):.0f}, "
                     f"p90 = {np.percentile(arr, 90):.0f}, max = {arr.max():.0f})")
        ax.set_xlabel("count within radius")
        ax.set_ylabel("listings")
    fig.suptitle("Neighborhood amenity density (OSM POIs)", y=1.01, fontsize=15)
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "49_poi_density_panels.png", dpi=130)
    plt.close()

    # PLOT C-e: noise proxy (distances to motorway, primary, rail)
    fig, ax = plt.subplots(figsize=(11, 5.5))
    colors = {"motorway": "#C0392B", "primary_road": "#E67E22", "rail": "#16A085"}
    for col, cl in [("dist_motorway_m", "motorway"), ("dist_primary_road_m", "primary_road"),
                    ("dist_rail_m", "rail")]:
        arr = np.array([r[0] for r in con.execute(
            f"SELECT {col} FROM listings_ranking_signals WHERE {col} IS NOT NULL").fetchall()])
        ax.hist(arr, bins=50, alpha=0.55, color=colors[cl], label=f"{cl} (n={len(arr):,})")
    ax.set_xlabel("distance (metres)")
    ax.set_ylabel("listings")
    ax.set_title("Noise proxy — distance to nearest motorway / primary road / rail line")
    ax.legend()
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "50_noise_proxy.png", dpi=130)
    plt.close()

    con.close()


# ---------------------------------------------------------------------------
# §D — embedding matrix
# ---------------------------------------------------------------------------

def section_D_embeddings(stats: dict) -> None:
    matrix_path = ROOT / "data" / "ranking" / "embeddings.fp16.npy"
    ids_path = ROOT / "data" / "ranking" / "embeddings_ids.json"
    if not matrix_path.exists():
        stats["section_D_embeddings"] = {"present": False}
        return
    mat = np.load(matrix_path)
    ids = json.loads(ids_path.read_text())
    info = {
        "present":           True,
        "shape":             list(mat.shape),
        "dtype":             str(mat.dtype),
        "size_mb":           round(mat.nbytes / 1024 / 1024, 1),
        "ids_len":           len(ids),
        "l2_norm_min":       float(np.linalg.norm(mat.astype(np.float32), axis=1).min()),
        "l2_norm_max":       float(np.linalg.norm(mat.astype(np.float32), axis=1).max()),
    }
    stats["section_D_embeddings"] = info

    # PLOT D-a: L2 norm histogram (should be ~1.0 for unit-normalised)
    norms = np.linalg.norm(mat.astype(np.float32), axis=1)
    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.hist(norms, bins=60, color="#8E44AD", alpha=0.85)
    ax.set_xlabel("L2 norm")
    ax.set_ylabel("vectors")
    ax.set_title(
        f"Embedding matrix L2 norms — should all be ≈ 1.0 (unit-normalised)\n"
        f"min = {norms.min():.4f}  max = {norms.max():.4f}  n = {len(norms):,}")
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "51_embedding_l2_norms.png", dpi=130)
    plt.close()


# ---------------------------------------------------------------------------
# §E — commute matrix (the showstopper)
# ---------------------------------------------------------------------------

def section_E_commute(stats: dict) -> None:
    con = _con()
    stats["section_E_commute"] = {}

    # E.1 headline numbers
    row = con.execute(
        "SELECT COUNT(*), COUNT(travel_min), COUNT(DISTINCT listing_id), "
        "       COUNT(DISTINCT landmark_key), MIN(travel_min), MAX(travel_min), "
        "       AVG(travel_min) "
        "FROM listing_commute_times").fetchone()
    headline = {
        "total_rows":        row[0],
        "non_null":          row[1],
        "unique_listings":   row[2],
        "unique_landmarks":  row[3],
        "travel_min_min":    row[4],
        "travel_min_max":    row[5],
        "travel_min_avg":    float(row[6] or 0),
    }
    stats["section_E_commute"]["headline"] = headline

    # E.2 coverage per landmark
    lm_cov_rows = con.execute(
        "SELECT landmark_key, COUNT(*) n, AVG(travel_min) avg_m "
        "FROM listing_commute_times WHERE travel_min IS NOT NULL "
        "GROUP BY landmark_key ORDER BY n DESC").fetchall()
    lm_cov = [(r["landmark_key"], r["n"], float(r["avg_m"] or 0)) for r in lm_cov_rows]
    stats["section_E_commute"]["coverage_per_landmark"] = {
        k: {"reachable": n, "avg_min": avg} for (k, n, avg) in lm_cov
    }

    # PLOT E-a: all landmarks ranked by reachable-listings
    df = pd.DataFrame(lm_cov, columns=["landmark", "reachable", "avg_min"])
    df = df.sort_values("reachable", ascending=True)

    fig, ax = plt.subplots(figsize=(12, 13))
    y = np.arange(len(df))
    # colour-code by reachable-count bucket
    colours = ["#C0392B" if v < 100 else "#F39C12" if v < 1000 else "#27AE60"
               for v in df["reachable"]]
    bars = ax.barh(y, df["reachable"], color=colours)
    for bar, v in zip(bars, df["reachable"]):
        ax.text(v + max(df["reachable"]) * 0.005,
                bar.get_y() + bar.get_height() / 2,
                f"{v:,}", va="center", fontsize=9)
    ax.set_yticks(y)
    ax.set_yticklabels(df["landmark"], fontsize=9)
    ax.set_xlabel("listings reachable within 90 min by transit")
    ax.set_title(f"Commute-matrix landmark coverage  "
                 f"(45 landmarks, {headline['non_null']:,} total reachable pairs)")
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "52_landmark_coverage.png", dpi=130)
    plt.close()

    # PLOT E-b: travel-time histogram
    tt = np.array([r[0] for r in con.execute(
        "SELECT travel_min FROM listing_commute_times WHERE travel_min IS NOT NULL"
    ).fetchall()])
    fig, ax = plt.subplots(figsize=(11, 5.5))
    ax.hist(tt, bins=45, color="#27AE60", alpha=0.85)
    for pct_name, pct_val in (("p25", np.percentile(tt, 25)),
                               ("p50", np.percentile(tt, 50)),
                               ("p75", np.percentile(tt, 75))):
        ax.axvline(pct_val, color="#2C3E50", ls="--", lw=1.2, alpha=0.7)
        ax.text(pct_val + 0.5, ax.get_ylim()[1] * 0.92,
                f"{pct_name} = {pct_val:.0f} min", fontsize=9, color="#2C3E50")
    ax.set_xlabel("travel_min  (door-to-door by public transport, Tue 08:00)")
    ax.set_ylabel("(listing, landmark) pairs")
    ax.set_title(f"Transit travel-time distribution across the whole matrix\n"
                 f"{len(tt):,} pairs with a reachable time; capped at 90 min")
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "53_travel_time_distribution.png", dpi=130)
    plt.close()

    # PLOT E-c: 'isochrones' — number of listings within X min for 6 major landmarks
    key_landmarks = ["hb_zurich", "eth_zentrum", "hb_geneve", "epfl",
                     "hb_bern", "hb_basel"]
    fig, ax = plt.subplots(figsize=(11, 6))
    for lm in key_landmarks:
        tt = np.array([r[0] for r in con.execute(
            "SELECT travel_min FROM listing_commute_times "
            "WHERE landmark_key=? AND travel_min IS NOT NULL ORDER BY travel_min",
            (lm,)).fetchall()])
        if len(tt) == 0:
            continue
        minutes = np.arange(1, 91)
        cumulative = np.searchsorted(tt, minutes, side="right")
        ax.plot(minutes, cumulative, lw=2, label=lm, alpha=0.85)
    ax.set_xlabel("minutes by public transport (cumulative)")
    ax.set_ylabel("listings reachable within X min")
    ax.set_title("Isochrone-like curves — how many listings sit within a given commute\n"
                 "to each major Swiss landmark (Tuesday 08:00 departure)")
    ax.legend(loc="lower right", fontsize=10)
    ax.set_xlim(0, 90)
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "54_isochrone_curves.png", dpi=130)
    plt.close()

    # E.3 anomalies: travel_min > 90
    anomalies = con.execute(
        "SELECT COUNT(*) FROM listing_commute_times WHERE travel_min > 90"
    ).fetchone()[0]
    stats["section_E_commute"]["anomalies_gt_90"] = anomalies

    con.close()


# ---------------------------------------------------------------------------
# §F — landmarks map
# ---------------------------------------------------------------------------

def section_F_landmarks_map(stats: dict) -> None:
    lm_path = ROOT / "data" / "ranking" / "landmarks.json"
    lm = json.loads(lm_path.read_text())
    stats["section_F_landmarks"] = {
        "count":   len(lm),
        "by_kind": dict(Counter(e["kind"] for e in lm)),
    }

    # PLOT F-a: landmarks scatter on Swiss map bbox
    # Also plot listings as background
    con = _con()
    listings_coords = np.array(con.execute(
        "SELECT latitude, longitude FROM listings "
        "WHERE latitude IS NOT NULL AND longitude IS NOT NULL "
        "AND latitude BETWEEN 45.7 AND 47.9 AND longitude BETWEEN 5.8 AND 10.6"
    ).fetchall())
    con.close()

    fig, ax = plt.subplots(figsize=(12, 8))
    ax.scatter(listings_coords[:, 1], listings_coords[:, 0],
               s=3, alpha=0.15, color="#7F8C8D", label=f"listings ({len(listings_coords):,})")
    kind_colours = {
        "university":   "#8E44AD",
        "transit":      "#2980B9",
        "lake":         "#16A085",
        "oldtown":      "#C0392B",
        "employer":     "#F39C12",
        "neighborhood": "#27AE60",
        "cultural":     "#E67E22",
    }
    for kind, colour in kind_colours.items():
        pts = [(e["lat"], e["lon"]) for e in lm if e["kind"] == kind]
        if not pts:
            continue
        xs = [p[1] for p in pts]
        ys = [p[0] for p in pts]
        ax.scatter(xs, ys, s=140, color=colour, edgecolor="black",
                   linewidth=0.6, label=f"{kind} ({len(pts)})", alpha=0.95, zorder=5)
    ax.set_xlim(5.8, 10.6)
    ax.set_ylim(45.7, 47.9)
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")
    ax.set_title(f"Switzerland — 45 landmarks overlaid on {len(listings_coords):,} listing locations")
    ax.legend(loc="upper right", fontsize=9, framealpha=0.95)
    ax.set_aspect("equal")
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "55_landmarks_map.png", dpi=130)
    plt.close()


# ---------------------------------------------------------------------------
# §G — before/after headline comparison
# ---------------------------------------------------------------------------

def section_G_before_after(stats: dict) -> None:
    con = _con()

    # Load v1 stats.json so we can diff against it
    v1_stats_path = ROOT / "analysis" / "data" / "stats.json"
    if v1_stats_path.exists():
        v1 = json.loads(v1_stats_path.read_text())
    else:
        v1 = {}

    # Pick ~9 high-level KPIs and make a before/after bar
    # We compare v1 raw-coverage (from the raw CSV columns via listings) vs enriched
    # (raw_col, raw_table, enriched_col). Some enriched fields have no direct
    # counterpart in `listings` (floor, year_built) — for those, "raw coverage"
    # means 0% and we surface the enrichment lift explicitly.
    fields_to_compare = [
        ("canton",          "listings", "canton"),
        ("postal_code",     "listings", "postal_code"),
        ("street",          "listings", "street"),
        ("feature_balcony", "listings", "feature_balcony"),
        ("feature_parking", "listings", "feature_parking"),
        ("feature_elevator","listings", "feature_elevator"),
        (None,              None,        "floor"),        # not in raw table
        (None,              None,        "year_built"),   # not in raw table
        ("available_from",  "listings", "available_from"),
    ]
    raw_pct, enriched_pct, labels = [], [], []
    for src_col, src_tbl, enriched_col in fields_to_compare:
        if src_col is None:
            raw = 0
        else:
            raw = con.execute(
                f"SELECT COUNT(*) FROM {src_tbl} "
                f"WHERE {src_col} IS NOT NULL AND {src_col} != ''"
            ).fetchone()[0]
        enr = con.execute(
            f"SELECT COUNT(*) FROM listings_enriched "
            f"WHERE {enriched_col}_source NOT IN ('UNKNOWN','UNKNOWN-pending','DROPPED_bad_data')"
        ).fetchone()[0]
        raw_pct.append(100 * raw / 25546)
        enriched_pct.append(100 * enr / 25546)
        labels.append(enriched_col)

    # PLOT G-a: grouped bar — raw vs enriched coverage
    fig, ax = plt.subplots(figsize=(13, 6.5))
    y = np.arange(len(labels))
    ax.barh(y, raw_pct, color="#95A5A6", label="raw CSV", height=0.35)
    ax.barh(y + 0.4, enriched_pct, color="#27AE60", label="after enrichment", height=0.35)
    for i, (r, e) in enumerate(zip(raw_pct, enriched_pct)):
        ax.text(r + 0.5, i - 0.05, f"{r:.1f}%", fontsize=9, color="#2C3E50")
        ax.text(e + 0.5, i + 0.4 - 0.05, f"{e:.1f}%", fontsize=9, color="#27AE60",
                weight="bold")
    ax.set_yticks(y + 0.2)
    ax.set_yticklabels(labels, fontsize=10)
    ax.set_xlim(0, 108)
    ax.set_xlabel("% of 25,546 listings with a real value")
    ax.set_title("Before / After — raw-CSV column coverage vs final enriched coverage")
    ax.legend(loc="lower right")
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "56_before_after_coverage.png", dpi=130)
    plt.close()

    stats["section_G_before_after"] = {
        "fields":       labels,
        "raw_pct":      raw_pct,
        "enriched_pct": enriched_pct,
    }

    # PLOT G-b: filter answerability — can we answer these queries now?
    # Each query listed with what % of 25,546 listings it can be evaluated on.
    #
    # Q patterns drawn from long_queries.md + queries_de.md.
    questions = [
        ("canton = ZH",                              "canton_filled NOT IN ('UNKNOWN')",        "canton from enriched"),
        ("postal_code in district",                  "postal_code_filled NOT IN ('UNKNOWN')",   "postal_code from enriched"),
        ("has balcony",                              "feature_balcony_filled IS NOT NULL AND feature_balcony_source != 'UNKNOWN'", "feature_balcony"),
        ("year_built > 2000",                        "year_built_filled NOT IN ('UNKNOWN')",    "year_built"),
        ("< 300 m to transit",                       "listing_id IN (SELECT listing_id FROM listings_ranking_signals WHERE dist_nearest_stop_m IS NOT NULL)", "GTFS signal"),
        ("≥ 3 schools within 1 km",                  "listing_id IN (SELECT listing_id FROM listings_ranking_signals WHERE poi_school_1km IS NOT NULL)", "POI signal"),
        ("≤ 25 min to HB Zurich",                    "listing_id IN (SELECT listing_id FROM listing_commute_times WHERE landmark_key='hb_zurich' AND travel_min IS NOT NULL)", "commute matrix"),
        ("≤ 25 min to EPFL",                         "listing_id IN (SELECT listing_id FROM listing_commute_times WHERE landmark_key='epfl' AND travel_min IS NOT NULL)", "commute matrix"),
        ("semantic 'modern bright apartment'",        "listing_id IN (SELECT listing_id FROM listings_ranking_signals WHERE embedding_row_index IS NOT NULL)", "embedding vector"),
    ]
    res = []
    for label, sql, _ in questions:
        n = con.execute(
            f"SELECT COUNT(*) FROM listings_enriched WHERE {sql}"
        ).fetchone()[0]
        res.append((label, n))

    fig, ax = plt.subplots(figsize=(12, 6.5))
    labels_q = [r[0] for r in res]
    vals_q = [100 * r[1] / 25546 for r in res]
    colours = ["#27AE60" if v >= 80 else "#F39C12" if v >= 40 else "#C0392B" for v in vals_q]
    y = np.arange(len(labels_q))
    bars = ax.barh(y, vals_q, color=colours)
    for bar, v, n in zip(bars, vals_q, [r[1] for r in res]):
        ax.text(v + 1, bar.get_y() + bar.get_height() / 2,
                f"{v:.1f}% ({n:,})", va="center", fontsize=9)
    ax.set_yticks(y)
    ax.set_yticklabels(labels_q, fontsize=10)
    ax.set_xlim(0, 112)
    ax.set_xlabel("% of 25,546 listings that this query can be answered on")
    ax.set_title("Query answerability — every query type the ranker needs to support")
    ax.invert_yaxis()
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "57_query_answerability.png", dpi=130)
    plt.close()

    stats["section_G_before_after"]["answerability"] = {
        r[0]: r[1] for r in res
    }

    con.close()


# ---------------------------------------------------------------------------
# §H — price-plausibility + lines_log (the hardening columns)
# ---------------------------------------------------------------------------

def section_H_hardening(stats: dict) -> None:
    con = _con()

    # price_plausibility distribution
    rows = con.execute(
        "SELECT price_plausibility, COUNT(*) FROM listings_ranking_signals "
        "GROUP BY price_plausibility"
    ).fetchall()
    plaus = {(r[0] or "NULL"): r[1] for r in rows}
    stats.setdefault("section_H_hardening", {})["price_plausibility"] = plaus

    # nearest_stop_lines_count vs lines_log — show why log was needed
    raw = np.array([r[0] for r in con.execute(
        "SELECT nearest_stop_lines_count FROM listings_ranking_signals "
        "WHERE nearest_stop_lines_count IS NOT NULL").fetchall()])
    log = np.array([r[0] for r in con.execute(
        "SELECT nearest_stop_lines_log FROM listings_ranking_signals "
        "WHERE nearest_stop_lines_log IS NOT NULL").fetchall()])

    fig, axs = plt.subplots(1, 2, figsize=(14, 5.5))
    axs[0].hist(raw, bins=80, color="#C0392B", alpha=0.85)
    axs[0].set_yscale("log")
    axs[0].set_xlabel("nearest_stop_lines_count (raw)")
    axs[0].set_title(f"Raw — max = {raw.max():,}  (Cornavin is an outlier)")
    axs[1].hist(log, bins=50, color="#27AE60", alpha=0.85)
    axs[1].set_xlabel("nearest_stop_lines_log  = ln(1 + count)")
    axs[1].set_title(f"Log-transformed — bounded and stable for ranking")
    fig.suptitle(
        "Why we added nearest_stop_lines_log: raw counts have a fat tail with outliers "
        "(Cornavin reports ~41 k distinct GTFS route_ids).", y=1.02, fontsize=12)
    plt.tight_layout()
    plt.savefig(OUT_PLOTS / "58_lines_count_vs_log.png", dpi=130)
    plt.close()

    con.close()


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

def main() -> int:
    stats: dict = {}
    print("[profile_v2] running — this takes ~30-60 s", flush=True)
    for name, fn in [
        ("§A corpus",           section_A_corpus),
        ("§B Layer 1",          section_B_layer1),
        ("§C Layer 2 signals",  section_C_layer2_signals),
        ("§D embeddings",       section_D_embeddings),
        ("§E commute matrix",   section_E_commute),
        ("§F landmarks map",    section_F_landmarks_map),
        ("§G before/after",     section_G_before_after),
        ("§H hardening",        section_H_hardening),
    ]:
        print(f"  {name} …", flush=True)
        fn(stats)

    out = OUT_DATA / "stats_v2.json"
    out.write_text(json.dumps(stats, indent=2, ensure_ascii=False, default=float))
    plots = sorted(OUT_PLOTS.glob("*.png"))
    print(f"[profile_v2] wrote {out} + {len(plots)} plots", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
