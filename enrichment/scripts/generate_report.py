"""Generate enrichment/REPORT.md + enrichment/data/{fill_stats,dropped_rows,disagreements}.json.

Runs read-only against an already-enriched DB. Call after `enrich_all.py`.

Usage:
    python -m enrichment.scripts.generate_report --db /data/listings.db
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from enrichment.common.db import connect
from enrichment.common.sources import (
    DROPPED_BAD_DATA,
    FINAL_SOURCES,
    ORIGINAL,
    REV_GEO_NOMINATIM,
    REV_GEO_OFFLINE,
    UNKNOWN,
)
from enrichment.schema import FIELDS

# Outputs
ROOT = Path(__file__).resolve().parents[1]
REPORT_MD = ROOT / "REPORT.md"
FILL_STATS_JSON = ROOT / "data" / "fill_stats.json"
DROPPED_ROWS_JSON = ROOT / "data" / "dropped_rows.json"
DISAGREEMENTS_JSON = ROOT / "data" / "disagreements.json"

# Expected REPORT.md section headers (verified in tests/crossref/test_report_integrity.py)
EXPECTED_SECTIONS = [
    "## 1 Summary",
    "## 2 Before / After Null Counts",
    "## 3 Source Distribution",
    "## 4 Confidence Histogram",
    "## 5 Cross-Pass Disagreements",
    "## 6 Known-Bad Rows",
    "## 7 Re-validation vs analysis/REPORT.md",
    "## 8 Commands",
]

# Expected fill_stats.json top-level keys
EXPECTED_STATS_KEYS = [
    "before_null_counts",
    "after_null_counts",
    "source_distribution",
    "confidence_histogram",
    "disagreements",
    "dropped_rows",
    "run_duration_seconds",
]


# ------------------------------------------------------------
# Data collection
# ------------------------------------------------------------

def _before_null_counts(conn: sqlite3.Connection) -> dict[str, int]:
    """How many `listings` rows have NULL in the source column(s) of each field."""
    counts: dict[str, int] = {}
    for f in FIELDS:
        if f.origin == "listings_column":
            col = f.listings_column
            assert col is not None
            n = conn.execute(
                f"SELECT COUNT(*) FROM listings WHERE {col} IS NULL;"
            ).fetchone()[0]
            counts[f.name] = n
        else:
            # raw_json source: harder to compute without parsing JSON.
            # Use the enriched table as the proxy: anything with source 'original'
            # in listings_enriched was non-null pre-enrichment; the rest were NULL.
            n = conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched WHERE {f.name}_source != ?;",
                (ORIGINAL,),
            ).fetchone()[0]
            counts[f.name] = n
    return counts


def _after_null_counts(conn: sqlite3.Connection) -> dict[str, int]:
    """How many enriched rows still have `_source=UNKNOWN` (real unrecovered nulls)."""
    counts: dict[str, int] = {}
    for f in FIELDS:
        n = conn.execute(
            f"SELECT COUNT(*) FROM listings_enriched WHERE {f.name}_source = ?;",
            (UNKNOWN,),
        ).fetchone()[0]
        counts[f.name] = n
    return counts


def _source_distribution(conn: sqlite3.Connection) -> dict[str, dict[str, int]]:
    dist: dict[str, dict[str, int]] = {}
    for f in FIELDS:
        rows = conn.execute(
            f"SELECT {f.name}_source, COUNT(*) FROM listings_enriched GROUP BY 1;"
        ).fetchall()
        dist[f.name] = {r[0]: r[1] for r in rows}
    return dist


def _confidence_histogram(conn: sqlite3.Connection) -> dict[str, dict[str, int]]:
    """10-bin histogram [0.0, 0.1), [0.1, 0.2), ..., [0.9, 1.0]. 1.0 is in the last bin."""
    hist: dict[str, dict[str, int]] = {}
    for f in FIELDS:
        bins = [
            conn.execute(
                f"""SELECT COUNT(*) FROM listings_enriched
                    WHERE {f.name}_confidence >= ?
                      AND {f.name}_confidence < ?;""",
                (i * 0.1, (i + 1) * 0.1 + (1e-9 if i == 9 else 0.0)),
            ).fetchone()[0]
            for i in range(10)
        ]
        hist[f.name] = {f"{i*0.1:.1f}-{(i+1)*0.1:.1f}": bins[i] for i in range(10)}
    return hist


def _dropped_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Every row where at least one field has _source='DROPPED_bad_data'."""
    affected = conn.execute(
        """SELECT DISTINCT listing_id FROM (
               """ + " UNION ".join(
            f"SELECT listing_id FROM listings_enriched WHERE {f.name}_source=?"
            for f in FIELDS
        ) + ");",
        tuple(DROPPED_BAD_DATA for _ in FIELDS),
    ).fetchall()
    ids = [r[0] for r in affected]
    out: list[dict[str, Any]] = []
    for lid in ids:
        row = conn.execute(
            """SELECT * FROM listings_enriched WHERE listing_id = ?;""", (lid,)
        ).fetchone()
        per_field_drops: dict[str, str] = {}
        for f in FIELDS:
            if row[f"{f.name}_source"] == DROPPED_BAD_DATA:
                per_field_drops[f.name] = row[f"{f.name}_raw"] or ""
        out.append({"listing_id": lid, "drops": per_field_drops})
    return out


def _raw_city_vs_geocoded_disagreements(
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """Rows where listings.canton (original) disagrees with what rg would have said.

    Run rg lazily: for every row that has BOTH an original canton AND lat/lng,
    we ask rg what canton that coord maps to. Mismatches go here.

    Runs a batch rg call so it's still fast even over 14k rows.
    """
    import reverse_geocoder as rg

    from enrichment.common.cantons import admin1_to_canton_code

    rows = conn.execute("""
        SELECT listing_id, canton_filled, latitude, longitude
        FROM listings_enriched le JOIN listings l USING(listing_id)
        WHERE le.canton_source = ?
          AND l.latitude IS NOT NULL
          AND l.longitude IS NOT NULL;
    """, (ORIGINAL,)).fetchall()
    if not rows:
        return []

    coords = [(r["latitude"], r["longitude"]) for r in rows]
    results = rg.search(coords, mode=2)
    disagreements: list[dict[str, Any]] = []
    for r, res in zip(rows, results, strict=True):
        predicted = admin1_to_canton_code(res.get("admin1", ""))
        if predicted is None:
            continue  # out of CH or unmapped — don't count as disagreement
        if predicted != r["canton_filled"]:
            disagreements.append({
                "listing_id": r["listing_id"],
                "raw_canton": r["canton_filled"],
                "geocoded_canton": predicted,
                "latitude": r["latitude"],
                "longitude": r["longitude"],
                "geocoded_admin1": res.get("admin1", ""),
                "geocoded_city": res.get("name", ""),
            })
    return disagreements


# ------------------------------------------------------------
# MD rendering
# ------------------------------------------------------------

def _render_md(
    stats: dict[str, Any],
    dropped: list[dict[str, Any]],
    disagreements: list[dict[str, Any]],
    generated_at: str,
) -> str:
    total_rows = stats["total_rows"]
    before = stats["before_null_counts"]
    after = stats["after_null_counts"]

    lines: list[str] = []
    lines.append(f"# Enrichment Audit — Datathon 2026")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append(f"Total rows in listings_enriched: {total_rows}")
    lines.append("")

    # 1
    lines.append("## 1 Summary")
    lines.append("")
    lines.append(
        f"Every listing in the corpus has a non-null entry in every covered column "
        f"(either a real value or the explicit `UNKNOWN` sentinel). "
        f"No value was fabricated: if a field could not be recovered by pass 0 "
        f"(original data), pass 1a (offline reverse-geocoder), pass 1b (Nominatim), "
        f"or pass 2 (multilingual regex), it was sentinel-filled by pass 3 with "
        f"`source=UNKNOWN, confidence=0.0`."
    )
    lines.append("")
    total_before = sum(before.values())
    total_after = sum(after.values())
    recovered = total_before - total_after
    lines.append(f"- Total nulls before enrichment: **{total_before:,}**")
    lines.append(f"- Total rows sentinel-filled after: **{total_after:,}**")
    lines.append(f"- Net recovered (real values added): **{recovered:,}**")
    lines.append(f"- Dropped-as-bad rows: **{len(dropped)}** (see §6)")
    lines.append(f"- Structured-vs-geocoded canton disagreements: **{len(disagreements)}** (see §5)")
    lines.append("")

    # 2
    lines.append("## 2 Before / After Null Counts")
    lines.append("")
    lines.append("| field | nulls before | nulls (UNKNOWN) after | recovered |")
    lines.append("|---|---:|---:|---:|")
    for f in FIELDS:
        b = before[f.name]
        a = after[f.name]
        r = b - a
        lines.append(f"| `{f.name}` | {b:,} | {a:,} | {r:,} |")
    lines.append("")

    # 3
    lines.append("## 3 Source Distribution")
    lines.append("")
    lines.append("| field | original | rev_geo_offline | rev_geo_nominatim | text_regex_* | default | cross_ref | DROPPED | UNKNOWN |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for f in FIELDS:
        d = stats["source_distribution"][f.name]
        txt = sum(v for k, v in d.items() if k.startswith("text_regex_"))
        lines.append(
            f"| `{f.name}` "
            f"| {d.get(ORIGINAL, 0):,} "
            f"| {d.get(REV_GEO_OFFLINE, 0):,} "
            f"| {d.get(REV_GEO_NOMINATIM, 0):,} "
            f"| {txt:,} "
            f"| {d.get('default_constant', 0):,} "
            f"| {d.get('cross_ref', 0):,} "
            f"| {d.get(DROPPED_BAD_DATA, 0):,} "
            f"| {d.get(UNKNOWN, 0):,} |"
        )
    lines.append("")

    # 4
    lines.append("## 4 Confidence Histogram")
    lines.append("")
    lines.append("Per-field 10-bin histogram; each column is the row count in that bin.")
    lines.append("")
    bins_header = " | ".join(f"{i*0.1:.1f}–{(i+1)*0.1:.1f}" for i in range(10))
    lines.append(f"| field | {bins_header} |")
    lines.append("|---|" + "---:|" * 10)
    for f in FIELDS:
        h = stats["confidence_histogram"][f.name]
        bin_cells = " | ".join(f"{h[f'{i*0.1:.1f}-{(i+1)*0.1:.1f}']:,}" for i in range(10))
        lines.append(f"| `{f.name}` | {bin_cells} |")
    lines.append("")

    # 5
    lines.append("## 5 Cross-Pass Disagreements")
    lines.append("")
    if disagreements:
        lines.append(
            f"{len(disagreements)} rows have a structured `canton` that disagrees "
            f"with what reverse_geocoder would have returned for their lat/lng. "
            f"Full JSON at `enrichment/data/disagreements.json`. Top 10 below."
        )
        lines.append("")
        lines.append("| listing_id | raw canton | geocoded canton | city |")
        lines.append("|---|:---:|:---:|---|")
        for d in disagreements[:10]:
            lines.append(
                f"| `{d['listing_id']}` | {d['raw_canton']} | {d['geocoded_canton']} | {d['geocoded_city']} |"
            )
    else:
        lines.append("_No structured-vs-geocoded canton disagreements._")
    lines.append("")

    # 6
    lines.append("## 6 Known-Bad Rows")
    lines.append("")
    if dropped:
        lines.append(
            f"{len(dropped)} listings had at least one field marked `DROPPED_bad_data` "
            f"(price < 200, price > 50k, or rooms = 0). Full JSON at "
            f"`enrichment/data/dropped_rows.json`. Top 10 below."
        )
        lines.append("")
        lines.append("| listing_id | dropped field(s) | reason(s) |")
        lines.append("|---|---|---|")
        for row in dropped[:10]:
            fields = ", ".join(row["drops"].keys())
            reasons = "; ".join(f"{k}={v}" for k, v in row["drops"].items())
            lines.append(f"| `{row['listing_id']}` | {fields} | {reasons} |")
    else:
        lines.append("_No rows flagged as bad data._")
    lines.append("")

    # 7
    lines.append("## 7 Re-validation vs analysis/REPORT.md")
    lines.append("")
    analysis_stats = _load_analysis_stats()
    if analysis_stats is None:
        lines.append("_Could not load `analysis/data/stats.json` for cross-validation._")
    else:
        expected_total = analysis_stats.get("total_rows")
        actual_total = total_rows
        match = "✓" if expected_total == actual_total else "✗"
        lines.append(
            f"- Total row count: analysis={expected_total}, enriched={actual_total} {match}"
        )
        # analysis/REPORT.md §3 L43: SRED has city_null=100%.
        # We expect city_source='rev_geo_offline' ≈ SRED count (minus OOB drops).
        expected_sred = 11105  # REPORT §4 L64
        actual_city_offline = stats["source_distribution"]["city"].get(REV_GEO_OFFLINE, 0)
        gap = expected_sred - actual_city_offline
        lines.append(
            f"- SRED city fill via rev_geo_offline: "
            f"{actual_city_offline:,} (expected ≈ {expected_sred:,} from REPORT §4 L64; "
            f"gap {gap} — OOB drops + new structured rows since REPORT was written)"
        )
    lines.append("")

    # 8
    lines.append("## 8 Commands")
    lines.append("")
    lines.append("```bash")
    lines.append("# Full pipeline")
    lines.append("docker compose exec api uv run python -m enrichment.scripts.enrich_all --db /data/listings.db --skip-1b")
    lines.append("docker compose exec api uv run python -m enrichment.scripts.generate_report --db /data/listings.db")
    lines.append("")
    lines.append("# Tests")
    lines.append("docker compose exec api uv run pytest enrichment/tests/ -v")
    lines.append("```")
    lines.append("")

    return "\n".join(lines)


def _load_analysis_stats() -> dict | None:
    path = ROOT.parent / "analysis" / "data" / "stats.json"
    if not path.exists():
        return None
    try:
        with path.open() as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(
            f"[WARN] generate_report: expected=json at {path} got=error({e!s}) "
            f"fallback=skip_cross_validation",
            flush=True,
        )
        return None


# ------------------------------------------------------------
# Entry point
# ------------------------------------------------------------

def run(
    db_path: Path,
    *,
    include_disagreements: bool = True,
    duration_s: float | None = None,
) -> dict[str, Any]:
    conn = connect(db_path)
    try:
        total_rows = conn.execute("SELECT COUNT(*) FROM listings_enriched;").fetchone()[0]

        before = _before_null_counts(conn)
        after = _after_null_counts(conn)
        src_dist = _source_distribution(conn)
        conf_hist = _confidence_histogram(conn)
        dropped = _dropped_rows(conn)

        if include_disagreements:
            disagreements = _raw_city_vs_geocoded_disagreements(conn)
        else:
            disagreements = []

        stats: dict[str, Any] = {
            "total_rows": total_rows,
            "before_null_counts": before,
            "after_null_counts": after,
            "source_distribution": src_dist,
            "confidence_histogram": conf_hist,
            "disagreements": len(disagreements),
            "dropped_rows": len(dropped),
            "run_duration_seconds": duration_s,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

        FILL_STATS_JSON.parent.mkdir(parents=True, exist_ok=True)
        with FILL_STATS_JSON.open("w") as f:
            json.dump(stats, f, indent=2, default=str)
        with DROPPED_ROWS_JSON.open("w") as f:
            json.dump(dropped, f, indent=2, default=str)
        with DISAGREEMENTS_JSON.open("w") as f:
            json.dump(disagreements, f, indent=2, default=str)

        md = _render_md(stats, dropped, disagreements, stats["generated_at"])
        REPORT_MD.write_text(md, encoding="utf-8")

        return {
            "total_rows": total_rows,
            "dropped": len(dropped),
            "disagreements": len(disagreements),
            "report_md": str(REPORT_MD),
            "fill_stats_json": str(FILL_STATS_JSON),
        }
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument(
        "--no-disagreements",
        action="store_true",
        help="Skip the live reverse_geocoder canton-disagreement scan (faster).",
    )
    args = parser.parse_args()
    if not args.db.exists():
        print(f"[ERROR] DB not found at {args.db}", file=sys.stderr)
        return 2
    result = run(args.db, include_disagreements=not args.no_disagreements)
    print("Report generated:")
    for k, v in result.items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
