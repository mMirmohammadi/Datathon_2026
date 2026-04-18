"""Produce FINAL_REPORT.md combining: run summary + verification + query top-20.

Inputs (read from the full-run output dir):
    store/verification.json      — from verify_results.py
    summary.json                 — from run_full.py
    queries.json                 — from query.py with --out

Output:
    FINAL_REPORT.md alongside them.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _table(rows: list[dict], cols: list[str]) -> str:
    lines = ["| " + " | ".join(cols) + " |",
             "|" + "|".join(["---"] * len(cols)) + "|"]
    for r in rows:
        lines.append("| " + " | ".join(str(r.get(c, "")) for c in cols) + " |")
    return "\n".join(lines)


def make(out_dir: Path, report_path: Path) -> int:
    summary = json.loads((out_dir / "summary.json").read_text())
    verif = json.loads((out_dir / "store" / "verification.json").read_text())
    queries_path = out_dir / "queries.json"
    queries = json.loads(queries_path.read_text()) if queries_path.exists() else []

    lines = [
        "# image_search — full-run FINAL REPORT",
        "",
        f"- model: `{summary['model_id']}`",
        f"- processed items: **{summary['n_processed']:,}**",
        f"- duration: **{summary['duration_s']} s**  "
        f"({summary['rate_img_per_s']} img/s)",
        f"- warnings (NaN/drops): **{summary['warnings_count']}**",
        "",
        "## Pipeline outcome",
        "",
        f"- KEPT (main index):      **{summary['kept_main']:,}**",
        f"- KEPT (floorplan index): **{summary['kept_floorplan']:,}**",
        f"- DROPPED:                **{summary['dropped']:,}**",
        "",
        "### Per-source counts",
        "",
        _table(
            [{"source": k, "count": v} for k, v in summary["per_source"].items()],
            ["source", "count"]),
        "",
        "### Per-label counts",
        "",
        _table(
            [{"label": k, "count": v} for k, v in
             sorted(summary["label_dist"].items(), key=lambda kv: -kv[1])],
            ["label", "count"]),
        "",
        "## Verification (cross-checks)",
        "",
        f"- main embeddings shape: `{verif['main_shape']}`",
        f"- floorplan embeddings shape: `{verif['floor_shape']}`",
        "",
        "### Passed checks",
        "",
    ]
    for p in verif["passes"]:
        lines.append(f"- ✅ {p}")
    if verif["failures"]:
        lines.append("")
        lines.append("### FAILURES")
        lines.append("")
        for f in verif["failures"]:
            lines.append(f"- ❌ {f}")

    lines.extend(["", "## Target-query results (top-k per query)", ""])
    for qr in queries:
        lines.append(f"### `{qr['query']}`")
        lines.append("")
        lines.append(_table(
            [{"rank": r["rank"], "sim": f"{r['sim']:.4f}",
              "source": r["source"], "platform_id": r["platform_id"],
              "image_id": r["image_id"], "label": r["label"]}
             for r in qr["top_k"]],
            ["rank", "sim", "source", "platform_id", "label"],
        ))
        lines.append("")

    report_path.write_text("\n".join(lines) + "\n")
    print(f"[OUT] wrote {report_path}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out-dir", type=Path, required=True,
                    help="Full-run output dir containing summary.json + store/ + queries.json")
    ap.add_argument("--report", type=Path, default=None,
                    help="Path for FINAL_REPORT.md (default: <out-dir>/FINAL_REPORT.md)")
    args = ap.parse_args()
    report_path = args.report or (args.out_dir / "FINAL_REPORT.md")
    return make(args.out_dir, report_path)


if __name__ == "__main__":
    sys.exit(main())
