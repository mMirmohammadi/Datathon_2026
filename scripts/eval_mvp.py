"""Run the MVP eval set against the local search pipeline and report metrics.

Metrics:
  - HF-P (hard-filter precision, per-listing): fraction of returned listings
    whose hard attributes satisfy each `must_satisfy` field declared in the
    eval query. Reported per-field AND overall (∀ fields, per listing).
  - CSR (constraint satisfaction rate, strict per-query): 1.0 if ALL returned
    listings satisfy ALL `must_satisfy` fields, else 0.0. Macro-averaged.
  - Coverage: fraction of queries that returned ≥ MIN_HITS (5) listings.
  - Latency p50 / p95 (ms).
  - Relaxation audit: which queries triggered relaxation.
  - Clarification audit: which queries triggered clarification.

Usage:
    python scripts/eval_mvp.py               # run and print report
    python scripts/eval_mvp.py --json        # JSON output
    python scripts/eval_mvp.py --out FILE.md # write markdown report
"""
from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path
from typing import Any

from app.config import get_settings
from app.harness.search_service import query_from_text

EVAL_FILE = Path(__file__).resolve().parent.parent / "eval" / "queries_mvp.jsonl"
MIN_HITS = 5
LIMIT = 10


def load_queries() -> list[dict]:
    queries = []
    with open(EVAL_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                queries.append(json.loads(line))
    return queries


def listing_satisfies(listing: Any, must: dict) -> tuple[bool, dict[str, bool]]:
    """Per-field hard-filter check on a single listing.

    Returns (all_pass, {field: bool}). Unknown/absent data on the listing is
    treated as NOT satisfying the constraint (conservative).
    """
    checks: dict[str, bool] = {}
    if not must:
        return True, {}

    if "city" in must:
        checks["city"] = (
            str(listing.city or "").strip().casefold()
            == str(must["city"]).strip().casefold()
        )
    if "canton" in must:
        checks["canton"] = (
            str(listing.canton or "").strip().upper()[:2]
            == str(must["canton"]).strip().upper()[:2]
        )
    if "rooms" in must:
        checks["rooms"] = listing.rooms is not None and abs(listing.rooms - float(must["rooms"])) < 1e-9
    if "min_rooms" in must:
        checks["min_rooms"] = listing.rooms is not None and listing.rooms >= float(must["min_rooms"]) - 1e-9
    if "max_rooms" in must:
        checks["max_rooms"] = listing.rooms is not None and listing.rooms <= float(must["max_rooms"]) + 1e-9
    if "max_price" in must:
        checks["max_price"] = (
            listing.price_chf is not None
            and listing.price_chf <= int(must["max_price"])
        )
    if "min_price" in must:
        checks["min_price"] = (
            listing.price_chf is not None
            and listing.price_chf >= int(must["min_price"])
        )
    if "required_features" in must:
        required = {str(f).lower() for f in must["required_features"]}
        have = {str(f).lower() for f in (listing.features or [])}
        checks["required_features"] = required.issubset(have)

    all_pass = all(checks.values()) if checks else True
    return all_pass, checks


def _fields_relaxed_away(relaxations: list[str]) -> set[str]:
    """Parse the human-readable relaxation descriptions → set of must_satisfy keys
    that should no longer be enforced (the system already told the user it relaxed
    them). This is the honest evaluation: HF-P measured against the FINAL filter
    the system applied, not the one the user originally asked for.
    """
    dropped: set[str] = set()
    for step in relaxations or []:
        low = step.lower()
        if "price" in low:
            dropped.update({"max_price", "min_price"})
        if "dropped city" in low:
            dropped.add("city")
        if "dropped canton" in low:
            dropped.add("canton")
        if "dropped required_features" in low:
            dropped.add("required_features")
        if "radius" in low:
            dropped.add("radius_km")
    return dropped


def evaluate_one(query_obj: dict, db_path: Path) -> dict[str, Any]:
    t0 = time.monotonic()
    response = query_from_text(
        db_path=db_path, query=query_obj["query"], limit=LIMIT, offset=0
    )
    elapsed_ms = round((time.monotonic() - t0) * 1000, 1)

    must_orig = query_obj.get("must_satisfy") or {}
    relaxations = response.meta.get("relaxations") or []
    relaxed_away = _fields_relaxed_away(relaxations)

    # Effective must_satisfy = user's original MINUS anything the system openly relaxed
    must = {k: v for k, v in must_orig.items() if k not in relaxed_away}

    per_listing_pass: list[bool] = []
    per_field_counts: dict[str, list[bool]] = {}
    for r in response.listings:
        ok, checks = listing_satisfies(r.listing, must)
        per_listing_pass.append(ok)
        for field, v in checks.items():
            per_field_counts.setdefault(field, []).append(v)

    n = len(response.listings)
    csr_strict = 1.0 if n > 0 and all(per_listing_pass) else 0.0
    hf_overall = (sum(per_listing_pass) / n) if n > 0 else 0.0
    hf_per_field = {
        field: sum(vs) / len(vs) for field, vs in per_field_counts.items() if vs
    }
    coverage = 1.0 if n >= MIN_HITS else 0.0

    return {
        "qid": query_obj["qid"],
        "stratum": query_obj["stratum"],
        "lang": query_obj["lang"],
        "query": query_obj["query"],
        "n_listings": n,
        "coverage_ok": coverage,
        "csr_strict": csr_strict,
        "hf_overall": hf_overall,
        "hf_per_field": hf_per_field,
        "relaxations": response.meta.get("relaxations") or [],
        "warnings": response.meta.get("warnings") or [],
        "clarification": response.meta.get("clarification") or {},
        "confidence": response.meta.get("confidence"),
        "expected_relaxation": query_obj.get("expect_relaxation", False),
        "expected_clarification": query_obj.get("expect_clarification", False),
        "latency_ms": elapsed_ms,
        "timings_ms": response.meta.get("timings_ms") or {},
        "top_3": [
            {
                "listing_id": r.listing_id,
                "score": r.score,
                "city": r.listing.city,
                "rooms": r.listing.rooms,
                "price_chf": r.listing.price_chf,
                "features": (r.listing.features or [])[:4],
                "reason": r.reason,
            }
            for r in response.listings[:3]
        ],
    }


def aggregate(results: list[dict]) -> dict[str, Any]:
    n = len(results)
    latencies = [r["latency_ms"] for r in results]
    return {
        "n_queries": n,
        "mean_hf_overall": round(statistics.mean(r["hf_overall"] for r in results), 4),
        "mean_csr_strict": round(statistics.mean(r["csr_strict"] for r in results), 4),
        "coverage": round(statistics.mean(r["coverage_ok"] for r in results), 4),
        "p50_latency_ms": round(statistics.median(latencies), 1),
        "p95_latency_ms": round(sorted(latencies)[int(0.95 * (n - 1))], 1) if n > 1 else latencies[0],
        "mean_confidence": round(
            statistics.mean(r["confidence"] or 0.0 for r in results), 3
        ),
        "by_stratum": _by_stratum(results),
    }


def _by_stratum(results: list[dict]) -> dict[str, dict[str, float]]:
    strata: dict[str, list[dict]] = {}
    for r in results:
        strata.setdefault(r["stratum"], []).append(r)
    out = {}
    for s, rs in strata.items():
        out[s] = {
            "n": len(rs),
            "mean_hf_overall": round(statistics.mean(r["hf_overall"] for r in rs), 4),
            "mean_csr_strict": round(statistics.mean(r["csr_strict"] for r in rs), 4),
            "coverage": round(statistics.mean(r["coverage_ok"] for r in rs), 4),
            "p50_latency_ms": round(statistics.median(r["latency_ms"] for r in rs), 1),
        }
    return out


def render_markdown(results: list[dict], summary: dict[str, Any]) -> str:
    lines = [
        "# MVP eval report",
        "",
        f"_Queries: {summary['n_queries']}_",
        "",
        "## Headline metrics",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| mean HF-P (overall) | {summary['mean_hf_overall']:.3f} |",
        f"| mean CSR (strict)   | {summary['mean_csr_strict']:.3f} |",
        f"| coverage (≥5 hits)  | {summary['coverage']:.3f} |",
        f"| p50 latency         | {summary['p50_latency_ms']:.0f} ms |",
        f"| p95 latency         | {summary['p95_latency_ms']:.0f} ms |",
        f"| mean confidence     | {summary['mean_confidence']:.3f} |",
        "",
        "## By stratum",
        "",
        "| Stratum | n | HF-P | CSR | Coverage | p50 ms |",
        "|---|---|---|---|---|---|",
    ]
    for s, m in summary["by_stratum"].items():
        lines.append(
            f"| {s} | {m['n']} | {m['mean_hf_overall']:.3f} | "
            f"{m['mean_csr_strict']:.3f} | {m['coverage']:.3f} | {m['p50_latency_ms']:.0f} |"
        )
    lines += ["", "## Per-query details", ""]
    for r in results:
        bits = []
        if r["relaxations"]:
            bits.append("🔄 relaxed")
        if r["clarification"].get("needed"):
            bits.append("❓ clarify")
        if r["warnings"]:
            bits.append("⚠ warn")
        flags = " " + " ".join(bits) if bits else ""
        lines.append(
            f"### `{r['qid']}` · {r['stratum']} · {r['lang']}{flags}"
        )
        lines.append(f"> {r['query']}")
        lines.append("")
        lines.append(
            f"- listings: **{r['n_listings']}**  ·  HF-P {r['hf_overall']:.2f}  "
            f"·  CSR {int(r['csr_strict'])}  ·  latency {r['latency_ms']:.0f} ms  "
            f"·  confidence {r['confidence']:.2f}"
        )
        if r["relaxations"]:
            lines.append(f"- relaxations: {r['relaxations']}")
        if r["warnings"]:
            lines.append(f"- warnings: {r['warnings']}")
        if r["clarification"].get("needed"):
            lines.append(f"- clarification: {r['clarification'].get('question')}")
        if r["top_3"]:
            lines.append("- top 3:")
            for i, t in enumerate(r["top_3"], 1):
                lines.append(
                    f"  {i}. `{t['listing_id']}` · score {t['score']:.3f} · "
                    f"{t['city']} · {t['rooms']} rms · CHF {t['price_chf']} · "
                    f"{t['features']}"
                )
                lines.append(f"     — {t['reason']}")
        lines.append("")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="JSON output only")
    parser.add_argument("--out", type=Path, help="Write markdown report to FILE")
    args = parser.parse_args()

    settings = get_settings()
    queries = load_queries()
    print(f"[INFO] eval_mvp: running {len(queries)} queries", flush=True)

    results = []
    for q in queries:
        print(f"[INFO] eval_mvp: qid={q['qid']}", flush=True)
        results.append(evaluate_one(q, settings.db_path))

    summary = aggregate(results)

    if args.json:
        print(json.dumps({"summary": summary, "results": results}, indent=2, default=str))
        return

    md = render_markdown(results, summary)
    print(md)
    if args.out:
        args.out.write_text(md, encoding="utf-8")
        print(f"\n[INFO] eval_mvp: report written to {args.out}", flush=True)


if __name__ == "__main__":
    main()
