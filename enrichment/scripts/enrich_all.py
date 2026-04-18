"""Orchestrator — runs every pass in the correct order, idempotent.

Order:
    1. pass0_create_table  — CREATE + backfill 'original'
    2. drop_bad_rows       — mark bogus price/rooms with DROPPED_bad_data
    3. pass1_geocode       — offline reverse_geocoder (1a)
    4. pass1b_nominatim    — Nominatim for postal/street (skippable via --skip-1b)
    5. pass2_*             — description extraction. Two implementations:
                              --pass2-impl=gpt     (default) OpenAI gpt-5.4-mini
                              --pass2-impl=regex   multilingual regex (legacy)
    6. pass3_sentinel_fill — convert UNKNOWN-pending to UNKNOWN
    7. assert_no_nulls     — final invariant check: every _filled non-null,
                              no _source = UNKNOWN-pending

Usage:
    python -m enrichment.scripts.enrich_all --db data/listings.db
    python -m enrichment.scripts.enrich_all --db data/listings.db --skip-1b
    python -m enrichment.scripts.enrich_all --db data/listings.db --pass2-impl=regex
    python -m enrichment.scripts.enrich_all --db data/listings.db --pass1b-limit 100
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from enrichment.common.db import connect
from enrichment.common.sources import UNKNOWN_PENDING
from enrichment.schema import FIELDS


def _assert_no_nulls(db_path: Path) -> None:
    """Final invariant: every *_filled column is non-null and no source is pending."""
    conn = connect(db_path)
    try:
        null_violations: list[tuple[str, int]] = []
        pending_violations: list[tuple[str, int]] = []
        for f in FIELDS:
            n_null = conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched WHERE {f.name}_filled IS NULL;"
            ).fetchone()[0]
            if n_null:
                null_violations.append((f.name, n_null))
            n_pending = conn.execute(
                f"SELECT COUNT(*) FROM listings_enriched WHERE {f.name}_source = ?;",
                (UNKNOWN_PENDING,),
            ).fetchone()[0]
            if n_pending:
                pending_violations.append((f.name, n_pending))
        if null_violations:
            raise RuntimeError(f"Post-condition FAILED: NULL _filled in {null_violations}")
        if pending_violations:
            raise RuntimeError(
                f"Post-condition FAILED: {UNKNOWN_PENDING} still present in {pending_violations}"
            )
    finally:
        conn.close()


def run(
    db_path: Path,
    *,
    skip_pass1b: bool = False,
    pass1b_limit: int | None = None,
    pass2_impl: str = "gpt",
) -> dict[str, object]:
    from enrichment.scripts.drop_bad_rows import run as drop_run
    from enrichment.scripts.pass0_create_table import run as pass0_run
    from enrichment.scripts.pass1_geocode import run as pass1_run
    from enrichment.scripts.pass1b_nominatim import run as pass1b_run
    from enrichment.scripts.pass3_sentinel_fill import run as pass3_run

    if pass2_impl == "gpt":
        from enrichment.scripts.pass2_gpt_extract import main as pass2_main_gpt

        def pass2_run(path: Path) -> dict:
            import sys as _sys
            saved = _sys.argv
            _sys.argv = ["pass2_gpt_extract", "--db", str(path)]
            try:
                rc = pass2_main_gpt()
            finally:
                _sys.argv = saved
            return {"exit_code": rc, "impl": "gpt-5.4-mini"}
    elif pass2_impl == "regex":
        from enrichment.scripts.pass2_text_extract import run as pass2_run  # noqa: F811
    else:
        raise ValueError(f"--pass2-impl must be 'gpt' or 'regex', got {pass2_impl!r}")

    results: dict[str, object] = {}

    t0 = time.monotonic()
    results["pass0"] = pass0_run(db_path)
    print(f"[enrich_all] pass0 done in {time.monotonic() - t0:.1f}s", flush=True)

    t1 = time.monotonic()
    results["drop_bad_rows"] = drop_run(db_path)
    print(f"[enrich_all] drop_bad_rows done in {time.monotonic() - t1:.1f}s", flush=True)

    t2 = time.monotonic()
    results["pass1a"] = pass1_run(db_path)
    print(f"[enrich_all] pass1a done in {time.monotonic() - t2:.1f}s", flush=True)

    if skip_pass1b:
        print("[enrich_all] pass1b SKIPPED (--skip-1b)", flush=True)
        results["pass1b"] = {"skipped": True}
    else:
        t3 = time.monotonic()
        results["pass1b"] = pass1b_run(db_path, limit=pass1b_limit)
        print(f"[enrich_all] pass1b done in {time.monotonic() - t3:.1f}s", flush=True)

    t4 = time.monotonic()
    results[f"pass2_{pass2_impl}"] = pass2_run(db_path)
    print(f"[enrich_all] pass2 ({pass2_impl}) done in {time.monotonic() - t4:.1f}s", flush=True)

    t5 = time.monotonic()
    results["pass3"] = pass3_run(db_path)
    print(f"[enrich_all] pass3 done in {time.monotonic() - t5:.1f}s", flush=True)

    _assert_no_nulls(db_path)
    print(f"[enrich_all] post-condition PASSED (zero NULLs, zero pending)", flush=True)

    results["total_seconds"] = round(time.monotonic() - t0, 2)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument(
        "--skip-1b",
        action="store_true",
        help="Skip Nominatim postal/street lookups. Pass 3 will sentinel-fill.",
    )
    parser.add_argument(
        "--pass1b-limit",
        type=int,
        default=None,
        help="Cap pass 1b at N unique coordinates (production default: unlimited).",
    )
    parser.add_argument(
        "--pass2-impl",
        choices=["gpt", "regex"],
        default="gpt",
        help=(
            "Which pass-2 implementation to use. Default 'gpt' = OpenAI "
            "gpt-5.4-mini structured extraction (~$50 per 25k rows, ~2h). "
            "'regex' = the legacy multilingual regex pass (~30s, needs patterns/*.yaml)."
        ),
    )
    parser.add_argument(
        "--json",
        type=Path,
        default=None,
        help="If set, write the full stats dict to this JSON path.",
    )
    args = parser.parse_args()
    if not args.db.exists():
        print(f"[ERROR] DB not found at {args.db}", file=sys.stderr)
        return 2

    results = run(
        args.db,
        skip_pass1b=args.skip_1b,
        pass1b_limit=args.pass1b_limit,
        pass2_impl=args.pass2_impl,
    )

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        with args.json.open("w") as f:
            json.dump(results, f, default=str, indent=2)
        print(f"[enrich_all] stats written to {args.json}", flush=True)

    print("\n=== Summary ===")
    for pass_name, stats in results.items():
        if pass_name == "total_seconds":
            continue
        print(f"{pass_name}: {stats}")
    print(f"total_seconds: {results['total_seconds']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
