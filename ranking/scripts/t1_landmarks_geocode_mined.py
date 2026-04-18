"""T1.3c — Forward-geocode mined landmark candidates via Nominatim.

Reads `data/ranking/landmarks_mined_candidates.json` (produced by
`t1_landmarks_aggregate.py`) and APPENDS successfully resolved entries to
`data/ranking/landmarks.json`. The hand-curated 30 entries already there
are never touched.

Nominatim rate-limit policy (1 req/s, CH/LI country filter) and retry pattern
are reused from [t1_landmarks_fetch.py](t1_landmarks_fetch.py). This script
exists as a sibling so the curated-vs-mined concerns stay cleanly separated.

Per CLAUDE.md §5:
  * Every non-200 / empty-result / non-CH result emits a `[WARN]` and the
    candidate is dropped (never fabricated coords).
  * The run is loud about failure rate: if >40% of candidates fail, we still
    write the successes but emit a final `[WARN]` with the count.

Usage:
    python -m ranking.scripts.t1_landmarks_geocode_mined
    python -m ranking.scripts.t1_landmarks_geocode_mined --limit 20   # smoke
    python -m ranking.scripts.t1_landmarks_geocode_mined --retry-failed   # re-try
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

# Reuse constants + helpers from the sibling script — they're the same
# Nominatim client, rate limiter, and User-Agent policy.
from ranking.scripts.t1_landmarks_fetch import (
    NOMINATIM_BASE_URL,
    OUT_PATH as LANDMARKS_PATH,
    RATE_SEC,
    REQUEST_TIMEOUT,
    _fetch_one,
    _headers,
    _load_existing,
)

CANDIDATES_PATH = Path("data/ranking/landmarks_mined_candidates.json")
MAX_ACCEPTABLE_FAIL_RATIO = 0.40


def _load_candidates() -> list[dict[str, Any]]:
    if not CANDIDATES_PATH.exists():
        raise FileNotFoundError(
            f"Candidates file not found at {CANDIDATES_PATH}. Run "
            "`python -m ranking.scripts.t1_landmarks_aggregate` first."
        )
    try:
        return json.loads(CANDIDATES_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Candidates file at {CANDIDATES_PATH} is not valid JSON: {exc!r}"
        )


def _is_in_switzerland(nominatim_record: dict[str, Any]) -> bool:
    """Nominatim jsonv2 response: country is in display_name or address block.

    We only trust the result if display_name ends in 'Switzerland' OR
    'Liechtenstein' (addressdetails off here to keep the request cheap —
    the countrycodes=ch,li filter on our side of the API should already
    prevent cross-border results but defence-in-depth).
    """
    dn = (nominatim_record.get("display_name") or "").strip()
    if not dn:
        return False
    return dn.endswith(("Switzerland", "Schweiz", "Suisse", "Svizzera",
                        "Liechtenstein"))


def run(limit: int | None, retry_failed: bool) -> dict[str, Any]:
    t0 = time.monotonic()

    candidates = _load_candidates()
    print(
        f"[INFO] t1_landmarks_geocode_mined: loaded {len(candidates)} "
        f"candidates from {CANDIDATES_PATH}",
        flush=True,
    )

    # Load existing landmarks; any mined-canonical already present is SKIPPED
    # (we don't re-geocode or overwrite — single source of truth preserved).
    existing = {r["key"]: r for r in _load_existing() if isinstance(r, dict)}
    already_present = {c["canonical"] for c in candidates if c["canonical"] in existing}
    print(
        f"[INFO] t1_landmarks_geocode_mined: {len(existing)} landmarks already in "
        f"landmarks.json; {len(already_present)} candidate canonicals collide "
        f"with existing keys — they will be skipped",
        flush=True,
    )

    # If --retry-failed, treat existing-without-lat entries as re-tryable.
    if retry_failed:
        retryable = {k for k, v in existing.items() if v.get("lat") is None}
        print(
            f"[INFO] t1_landmarks_geocode_mined: --retry-failed: "
            f"{len(retryable)} entries will be re-queried",
            flush=True,
        )
    else:
        retryable = set()

    todo = [
        c for c in candidates
        if c["canonical"] not in already_present or c["canonical"] in retryable
    ]
    if limit is not None:
        todo = todo[:limit]
    print(f"[INFO] t1_landmarks_geocode_mined: to_fetch={len(todo)}", flush=True)

    out_records: list[dict[str, Any]] = list(existing.values())
    last_ts_ref = [0.0]
    n_ok = 0
    n_failed = 0
    n_out_of_country = 0

    with httpx.Client(headers=_headers()) as client:
        for i, c in enumerate(todo, start=1):
            query = c["best_query"]
            r = _fetch_one(client, query, last_ts_ref)
            if r is None:
                n_failed += 1
                if i % 20 == 0:
                    print(
                        f"[INFO] t1_landmarks_geocode_mined: progress {i}/{len(todo)} "
                        f"ok={n_ok} failed={n_failed}",
                        flush=True,
                    )
                continue

            if not _is_in_switzerland(r):
                print(
                    f"[WARN] t1_landmarks_geocode_mined: expected=CH/LI result, "
                    f"got={r.get('display_name')!r}, fallback=skip canonical={c['canonical']!r}",
                    flush=True,
                )
                n_out_of_country += 1
                continue

            try:
                lat = float(r["lat"])
                lon = float(r["lon"])
            except (KeyError, TypeError, ValueError):
                print(
                    f"[WARN] t1_landmarks_geocode_mined: expected=(lat,lon) floats, "
                    f"got={r.keys() if isinstance(r, dict) else type(r).__name__}, "
                    f"fallback=skip canonical={c['canonical']!r}",
                    flush=True,
                )
                n_failed += 1
                continue

            entry = {
                "key":            c["canonical"],
                "kind":           c["kind"],
                "query":          query,
                "display_name":   r.get("display_name", ""),
                "lat":            lat,
                "lon":            lon,
                "osm_type":       r.get("osm_type"),
                "osm_id":         r.get("osm_id"),
                "aliases":        c["names"][:8],
                "mention_count":  c["mention_count"],
                "source":         "mined_gpt_5_4_nano",
                "fetched_at":     datetime.now(timezone.utc).isoformat(),
            }

            # Replace if retry_failed and entry exists; else append.
            replaced = False
            for i2, existing_rec in enumerate(out_records):
                if existing_rec.get("key") == c["canonical"]:
                    out_records[i2] = entry
                    replaced = True
                    break
            if not replaced:
                out_records.append(entry)

            n_ok += 1
            if i % 20 == 0:
                print(
                    f"[INFO] t1_landmarks_geocode_mined: progress {i}/{len(todo)} "
                    f"ok={n_ok} failed={n_failed}",
                    flush=True,
                )

    # Atomic write
    tmp = LANDMARKS_PATH.with_suffix(LANDMARKS_PATH.suffix + ".tmp")
    tmp.write_text(json.dumps(out_records, ensure_ascii=False, indent=2),
                   encoding="utf-8")
    tmp.replace(LANDMARKS_PATH)

    elapsed = time.monotonic() - t0
    stats = {
        "candidates_loaded":   len(candidates),
        "attempted":           len(todo),
        "ok":                  n_ok,
        "failed":              n_failed,
        "out_of_country":      n_out_of_country,
        "final_records":       len(out_records),
        "elapsed_s":           round(elapsed, 1),
        "out_path":            str(LANDMARKS_PATH),
    }
    print(
        f"[INFO] t1_landmarks_geocode_mined: DONE ok={n_ok} failed={n_failed} "
        f"out_of_country={n_out_of_country} total_records={len(out_records)} "
        f"elapsed_s={elapsed:.1f} → {LANDMARKS_PATH}",
        flush=True,
    )

    if n_ok == 0 and len(todo) > 0:
        raise RuntimeError(
            "Zero mined landmarks resolved. Nominatim unreachable, bad queries, "
            "or contact header rejected?"
        )
    if len(todo) and (n_failed + n_out_of_country) / len(todo) > MAX_ACCEPTABLE_FAIL_RATIO:
        print(
            f"[WARN] t1_landmarks_geocode_mined: failure ratio "
            f"{(n_failed + n_out_of_country) / len(todo):.1%} > "
            f"{MAX_ACCEPTABLE_FAIL_RATIO:.0%}; successful records written, "
            f"but review the [WARN] lines for common causes",
            flush=True,
        )
    return stats


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=None, help="smoke-test cap")
    p.add_argument("--retry-failed", action="store_true",
                   help="Re-query entries whose lat/lon is null.")
    args = p.parse_args()
    try:
        run(args.limit, args.retry_failed)
    except (FileNotFoundError, RuntimeError) as exc:
        print(f"[ERROR] t1_landmarks_geocode_mined: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
