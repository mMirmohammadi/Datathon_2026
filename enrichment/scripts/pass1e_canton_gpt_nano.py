"""Pass 1e — GPT-5.4-nano canton resolution for the residual UNKNOWN rows.

Pass 1d closed 3,113 of 3,177 canton UNKNOWNs via offline reverse_geocoder +
corpus PLZ majority vote. This script handles the 64 residual rows that
neither channel could resolve:

  * 53 rows with a postal_code that doesn't appear anywhere else in the corpus
    (genuinely rare Swiss PLZs — e.g. 1186, 6930, 9486).
  * 9 rows with lat/lng = (0, 0) null-island but a city name in the raw data
    (ingest bugs — valid Genève / Vernier / Châtelaine listings with zeros).
  * 1 row with coord 47.60°/8.60° that reverse_geocoder snaps to DE.
  * 1 row with a PLZ-city mismatch (listing 9658).

Model: `gpt-5.4-nano-2026-03-17` (pinned snapshot — confirmed available via
`/v1/models` on 2026-04-19). Reasoning: the cheapest GPT-5.4 variant. There
is no "tiny" — the family is {nano, mini, full, pro}.

Cost bound: 64 rows × ~400 input + 80 output tokens ≈ 30k tokens ≈ $0.02.

Per CLAUDE.md §5:
  * Every fallback path (no key, timeout, refusal, bad JSON, canton not in
    the 26-code set) emits a `[WARN]` line with context/expected/got/fallback.
  * If GPT's answer can't be validated, we DON'T write — row stays UNKNOWN.
  * No silent suppression.

Cross-validation:
  * For rows where lat/lng is in the CH bbox (1 of the 64), we also run
    `reverse_geocoder` and compare. If rg disagrees with GPT, rg wins
    (physical coord > model inference) and we log the disagreement.

Idempotency:
  * Only writes when `canton_source='UNKNOWN'`. Re-runs are no-ops.
  * Cache is append-only JSONL at `enrichment/data/cache/gpt_canton_nano.jsonl`.

Usage:
    python -m enrichment.scripts.pass1e_canton_gpt_nano --db data/listings.db
    python -m enrichment.scripts.pass1e_canton_gpt_nano --db data/listings.db --dry-run
    python -m enrichment.scripts.pass1e_canton_gpt_nano --db data/listings.db --limit 5

Env:
    OPENAI_API_KEY                 required
    OPENAI_CANTON_MODEL            default: gpt-5.4-nano-2026-03-17
    GPT_PASS1E_CONCURRENCY         default: 8  (well below any rate limit for 64 rows)
    GPT_PASS1E_TIMEOUT_S           default: 30
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Optional

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict

from enrichment.common.cantons import admin1_to_canton_code
from enrichment.common.db import connect
from enrichment.common.provenance import write_field
from enrichment.common.sources import TEXT_GPT_5_4_NANO, UNKNOWN

load_dotenv()

MODEL = os.getenv("OPENAI_CANTON_MODEL", "gpt-5.4-nano-2026-03-17")
CONCURRENCY = int(os.getenv("GPT_PASS1E_CONCURRENCY", "8"))
TIMEOUT_S = float(os.getenv("GPT_PASS1E_TIMEOUT_S", "30"))

CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "cache"
CACHE_PATH = CACHE_DIR / "gpt_canton_nano.jsonl"
AUDIT_PATH = Path(__file__).resolve().parents[1] / "data" / "pass1e_audit.json"

# Swiss bbox used for rg cross-check (keep in sync with pass 1a).
CH_LAT_MIN, CH_LAT_MAX = 45.8, 47.9
CH_LNG_MIN, CH_LNG_MAX = 5.9, 10.5

# 26 official Swiss cantons, ISO 3166-2:CH codes.
CantonCode = Literal[
    "AG","AI","AR","BE","BL","BS","FR","GE","GL","GR","JU","LU","NE","NW",
    "OW","SG","SH","SO","SZ","TG","TI","UR","VD","VS","ZG","ZH",
]


class CantonInference(BaseModel):
    """GPT-5.4-nano output schema.

    The model must return exactly one of the 26 canton codes (Literal enforces
    this at parse-time via Structured-Outputs strict JSON Schema), a
    confidence in [0, 1], and a short reason citing which input fields
    drove the answer.
    """
    model_config = ConfigDict(extra="forbid")
    canton_code: CantonCode
    confidence: float
    reasoning: str


def _load_cache() -> dict[str, dict]:
    """Append-only JSONL cache — one line per (listing_id)."""
    if not CACHE_PATH.exists():
        return {}
    out: dict[str, dict] = {}
    with CACHE_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                print(f"[WARN] pass1e._load_cache: expected=jsonl line, "
                      f"got=invalid json, fallback=skip: {line[:80]!r}", flush=True)
                continue
            lid = rec.get("listing_id")
            if lid:
                out[lid] = rec
    return out


def _append_cache(rec: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with CACHE_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _parse_float(v: Optional[str]) -> Optional[float]:
    if v is None or v == "UNKNOWN":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _collect_rows(conn: sqlite3.Connection, limit: Optional[int]) -> list[dict]:
    q = """
        SELECT le.listing_id,
               le.latitude_source, le.latitude_filled,
               le.longitude_source, le.longitude_filled,
               le.postal_code_source, le.postal_code_filled,
               le.city_filled,
               l.scrape_source,
               substr(COALESCE(l.description, l.title, ''), 1, 300) AS description_head
          FROM listings_enriched le
          JOIN listings l USING (listing_id)
         WHERE le.canton_source = ?
    """
    params: tuple = (UNKNOWN,)
    if limit is not None and limit > 0:
        q += " LIMIT ?"
        params = (*params, limit)
    rows = conn.execute(q, params).fetchall()
    return [dict(r) for r in rows]


def _build_input(row: dict) -> str:
    """One compact human-readable block sent as the `input` to Structured Outputs."""
    lat = _parse_float(row["latitude_filled"])
    lng = _parse_float(row["longitude_filled"])
    if (lat is None or lng is None) or (lat == 0.0 and lng == 0.0):
        coord_str = "(not available — coord is (0,0) or missing)"
    else:
        coord_str = f"({lat:.5f}, {lng:.5f})"

    plz = row["postal_code_filled"] if row["postal_code_filled"] != "UNKNOWN" else "(not available)"
    city = row["city_filled"] if row["city_filled"] != "UNKNOWN" else "(not available)"

    return (
        f"Swiss real-estate listing (source: {row['scrape_source']}).\n"
        f"- postal_code: {plz}\n"
        f"- city_filled: {city!r}\n"
        f"- coord (lat, lng): {coord_str}\n"
        f"- description_head: {row['description_head']!r}\n"
        f"Task: identify the canton this listing belongs to. "
        f"Return exactly one of the 26 Swiss canton codes.\n"
        f"Use the postal_code (deterministic Swiss PLZ→canton mapping) "
        f"as the primary signal; use city_filled as a secondary check; use "
        f"coord only if it's a real (not (0,0)) pair and helps disambiguate. "
        f"If signals conflict, trust the postal_code. "
        f"Do not fabricate — if evidence is weak, return the single most "
        f"likely canton with a correspondingly low confidence."
    )


async def _infer_one(client, row: dict, sem: asyncio.Semaphore) -> tuple[str, Optional[dict]]:
    """Return (listing_id, {canton_code, confidence, reasoning}) or (..., None)."""
    lid = row["listing_id"]
    async with sem:
        try:
            resp = await asyncio.wait_for(
                client.responses.parse(
                    model=MODEL,
                    input=_build_input(row),
                    text_format=CantonInference,
                ),
                timeout=TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            print(f"[WARN] pass1e._infer_one: expected=response_under_{TIMEOUT_S}s, "
                  f"got=timeout, listing_id={lid}, fallback=skip", flush=True)
            return lid, None
        except Exception as exc:
            print(f"[WARN] pass1e._infer_one: expected=ok_response, "
                  f"got={type(exc).__name__}: {exc}, listing_id={lid}, fallback=skip",
                  flush=True)
            return lid, None

    parsed = resp.output_parsed
    if parsed is None:
        print(f"[WARN] pass1e._infer_one: expected=parsed_output, got=None, "
              f"listing_id={lid}, fallback=skip", flush=True)
        return lid, None

    return lid, {
        "canton_code": parsed.canton_code,
        "confidence": float(parsed.confidence),
        "reasoning": parsed.reasoning,
        "model": MODEL,
        "inferred_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def _rg_cross_check(row: dict):
    """Return canton the reverse_geocoder would assign, or None if not applicable."""
    lat = _parse_float(row["latitude_filled"])
    lng = _parse_float(row["longitude_filled"])
    if lat is None or lng is None or (lat == 0.0 and lng == 0.0):
        return None
    if not (CH_LAT_MIN <= lat <= CH_LAT_MAX and CH_LNG_MIN <= lng <= CH_LNG_MAX):
        return None
    import reverse_geocoder as rg
    res = rg.search([(lat, lng)], mode=2)[0]
    if res.get("cc", "") != "CH":
        return None
    return admin1_to_canton_code(res.get("admin1", ""))


async def _run(db_path: Path, limit: Optional[int], dry_run: bool) -> dict:
    from openai import AsyncOpenAI
    client = AsyncOpenAI()

    conn = connect(db_path)
    try:
        rows = _collect_rows(conn, limit)
        stats = {
            "rows_in_scope": len(rows),
            "cache_hits": 0,
            "gpt_calls": 0,
            "gpt_failed": 0,
            "written": 0,
            "skipped_low_confidence": 0,
            "rg_agreement": 0,
            "rg_disagreement": 0,
            "rg_no_coord": 0,
            "by_canton": {},
        }
        if not rows:
            return stats

        cache = _load_cache()
        disagreements = []
        sem = asyncio.Semaphore(CONCURRENCY)
        work: list[tuple[dict, dict]] = []  # (row, result)

        # Split: use cache where available.
        need_call = []
        for row in rows:
            if row["listing_id"] in cache:
                stats["cache_hits"] += 1
                work.append((row, cache[row["listing_id"]]))
            else:
                need_call.append(row)

        if need_call:
            results = await asyncio.gather(
                *(_infer_one(client, row, sem) for row in need_call),
                return_exceptions=False,
            )
            for row, (lid, res) in zip(need_call, results, strict=True):
                assert lid == row["listing_id"]
                stats["gpt_calls"] += 1
                if res is None:
                    stats["gpt_failed"] += 1
                    continue
                _append_cache({"listing_id": lid, **res})
                cache[lid] = res
                work.append((row, res))

        # Apply to DB with cross-validation + confidence gate.
        for row, res in work:
            lid = row["listing_id"]
            gpt_canton = res["canton_code"]
            gpt_conf = res["confidence"]

            # Confidence gate: GPT must be ≥ 0.70 to qualify as a write.
            if gpt_conf < 0.70:
                stats["skipped_low_confidence"] += 1
                print(f"[WARN] pass1e.apply: expected=conf>=0.70, got={gpt_conf:.2f}, "
                      f"listing_id={lid} canton={gpt_canton}, fallback=leave_unknown",
                      flush=True)
                continue

            # Cross-validate against rev_geocoder if usable coords exist.
            rg_canton = _rg_cross_check(row)
            final_canton = gpt_canton
            if rg_canton is None:
                stats["rg_no_coord"] += 1
            elif rg_canton == gpt_canton:
                stats["rg_agreement"] += 1
            else:
                stats["rg_disagreement"] += 1
                # rg wins — physical coord > model inference (plan §3 policy).
                disagreements.append({
                    "listing_id": lid,
                    "gpt_canton": gpt_canton,
                    "rg_canton": rg_canton,
                    "gpt_confidence": gpt_conf,
                    "winner": "rg_canton",
                    "reasoning": res.get("reasoning", ""),
                })
                final_canton = rg_canton

            # Confidence ladder for the DB write.
            # GPT_5_4 nano inference is reliable for PLZ→canton lookup (the task
            # is essentially a table lookup with city as tiebreaker), so apply
            # only a modest discount: write at 0.85 × gpt_conf, capped at 0.80.
            db_conf = min(0.80, round(0.85 * gpt_conf, 3))
            raw = (
                f"gpt_nano:{final_canton} (conf={gpt_conf:.2f}"
                + (f", rg_override={rg_canton}" if rg_canton and rg_canton != gpt_canton else "")
                + ")"
            )

            if dry_run:
                print(f"  [dry-run] {lid}: {final_canton} conf={db_conf}  {raw}")
                continue

            # Gate: only write if still UNKNOWN (explicit WHERE per auditor R4).
            cur = conn.execute(
                "UPDATE listings_enriched SET canton_filled=?, canton_source=?, "
                "canton_confidence=?, canton_raw=? "
                "WHERE listing_id=? AND canton_source=?",
                (final_canton, TEXT_GPT_5_4_NANO, db_conf, raw, lid, UNKNOWN),
            )
            if cur.rowcount == 1:
                stats["written"] += 1
                stats["by_canton"][final_canton] = stats["by_canton"].get(final_canton, 0) + 1

        if not dry_run:
            conn.commit()

        # Write audit file.
        AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
        AUDIT_PATH.write_text(json.dumps({
            "run_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "model": MODEL,
            "stats": stats,
            "disagreements": disagreements,
        }, indent=2, ensure_ascii=False))

        stats["residual_unknown"] = conn.execute(
            "SELECT COUNT(*) FROM listings_enriched WHERE canton_source=?",
            (UNKNOWN,),
        ).fetchone()[0]
        return stats
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=None,
                        help="Process at most N rows (smoke test).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Call GPT but do not write to DB.")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"[ERROR] DB not found at {args.db}", file=sys.stderr)
        return 2
    if not os.getenv("OPENAI_API_KEY"):
        print("[ERROR] OPENAI_API_KEY not set (use .env or shell env).", file=sys.stderr)
        return 3

    stats = asyncio.run(_run(args.db, args.limit, args.dry_run))
    print("Pass 1e complete:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    print(f"\nAudit: {AUDIT_PATH}")
    print(f"Cache: {CACHE_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
