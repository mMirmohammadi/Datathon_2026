"""Pass 1e verifier — Nominatim forward-geocode to ground-truth GPT-nano fills.

For every row where `canton_source='text_gpt_5_4_nano'`, query Nominatim with
the PLZ + city + "Switzerland" and extract `ISO3166-2-lvl4`. If Nominatim
returns a different canton than GPT, override the DB row with the Nominatim
answer — physical address > model inference.

This is the "trust but verify" step for §1e. GPT-nano is cheap and scales
but has a ~3% error rate on rare PLZs; Nominatim is deterministic, free
(at 1 req/s), and has ground-truth for every valid Swiss PLZ.

Writes:
  * Overrides go in as `canton_source='rev_geo_nominatim'`, confidence 0.92,
    raw=`"nom_verify: gpt_nano='{old}' → nominatim='{new}'"`.
  * If Nominatim returns empty or non-CH, leave the GPT fill alone
    (downstream can still inspect the confidence).

CLAUDE.md §5: every fallback emits [WARN].

Usage:
    python -m enrichment.scripts.pass1e_verify --db data/listings.db
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

import httpx
from dotenv import load_dotenv

from enrichment.common.db import connect
from enrichment.common.provenance import write_field
from enrichment.common.sources import REV_GEO_NOMINATIM, TEXT_GPT_5_4_NANO

load_dotenv()

NOMINATIM_BASE = os.getenv("NOMINATIM_BASE_URL", "https://nominatim.openstreetmap.org")
CONTACT_EMAIL = os.getenv("NOMINATIM_CONTACT_EMAIL", "datathon2026-robin@example.invalid")
USER_AGENT = f"datathon2026-enrichment/1.0 ({CONTACT_EMAIL})"
RATE_SEC = max(1.0, float(os.getenv("NOMINATIM_RATE_SEC", "1.0")))  # ToS non-bypassable
TIMEOUT_S = 15.0
AUDIT_PATH = Path(__file__).resolve().parents[1] / "data" / "pass1e_verify_audit.json"
CACHE_PATH = Path(__file__).resolve().parents[1] / "data" / "cache" / "nominatim_plz_city.json"


def _load_cache() -> dict:
    if not CACHE_PATH.exists():
        return {}
    try:
        return json.loads(CACHE_PATH.read_text())
    except json.JSONDecodeError:
        print(f"[WARN] pass1e_verify._load_cache: expected=valid_json, "
              f"got=corrupt, fallback={{}}; preserving old at .corrupt", flush=True)
        CACHE_PATH.rename(CACHE_PATH.with_suffix(".corrupt"))
        return {}


def _save_cache(cache: dict) -> None:
    tmp = CACHE_PATH.with_suffix(".tmp")
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2))
    tmp.replace(CACHE_PATH)


def _query_nominatim(query: str, session: httpx.Client, cache: dict) -> dict | None:
    if query in cache:
        return cache[query]
    params = urlencode({
        "q": query,
        "format": "jsonv2",
        "addressdetails": 1,
        "countrycodes": "ch",
        "limit": 1,
    })
    url = f"{NOMINATIM_BASE}/search?{params}"
    try:
        resp = session.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT_S)
    except httpx.HTTPError as exc:
        print(f"[WARN] pass1e_verify._query: expected=ok_response, "
              f"got={type(exc).__name__}, query={query!r}, fallback=None", flush=True)
        cache[query] = None
        return None
    if resp.status_code != 200:
        print(f"[WARN] pass1e_verify._query: expected=200, got={resp.status_code}, "
              f"query={query!r}, fallback=None", flush=True)
        cache[query] = None
        return None
    try:
        data = resp.json()
    except json.JSONDecodeError:
        print(f"[WARN] pass1e_verify._query: expected=json, got=non-json, "
              f"query={query!r}, fallback=None", flush=True)
        cache[query] = None
        return None
    if not data:
        cache[query] = {"_empty": True}
        return cache[query]
    cache[query] = data[0]
    return cache[query]


def _extract_canton(nom_resp: dict) -> str | None:
    if not nom_resp or nom_resp.get("_empty"):
        return None
    addr = nom_resp.get("address") or {}
    iso = addr.get("ISO3166-2-lvl4", "")
    if iso.startswith("CH-") and len(iso) == 5:
        return iso[3:]
    return None


def run(db_path: Path, limit: int | None) -> dict:
    conn = connect(db_path)
    try:
        q = """
            SELECT le.listing_id, le.postal_code_filled, le.city_filled,
                   le.canton_filled, le.canton_confidence
              FROM listings_enriched le
             WHERE le.canton_source = ?
        """
        params: tuple = (TEXT_GPT_5_4_NANO,)
        if limit:
            q += " LIMIT ?"
            params = (*params, limit)
        rows = [dict(r) for r in conn.execute(q, params).fetchall()]

        stats = {
            "rows_in_scope": len(rows),
            "nominatim_hits": 0,
            "nominatim_empty": 0,
            "gpt_confirmed": 0,
            "gpt_overridden": 0,
            "no_plz_skipped": 0,
            "overrides": [],
        }
        cache = _load_cache()
        with httpx.Client() as session:
            last_call = 0.0
            for i, r in enumerate(rows):
                plz = r["postal_code_filled"]
                city = r["city_filled"]
                if not plz or plz == "UNKNOWN":
                    stats["no_plz_skipped"] += 1
                    continue
                query = f"{plz} {city} Switzerland" if city and city != "UNKNOWN" else f"{plz} Switzerland"
                # Rate limit if this query will actually hit the network
                if query not in cache:
                    elapsed = time.monotonic() - last_call
                    if elapsed < RATE_SEC:
                        time.sleep(RATE_SEC - elapsed)
                    nom = _query_nominatim(query, session, cache)
                    last_call = time.monotonic()
                    _save_cache(cache)
                else:
                    nom = cache[query]

                canton_nom = _extract_canton(nom) if nom else None
                if canton_nom is None:
                    stats["nominatim_empty"] += 1
                    continue
                stats["nominatim_hits"] += 1
                if canton_nom == r["canton_filled"]:
                    stats["gpt_confirmed"] += 1
                else:
                    stats["gpt_overridden"] += 1
                    stats["overrides"].append({
                        "listing_id": r["listing_id"],
                        "plz": plz, "city": city,
                        "gpt_canton": r["canton_filled"],
                        "nominatim_canton": canton_nom,
                    })
                    # Overwrite gated on source=TEXT_GPT_5_4_NANO so we don't
                    # step on other sources.
                    conn.execute(
                        "UPDATE listings_enriched SET canton_filled=?, canton_source=?, "
                        "canton_confidence=?, canton_raw=? "
                        "WHERE listing_id=? AND canton_source=?",
                        (canton_nom, REV_GEO_NOMINATIM, 0.92,
                         f"nom_verify: gpt_nano={r['canton_filled']!r} -> nominatim={canton_nom!r}",
                         r["listing_id"], TEXT_GPT_5_4_NANO),
                    )

        conn.commit()

        AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
        AUDIT_PATH.write_text(json.dumps({
            "run_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "stats": stats,
        }, indent=2, ensure_ascii=False))
        return stats
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    if not args.db.exists():
        print(f"[ERROR] DB not found at {args.db}", file=sys.stderr)
        return 2
    stats = run(args.db, args.limit)
    print("Pass 1e verify complete:")
    for k, v in stats.items():
        if k == "overrides":
            print(f"  {k}: {len(v)} (see audit json)")
        else:
            print(f"  {k}: {v}")
    print(f"\nAudit: {AUDIT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
