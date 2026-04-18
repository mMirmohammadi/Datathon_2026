"""Pass 1b — Nominatim reverse-geocode for `postal_code` + `street`.

Fills postal_code and street for rows where pass 1a (offline reverse_geocoder)
couldn't — specifically, the postcode + street-name, which the offline KD-tree
doesn't carry.

Production constraints (non-negotiable):
  * Nominatim public policy: HARD max 1 request / second per source IP, plus a
    "not for commercial heavy use" restriction. We enforce the rate limit
    locally; going faster risks being banned for the whole team.
    Source: https://operations.osmfoundation.org/policies/nominatim/
  * User-Agent required. We advertise app name + contact email so Nominatim ops
    can reach us if there's a problem.
  * Cache EVERY response to `data/cache/nominatim.json`, keyed by rounded
    (lat, lng) at 4 decimal places (~11m resolution). Multiple listings at the
    same building share one API call.
  * Retries: exponential backoff on 429 / 5xx (3 tries, 2/4/8s).
  * Timeout: 30s per request.
  * Never fabricates. If the API returns no `postcode`, the row stays pending
    and pass 3 sentinel-fills.

Usage:
    python -m enrichment.scripts.pass1b_nominatim --db /data/listings.db
    python -m enrichment.scripts.pass1b_nominatim --db /data/listings.db --limit 100

Env overrides:
    NOMINATIM_BASE_URL      default: https://nominatim.openstreetmap.org
    NOMINATIM_CONTACT_EMAIL default: datathon2026-robin@example.invalid
    NOMINATIM_RATE_SEC      default: 1.0   (min seconds between requests)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from enrichment.common.db import connect
from enrichment.common.provenance import UNKNOWN_VALUE, write_field
from enrichment.common.sources import REV_GEO_NOMINATIM, UNKNOWN_PENDING

# --- Constants ---
CACHE_PATH = Path(__file__).resolve().parents[1] / "data" / "cache" / "nominatim.json"
USER_AGENT_TEMPLATE = "datathon2026-enrichment/1.0 ({contact})"
POSTAL_CONFIDENCE = 0.85
STREET_CONFIDENCE = 0.75
ROUND_DECIMALS = 4        # 4dp ≈ 11 m — buildings at the same facade share a cache key
REQUEST_TIMEOUT = 30.0
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2.0  # 2, 4, 8 seconds


@dataclass(slots=True)
class NominatimConfig:
    base_url: str
    user_agent: str
    rate_sec: float


def _load_config() -> NominatimConfig:
    base = os.environ.get("NOMINATIM_BASE_URL", "https://nominatim.openstreetmap.org").rstrip("/")
    contact = os.environ.get("NOMINATIM_CONTACT_EMAIL", "datathon2026-robin@example.invalid").strip()
    if not contact:
        # CLAUDE.md §5: announce the fallback before defaulting
        print(
            "[WARN] pass1b_nominatim: expected=NOMINATIM_CONTACT_EMAIL env set "
            "got=empty fallback=placeholder_email",
            flush=True,
        )
        contact = "datathon2026-robin@example.invalid"
    try:
        rate_sec = float(os.environ.get("NOMINATIM_RATE_SEC", "1.0"))
    except ValueError:
        print("[WARN] pass1b_nominatim: expected=NOMINATIM_RATE_SEC float got=invalid fallback=1.0", flush=True)
        rate_sec = 1.0
    if rate_sec < 1.0:
        # Hard cap — don't let callers bypass the ToS.
        print(
            f"[WARN] pass1b_nominatim: expected=rate_sec>=1.0 (Nominatim ToS) "
            f"got={rate_sec} fallback=1.0",
            flush=True,
        )
        rate_sec = 1.0
    return NominatimConfig(
        base_url=base,
        user_agent=USER_AGENT_TEMPLATE.format(contact=contact),
        rate_sec=rate_sec,
    )


# ---------- Cache ----------

def _quarantine_corrupt_cache(reason: str) -> None:
    """Rename a corrupt cache file so `_save_cache` doesn't immediately overwrite it.

    Accumulated hours of rate-limited geocoding would otherwise be lost silently.
    """
    if not CACHE_PATH.exists():
        return
    import time as _t
    quarantine = CACHE_PATH.with_suffix(f".json.corrupt.{int(_t.time())}")
    try:
        CACHE_PATH.rename(quarantine)
        print(
            f"[WARN] pass1b_nominatim: cache at {CACHE_PATH} is corrupt ({reason}); "
            f"moved to {quarantine} fallback=start_empty_cache",
            flush=True,
        )
    except OSError as e:
        print(
            f"[WARN] pass1b_nominatim: could not quarantine corrupt cache at "
            f"{CACHE_PATH} err={e!s} fallback=will_overwrite",
            flush=True,
        )


def _load_cache() -> dict[str, dict]:
    if not CACHE_PATH.exists():
        return {}
    try:
        with CACHE_PATH.open() as f:
            data = json.load(f)
        if not isinstance(data, dict):
            _quarantine_corrupt_cache(reason=f"not_a_dict (got {type(data).__name__})")
            return {}
        return data
    except json.JSONDecodeError as e:
        _quarantine_corrupt_cache(reason=f"json_decode_error: {e!s}")
        return {}


def _save_cache(cache: dict[str, dict]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CACHE_PATH.with_suffix(".json.tmp")
    with tmp.open("w") as f:
        json.dump(cache, f, ensure_ascii=False, indent=0, separators=(",", ":"))
    tmp.replace(CACHE_PATH)  # atomic


def _coord_key(lat: float, lng: float) -> str:
    return f"{round(lat, ROUND_DECIMALS)},{round(lng, ROUND_DECIMALS)}"


# ---------- HTTP ----------

class NominatimClient:
    def __init__(self, cfg: NominatimConfig, client: httpx.Client | None = None) -> None:
        self.cfg = cfg
        self._owns_client = client is None
        self._client = client or httpx.Client(
            headers={"User-Agent": cfg.user_agent, "Accept-Language": "en"},
            timeout=REQUEST_TIMEOUT,
        )
        self._last_request_ts: float = 0.0

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def _throttle(self) -> None:
        """Block until at least `rate_sec` has elapsed since the last request."""
        elapsed = time.monotonic() - self._last_request_ts
        wait = self.cfg.rate_sec - elapsed
        if wait > 0:
            time.sleep(wait)

    def reverse(self, lat: float, lng: float) -> dict[str, Any] | None:
        """Call /reverse; return the parsed JSON dict, or None on final failure.

        On final failure, emits [WARN] per CLAUDE.md §5. Callers leave the row
        pending; they must not fabricate a postcode.
        """
        url = f"{self.cfg.base_url}/reverse"
        params = {
            "format": "jsonv2",
            "lat": lat,
            "lon": lng,
            "zoom": 18,
            "addressdetails": 1,
        }
        last_err: Exception | None = None
        for attempt in range(MAX_RETRIES):
            self._throttle()
            self._last_request_ts = time.monotonic()
            try:
                resp = self._client.get(url, params=params)
            except httpx.HTTPError as e:
                last_err = e
                sleep_s = RETRY_BACKOFF_BASE ** (attempt + 1)
                print(
                    f"[WARN] pass1b_nominatim: http_error attempt={attempt + 1}/{MAX_RETRIES} "
                    f"lat={lat} lng={lng} err={e!s} fallback=retry_in_{sleep_s}s",
                    flush=True,
                )
                time.sleep(sleep_s)
                continue

            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError as e:
                    print(
                        f"[WARN] pass1b_nominatim: expected=json_response "
                        f"got=non_json body_head={resp.text[:100]!r} err={e!s} "
                        f"fallback=treat_as_empty_response",
                        flush=True,
                    )
                    return {}  # empty dict → no postcode → row stays pending

            if resp.status_code in (429, 500, 502, 503, 504):
                sleep_s = RETRY_BACKOFF_BASE ** (attempt + 1)
                print(
                    f"[WARN] pass1b_nominatim: status={resp.status_code} "
                    f"attempt={attempt + 1}/{MAX_RETRIES} lat={lat} lng={lng} "
                    f"fallback=retry_in_{sleep_s}s",
                    flush=True,
                )
                time.sleep(sleep_s)
                continue

            # Unexpected 4xx — don't retry.
            print(
                f"[WARN] pass1b_nominatim: status={resp.status_code} "
                f"lat={lat} lng={lng} body_head={resp.text[:100]!r} "
                f"fallback=no_retry_return_none",
                flush=True,
            )
            return None

        # Exhausted retries
        print(
            f"[WARN] pass1b_nominatim: all {MAX_RETRIES} attempts failed "
            f"lat={lat} lng={lng} last_err={last_err!s} fallback=row_stays_pending",
            flush=True,
        )
        return None


# ---------- Parsing ----------

def _extract_postcode(resp: dict) -> str | None:
    addr = resp.get("address") or {}
    pc = addr.get("postcode")
    if isinstance(pc, str) and pc.strip():
        return pc.strip()
    return None


def _extract_street(resp: dict) -> str | None:
    addr = resp.get("address") or {}
    road = (addr.get("road") or addr.get("pedestrian") or addr.get("path") or "").strip()
    num = (addr.get("house_number") or "").strip()
    if road and num:
        return f"{road} {num}"
    if road:
        return road
    return None


def _is_ch_response(resp: dict) -> bool:
    addr = resp.get("address") or {}
    cc = (addr.get("country_code") or "").lower()
    return cc == "ch"


# ---------- Main ----------

def _collect_pending_rows(conn) -> list[tuple[str, float, float]]:
    rows = conn.execute(
        """
        SELECT le.listing_id, l.latitude, l.longitude
        FROM listings_enriched le
        JOIN listings l USING(listing_id)
        WHERE (le.postal_code_source = ? OR le.street_source = ?)
          AND l.latitude IS NOT NULL
          AND l.longitude IS NOT NULL;
        """,
        (UNKNOWN_PENDING, UNKNOWN_PENDING),
    ).fetchall()
    return [(r["listing_id"], r["latitude"], r["longitude"]) for r in rows]


def run(
    db_path: Path,
    *,
    limit: int | None = None,
    client: NominatimClient | None = None,
) -> dict[str, int]:
    cfg = _load_config()
    owns_client = client is None
    client = client or NominatimClient(cfg)

    stats = {
        "pending_in": 0,
        "unique_coords": 0,
        "cache_hits": 0,
        "cache_misses": 0,
        "http_failed": 0,
        "postal_filled": 0,
        "street_filled": 0,
        "non_ch_skipped": 0,
    }

    cache = _load_cache()
    conn = connect(db_path)

    try:
        pending = _collect_pending_rows(conn)
        stats["pending_in"] = len(pending)

        # Group rows by rounded coord so we make one API call per unique key.
        groups: dict[str, list[tuple[str, float, float]]] = {}
        for row in pending:
            groups.setdefault(_coord_key(row[1], row[2]), []).append(row)
        stats["unique_coords"] = len(groups)

        keys = list(groups.keys())
        if limit is not None:
            keys = keys[:limit]

        # Periodic cache flush + DB commit for long runs.
        # The commit is CRITICAL: without it, this script holds a write lock
        # for the entire ~4-hour run and blocks any concurrent pass (e.g. the
        # GPT pass 2). Committing every FLUSH_EVERY coords releases the lock.
        FLUSH_EVERY = 50
        flush_counter = 0

        for key in keys:
            group = groups[key]
            listing_id, lat, lng = group[0]  # representative

            if key in cache:
                stats["cache_hits"] += 1
                resp = cache[key]
            else:
                resp = client.reverse(lat, lng)
                stats["cache_misses"] += 1
                if resp is None:
                    stats["http_failed"] += 1
                    continue
                cache[key] = resp
                flush_counter += 1
                if flush_counter >= FLUSH_EVERY:
                    _save_cache(cache)
                    flush_counter = 0

            if not _is_ch_response(resp):
                stats["non_ch_skipped"] += 1
                continue

            postcode = _extract_postcode(resp)
            street = _extract_street(resp)

            for lid, _, _ in group:
                if postcode:
                    # Gate: only write if still pending.
                    current = conn.execute(
                        "SELECT postal_code_source FROM listings_enriched WHERE listing_id=?;",
                        (lid,),
                    ).fetchone()
                    if current and current[0] == UNKNOWN_PENDING:
                        write_field(
                            conn,
                            listing_id=lid,
                            field="postal_code",
                            filled=postcode,
                            source=REV_GEO_NOMINATIM,
                            confidence=POSTAL_CONFIDENCE,
                            raw=None,
                        )
                        stats["postal_filled"] += 1
                if street:
                    current = conn.execute(
                        "SELECT street_source FROM listings_enriched WHERE listing_id=?;",
                        (lid,),
                    ).fetchone()
                    if current and current[0] == UNKNOWN_PENDING:
                        write_field(
                            conn,
                            listing_id=lid,
                            field="street",
                            filled=street,
                            source=REV_GEO_NOMINATIM,
                            confidence=STREET_CONFIDENCE,
                            raw=None,
                        )
                        stats["street_filled"] += 1

            # CRITICAL: commit once per coord so concurrent writers (pass 2 GPT)
            # can acquire the write lock within their 30s busy_timeout. Without
            # this, pass 1b holds the lock for many minutes and blocks every
            # other writer. At 1 req/s the per-row commit cost (<1 ms) is free.
            conn.commit()

        conn.commit()
        _save_cache(cache)
        return stats
    finally:
        conn.close()
        if owns_client:
            client.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process at most N unique rounded coordinates. Each coord may cover "
             "multiple listings; the full enrichment takes hours at 1 req/s.",
    )
    args = parser.parse_args()
    if not args.db.exists():
        print(f"[ERROR] DB not found at {args.db}", file=sys.stderr)
        return 2

    stats = run(args.db, limit=args.limit)
    print("Pass 1b complete:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
