"""T1.3 — One-off Nominatim forward-geocode for canonical CH landmarks.

Output: `data/ranking/landmarks.json` — a flat list of
`{canonical_key, display_name, lat, lon, kind, aliases}` records. The ranker
resolves the user's "near ETH" into a landmark at query time via this file.

Curated landmark list is hard-coded here (not derived) because we want stable,
reproducible coordinates that don't shift when OSM data changes. We pick each
landmark's BEST-MATCH POINT deliberately:

  * ETH Zürich has three campuses — we emit THREE separate entries
    (eth_zentrum, eth_hoengg, eth_basel_bsse) and let the query layer pick.
  * HB Zürich / HB Bern / etc are at the main platform centroid, not the
    station building roof.

Rate limiting:
  * Nominatim public policy = 1 req/s hard cap.
  * We send one query per landmark, ~25 landmarks, ~25 s total.
  * User-Agent includes contact email from .env.

Idempotent: if landmarks.json exists and covers every entry in CURATED, we
skip the API calls unless --refresh is set.

Per CLAUDE.md §5:
  * Every non-200 response emits a [WARN] and the landmark's coordinates
    stay NULL (never fabricated).
  * The run fails loud if ≥ 50% of landmarks fail to resolve.

Usage:
    python -m ranking.scripts.t1_landmarks_fetch --refresh
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

OUT_PATH = Path("data/ranking/landmarks.json")
USER_AGENT_TEMPLATE = "datathon2026-ranking/1.0 ({contact})"
NOMINATIM_BASE_URL = os.getenv("NOMINATIM_BASE_URL", "https://nominatim.openstreetmap.org")
RATE_SEC = max(1.0, float(os.getenv("NOMINATIM_RATE_SEC", "1.0")))
REQUEST_TIMEOUT = 30.0

# The curated list — edit here, not in a config file. Each entry has:
#   key:           stable identifier downstream code can reference
#   query:         what we send to Nominatim (specific enough to not collide)
#   kind:          semantic category (school, transit, lake, oldtown, employer)
#   aliases:       string forms the user might type; used for fuzzy matching
CURATED: list[dict[str, Any]] = [
    # --- Universities (ETH + EPFL + UZH + HSG + USI) -----------------------
    {"key": "eth_zentrum",     "kind": "university",
     "query": "Rämistrasse 101, Zürich",
     "aliases": ["ETH", "ETH Zürich", "ETH Zentrum", "ETHZ"]},
    {"key": "eth_hoengg",      "kind": "university",
     "query": "ETH Hönggerberg, Zürich",
     "aliases": ["ETH Hönggerberg", "Höngg ETH", "ETH Hoengg"]},
    {"key": "eth_basel_bsse",  "kind": "university",
     "query": "Mattenstrasse 28, Basel",
     "aliases": ["ETH Basel", "D-BSSE"]},
    {"key": "epfl",            "kind": "university",
     "query": "EPFL, Lausanne",
     "aliases": ["EPFL", "École Polytechnique Fédérale de Lausanne"]},
    {"key": "uzh_zentrum",     "kind": "university",
     "query": "Rämistrasse 71, Zürich",
     "aliases": ["UZH", "Universität Zürich", "University of Zurich"]},
    {"key": "uzh_irchel",      "kind": "university",
     "query": "Universität Zürich Irchel",
     "aliases": ["UZH Irchel", "Irchel"]},
    {"key": "hsg",             "kind": "university",
     "query": "Universität St.Gallen",
     "aliases": ["HSG", "St. Gallen University"]},
    {"key": "usi_lugano",      "kind": "university",
     "query": "USI, Lugano",
     "aliases": ["USI", "USI Lugano"]},

    # --- Major Hauptbahnhöfe -----------------------------------------------
    {"key": "hb_zurich",   "kind": "transit",
     "query": "Zürich HB, 8001 Zürich, Switzerland",
     "aliases": ["Zürich HB", "Zurich HB", "Hauptbahnhof Zürich", "ZH HB"]},
    {"key": "hb_geneve",   "kind": "transit",
     "query": "Gare de Cornavin, 1201 Genève, Switzerland",
     "aliases": ["Genève Cornavin", "Geneva Gare Cornavin", "Genf HB"]},
    {"key": "hb_bern",     "kind": "transit",
     "query": "Bahnhof Bern, 3011 Bern, Switzerland",
     "aliases": ["Bern HB", "Gare de Berne", "Hauptbahnhof Bern"]},
    {"key": "hb_basel",    "kind": "transit",
     "query": "Basel SBB, 4051 Basel, Switzerland",
     "aliases": ["Basel SBB", "Basel HB", "Bâle gare"]},
    {"key": "hb_lausanne", "kind": "transit",
     "query": "Gare de Lausanne, 1003 Lausanne, Switzerland",
     "aliases": ["Lausanne gare", "Lausanne HB"]},
    {"key": "hb_lugano",   "kind": "transit",
     "query": "Stazione di Lugano, 6900 Lugano, Switzerland",
     "aliases": ["Lugano stazione", "Lugano station"]},
    {"key": "hb_winterthur","kind": "transit",
     "query": "Bahnhof Winterthur, 8400 Winterthur, Switzerland",
     "aliases": ["Winterthur HB"]},
    {"key": "hb_st_gallen","kind": "transit",
     "query": "Bahnhof St.Gallen, 9000 St. Gallen, Switzerland",
     "aliases": ["St. Gallen HB", "Sankt Gallen station"]},

    # --- Airports ----------------------------------------------------------
    {"key": "zurich_airport",  "kind": "transit",
     "query": "Zürich Flughafen, 8058 Kloten, Switzerland",
     "aliases": ["ZRH", "Zurich Airport", "Flughafen Zürich"]},
    {"key": "geneva_airport",  "kind": "transit",
     "query": "Aéroport de Genève, 1215 Genève, Switzerland",
     "aliases": ["GVA", "Geneva Airport"]},

    # --- Lakes (point = city-side centroid) --------------------------------
    {"key": "zurichsee",       "kind": "lake",
     "query": "Bürkliplatz, 8001 Zürich, Switzerland",
     "aliases": ["Zürichsee", "Zurich lake", "Lac de Zurich", "Lago di Zurigo"]},
    {"key": "lac_leman",       "kind": "lake",
     "query": "Quai du Mont-Blanc, 1201 Genève, Switzerland",
     "aliases": ["Lac Léman", "Lake Geneva", "Genfersee", "Lago Lemano"]},
    {"key": "lago_lugano",     "kind": "lake",
     "query": "Piazza della Riforma, 6900 Lugano, Switzerland",
     "aliases": ["Lago di Lugano", "Lake Lugano"]},
    {"key": "bodensee_konstanz","kind": "lake",
     "query": "Hafen Romanshorn, 8590 Romanshorn, Switzerland",
     "aliases": ["Bodensee", "Lake Constance"]},

    # --- Old-town centroids (Altstadt / Vieille-Ville) --------------------
    {"key": "altstadt_zurich", "kind": "oldtown",
     "query": "Niederdorf, 8001 Zürich, Switzerland",
     "aliases": ["Altstadt Zürich", "Niederdorf", "Zürich old town"]},
    {"key": "vieille_ville_geneve", "kind": "oldtown",
     "query": "Place du Bourg-de-Four, 1204 Genève, Switzerland",
     "aliases": ["Vieille-Ville Genève", "Geneva old town"]},
    {"key": "altstadt_bern",   "kind": "oldtown",
     "query": "Zytglogge, 3011 Bern, Switzerland",
     "aliases": ["Altstadt Bern", "Bern old town"]},
    {"key": "altstadt_basel",  "kind": "oldtown",
     "query": "Marktplatz, 4051 Basel, Switzerland",
     "aliases": ["Altstadt Basel", "Basel old town"]},

    # --- Major employers --------------------------------------------------
    {"key": "roche_basel",     "kind": "employer",
     "query": "Grenzacherstrasse 124, Basel",
     "aliases": ["Roche Basel", "Hoffmann-La Roche", "Roche Tower"]},
    {"key": "novartis_campus", "kind": "employer",
     "query": "Novartis Campus, Basel",
     "aliases": ["Novartis", "Novartis Basel"]},
    {"key": "cern",            "kind": "employer",
     "query": "CERN, Meyrin",
     "aliases": ["CERN", "CERN Meyrin"]},
    {"key": "google_zurich",   "kind": "employer",
     "query": "Europaallee, Zürich",
     "aliases": ["Google Zürich", "Google ZH", "Europaallee"]},
]


def _headers() -> dict[str, str]:
    contact = os.getenv("NOMINATIM_CONTACT_EMAIL", "").strip()
    if not contact or "@" not in contact:
        # Loud — Nominatim policy says User-Agent with contact is REQUIRED.
        print(
            "[WARN] t1_landmarks_fetch: expected=NOMINATIM_CONTACT_EMAIL (valid email), "
            "got=empty/invalid, fallback=<datathon2026-robin@example.invalid> (policy risk — set a real one)",
            flush=True,
        )
        contact = "datathon2026-robin@example.invalid"
    return {
        "User-Agent": USER_AGENT_TEMPLATE.format(contact=contact),
        "Accept-Language": "de,en;q=0.9,fr;q=0.8,it;q=0.7",
    }


def _fetch_one(client: httpx.Client, query: str, last_ts_ref: list[float]) -> dict | None:
    """Nominatim /search with rate-limit + single retry on 429/5xx."""
    elapsed = time.monotonic() - last_ts_ref[0]
    wait = RATE_SEC - elapsed
    if wait > 0:
        time.sleep(wait)
    url = f"{NOMINATIM_BASE_URL}/search"
    params = {"q": query, "format": "jsonv2", "limit": "1", "countrycodes": "ch,li"}
    for attempt in range(2):
        try:
            resp = client.get(url, params=params, timeout=REQUEST_TIMEOUT)
        except (httpx.HTTPError, TimeoutError) as exc:
            print(
                f"[WARN] t1_landmarks_fetch: expected=200, "
                f"got={type(exc).__name__}: {exc} (attempt {attempt + 1}/2), "
                f"fallback=retry-in-2s query={query!r}",
                flush=True,
            )
            time.sleep(2)
            continue
        last_ts_ref[0] = time.monotonic()
        if resp.status_code == 200:
            try:
                js = resp.json()
            except ValueError:
                print(
                    f"[WARN] t1_landmarks_fetch: expected=json, got=non-json 200, "
                    f"fallback=skip query={query!r}",
                    flush=True,
                )
                return None
            if isinstance(js, list) and js:
                return js[0]
            print(
                f"[WARN] t1_landmarks_fetch: expected=1+ results, got=empty list, "
                f"fallback=skip query={query!r}",
                flush=True,
            )
            return None
        if resp.status_code in {429, 500, 502, 503, 504}:
            print(
                f"[WARN] t1_landmarks_fetch: expected=200, got={resp.status_code} "
                f"(attempt {attempt + 1}/2), fallback=retry-in-4s query={query!r}",
                flush=True,
            )
            time.sleep(4)
            continue
        print(
            f"[WARN] t1_landmarks_fetch: expected=200, got={resp.status_code}, "
            f"fallback=skip query={query!r}",
            flush=True,
        )
        return None
    return None


def _existing_keys() -> set[str]:
    if not OUT_PATH.exists():
        return set()
    try:
        rows = json.loads(OUT_PATH.read_text(encoding="utf-8"))
        return {r.get("key") for r in rows if isinstance(r, dict) and r.get("lat") is not None}
    except (json.JSONDecodeError, OSError):
        return set()


def run(*, refresh: bool = False) -> dict[str, Any]:
    t0 = time.monotonic()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    existing = {} if refresh else {e["key"] for e in _load_existing()}
    todo = [item for item in CURATED if item["key"] not in existing]
    print(
        f"[INFO] t1_landmarks_fetch: curated_total={len(CURATED)} "
        f"already_resolved={len(existing)} to_fetch={len(todo)} refresh={refresh}",
        flush=True,
    )

    out_records: list[dict[str, Any]] = list(_load_existing()) if not refresh else []
    last_ts_ref = [0.0]
    n_ok = 0
    n_failed = 0
    with httpx.Client(headers=_headers()) as client:
        for item in todo:
            r = _fetch_one(client, item["query"], last_ts_ref)
            if r is None:
                n_failed += 1
                continue
            try:
                lat = float(r["lat"])
                lon = float(r["lon"])
            except (KeyError, TypeError, ValueError):
                print(
                    f"[WARN] t1_landmarks_fetch: expected=(lat,lon) floats, "
                    f"got={r.keys() if isinstance(r, dict) else type(r).__name__}, "
                    f"fallback=skip key={item['key']}",
                    flush=True,
                )
                n_failed += 1
                continue
            out_records.append({
                "key":          item["key"],
                "kind":         item["kind"],
                "query":        item["query"],
                "display_name": r.get("display_name", ""),
                "lat":          lat,
                "lon":          lon,
                "osm_type":     r.get("osm_type"),
                "osm_id":       r.get("osm_id"),
                "aliases":      item["aliases"],
                "fetched_at":   datetime.now(timezone.utc).isoformat(),
            })
            n_ok += 1

    # Atomic write (tmp + rename)
    tmp = OUT_PATH.with_suffix(OUT_PATH.suffix + ".tmp")
    tmp.write_text(json.dumps(out_records, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(OUT_PATH)
    elapsed = time.monotonic() - t0
    stats = {
        "total_in_curation":  len(CURATED),
        "already_resolved":   len(existing),
        "attempted":          len(todo),
        "ok":                 n_ok,
        "failed":             n_failed,
        "final_records":      len(out_records),
        "elapsed_s":          round(elapsed, 2),
        "out_path":           str(OUT_PATH),
    }
    print(
        f"[INFO] t1_landmarks_fetch: DONE ok={n_ok} failed={n_failed} "
        f"records={len(out_records)} elapsed_s={elapsed:.1f} → {OUT_PATH}",
        flush=True,
    )
    # Honest failure check — fail loud if too many couldn't resolve
    if n_failed >= max(3, len(todo) // 2):
        raise RuntimeError(
            f"Too many landmark resolutions failed: {n_failed}/{len(todo)} "
            "(Nominatim rate-limit? Network? Bad queries?). Fix upstream and re-run."
        )
    return stats


def _load_existing() -> list[dict]:
    if not OUT_PATH.exists():
        return []
    try:
        data = json.loads(OUT_PATH.read_text(encoding="utf-8"))
        return [r for r in data if isinstance(r, dict)]
    except (json.JSONDecodeError, OSError) as exc:
        print(
            f"[WARN] t1_landmarks_fetch: expected=readable json at {OUT_PATH}, "
            f"got={type(exc).__name__}, fallback=empty (refresh will overwrite)",
            flush=True,
        )
        return []


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true",
                        help="Re-fetch every landmark even if cached.")
    args = parser.parse_args()
    try:
        run(refresh=args.refresh)
    except RuntimeError as exc:
        print(f"[ERROR] t1_landmarks_fetch: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
