"""T1.3d — Prune bad entries from `data/ranking/landmarks.json`.

Manual review after geocoding revealed that some GPT-mined + Nominatim-resolved
entries are NOT useful landmarks. This script applies a reviewed DROP list so
the decisions are versioned in source, not hidden in a one-off edit.

Categories of drops (30 entries on the 2026-04-19 review):

  1. DUPLICATES of curated or mined entries (same landmark, same lat/lng).
     Example: `lake` (Vierwaldstättersee) duplicates `lac`;
              `geneve_aeroport` duplicates curated `geneva_airport`.

  2. CHAIN STORES geocoded to an arbitrary single branch — NOT a distinctive
     landmark users would query for proximity. The "Coop" in Stein am Rhein
     is irrelevant to a user 100 km away asking for "nearby shopping".
     Drops: coop, migros, denner, aldi, voi, spar, volg, jumbo, manor,
     centre_commercial, roche, ubs, swisscom, nexus.

  3. GENERIC NOUNS that GPT emitted verbatim without city qualifiers and
     Nominatim resolved to arbitrary instances (random school in Basel,
     random kindergarten in Allschwil, etc.). Drops: primarschule,
     schulhaus, kindergarten, schule.

  4. GEOCODE ERRORS where Nominatim returned the wrong place:
     - `bus` → "Bahnhof, Sissach" (bogus canonical even reached Nominatim).
     - `hb_schaffhausen` → "Bahnhof Stein am Rhein" (wrong town, 20 km off).
     - `stadt` → "Gällen, Weggis" (garbled).

  5. RANDOM SUB-POINTS of cities we already anchor via Hauptbahnhof /
     Altstadt: `bern` (Eigerplatz), `basel` (canton centroid),
     `zurich` (Spitex-Zentrum in Oerlikon), `lausanne` (Stade Olympique).

  6. NON-LAKE entries mislabeled as lake: `see` (Aare river),
     `seepromenade` (random shoreline), `reuss` (river).

Kept mined entries (15) — the genuinely useful additions:
  * `altstadt` (Bern) — users say "old town" without specifying
  * `lac` → Vierwaldstättersee — Swiss lake we didn't curate
  * `rhein` (Basel) — river, high mention count (21), real landmark
  * `zugersee`, `aegerisee` — lakes we didn't curate
  * `sion`, `fribourg`, `plainpalais`, `meyrin`, `cham`, `kriens`,
    `crans_montana`, `herisau` — cities/neighborhoods without curated anchor
  * `hb_luzern`, `hb_olten` — Hauptbahnhöfe we missed in the curated set

Per CLAUDE.md §5: every drop logs the reason before removal.

Usage:
    python -m ranking.scripts.t1_landmarks_prune
    python -m ranking.scripts.t1_landmarks_prune --dry-run    # audit without writing
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LANDMARKS_PATH = Path("data/ranking/landmarks.json")

# --- the reviewed DROP list ------------------------------------------------
# Grouped + commented by category so future re-reviewers see the reasoning.

DROP_KEYS: dict[str, str] = {
    # Category 1: duplicates
    "lake":           "duplicate of `lac` — both resolve to Vierwaldstättersee",
    "geneve_aeroport": "duplicate of curated `geneva_airport`",

    # Category 2: chain stores at arbitrary single branch
    "coop":    "chain store — single Stein am Rhein branch, not a distinctive landmark",
    "migros":  "chain store — single St. Gallen branch",
    "denner":  "chain store — single Cernier branch",
    "aldi":    "chain store — single Delsberg branch",
    "voi":     "chain store — single Bettlach branch",
    "spar":    "chain store — single Davos Dorf branch",
    "volg":    "chain store — single Watt branch",
    "jumbo":   "chain store — single Allschwil branch",
    "manor":   "chain dept-store — single Geneva branch",
    "centre_commercial": "generic — single Uvrier mall, users don't query by this",
    "roche":   "duplicate of curated `roche_basel` (same company, nearby location)",
    "ubs":     "chain bank — single Basel branch, not a distinctive UBS landmark",
    "swisscom": "chain telco — single Basel branch",
    "nexus":   "non-famous local office in Baar",

    # Category 3: generic nouns → arbitrary schools
    "primarschule": "generic — random primary school in Basel",
    "schulhaus":    "generic — random schoolhouse in Glarus Nord",
    "kindergarten": "generic — random kindergarten in Allschwil",
    "schule":       "generic — random school in Watt",

    # Category 4: outright geocode errors
    "bus":            "bogus — canonical 'bus' geocoded to Bahnhof Sissach",
    "hb_schaffhausen": "geocoded to Bahnhof Stein am Rhein (20 km from Schaffhausen)",
    "stadt":          "garbled — resolved to 'Gällen, Weggis'",

    # Category 5: random city sub-points (covered by curated HB / Altstadt)
    "bern":     "random Bern sub-point (Eigerplatz); covered by altstadt_bern + hb_bern",
    "basel":    "canton centroid; covered by altstadt_basel + hb_basel",
    "zurich":   "random Spitex center in Oerlikon; covered by altstadt_zurich + hb_zurich",
    "lausanne": "Stade Olympique, not city centre; covered by hb_lausanne",

    # Category 6: non-lake entries mislabeled under `kind=lake`
    "see":          "resolved to Aare (river), not a lake",
    "seepromenade": "random Seepromenade Buonas in Risch-Rotkreuz",
    "reuss":        "Reuss river, users rarely query by it",
}


def _backup_path(src: Path) -> Path:
    """Timestamped backup of the source file for rollback safety."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return src.with_suffix(f".{ts}.json.bak")


def run(dry_run: bool) -> dict[str, Any]:
    if not LANDMARKS_PATH.exists():
        raise FileNotFoundError(f"{LANDMARKS_PATH} not found")

    data: list[dict[str, Any]] = json.loads(LANDMARKS_PATH.read_text(encoding="utf-8"))
    print(f"[INFO] t1_landmarks_prune: loaded {len(data)} entries from {LANDMARKS_PATH}",
          flush=True)

    kept: list[dict[str, Any]] = []
    dropped: list[tuple[str, str]] = []      # (key, reason)
    missing_drops: list[str] = []              # keys in DROP_KEYS but not in DB

    db_keys = {e.get("key") for e in data if isinstance(e, dict)}
    for k in DROP_KEYS:
        if k not in db_keys:
            missing_drops.append(k)

    for entry in data:
        if not isinstance(entry, dict):
            continue
        key = entry.get("key")
        if key in DROP_KEYS:
            reason = DROP_KEYS[key]
            dropped.append((key, reason))
            print(
                f"[INFO] t1_landmarks_prune: DROP key={key!r} kind={entry.get('kind')!r} "
                f"reason={reason!r}",
                flush=True,
            )
        else:
            kept.append(entry)

    if missing_drops:
        # DROP_KEYS declares things that no longer exist in the JSON — the
        # list is stale. Log loud but don't fail; keeps the script idempotent.
        print(
            f"[WARN] t1_landmarks_prune: expected=DROP keys present in "
            f"{LANDMARKS_PATH}, got={len(missing_drops)} missing "
            f"({missing_drops}), fallback=continue (prune list ahead of DB)",
            flush=True,
        )

    print(
        f"[INFO] t1_landmarks_prune: kept={len(kept)} dropped={len(dropped)} "
        f"dry_run={dry_run}",
        flush=True,
    )

    if dry_run:
        return {"kept": len(kept), "dropped": len(dropped),
                "missing_drops": missing_drops, "written": False}

    # Atomic: tmp + rename. Backup first for rollback safety.
    backup = _backup_path(LANDMARKS_PATH)
    LANDMARKS_PATH.rename(backup)
    print(f"[INFO] t1_landmarks_prune: backed up previous file → {backup}",
          flush=True)

    tmp = LANDMARKS_PATH.with_suffix(LANDMARKS_PATH.suffix + ".tmp")
    tmp.write_text(json.dumps(kept, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(LANDMARKS_PATH)
    print(f"[INFO] t1_landmarks_prune: wrote {len(kept)} entries → {LANDMARKS_PATH}",
          flush=True)

    return {
        "kept":           len(kept),
        "dropped":        len(dropped),
        "missing_drops":  missing_drops,
        "backup_path":    str(backup),
        "written":        True,
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would be dropped without writing.")
    args = p.parse_args()
    try:
        stats = run(args.dry_run)
    except FileNotFoundError as exc:
        print(f"[ERROR] t1_landmarks_prune: {exc}", file=sys.stderr)
        return 2
    print(f"[INFO] t1_landmarks_prune DONE {stats}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
