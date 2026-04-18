"""T1.3b — Aggregate GPT-mined landmark mentions into geocoding candidates.

Reads the Pass 4 JSONL cache (`enrichment/data/cache/gpt_landmark_mining.jsonl`)
and produces a deduplicated list of landmark CANDIDATES for the geocode step:

    data/ranking/landmarks_mined_candidates.json

Each candidate has:
    - canonical:     lowercase snake_case stable key
    - kind:          LandmarkKind from the pass4 schema
    - best_query:    the string to forward-geocode via Nominatim
    - names[]:       every surface form we saw for this canonical (for aliases)
    - mention_count: number of DISTINCT listings that mentioned it
    - cities[]:      up to 5 most-common cities the mentioning listings lie in
                     (helps disambiguate "Bahnhof" → which Bahnhof)

Dedup logic:
  1. Primary group by (canonical, kind).
  2. Fuzzy-merge within kind: canonicals with rapidfuzz token_sort_ratio >= 90
     AND one being a strict substring of the other → merge into the shorter.
     e.g. `eth_zentrum_zurich` + `eth_zentrum` → `eth_zentrum`.
  3. Drop groups with mention_count < MIN_MENTIONS (default 5).
  4. Drop groups whose canonical is in the already-curated set (they're
     the hand-picked 30 that we already have and don't want to re-geocode).

Per CLAUDE.md §5: every drop reason is counted in `stats` and summarised.

Usage:
    python -m ranking.scripts.t1_landmarks_aggregate
    python -m ranking.scripts.t1_landmarks_aggregate --min-mentions 3 --max-candidates 300
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

CACHE_PATH = Path("enrichment/data/cache/gpt_landmark_mining.jsonl")
EXISTING_LANDMARKS = Path("data/ranking/landmarks.json")
OUT_PATH = Path("data/ranking/landmarks_mined_candidates.json")
LISTINGS_DB = Path("data/listings.db")

# Kinds that we WILL geocode via Nominatim. "other" is dropped unless it was
# explicitly given a specific-enough canonical (post-dedup heuristic).
GEOCODABLE_KINDS: frozenset[str] = frozenset({
    "transit", "university", "school", "employer", "shopping",
    "park", "hospital", "neighborhood", "cultural", "lake",
})

DEFAULT_MIN_MENTIONS = 5
DEFAULT_MAX_CANDIDATES = 300
# Kept for backwards compatibility with tests that may reference it; the
# merge algorithm no longer uses rapidfuzz ratios — see
# `_is_token_boundary_substring` for the actual gate.
FUZZY_MERGE_THRESHOLD = 90


def _load_cache() -> list[dict[str, Any]]:
    """Read every record from the Pass-4 JSONL cache."""
    if not CACHE_PATH.exists():
        raise FileNotFoundError(
            f"Pass-4 cache not found at {CACHE_PATH}. Run "
            "`python -m enrichment.scripts.pass4_landmark_mining --db data/listings.db` first."
        )
    recs: list[dict[str, Any]] = []
    with CACHE_PATH.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                recs.append(json.loads(line))
            except json.JSONDecodeError as exc:
                print(
                    f"[WARN] t1_landmarks_aggregate: expected=valid jsonl "
                    f"at {CACHE_PATH}:{line_no}, got={exc!r}, fallback=skip line",
                    flush=True,
                )
    return recs


def _load_existing_canonicals() -> set[str]:
    """Return the set of keys already in data/ranking/landmarks.json.

    We won't emit mined candidates whose canonical overlaps an existing key —
    the hand-curated entries are the source of truth for those landmarks.
    """
    if not EXISTING_LANDMARKS.exists():
        return set()
    try:
        data = json.loads(EXISTING_LANDMARKS.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(
            f"[WARN] t1_landmarks_aggregate: expected=readable json at "
            f"{EXISTING_LANDMARKS}, got={exc!r}, fallback=empty_set",
            flush=True,
        )
        return set()
    return {r["key"] for r in data if isinstance(r, dict) and "key" in r}


def _listing_city_map() -> dict[str, str]:
    """{listing_id → city_filled} so we can enrich candidates with city hints."""
    if not LISTINGS_DB.exists():
        return {}
    con = sqlite3.connect(LISTINGS_DB)
    try:
        rows = con.execute(
            "SELECT listing_id, city_filled FROM listings_enriched "
            "WHERE city_filled IS NOT NULL AND city_filled != 'UNKNOWN'"
        ).fetchall()
        return {r[0]: r[1] for r in rows}
    finally:
        con.close()


def _stratified_listing_ids(n_per_canton: int) -> set[str]:
    """Return the set of listing_ids that a canton-stratified
    `--stratify-canton-n N` pass4 run would target.

    Mirror of the window-function SQL in `pass4_landmark_mining._fetch_listings`
    (kept as a sibling here so the aggregator can reproduce the selection
    without depending on pass4's internals). Using the same listing set for
    aggregation that we asked GPT to mine is the whole point of stratified
    sampling — random over-representation from large cantons (ZH, BE, VD)
    would otherwise dominate the mention counts.
    """
    if not LISTINGS_DB.exists():
        raise FileNotFoundError(f"{LISTINGS_DB} not found — run bootstrap.")
    con = sqlite3.connect(LISTINGS_DB)
    try:
        rows = con.execute(
            """
            WITH ranked AS (
                SELECT l.listing_id,
                       ROW_NUMBER() OVER (PARTITION BY le.canton_filled
                                          ORDER BY l.listing_id) AS rk
                FROM listings l
                JOIN listings_enriched le USING(listing_id)
                WHERE l.description IS NOT NULL AND l.description != ''
                  AND le.canton_filled != 'UNKNOWN'
            )
            SELECT listing_id FROM ranked WHERE rk <= ?
            """,
            (int(n_per_canton),),
        ).fetchall()
        return {r[0] for r in rows}
    finally:
        con.close()


def _group_by_canonical(
    recs: list[dict[str, Any]],
    city_map: dict[str, str],
) -> dict[tuple[str, str], dict[str, Any]]:
    """First-pass groupby (canonical, kind).

    Each group accumulates:
      - listing_ids:   set of distinct listings
      - names:         Counter of surface forms
      - cities:        Counter of enriched city names from the mentioning listings
    """
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for rec in recs:
        lid = rec["listing_id"]
        city = city_map.get(lid)
        for m in rec.get("mentions", []):
            canonical = (m.get("canonical") or "").strip().lower()
            kind = m.get("kind", "other")
            name = (m.get("name") or "").strip()
            if not canonical:
                continue
            key = (canonical, kind)
            if key not in groups:
                groups[key] = {
                    "canonical": canonical,
                    "kind":      kind,
                    "listing_ids": set(),
                    "names":     Counter(),
                    "cities":    Counter(),
                }
            g = groups[key]
            g["listing_ids"].add(lid)
            if name:
                g["names"][name] += 1
            if city:
                g["cities"][city] += 1
    return groups


def _is_token_boundary_substring(shorter: str, longer: str) -> bool:
    """True iff `shorter` appears as a whole-token subsequence of `longer`,
    with tokens delimited by `_` or string boundaries.

    Merge-worthy pairs (return True):
      - shorter='eth_zentrum', longer='eth_zentrum_zurich'
      - shorter='bahnhof_bern', longer='bahnhof_bern_bundesplatz'
      - shorter='hb_zurich', longer='hb_zurich_sbb'

    Non-merge pairs (return False):
      - shorter='eth', longer='something_different_ending_in_eth_xyz' — NO,
        actually that DOES match at token boundary, but:
      - shorter='a', longer='abcdefg' — NO (no underscore, not at boundary)
      - shorter='hb', longer='hbz_zurich' — NO (hb isn't a whole token in hbz)

    Pure-logic — no rapidfuzz needed because `_A_ in _B_` is the exact
    semantics we want.
    """
    # Degenerate cases: reject very-short shorts to avoid nonsense merges.
    if not shorter or not longer or shorter == longer:
        return False
    if len(shorter) < 3:
        return False
    return f"_{shorter}_" in f"_{longer}_"


def _fuzzy_merge(groups: dict[tuple[str, str], dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    """Union-find merge of canonicals that are token-boundary substrings of
    each other within the same kind. Always prefers the shorter canonical
    as the merge root (more general form wins).

    Transitive closure is computed — if A ⊂ B ⊂ C, all three merge to A.

    Example merges:
      - ('eth_zentrum_zurich', 'university') + ('eth_zentrum', 'university')
        → single ('eth_zentrum', 'university')
      - ('bahnhof', 'transit') + ('bahnhof_bern', 'transit') +
        ('bahnhof_bern_sbb', 'transit') → all merge to ('bahnhof', 'transit')
    """
    # Index canonicals by kind.
    by_kind: dict[str, list[str]] = defaultdict(list)
    for (canonical, kind) in groups.keys():
        by_kind[kind].append(canonical)

    # Union-find within each kind. Root = shortest canonical in the class.
    parent_by_kind: dict[str, dict[str, str]] = {}
    for kind, canonicals in by_kind.items():
        parent: dict[str, str] = {c: c for c in canonicals}

        def _find(x: str) -> str:
            # Path-compress up to the root.
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def _union(a: str, b: str) -> None:
            ra, rb = _find(a), _find(b)
            if ra == rb:
                return
            # Prefer shorter as root; stable tie-break on lexicographic order.
            if (len(ra), ra) <= (len(rb), rb):
                parent[rb] = ra
            else:
                parent[ra] = rb

        # O(n²) scan — n is tiny (≤ few hundred canonicals per kind).
        sorted_canons = sorted(canonicals, key=lambda s: (len(s), s))
        for i, shorter in enumerate(sorted_canons):
            for longer in sorted_canons[i + 1:]:
                if _is_token_boundary_substring(shorter, longer):
                    _union(shorter, longer)
        parent_by_kind[kind] = parent

    # Apply merges: each (canonical, kind) → (root, kind).
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    n_merged_pairs = 0
    for (canonical, kind), g in groups.items():
        parent = parent_by_kind[kind]
        # Resolve to the root.
        root = canonical
        while parent[root] != root:
            root = parent[root]
        target = (root, kind)
        if root != canonical:
            n_merged_pairs += 1
        if target not in merged:
            merged[target] = {
                "canonical": target[0],
                "kind":      target[1],
                "listing_ids": set(),
                "names":     Counter(),
                "cities":    Counter(),
            }
        merged[target]["listing_ids"] |= g["listing_ids"]
        merged[target]["names"].update(g["names"])
        merged[target]["cities"].update(g["cities"])

    if n_merged_pairs:
        print(
            f"[INFO] t1_landmarks_aggregate: fuzzy-merged {n_merged_pairs} canonicals "
            f"(before={len(groups)}, after={len(merged)})",
            flush=True,
        )
    return merged


def _best_query(g: dict[str, Any]) -> str:
    """Pick the best Nominatim query string from the collected surface names.

    Strategy (tuned after observing 90% Nominatim miss rate on naive picks):

      1. Start with the MOST-FREQUENT surface form (tie-break shortest). The
         most-common phrasing is usually the canonical one, not the
         longest adjective-laden variant ("UNESCO-geschützte Altstadt von
         Bern" is longer but "Altstadt" is more queryable).
      2. Reject surface forms that look like generic nouns (no capital
         letter, or <=3 chars, or contains only a single word) — these
         come from loose GPT extractions on phrases like "der Bahnhof"
         that don't include a city.
      3. If no name passes, fall back to `canonical.replace("_", " ")`.
      4. Append ", <top_city>" when the query lacks an explicit city.

    Result: queries like "Altstadt, Bern" instead of
    "UNESCO-geschützte Altstadt von Bern" — which Nominatim resolves.
    """
    def _looks_queryable(s: str) -> bool:
        s = s.strip()
        if len(s) < 4:
            return False
        # Require at least one uppercase letter (proper noun) OR a digit.
        return any(c.isupper() for c in s) or any(c.isdigit() for c in s)

    # Try the most-common name first, then shorter variants.
    for name, _freq in sorted(
        g["names"].items(),
        key=lambda kv: (-kv[1], len(kv[0])),  # freq desc, len asc
    ):
        if _looks_queryable(name):
            best_name = name
            break
    else:
        # Every surface form rejected — synthesise from canonical.
        best_name = g["canonical"].replace("_", " ").title()

    top_city = g["cities"].most_common(1)[0][0] if g["cities"] else None
    if top_city and top_city.lower() not in best_name.lower():
        return f"{best_name}, {top_city}"
    return best_name


def run(
    min_mentions: int,
    max_candidates: int,
    *,
    stratify_canton_n: int | None = None,
) -> dict[str, Any]:
    """Aggregate + emit candidates JSON.

    Returns a stats dict for the caller / main to print.

    `stratify_canton_n`: if set, only cache records whose listing_id is in
    the canton-stratified set (first N by listing_id per canton_filled,
    UNKNOWN excluded) contribute to the aggregation. This mirrors the
    pass4 `--stratify-canton-n` flag and keeps per-canton landmark
    coverage balanced — without it, large-canton listings (ZH/BE/VD)
    dominate and rural-canton landmarks get sub-threshold mention counts.
    """
    stats: Counter = Counter()

    recs = _load_cache()
    stats["cache_records"] = len(recs)
    print(
        f"[INFO] t1_landmarks_aggregate: loaded {len(recs)} cache records",
        flush=True,
    )

    # Optional stratified filter — drop cache records whose listing_id isn't
    # in the target stratified set.
    if stratify_canton_n is not None:
        allowed = _stratified_listing_ids(stratify_canton_n)
        before = len(recs)
        recs = [r for r in recs if r["listing_id"] in allowed]
        stats["stratified_kept"] = len(recs)
        stats["stratified_dropped"] = before - len(recs)
        print(
            f"[INFO] t1_landmarks_aggregate: stratified filter "
            f"(n_per_canton={stratify_canton_n}) kept {len(recs)}/{before} "
            f"records ({before - len(recs)} dropped for not being in the "
            f"target set)",
            flush=True,
        )

    existing = _load_existing_canonicals()
    print(
        f"[INFO] t1_landmarks_aggregate: {len(existing)} existing curated "
        f"landmarks will be excluded from candidates",
        flush=True,
    )

    city_map = _listing_city_map()
    print(
        f"[INFO] t1_landmarks_aggregate: {len(city_map)} listing→city entries "
        f"loaded from listings_enriched",
        flush=True,
    )

    groups = _group_by_canonical(recs, city_map)
    stats["raw_groups"] = len(groups)
    print(f"[INFO] t1_landmarks_aggregate: raw (canonical, kind) groups: {len(groups)}",
          flush=True)

    groups = _fuzzy_merge(groups)
    stats["groups_after_merge"] = len(groups)

    candidates: list[dict[str, Any]] = []
    for (canonical, kind), g in groups.items():
        mention_count = len(g["listing_ids"])
        if mention_count < min_mentions:
            stats["dropped_below_threshold"] += 1
            continue
        if canonical in existing:
            stats["dropped_already_curated"] += 1
            continue
        if kind not in GEOCODABLE_KINDS:
            stats[f"dropped_kind_{kind}"] += 1
            continue
        best_query = _best_query(g)
        candidates.append({
            "canonical":     canonical,
            "kind":          kind,
            "best_query":    best_query,
            "names":         [n for n, _ in g["names"].most_common(10)],
            "mention_count": mention_count,
            "cities":        [c for c, _ in g["cities"].most_common(5)],
        })

    # Sort candidates by mention_count descending; truncate at max.
    candidates.sort(key=lambda c: -c["mention_count"])
    if len(candidates) > max_candidates:
        print(
            f"[WARN] t1_landmarks_aggregate: expected=<={max_candidates} candidates, "
            f"got={len(candidates)}, fallback=truncating to max_candidates "
            f"(keeping highest mention_count)",
            flush=True,
        )
        stats["truncated_at_max"] = len(candidates) - max_candidates
        candidates = candidates[:max_candidates]
    stats["emitted"] = len(candidates)

    # Atomic write
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT_PATH.with_suffix(OUT_PATH.suffix + ".tmp")
    tmp.write_text(json.dumps(candidates, ensure_ascii=False, indent=2),
                   encoding="utf-8")
    tmp.replace(OUT_PATH)

    print(f"[INFO] t1_landmarks_aggregate: wrote {len(candidates)} candidates → {OUT_PATH}",
          flush=True)
    # Top 20 preview
    print("[INFO] t1_landmarks_aggregate: top 20 by mention_count:", flush=True)
    for c in candidates[:20]:
        print(
            f"    {c['canonical']:40s} kind={c['kind']:12s} "
            f"n={c['mention_count']:>5}  query={c['best_query']!r}",
            flush=True,
        )
    return {**stats, "total_candidates": len(candidates)}


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--min-mentions", type=int, default=DEFAULT_MIN_MENTIONS,
                   help=f"Drop canonicals with fewer distinct mentioning "
                        f"listings (default {DEFAULT_MIN_MENTIONS}).")
    p.add_argument("--max-candidates", type=int, default=DEFAULT_MAX_CANDIDATES,
                   help=f"Emit at most N candidates (default "
                        f"{DEFAULT_MAX_CANDIDATES}), ranked by mention_count.")
    p.add_argument("--stratify-canton-n", type=int, default=None,
                   help="Only aggregate from the canton-stratified listing "
                        "subset (first N by listing_id per canton, UNKNOWN "
                        "canton excluded). Mirrors pass4's flag of the same "
                        "name; pass the same value to both scripts.")
    args = p.parse_args()
    try:
        stats = run(
            args.min_mentions,
            args.max_candidates,
            stratify_canton_n=args.stratify_canton_n,
        )
    except FileNotFoundError as exc:
        print(f"[ERROR] t1_landmarks_aggregate: {exc}", file=sys.stderr)
        return 2
    print(f"[INFO] t1_landmarks_aggregate DONE {dict(stats)}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
