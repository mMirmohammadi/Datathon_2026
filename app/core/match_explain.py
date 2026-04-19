"""Per-listing match explanation for the demo UI.

For each listing returned to the user we need to answer:
- Which hard constraints were requested, and what is this listing's value?
- Which BM25 keywords actually appear in this listing's text?
- For every activated soft preference, what is this listing's raw signal?

This module is read-only (no schema mutations, no DB writes) and takes the
candidate dicts, the ``HardFilters`` object the LLM emitted, and the
``listings_ranking_signals`` row (already loaded by the soft-ranker upstream
or fetched here once for the top-K).

Design notes:
- "hard_checks" emits one row per *requested* hard constraint, not per field
  on the schema. A listing can only be here because it passed every one, so
  ``ok`` is always ``True``. We still emit the rows so the UI can show the
  user what was checked and the concrete value.
- "matched_keywords" is a case-insensitive substring check over
  ``title + description + street + city``. This mirrors what FTS5 actually
  indexed so the UI faithfully shows why BM25 hit or missed.
- "soft_facts" interpretation tier is a simple data-driven rule per axis; we
  deliberately avoid percentiles to keep this cheap (no extra query).
"""
from __future__ import annotations

import math
import sqlite3
from typing import Any, Iterable

from app.core import landmarks
from app.core.hard_filters import FEATURE_COLUMN_MAP
from app.models.schemas import HardCheck, HardFilters, MatchDetail, MatchFact, SoftPreferences


_INTERP_GOOD = "good"
_INTERP_OK = "ok"
_INTERP_POOR = "poor"
_INTERP_UNKNOWN = "unknown"


def _fmt_m(value: float | int | None) -> str:
    if value is None:
        return "—"
    if value >= 1000:
        return f"{value / 1000:.1f} km"
    return f"{int(round(value))} m"


def _fmt_int(value: int | float | None) -> str:
    if value is None:
        return "—"
    return str(int(value))


def _fmt_float(value: float | None, precision: int = 1, unit: str = "") -> str:
    if value is None:
        return "—"
    return f"{value:.{precision}f}{unit}"


def _safe(row: sqlite3.Row | dict | None, name: str) -> Any:
    if row is None:
        return None
    try:
        return row[name]
    except (IndexError, KeyError):
        return None


# ---------- hard-check synthesis --------------------------------------------


def _format_city_value(listing: dict[str, Any]) -> str:
    city = listing.get("city") or "—"
    postal = listing.get("postal_code")
    canton = listing.get("canton")
    tail = " ".join(str(part) for part in (postal, canton) if part)
    return f"{city} ({tail})" if tail else str(city)


def _format_price(listing: dict[str, Any]) -> str:
    price = listing.get("price")
    return f"{price} CHF" if price is not None else "— CHF"


def _format_rooms(listing: dict[str, Any]) -> str:
    rooms = listing.get("rooms")
    return f"{rooms} rooms" if rooms is not None else "— rooms"


def _build_hard_checks(
    listing: dict[str, Any], hard: HardFilters
) -> list[HardCheck]:
    checks: list[HardCheck] = []

    if hard.city:
        checks.append(HardCheck(
            label="city",
            requested=", ".join(hard.city),
            value=_format_city_value(listing),
            ok=True,
        ))
    if hard.postal_code:
        checks.append(HardCheck(
            label="postal_code",
            requested=", ".join(hard.postal_code),
            value=str(listing.get("postal_code") or "—"),
            ok=True,
        ))
    if hard.canton:
        checks.append(HardCheck(
            label="canton",
            requested=hard.canton,
            value=str(listing.get("canton") or "—"),
            ok=True,
        ))
    if hard.min_price is not None or hard.max_price is not None:
        lo = hard.min_price if hard.min_price is not None else "−∞"
        hi = hard.max_price if hard.max_price is not None else "+∞"
        checks.append(HardCheck(
            label="price",
            requested=f"{lo} … {hi} CHF",
            value=_format_price(listing),
            ok=True,
        ))
    if hard.min_rooms is not None or hard.max_rooms is not None:
        lo = hard.min_rooms if hard.min_rooms is not None else "−∞"
        hi = hard.max_rooms if hard.max_rooms is not None else "+∞"
        checks.append(HardCheck(
            label="rooms",
            requested=f"{lo} … {hi}",
            value=_format_rooms(listing),
            ok=True,
        ))
    if hard.min_area is not None or hard.max_area is not None:
        lo = hard.min_area if hard.min_area is not None else "−∞"
        hi = hard.max_area if hard.max_area is not None else "+∞"
        area = listing.get("area")
        checks.append(HardCheck(
            label="area",
            requested=f"{lo} … {hi} m²",
            value=f"{int(area)} m²" if area is not None else "—",
            ok=True,
        ))
    if hard.min_floor is not None or hard.max_floor is not None:
        lo = hard.min_floor if hard.min_floor is not None else "−∞"
        hi = hard.max_floor if hard.max_floor is not None else "+∞"
        floor = listing.get("floor")
        checks.append(HardCheck(
            label="floor",
            requested=f"{lo} … {hi}",
            value=str(floor) if floor is not None else "—",
            ok=True,
        ))
    if hard.min_year_built is not None or hard.max_year_built is not None:
        lo = hard.min_year_built if hard.min_year_built is not None else "−∞"
        hi = hard.max_year_built if hard.max_year_built is not None else "+∞"
        yr = listing.get("year_built")
        checks.append(HardCheck(
            label="year_built",
            requested=f"{lo} … {hi}",
            value=str(yr) if yr is not None else "—",
            ok=True,
        ))
    if hard.available_from_after:
        checks.append(HardCheck(
            label="available_after",
            requested=hard.available_from_after,
            value=str(listing.get("available_from") or "—"),
            ok=True,
        ))
    if hard.object_category:
        checks.append(HardCheck(
            label="object_category",
            requested=", ".join(hard.object_category),
            value=str(listing.get("object_category") or "—"),
            ok=True,
        ))

    listing_features = set(listing.get("features") or [])
    for feat in hard.features or []:
        column = FEATURE_COLUMN_MAP.get(feat)
        # Prefer the listing.features array (already parsed from features_json).
        present = feat in listing_features
        if not present and column is not None:
            present = bool(listing.get(column))
        checks.append(HardCheck(
            label=f"feature: {feat}",
            requested="required",
            value="present" if present else "absent (hard filter checked but missing)",
            ok=True,
        ))
    for feat in hard.features_excluded or []:
        present = feat in listing_features
        checks.append(HardCheck(
            label=f"feature: {feat}",
            requested="excluded",
            value="absent" if not present else "present (should not happen)",
            ok=True,
        ))

    return checks


# ---------- keyword hits -----------------------------------------------------


def _keyword_hits(
    listing: dict[str, Any], keywords: list[str] | None
) -> tuple[list[str], list[str]]:
    """Split requested keywords into (matched, unmatched) against listing text.

    Matches title/description/street/city case-insensitively, which is the
    same set of fields FTS5 indexes. Accents are not folded — BM25 doesn't
    fold them either, so this stays faithful to the real retrieval layer.
    """
    if not keywords:
        return [], []
    haystack = " ".join(
        str(listing.get(field) or "")
        for field in ("title", "description", "street", "city")
    ).lower()
    matched: list[str] = []
    unmatched: list[str] = []
    for kw in keywords:
        needle = kw.strip().lower()
        if not needle:
            continue
        if needle in haystack:
            matched.append(kw)
        else:
            unmatched.append(kw)
    return matched, unmatched


# ---------- soft-axis facts -------------------------------------------------


def _price_fact(row: sqlite3.Row | None, sentiment: str) -> MatchFact:
    canton_delta = _safe(row, "price_delta_pct_canton_rooms")
    plz_delta = _safe(row, "price_delta_pct_plz_rooms")
    delta = canton_delta if canton_delta is not None else plz_delta
    basis = "canton×rooms" if canton_delta is not None else "PLZ×rooms"
    # Tier 3b: pull the actual baseline CHF + sample size so the MatchFact
    # can say "… (baseline 2,380 CHF, n=47)" instead of just a relative %.
    baseline_chf: float | None = None
    baseline_n: int | None = None
    if canton_delta is not None:
        baseline_chf = _safe(row, "price_baseline_chf_canton_rooms")
        baseline_n = _safe(row, "price_baseline_n_canton_rooms")
    else:
        baseline_chf = _safe(row, "price_baseline_chf_plz_rooms")
        baseline_n = _safe(row, "price_baseline_n_plz_rooms")
    plausibility = _safe(row, "price_plausibility")

    if delta is None:
        return MatchFact(
            axis="price",
            label="Price vs neighbourhood baseline",
            value="— (no baseline for this bucket)",
            interpretation=_INTERP_UNKNOWN,
        )

    # ``price_delta_pct_*`` is stored as a ratio by t1_price_baselines.py
    # (e.g. -0.15 for "15% below bucket median"), despite the "_pct_" name.
    # See ranking/schema.py:56-64 and ranking/scripts/t1_price_baselines.py:172.
    # Convert to percent exactly once here for both display and thresholding.
    delta_pct = float(delta) * 100.0
    sign = "below" if delta_pct < 0 else "above"
    value = f"{abs(delta_pct):.0f}% {sign} {basis} baseline"
    # Enrich with the concrete numbers when we have them.
    baseline_parts: list[str] = []
    if baseline_chf is not None:
        baseline_parts.append(f"≈ {int(round(baseline_chf)):,} CHF".replace(",", "'"))
    if baseline_n is not None:
        baseline_parts.append(f"n={int(baseline_n)}")
    if baseline_parts:
        value += " (" + ", ".join(baseline_parts) + ")"
    if plausibility == "suspect":
        value += " · flagged as suspect"

    wants_cheap = sentiment == "cheap"
    # Rule: cheap wants lower, premium wants higher. A 10-point gap is
    # "good"; within ±10 is "ok"; opposite sign is "poor".
    good = (wants_cheap and delta_pct <= -10) or (not wants_cheap and delta_pct >= 10)
    poor = (wants_cheap and delta_pct >= 10) or (not wants_cheap and delta_pct <= -10)
    interp = (
        _INTERP_GOOD if good
        else _INTERP_POOR if poor
        else _INTERP_OK
    )
    if plausibility == "suspect":
        interp = _INTERP_POOR
    return MatchFact(
        axis="price",
        label=f"Price vs {basis} baseline (wants {sentiment})",
        value=value,
        interpretation=interp,
    )


def _quiet_fact(row: sqlite3.Row | None) -> MatchFact:
    mway = _safe(row, "dist_motorway_m")
    primary = _safe(row, "dist_primary_road_m")
    rail = _safe(row, "dist_rail_m")
    if mway is None and primary is None:
        return MatchFact(
            axis="quiet",
            label="Distance to motorway + primary road",
            value="— (no coverage for this listing)",
            interpretation=_INTERP_UNKNOWN,
        )
    composite = (mway or 0) + (primary or 0)
    extras: list[str] = []
    if mway is not None:
        extras.append(f"motorway {_fmt_m(mway)}")
    if primary is not None:
        extras.append(f"primary {_fmt_m(primary)}")
    if rail is not None:
        extras.append(f"rail {_fmt_m(rail)}")
    # Heuristic: > 1 km sum → good; 300 m – 1 km → ok; < 300 m → poor.
    if composite >= 1000:
        interp = _INTERP_GOOD
    elif composite >= 300:
        interp = _INTERP_OK
    else:
        interp = _INTERP_POOR
    return MatchFact(
        axis="quiet",
        label="Distance to noise sources",
        value=" · ".join(extras),
        interpretation=interp,
    )


_STOP_TYPE_ICON = {
    # GTFS route_type / simplified OSM stop category → a short label prefix.
    "train": "🚆",
    "rail": "🚆",
    "sbahn": "🚆",
    "s-bahn": "🚆",
    "tram": "🚋",
    "bus": "🚌",
    "funicular": "🚠",
    "ferry": "⛴",
    "metro": "🚇",
    "subway": "🚇",
}


def _stop_icon(stop_type: str | None) -> str:
    """Render a one-char icon for the stop type, or empty on miss."""
    if not stop_type:
        return ""
    key = str(stop_type).strip().lower()
    return _STOP_TYPE_ICON.get(key, "")


def _transit_fact(row: sqlite3.Row | None) -> MatchFact:
    dist = _safe(row, "dist_nearest_stop_m")
    lines = _safe(row, "nearest_stop_lines_count_clamped")
    if lines is None:
        lines = _safe(row, "nearest_stop_lines_count")
    name = _safe(row, "nearest_stop_name")
    stop_type = _safe(row, "nearest_stop_type")
    if dist is None:
        return MatchFact(
            axis="near_public_transport",
            label="Nearest public transport stop",
            value="— (no GTFS match)",
            interpretation=_INTERP_UNKNOWN,
        )
    icon = _stop_icon(stop_type)
    # Tier 3c: prefix the stop with its modal icon so users can tell tram /
    # train / bus apart at a glance. Falls back to no icon if stop_type is
    # missing or not in the lookup.
    name_part = f"{icon} {name}".strip() if icon and name else (name or "")
    pieces = [
        f"{name_part} ({_fmt_m(dist)})" if name_part else _fmt_m(dist)
    ]
    if stop_type:
        pieces.append(f"type: {stop_type}")
    if lines is not None:
        pieces.append(f"{int(lines)} line{'s' if int(lines) != 1 else ''}")
    # < 300 m + ≥ 2 lines → good; < 600 m → ok; otherwise poor.
    if dist < 300 and (lines or 0) >= 2:
        interp = _INTERP_GOOD
    elif dist < 600:
        interp = _INTERP_OK
    else:
        interp = _INTERP_POOR
    return MatchFact(
        axis="near_public_transport",
        label="Nearest public transport stop",
        value=" · ".join(pieces),
        interpretation=interp,
    )


def _commute_fact(
    row: sqlite3.Row | None,
    target: str,
    *,
    commute_rows: dict[tuple[str, str], int] | None = None,
    listing_id: str | None = None,
) -> MatchFact:
    """Commute-minutes fact for the requested HB target.

    Primary source is the r5py / GTFS ``listing_commute_times`` table (real
    door-to-door transit minutes, peak Tuesday 8 AM). Fallback is the wide
    ``commute_proxy_<city>_min`` column (a crude walk-to-stop + Haversine/60 km/h
    approximation) when r5py had no path for this listing. We say which
    source we used in the label so users can tell a real 14-min measurement
    apart from a proxy 2-min estimate.
    """
    short = target.removesuffix("_hb")
    landmark_key = f"hb_{short}"
    real_min: int | None = None
    if commute_rows and listing_id is not None:
        real_min = commute_rows.get((listing_id, landmark_key))

    if real_min is not None:
        val: float = float(real_min)
        source = "r5py transit"
    else:
        proxy = _safe(row, f"commute_proxy_{short}_min")
        if proxy is None:
            return MatchFact(
                axis=f"commute_{short}",
                label=f"Commute to {target}",
                value="— (no commute value for this listing)",
                interpretation=_INTERP_UNKNOWN,
            )
        val = float(proxy)
        source = "proxy (walk + Haversine/60 km/h)"

    if val <= 20:
        interp = _INTERP_GOOD
    elif val <= 45:
        interp = _INTERP_OK
    else:
        interp = _INTERP_POOR
    return MatchFact(
        axis=f"commute_{short}",
        label=f"Commute to {target}",
        value=f"{int(round(val))} min · {source}",
        interpretation=interp,
    )


def _poi_fact(
    row: sqlite3.Row | None, axis: str, label: str, col: str,
    good_at: int, ok_at: int,
) -> MatchFact:
    val = _safe(row, col)
    if val is None:
        return MatchFact(
            axis=axis, label=label,
            value="— (no POI coverage)",
            interpretation=_INTERP_UNKNOWN,
        )
    if val >= good_at:
        interp = _INTERP_GOOD
    elif val >= ok_at:
        interp = _INTERP_OK
    else:
        interp = _INTERP_POOR
    return MatchFact(axis=axis, label=label, value=str(int(val)), interpretation=interp)


def _family_fact(row: sqlite3.Row | None) -> MatchFact:
    play = _safe(row, "poi_playground_500m")
    kinder = _safe(row, "poi_kindergarten_500m")
    if play is None and kinder is None:
        return MatchFact(
            axis="family_friendly",
            label="Playgrounds + kindergartens within 500 m",
            value="— (no POI coverage)",
            interpretation=_INTERP_UNKNOWN,
        )
    total = (play or 0) + (kinder or 0)
    pieces = []
    if play is not None:
        pieces.append(f"{int(play)} playground{'s' if play != 1 else ''}")
    if kinder is not None:
        pieces.append(f"{int(kinder)} kindergarten{'s' if kinder != 1 else ''}")
    if total >= 4:
        interp = _INTERP_GOOD
    elif total >= 1:
        interp = _INTERP_OK
    else:
        interp = _INTERP_POOR
    return MatchFact(
        axis="family_friendly",
        label="Playgrounds + kindergartens within 500 m",
        value=" · ".join(pieces),
        interpretation=interp,
    )


def _landmark_fact(
    row: sqlite3.Row | None,
    name: str,
    *,
    commute_rows: dict[tuple[str, str], int] | None = None,
    listing_id: str | None = None,
) -> MatchFact | None:
    lm = landmarks.resolve(name)
    if lm is None:
        # Unknown landmark — consistent with the silent-disable WARN
        # policy, this is reported as unknown rather than skipped entirely.
        return MatchFact(
            axis=f"landmark:{name}",
            label=f"Distance to '{name}' (not in gazetteer)",
            value="— (unresolved)",
            interpretation=_INTERP_UNKNOWN,
        )
    col = landmarks.column_for(lm.key)
    dist_m = _safe(row, col)
    # Pull the r5py transit-time row too, if available. This is the Tier 2c
    # upgrade: a landmark fact that pairs straight-line distance with real
    # door-to-door transit minutes, so the user sees BOTH axes rather than
    # having to guess whether "1.9 km" is walkable or 40 min by bus.
    transit_min: int | None = None
    if commute_rows and listing_id is not None:
        transit_min = commute_rows.get((listing_id, lm.key))

    if dist_m is None and transit_min is None:
        return MatchFact(
            axis=f"landmark_{lm.key}",
            label=f"Distance to {name}",
            value="—",
            interpretation=_INTERP_UNKNOWN,
        )

    # Interpretation is driven by the strongest signal we have. Transit
    # minutes win when present; Haversine is the fallback.
    if transit_min is not None:
        if transit_min <= 15:
            interp = _INTERP_GOOD
        elif transit_min <= 30:
            interp = _INTERP_OK
        else:
            interp = _INTERP_POOR
    else:
        if dist_m <= 1500:
            interp = _INTERP_GOOD
        elif dist_m <= 5000:
            interp = _INTERP_OK
        else:
            interp = _INTERP_POOR

    parts: list[str] = []
    if dist_m is not None:
        parts.append(_fmt_m(dist_m))
    if transit_min is not None:
        parts.append(f"{int(transit_min)} min by transit")
    value = " · ".join(parts) if parts else "—"

    return MatchFact(
        axis=f"landmark_{lm.key}",
        label=f"Distance to {name}",
        value=value,
        interpretation=interp,
    )


def _build_soft_facts(
    row: sqlite3.Row | None,
    soft: SoftPreferences | None,
    *,
    commute_rows: dict[tuple[str, str], int] | None = None,
    listing_id: str | None = None,
) -> list[MatchFact]:
    if soft is None:
        return []
    facts: list[MatchFact] = []

    if soft.price_sentiment in ("cheap", "premium"):
        facts.append(_price_fact(row, soft.price_sentiment))
    if soft.quiet:
        facts.append(_quiet_fact(row))
    if soft.near_public_transport:
        facts.append(_transit_fact(row))
    if soft.commute_target:
        facts.append(_commute_fact(
            row, soft.commute_target,
            commute_rows=commute_rows, listing_id=listing_id,
        ))
    if soft.near_schools:
        facts.append(_poi_fact(
            row, "near_schools", "Schools within 1 km",
            "poi_school_1km", good_at=3, ok_at=1,
        ))
    if soft.near_supermarket:
        facts.append(_poi_fact(
            row, "near_supermarket", "Supermarkets within 300 m",
            "poi_supermarket_300m", good_at=2, ok_at=1,
        ))
    if soft.near_park:
        facts.append(_poi_fact(
            row, "near_park", "Parks within 500 m",
            "poi_park_500m", good_at=2, ok_at=1,
        ))
    if soft.family_friendly:
        facts.append(_family_fact(row))
    for name in soft.near_landmark or []:
        fact = _landmark_fact(
            row, name,
            commute_rows=commute_rows, listing_id=listing_id,
        )
        if fact is not None:
            facts.append(fact)
    return facts


# ---------- entry point ------------------------------------------------------


def build_match_detail(
    *,
    listing: dict[str, Any],
    hard: HardFilters,
    signal_row: sqlite3.Row | None,
    commute_rows: dict[tuple[str, str], int] | None = None,
) -> MatchDetail:
    matched, unmatched = _keyword_hits(listing, hard.bm25_keywords)
    listing_id = str(listing["listing_id"]) if "listing_id" in listing else None
    return MatchDetail(
        hard_checks=_build_hard_checks(listing, hard),
        matched_keywords=matched,
        unmatched_keywords=unmatched,
        soft_facts=_build_soft_facts(
            signal_row,
            hard.soft_preferences,
            commute_rows=commute_rows,
            listing_id=listing_id,
        ),
    )


def iter_activated_soft_keys(soft: SoftPreferences | None) -> Iterable[str]:
    """Enumerate every activated soft key for the UI (also used by tests)."""
    if soft is None:
        return []
    keys: list[str] = []
    if soft.price_sentiment in ("cheap", "premium"):
        keys.append(f"price:{soft.price_sentiment}")
    if soft.quiet:
        keys.append("quiet")
    if soft.near_public_transport:
        keys.append("near_public_transport")
    if soft.commute_target:
        keys.append(f"commute:{soft.commute_target}")
    if soft.near_schools:
        keys.append("near_schools")
    if soft.near_supermarket:
        keys.append("near_supermarket")
    if soft.near_park:
        keys.append("near_park")
    if soft.family_friendly:
        keys.append("family_friendly")
    for name in soft.near_landmark or []:
        keys.append(f"landmark:{name}")
    return keys
