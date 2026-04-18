"""Pass 2 — multilingual regex extraction from `description` + `title`.

Full M4 scope:
  * 12 feature flags (balcony, elevator, parking, garage, fireplace,
    child_friendly, pets_allowed, temporary, new_build,
    wheelchair_accessible, private_laundry, minergie_certified)
  * year_built (1800..now+5, captured as string)
  * agency_phone (Swiss phone, normalized to `+41 AA BBB CC DD`)
  * agency_email (RFC-5322-lite, lowercased)
  * floor (ground/basement lexemes + numeric patterns)
  * area (m² with 10..500 validation)
  * available_from (immediate lexemes + ISO + European date)
  * agency_name (derived from agency_email domain)

Rules:
  * Only overwrites rows where <field>_source='UNKNOWN-pending'. Never clobbers
    'original' or 'rev_geo_offline' or any other persisted source.
  * HTML is stripped before regex scanning (common.langdet:strip_html).
  * Language detected via common.langdet:guess_lang; detected_lang is tried
    first; other languages are tried next at 0.6× base confidence.
  * Negation guard uses a 3-token lookback window. Negated matches: for
    boolean features they become '0' (explicit denial); for numeric/text
    fields they're dropped (row stays pending).

Usage:
    python -m enrichment.scripts.pass2_text_extract --db /data/listings.db
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from enrichment.common.confidence import compute_confidence
from enrichment.common.db import connect
from enrichment.common.langdet import guess_lang, strip_html
from enrichment.common.provenance import UNKNOWN_VALUE, write_field
from enrichment.common.sources import UNKNOWN_PENDING
from enrichment.common.text_extract import ExtractionHit, find_first_match

PATTERNS_DIR = Path(__file__).resolve().parents[1] / "patterns"

# 12 feature flag field names — keep in sync with enrichment.schema.FIELDS.
FEATURE_NAMES: tuple[str, ...] = (
    "balcony", "elevator", "parking", "garage", "fireplace",
    "child_friendly", "pets_allowed", "temporary", "new_build",
    "wheelchair_accessible", "private_laundry", "minergie_certified",
)


def _load_yaml(name: str) -> dict:
    path = PATTERNS_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"Pattern file missing: {path}")
    with path.open() as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"{path} must be a dict, got {type(data).__name__}")
    return data


def _source_for(lang_used: str) -> str:
    mapping = {
        "de": "text_regex_de",
        "fr": "text_regex_fr",
        "it": "text_regex_it",
        "en": "text_regex_en",
    }
    if lang_used in mapping:
        return mapping[lang_used]
    # CLAUDE.md §5: never silently default. find_first_match should only emit
    # one of the four languages; if we see something else, announce and map to 'en'.
    print(
        f"[WARN] pass2_text_extract._source_for: expected=lang_in_{set(mapping)} "
        f"got={lang_used!r} fallback=text_regex_en",
        flush=True,
    )
    return "text_regex_en"


def _build_lang_dict(spec: dict) -> dict[str, list[str]]:
    """Convert a YAML pattern spec to {lang -> [regex, ...]}.

    Supports either per-language keys (de/fr/it/en) OR a single 'all' key
    for language-agnostic patterns (phone, email, Minergie brand).
    """
    if "all" in spec:
        all_pats = spec["all"]
        return {"de": all_pats, "fr": all_pats, "it": all_pats, "en": all_pats}
    return {
        "de": spec.get("de", []),
        "fr": spec.get("fr", []),
        "it": spec.get("it", []),
        "en": spec.get("en", []),
    }


def _validate_year(value: str, now_year: int) -> str | None:
    try:
        y = int(value)
    except (TypeError, ValueError):
        return None
    if 1800 <= y <= now_year + 5:
        return str(y)
    return None


# ---------- Floor ----------

def _extract_floor(
    text: str,
    detected_lang: str,
    patterns: dict[str, tuple[float, dict[str, list[str]]]],
    negation_patterns: dict[str, list[str]],
    lookback: int,
) -> ExtractionHit | tuple[str, float, str, str] | None:
    """Priority: ground > basement > numeric.

    Returns (value_str, confidence, lang_used, raw_snippet) OR None.
    value_str: "0" for ground, "-1" for basement, "1".."99" for numeric.
    """
    # ground
    base, langs = patterns["floor_ground"]
    hit = find_first_match(text, langs, detected_lang, negation_patterns, lookback)
    if hit and not hit.negated:
        conf = compute_confidence(base, lang_match=(hit.lang_used == detected_lang))
        return ("0", conf, hit.lang_used, hit.value)
    # basement
    base, langs = patterns["floor_basement"]
    hit = find_first_match(text, langs, detected_lang, negation_patterns, lookback)
    if hit and not hit.negated:
        conf = compute_confidence(base, lang_match=(hit.lang_used == detected_lang))
        return ("-1", conf, hit.lang_used, hit.value)
    # numeric
    base, langs = patterns["floor_numeric"]
    hit = find_first_match(text, langs, detected_lang, negation_patterns, lookback)
    if hit and not hit.negated and hit.groups:
        try:
            n = int(hit.groups[0])
        except ValueError:
            return None
        if not (1 <= n <= 99):
            return None
        conf = compute_confidence(base, lang_match=(hit.lang_used == detected_lang))
        return (str(n), conf, hit.lang_used, hit.value)
    return None


# ---------- Area ----------

AREA_MIN_SQM = 10
AREA_MAX_SQM = 500


def _validate_area(value: str) -> str | None:
    try:
        a = int(value)
    except (TypeError, ValueError):
        return None
    if AREA_MIN_SQM <= a <= AREA_MAX_SQM:
        return str(a)
    return None


# ---------- Available-from ----------

def _today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _european_to_iso(day: str, month: str, year: str) -> str | None:
    try:
        d, m, y = int(day), int(month), int(year)
    except (TypeError, ValueError):
        return None
    try:
        dt = datetime(year=y, month=m, day=d).date()
    except ValueError:
        return None
    return _validate_availability_date(dt.isoformat())


def _validate_availability_date(iso_date: str) -> str | None:
    """Reject dates outside [today - 90d, today + 2y]."""
    try:
        dt = datetime.fromisoformat(iso_date).date()
    except ValueError:
        return None
    today = datetime.now(timezone.utc).date()
    from datetime import timedelta
    lo = today - timedelta(days=90)
    hi = today + timedelta(days=2 * 365)
    if lo <= dt <= hi:
        return dt.isoformat()
    return None


def _extract_available_from(
    text: str,
    detected_lang: str,
    patterns: dict[str, tuple[float, dict[str, list[str]]]],
    negation_patterns: dict[str, list[str]],
    lookback: int,
) -> tuple[str, float, str, str] | None:
    """Priority: immediate > ISO > European DD.MM.YYYY.

    Returns (iso_date, confidence, lang_used, raw_snippet) OR None.
    """
    # immediate
    base, langs = patterns["available_from_immediate"]
    hit = find_first_match(text, langs, detected_lang, negation_patterns, lookback)
    if hit and not hit.negated:
        conf = compute_confidence(base, lang_match=(hit.lang_used == detected_lang))
        return (_today_iso(), conf, hit.lang_used, hit.value)
    # ISO date
    base, langs = patterns["available_from_iso"]
    hit = find_first_match(text, langs, detected_lang, negation_patterns, lookback)
    if hit and not hit.negated and hit.groups:
        validated = _validate_availability_date(hit.groups[0])
        if validated:
            conf = compute_confidence(base, lang_match=(hit.lang_used == detected_lang))
            return (validated, conf, hit.lang_used, hit.value)
    # European date
    base, langs = patterns["available_from_european"]
    hit = find_first_match(text, langs, detected_lang, negation_patterns, lookback)
    if hit and not hit.negated and len(hit.groups) >= 3:
        validated = _european_to_iso(hit.groups[0], hit.groups[1], hit.groups[2])
        if validated:
            conf = compute_confidence(base, lang_match=(hit.lang_used == detected_lang))
            return (validated, conf, hit.lang_used, hit.value)
    return None


# ---------- Agency name ----------

def _derive_agency_name_from_email(email: str) -> str | None:
    """Turn 'info@robinreal.ch' → 'Robinreal'. None on invalid input."""
    if not email or "@" not in email:
        return None
    _, _, domain = email.partition("@")
    domain = domain.strip().lower()
    if not domain:
        return None
    parts = domain.split(".")
    # Heuristic: the "business name" is the second-to-last label (e.g.
    # 'comparis.ch' -> 'comparis', 'info.robinreal.ch' -> 'robinreal').
    # For plain 'robinreal.ch' that's parts[-2].
    if len(parts) < 2:
        return None
    name = parts[-2].strip()
    if not name:
        return None
    # Guard against generic email providers masquerading as agency domains.
    if name in {"gmail", "bluewin", "hotmail", "yahoo", "outlook", "gmx", "icloud",
                "protonmail", "mail", "proton", "swissmail"}:
        return None
    # Title-case for display (robinreal → Robinreal, comparis → Comparis).
    return name.capitalize()


def _normalize_phone(hit: ExtractionHit) -> str:
    """Return a normalized `+41 AA BBB CC DD` string from a phone match.

    The YAML pattern has 4 capture groups (area_code, g2, g3, g4); reassemble
    them into E.164-style. If groups are missing (shouldn't happen given the
    pattern), log [WARN] and fall back to stripping the raw match — so a
    downstream consumer can't mistake an un-normalized phone for a normalized one
    without the log being visible.
    """
    if len(hit.groups) == 4 and all(hit.groups):
        g1, g2, g3, g4 = hit.groups
        return f"+41 {g1} {g2} {g3} {g4}"
    print(
        f"[WARN] pass2_text_extract._normalize_phone: expected=4_capture_groups "
        f"got={hit.groups!r} raw_match={hit.value!r} fallback=stripped_raw_value",
        flush=True,
    )
    return hit.value.strip()


def run(db_path: Path) -> dict[str, int]:
    conn = connect(db_path)
    stats: Counter[str] = Counter()
    try:
        # --- Load patterns ---
        features_yaml = _load_yaml("features.yaml")
        year_built_yaml = _load_yaml("year_built.yaml")
        agency_phone_yaml = _load_yaml("agency_phone.yaml")
        agency_email_yaml = _load_yaml("agency_email.yaml")
        floor_yaml = _load_yaml("floor.yaml")
        area_yaml = _load_yaml("area.yaml")
        available_from_yaml = _load_yaml("available_from.yaml")
        negation_yaml = _load_yaml("negation.yaml")

        negation_patterns: dict[str, list[str]] = {
            "de": negation_yaml.get("de", []),
            "fr": negation_yaml.get("fr", []),
            "it": negation_yaml.get("it", []),
            "en": negation_yaml.get("en", []),
        }
        lookback = int(negation_yaml.get("lookback_tokens", 3))

        # Feature specs: {feature_name -> (base_confidence, {lang -> [regex]})}
        feature_specs: dict[str, tuple[float, dict[str, list[str]]]] = {}
        for name in FEATURE_NAMES:
            if name not in features_yaml:
                raise RuntimeError(f"features.yaml missing entry for {name!r}")
            spec = features_yaml[name]
            feature_specs[name] = (
                float(spec.get("base_confidence", 0.7)),
                _build_lang_dict(spec),
            )

        year_base = float(year_built_yaml["year_built"]["base_confidence"])
        year_langs = _build_lang_dict(year_built_yaml["year_built"])

        phone_base = float(agency_phone_yaml["agency_phone"]["base_confidence"])
        phone_langs = _build_lang_dict(agency_phone_yaml["agency_phone"])

        email_base = float(agency_email_yaml["agency_email"]["base_confidence"])
        email_langs = _build_lang_dict(agency_email_yaml["agency_email"])

        # Floor sub-patterns: 3 keys, each with its own base_confidence + lang map.
        floor_patterns: dict[str, tuple[float, dict[str, list[str]]]] = {}
        for k in ("floor_ground", "floor_basement", "floor_numeric"):
            if k not in floor_yaml:
                raise RuntimeError(f"floor.yaml missing entry for {k!r}")
            floor_patterns[k] = (
                float(floor_yaml[k].get("base_confidence", 0.8)),
                _build_lang_dict(floor_yaml[k]),
            )

        area_base = float(area_yaml["area"]["base_confidence"])
        area_langs = _build_lang_dict(area_yaml["area"])

        avail_patterns: dict[str, tuple[float, dict[str, list[str]]]] = {}
        for k in ("available_from_immediate", "available_from_iso", "available_from_european"):
            if k not in available_from_yaml:
                raise RuntimeError(f"available_from.yaml missing entry for {k!r}")
            avail_patterns[k] = (
                float(available_from_yaml[k].get("base_confidence", 0.8)),
                _build_lang_dict(available_from_yaml[k]),
            )

        now_year = datetime.now(timezone.utc).year

        # --- Select candidates ---
        # Any row with at least one pending target field is a candidate. Simpler:
        # just iterate every row; the write_field guard + pre-check skip ones
        # where nothing is pending.
        rows = conn.execute("""
            SELECT l.listing_id, l.title, l.description
            FROM listings l
            JOIN listings_enriched le USING(listing_id);
        """).fetchall()
        stats["rows_scanned"] = len(rows)

        # Pre-fetch per-row source state in one batch so we skip non-pending fields
        # quickly. All fields pass 2 can possibly fill:
        pending_cols = [f"feature_{f}_source" for f in FEATURE_NAMES] + [
            "year_built_source", "agency_phone_source", "agency_email_source",
            "agency_name_source", "floor_source", "area_source", "available_from_source",
        ]
        sources_map: dict[str, dict[str, str]] = {}
        for r in conn.execute(f"""
            SELECT listing_id, {", ".join(pending_cols)} FROM listings_enriched;
        """):
            sources_map[r[0]] = {col: r[i] for i, col in enumerate(pending_cols, start=1)}

        for row in rows:
            listing_id = row["listing_id"]
            raw_text = f"{row['title'] or ''}\n{row['description'] or ''}"
            text = strip_html(raw_text)
            if not text.strip():
                continue

            detected_lang = guess_lang(text)
            if detected_lang == "unk":
                # Still try all languages; compute_confidence will cross-penalize.
                detected_lang = "de"  # majority per REPORT §8 L161

            row_sources = sources_map.get(listing_id, {})

            # --- 12 features ---
            for feat in FEATURE_NAMES:
                col = f"feature_{feat}_source"
                if row_sources.get(col) != UNKNOWN_PENDING:
                    continue
                base, langs = feature_specs[feat]
                hit = find_first_match(text, langs, detected_lang, negation_patterns, lookback)
                if hit is None:
                    continue
                lang_match = hit.lang_used == detected_lang
                if hit.negated:
                    # Explicit denial: write '0', confidence = 0.0 (from compute_confidence).
                    # But we still want to RECORD the finding so future passes don't
                    # overwrite it; use a small positive confidence to avoid the
                    # provenance.write_field coerce-to-0 clash. Use 0.5 to signal
                    # "low confidence, regex says no, explicitly".
                    write_field(
                        conn,
                        listing_id=listing_id,
                        field=f"feature_{feat}",
                        filled="0",
                        source=_source_for(hit.lang_used),
                        confidence=min(0.5, base * 0.6),
                        raw=f"NEG:{hit.value}",
                    )
                    stats[f"feature_{feat}_negated"] += 1
                else:
                    conf = compute_confidence(base, lang_match=lang_match, negated=False)
                    write_field(
                        conn,
                        listing_id=listing_id,
                        field=f"feature_{feat}",
                        filled="1",
                        source=_source_for(hit.lang_used),
                        confidence=conf,
                        raw=hit.value,
                    )
                    stats[f"feature_{feat}_filled"] += 1

            # --- year_built ---
            if row_sources.get("year_built_source") == UNKNOWN_PENDING:
                hit = find_first_match(text, year_langs, detected_lang, negation_patterns, lookback)
                if hit is not None and not hit.negated and hit.groups:
                    # Regex captures the year in group(1); validate it's plausible.
                    validated = _validate_year(hit.groups[0], now_year)
                    if validated:
                        lang_match = hit.lang_used == detected_lang
                        conf = compute_confidence(year_base, lang_match=lang_match)
                        write_field(
                            conn,
                            listing_id=listing_id,
                            field="year_built",
                            filled=validated,
                            source=_source_for(hit.lang_used),
                            confidence=conf,
                            raw=hit.value,
                        )
                        stats["year_built_filled"] += 1

            # --- agency_phone ---
            if row_sources.get("agency_phone_source") == UNKNOWN_PENDING:
                hit = find_first_match(text, phone_langs, detected_lang, negation_patterns, lookback)
                if hit is not None and not hit.negated:
                    lang_match = hit.lang_used == detected_lang
                    conf = compute_confidence(phone_base, lang_match=lang_match)
                    write_field(
                        conn,
                        listing_id=listing_id,
                        field="agency_phone",
                        filled=_normalize_phone(hit),
                        source=_source_for(hit.lang_used),
                        confidence=conf,
                        raw=hit.value,
                    )
                    stats["agency_phone_filled"] += 1

            # --- agency_email ---
            filled_email: str | None = None
            filled_email_lang: str | None = None
            if row_sources.get("agency_email_source") == UNKNOWN_PENDING:
                hit = find_first_match(text, email_langs, detected_lang, negation_patterns, lookback)
                if hit is not None and not hit.negated:
                    lang_match = hit.lang_used == detected_lang
                    conf = compute_confidence(email_base, lang_match=lang_match)
                    filled_email = hit.value.strip().lower()
                    filled_email_lang = hit.lang_used
                    write_field(
                        conn,
                        listing_id=listing_id,
                        field="agency_email",
                        filled=filled_email,
                        source=_source_for(hit.lang_used),
                        confidence=conf,
                        raw=hit.value,
                    )
                    stats["agency_email_filled"] += 1

            # --- agency_name (derived from agency_email when available) ---
            if row_sources.get("agency_name_source") == UNKNOWN_PENDING:
                if filled_email is None:
                    # Try reading agency_email_filled — it may have been filled by
                    # pass 0 from raw_json, in which case it's still usable.
                    existing = conn.execute(
                        "SELECT agency_email_filled, agency_email_source "
                        "FROM listings_enriched WHERE listing_id = ?;",
                        (listing_id,),
                    ).fetchone()
                    if existing and existing[1] != UNKNOWN_PENDING and existing[0] != UNKNOWN_VALUE:
                        filled_email = existing[0]
                        filled_email_lang = detected_lang
                if filled_email:
                    derived = _derive_agency_name_from_email(filled_email)
                    if derived:
                        write_field(
                            conn,
                            listing_id=listing_id,
                            field="agency_name",
                            filled=derived,
                            source=_source_for(filled_email_lang or detected_lang),
                            confidence=0.70,
                            raw=f"derived_from_email:{filled_email}",
                        )
                        stats["agency_name_derived"] += 1

            # --- floor ---
            if row_sources.get("floor_source") == UNKNOWN_PENDING:
                result = _extract_floor(
                    text, detected_lang, floor_patterns, negation_patterns, lookback
                )
                if result is not None:
                    value, conf, lang_used, raw_snippet = result
                    write_field(
                        conn,
                        listing_id=listing_id,
                        field="floor",
                        filled=value,
                        source=_source_for(lang_used),
                        confidence=conf,
                        raw=raw_snippet,
                    )
                    stats["floor_filled"] += 1

            # --- area ---
            if row_sources.get("area_source") == UNKNOWN_PENDING:
                hit = find_first_match(text, area_langs, detected_lang, negation_patterns, lookback)
                if hit is not None and not hit.negated and hit.groups:
                    validated = _validate_area(hit.groups[0])
                    if validated:
                        lang_match = hit.lang_used == detected_lang
                        conf = compute_confidence(area_base, lang_match=lang_match)
                        write_field(
                            conn,
                            listing_id=listing_id,
                            field="area",
                            filled=validated,
                            source=_source_for(hit.lang_used),
                            confidence=conf,
                            raw=hit.value,
                        )
                        stats["area_filled"] += 1

            # --- available_from ---
            if row_sources.get("available_from_source") == UNKNOWN_PENDING:
                result = _extract_available_from(
                    text, detected_lang, avail_patterns, negation_patterns, lookback
                )
                if result is not None:
                    value, conf, lang_used, raw_snippet = result
                    write_field(
                        conn,
                        listing_id=listing_id,
                        field="available_from",
                        filled=value,
                        source=_source_for(lang_used),
                        confidence=conf,
                        raw=raw_snippet,
                    )
                    stats["available_from_filled"] += 1

        conn.commit()
        return dict(stats)
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    args = parser.parse_args()
    if not args.db.exists():
        print(f"[ERROR] DB not found at {args.db}", file=sys.stderr)
        return 2
    stats = run(args.db)
    print("Pass 2 complete:")
    for k in sorted(stats.keys()):
        print(f"  {k}: {stats[k]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
