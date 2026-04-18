"""Language-aware text extraction with a negation guard.

Used by pass 2 to pull feature flags, year_built, agency_*, etc. out of
the free-text `description` column where they weren't captured in the
structured columns.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True, slots=True)
class ExtractionHit:
    """A single regex match result, post-negation-check."""
    value: str             # the full matched substring (m.group(0))
    groups: tuple[str, ...]  # captured groups (m.groups()); empty if no groups in pattern
    lang_used: str         # 'de' | 'fr' | 'it' | 'en' — which pattern family matched
    pattern: str           # the regex string that matched (for debugging/auditing)
    match_start: int       # byte offset of the match start in the cleaned text
    negated: bool          # True if a negation token was detected within the lookback window


@lru_cache(maxsize=2048)
def _compiled(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern, flags=re.IGNORECASE | re.UNICODE)


def is_negated(
    text: str,
    match_start: int,
    neg_patterns_for_lang: tuple[str, ...],
    lookback_tokens: int = 3,
) -> bool:
    """True if any of `neg_patterns_for_lang` matches within the last
    `lookback_tokens` whitespace-separated tokens before `match_start`.

    Tokens are split on whitespace; punctuation stays attached to its token
    (cheap and good enough for the short contexts we scan).
    """
    before = text[:match_start]
    tokens = before.split()[-lookback_tokens:]
    window = " ".join(tokens).lower()
    if not window:
        return False
    for pat in neg_patterns_for_lang:
        if _compiled(r"\b" + pat + r"\b").search(window):
            return True
    return False


def find_first_match(
    text: str,
    patterns_by_lang: dict[str, list[str]],
    detected_lang: str,
    negation_patterns: dict[str, list[str]],
    lookback_tokens: int = 3,
) -> ExtractionHit | None:
    """Try each language's patterns against `text` until one matches that
    isn't negated. Starts with `detected_lang` so the happy-path cost is
    one regex scan; falls back to other languages after that.

    Returns None if no non-negated match exists in any language.
    """
    if not text:
        return None

    # Language priority: detected first, then the rest (preserving dict order).
    lang_order: list[str] = [detected_lang] if detected_lang in patterns_by_lang else []
    lang_order.extend(k for k in patterns_by_lang if k != detected_lang)

    for lang in lang_order:
        for pattern in patterns_by_lang[lang]:
            m = _compiled(pattern).search(text)
            if m is None:
                continue
            neg_for_lang = tuple(negation_patterns.get(lang, []))
            negated = is_negated(text, m.start(), neg_for_lang, lookback_tokens)
            return ExtractionHit(
                value=m.group(0),
                groups=tuple(g or "" for g in m.groups()),
                lang_used=lang,
                pattern=pattern,
                match_start=m.start(),
                negated=negated,
            )
    return None
