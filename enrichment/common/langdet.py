"""HTML stripping + lightweight Swiss-specific language detection.

This module is intentionally self-contained and depends only on stdlib —
`analysis/profile.py` has a matplotlib import at module load that we don't
want to pull into the pipeline. The strip_html and guess_lang logic below
is a verbatim copy of that file's implementation (cross-check at
`analysis/profile.py:64-108`); keep in sync if the analysis version evolves.
"""
from __future__ import annotations

from html.parser import HTMLParser


class _HTMLStripper(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:  # pragma: no cover — trivial
        self.parts.append(data)


def strip_html(s: str | None) -> str:
    if not s:
        return ""
    if "<" not in s:
        return s
    p = _HTMLStripper()
    try:
        p.feed(s)
    except Exception as exc:
        # CLAUDE.md §5: announce fallback rather than silently eating malformed HTML
        print(
            f"[WARN] strip_html: expected=valid_html got={exc!r} fallback=raw_string",
            flush=True,
        )
        return s
    return "".join(p.parts)


# Token heuristics. Matches analysis/profile.py:89-96 exactly.
_DE_TOKENS = {
    "und", "die", "der", "das", "ist", "mit", "wohnung", "zimmer", "nicht",
    "für", "schön", "balkon", "küche", "neu", "helle", "hell", "stock",
    "bahnhof", "sehr", "grosse", "grosser",
}
_FR_TOKENS = {
    "et", "le", "la", "les", "une", "avec", "chambre", "cuisine", "appartement",
    "pour", "située", "belle", "studio", "meublé", "proche", "gare", "balcon",
}
_IT_TOKENS = {
    "e", "il", "la", "con", "camera", "cucina", "appartamento", "per",
    "bellissimo", "luminoso", "stanza", "bagno", "vicino", "stazione", "balcone",
}
_EN_TOKENS = {
    "and", "the", "with", "room", "kitchen", "apartment", "flat", "bright",
    "modern", "near", "station", "for", "rent", "studio",
}


def guess_lang(text: str | None) -> str:
    """Return one of: 'de', 'fr', 'it', 'en', 'unk' (<2 token matches)."""
    if not text:
        return "unk"
    t = text.lower()
    scores = {
        "de": sum(w in t for w in _DE_TOKENS),
        "fr": sum(w in t for w in _FR_TOKENS),
        "it": sum(w in t for w in _IT_TOKENS),
        "en": sum(w in t for w in _EN_TOKENS),
    }
    best = max(scores, key=scores.get)
    return best if scores[best] >= 2 else "unk"
