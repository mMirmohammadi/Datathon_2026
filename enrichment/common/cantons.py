"""Map reverse_geocoder admin1 strings → Swiss ISO canton codes.

The keys below were collected by calling reverse_geocoder.search() on
hand-picked coordinates across every canton and reading the admin1 field
from the result. Do NOT edit by hypothesis — re-verify against a live rg
call if the package version changes, otherwise pass 1 will silently
emit [WARN] for every row in the unmapped canton.
"""
from __future__ import annotations

from typing import Final

ADMIN1_TO_CANTON_CODE: Final[dict[str, str]] = {
    "Zurich": "ZH",
    "Bern": "BE",
    "Lucerne": "LU",
    "Uri": "UR",
    "Schwyz": "SZ",
    "Obwalden": "OW",
    "Nidwalden": "NW",
    "Glarus": "GL",
    "Zug": "ZG",
    "Fribourg": "FR",
    "Solothurn": "SO",
    "Basel-City": "BS",
    "Basel-Landschaft": "BL",
    "Schaffhausen": "SH",
    "Appenzell Ausserrhoden": "AR",
    "Appenzell Innerrhoden": "AI",
    "Saint Gallen": "SG",
    "Grisons": "GR",
    "Aargau": "AG",
    "Thurgau": "TG",
    "Ticino": "TI",
    "Vaud": "VD",
    "Valais": "VS",
    "Neuchatel": "NE",
    "Geneva": "GE",
    "Jura": "JU",
}

# Sanity at import — fail fast if someone edits the dict incorrectly.
assert len(ADMIN1_TO_CANTON_CODE) == 26, (
    f"Expected 26 cantons, got {len(ADMIN1_TO_CANTON_CODE)}"
)
assert len(set(ADMIN1_TO_CANTON_CODE.values())) == 26, (
    "Duplicate canton codes in ADMIN1_TO_CANTON_CODE"
)
for _code in ADMIN1_TO_CANTON_CODE.values():
    assert len(_code) == 2 and _code.isupper() and _code.isalpha(), (
        f"Invalid canton code {_code!r}"
    )


def admin1_to_canton_code(admin1: str | None) -> str | None:
    """Return the 2-letter canton code for a reverse_geocoder admin1 string.

    Returns None if admin1 is empty or unmapped — callers should emit [WARN]
    per CLAUDE.md §5 rather than defaulting silently.
    """
    if not admin1:
        return None
    return ADMIN1_TO_CANTON_CODE.get(admin1.strip())
