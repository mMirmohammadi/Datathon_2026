from __future__ import annotations

import re
import unicodedata


CITY_ALIAS: dict[str, str] = {
    "zurich": "zurich", "zuerich": "zurich",
    "geneva": "geneva", "geneve": "geneva", "genf": "geneva",
    "bern": "bern", "berne": "bern",
    "basel": "basel", "bale": "basel",
    "lucerne": "lucerne", "luzern": "lucerne",
    "biel": "biel", "bienne": "biel", "biel/bienne": "biel", "biel-bienne": "biel",
    "neuchatel": "neuchatel", "neuenburg": "neuchatel",
    "fribourg": "fribourg", "freiburg": "fribourg",
    "st gallen": "st-gallen", "st. gallen": "st-gallen", "saint-gall": "st-gallen",
    "st-gallen": "st-gallen",
    "lausanne": "lausanne",
    "lugano": "lugano",
    "sion": "sion",
    "winterthur": "winterthur",
}


OBJECT_CATEGORY_MAP: dict[str, str] = {
    "Wohnung": "apartment",
    "Möblierte Wohnung": "furnished_apartment",
    "Dachwohnung": "attic_apartment",
    "Attika": "penthouse",
    "Maisonette": "maisonette",
    "Loft": "loft",
    "Studio": "studio",
    "Terrassenwohnung": "terrace_apartment",
    "Haus": "house",
    "Doppeleinfamilienhaus": "semi_detached_house",
    "Reihenhaus": "terraced_house",
    "Villa": "villa",
    "Bauernhaus": "farmhouse",
    "Mehrfamilienhaus": "apartment_building",
    "Terrassenhaus": "terrace_house",
    "Einzelzimmer": "room",
    "WG-Zimmer": "shared_room",
    "Ferienwohnung": "holiday_apartment",
    "Ferienimmobilie": "holiday_property",
    "Gewerbeobjekt": "commercial",
    "Parkplatz": "parking",
    "Parkplatz, Garage": "parking",
    "Tiefgarage": "underground_parking",
    "Einzelgarage": "garage",
    "Bastelraum": "hobby_room",
    "Wohnnebenraeume": "auxiliary_space",
    "Grundstück": "land",
    "Gastgewerbe": "hospitality",
    "Diverses": "other",
}

OBJECT_CATEGORY_ENGLISH: list[str] = sorted(set(OBJECT_CATEGORY_MAP.values()) | {"other"})


def slug(value: str | None) -> str | None:
    """Lowercase, strip, NFKD ASCII-fold, then route through CITY_ALIAS."""
    if value is None:
        return None
    text = value.strip().lower()
    if not text:
        return None
    folded = (
        unicodedata.normalize("NFKD", text)
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    return CITY_ALIAS.get(folded, folded)


def translate_object_category(raw: str | None) -> str | None:
    """Map a German object_category to the English canonical enum.

    Unknown, non-null values emit a [WARN] and fall back to "other".
    """
    if raw is None or raw == "":
        return None
    mapped = OBJECT_CATEGORY_MAP.get(raw)
    if mapped is None:
        print(
            f"[WARN] translate_object_category: expected=key in OBJECT_CATEGORY_MAP, "
            f"got={raw!r}, fallback='other'",
            flush=True,
        )
        return "other"
    return mapped


_STREET_SPLIT_RE = re.compile(
    r"^(?P<street>.+?)\s+(?P<house_number>\d+[A-Za-z]*(?:\s+\d+[A-Za-z]*)?)\s*$"
)


_COMPARIS_PLATFORM_ID_RE = re.compile(r"/show/(\d+)(?:$|\?)")


def extract_comparis_platform_id(url: str | None) -> str | None:
    """Pull the numeric platform_id out of a comparis listing URL.

    Comparis URLs look like
    https://www.comparis.ch/immobilien/marktplatz/details/show/36493173.
    Returns None when the URL is missing, empty, or not in the expected shape.
    """
    if not url:
        return None
    match = _COMPARIS_PLATFORM_ID_RE.search(url)
    if match is None:
        return None
    return match.group(1)


def split_street(value: str | None) -> tuple[str | None, str | None]:
    """Split a raw address string into (lowercased street_name, house_number).

    If no trailing number is present, returns (lowercased-full-string, None).
    """
    if value is None:
        return None, None
    text = value.strip()
    if not text:
        return None, None

    match = _STREET_SPLIT_RE.match(text)
    if match:
        street = match.group("street").strip().lower()
        house_number = match.group("house_number").strip()
        return (street or None, house_number or None)
    return text.lower(), None
