"""Provenance sources for the `*_source` column in listings_enriched.

Adding a new source: append to `VALID_SOURCES`. Validation happens at
write-time in common.provenance.write_field.
"""
from __future__ import annotations

from typing import Final

# Persistent sources (survive pass 3)
ORIGINAL: Final[str]             = "original"             # value was non-null in listings or raw_json
REV_GEO_OFFLINE: Final[str]      = "rev_geo_offline"      # pass 1a: reverse_geocoder KDTree
REV_GEO_NOMINATIM: Final[str]    = "rev_geo_nominatim"    # pass 1b: Nominatim HTTP
TEXT_REGEX_DE: Final[str]        = "text_regex_de"
TEXT_REGEX_FR: Final[str]        = "text_regex_fr"
TEXT_REGEX_IT: Final[str]        = "text_regex_it"
TEXT_REGEX_EN: Final[str]        = "text_regex_en"
TEXT_GPT_5_4: Final[str]         = "text_gpt_5_4"         # pass 2 GPT: OpenAI gpt-5.4-mini extraction
DEFAULT_CONSTANT: Final[str]     = "default_constant"     # e.g. offer_type='RENT' default
CROSS_REF: Final[str]            = "cross_ref"            # reconciled across sources
UNKNOWN: Final[str]              = "UNKNOWN"              # pass 3 sentinel
DROPPED_BAD_DATA: Final[str]     = "DROPPED_bad_data"     # null-island, OOB, price<1

# Transient sources (must not exist after pass 3)
UNKNOWN_PENDING: Final[str]      = "UNKNOWN-pending"      # pass 0 marker for nulls

VALID_SOURCES: frozenset[str] = frozenset({
    ORIGINAL,
    REV_GEO_OFFLINE, REV_GEO_NOMINATIM,
    TEXT_REGEX_DE, TEXT_REGEX_FR, TEXT_REGEX_IT, TEXT_REGEX_EN,
    TEXT_GPT_5_4,
    DEFAULT_CONSTANT, CROSS_REF,
    UNKNOWN, DROPPED_BAD_DATA,
    UNKNOWN_PENDING,
})

# The sources allowed to remain after pass 3 completes (UNKNOWN_PENDING must be gone).
FINAL_SOURCES: frozenset[str] = VALID_SOURCES - {UNKNOWN_PENDING}
