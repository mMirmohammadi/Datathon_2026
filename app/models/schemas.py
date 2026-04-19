from __future__ import annotations

import re
from typing import Any
from typing import Literal

from pydantic import BaseModel, EmailStr, Field, field_validator


class SoftPreferences(BaseModel):
    """Soft ranking signals extracted from the user query.

    The ranker (``app/core/soft_signals.py``) turns every activated key into
    its own listing ranking that is then fused via RRF with the BM25, text-
    embedding and image-embedding channels. NULL signals are omitted from
    their ranking (they neither help nor hurt the listing on that axis).
    """

    price_sentiment: Literal["cheap", "moderate", "premium"] | None = None
    quiet: bool = False
    near_public_transport: bool = False
    near_schools: bool = False
    near_supermarket: bool = False
    near_park: bool = False
    family_friendly: bool = False
    commute_target: Literal[
        "zurich_hb", "bern_hb", "basel_hb", "geneve_hb",
        "lausanne_hb", "lugano_hb", "winterthur_hb", "st_gallen_hb",
    ] | None = None
    near_landmark: list[str] = Field(default_factory=list)


class HardFilters(BaseModel):
    city: list[str] | None = None
    postal_code: list[str] | None = None
    canton: str | None = None
    min_price: int | None = Field(default=None, ge=0)
    max_price: int | None = Field(default=None, ge=0)
    min_rooms: float | None = Field(default=None, ge=0)
    max_rooms: float | None = Field(default=None, ge=0)
    min_area: int | None = Field(default=None, ge=0)
    max_area: int | None = Field(default=None, ge=0)
    min_floor: int | None = None
    max_floor: int | None = None
    min_year_built: int | None = Field(default=None, ge=0)
    max_year_built: int | None = Field(default=None, ge=0)
    available_from_after: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    radius_km: float | None = Field(default=None, ge=0)
    features: list[str] | None = None
    features_excluded: list[str] | None = None
    object_category: list[str] | None = None
    # Pass 2b enriched fields. Hard-filter semantics: UNKNOWN sentinels never
    # satisfy a positive constraint (e.g. has_cellar=true excludes rows whose
    # has_cellar_filled is 'UNKNOWN').
    min_bathrooms: int | None = Field(default=None, ge=0)
    max_bathrooms: int | None = Field(default=None, ge=0)
    bathroom_shared: bool | None = None
    has_cellar: bool | None = None
    kitchen_shared: bool | None = None
    bm25_keywords: list[str] | None = None
    soft_preferences: SoftPreferences | None = None
    # Internal cap for the candidate pool passed into the RRF ranker. Bumped
    # above the external API cap so image-only queries can consider the whole
    # corpus when the hard-filter channel has nothing to narrow on.
    limit: int = Field(default=20, ge=1, le=30000)
    offset: int = Field(default=0, ge=0)
    sort_by: Literal["price_asc", "price_desc", "rooms_asc", "rooms_desc"] | None = None


class ListingsQueryRequest(BaseModel):
    query: str = Field(min_length=1)
    limit: int = Field(default=25, ge=1, le=500)
    offset: int = Field(default=0, ge=0)
    personalize: bool = Field(
        default=True,
        description=(
            "When True and the caller is authenticated, memory-based "
            "personalization rankings are added to the RRF fusion. Anonymous "
            "callers ignore this flag (no history to personalize on)."
        ),
    )


class ListingsSearchRequest(BaseModel):
    hard_filters: HardFilters | None = None


class ListingData(BaseModel):
    id: str
    title: str
    description: str | None = None
    street: str | None = None
    city: str | None = None
    postal_code: str | None = None
    canton: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    price_chf: int | None = None
    rooms: float | None = None
    living_area_sqm: int | None = None
    available_from: str | None = None
    image_urls: list[str] | None = None
    hero_image_url: str | None = None
    original_listing_url: str | None = None
    features: list[str] = Field(default_factory=list)
    offer_type: str | None = None
    object_category: str | None = None
    object_type: str | None = None
    # Pass 2b enriched fields (gpt-5.4-nano extraction). None = UNKNOWN — the
    # listing description did not mention the feature. UI should render "—"
    # rather than a false/zero claim.
    bathroom_count: int | None = None
    bathroom_shared: bool | None = None
    has_cellar: bool | None = None
    kitchen_shared: bool | None = None


class RankingBreakdown(BaseModel):
    """Per-listing scoring breakdown for UI explainability.

    Every field maps 1:1 to a signal produced by ``_rerank_hybrid``. A ``None``
    field means that channel did not contribute for this listing (either the
    channel was disabled, or the listing had no data on that axis). The fused
    ``rrf_score`` is the number the ranker sorts by; the per-channel fields
    are the raw inputs so the UI can explain WHY a listing ranked where it did.

    The memory_* fields are the 4 personalization channels from
    :class:`app.memory.rankings.MemorySignals`. Exposing them lets the UI
    show which axis of the user's history this listing looks like (semantic
    description taste, visual photo taste, feature checklist, price habit).
    """

    rrf_score: float | None = None
    bm25_score: float | None = None
    visual_score: float | None = None
    text_embed_score: float | None = None
    # DINOv2 channel only contributes when the caller uploaded a photo on
    # /listings/search/multi. None for pure-text queries.
    dinov2_image_score: float | None = None
    soft_signals_activated: int = 0
    memory_rankings_activated: int = 0
    memory_score: float | None = None
    memory_semantic: float | None = None
    memory_visual: float | None = None
    memory_feature: float | None = None
    memory_price: float | None = None


class HardCheck(BaseModel):
    """One row of the 'which hard constraints did this listing satisfy' table.

    Every listing returned has passed the hard-filter gate, so ``ok`` is
    ``True`` in practice — but we still emit one row per requested constraint
    so the UI can show the user exactly what was checked and the listing's
    actual value for that axis.
    """

    label: str
    requested: str
    value: str
    ok: bool


class MatchFact(BaseModel):
    """One row of the 'soft signal value' table, shown per listing.

    ``axis`` is the machine-readable soft-preference key (``quiet``,
    ``near_schools``, ``landmark_eth_zentrum``, ...). ``value`` is already
    formatted for display (``"409 m"``, ``"12% below canton×rooms baseline"``).
    ``interpretation`` is one of ``"good" | "ok" | "poor" | "unknown"`` and
    drives the badge colour in the UI.
    """

    axis: str
    label: str
    value: str
    interpretation: str


class MatchDetail(BaseModel):
    """Per-listing explanation surfaced on click in the demo UI.

    Everything needed to answer "why did this listing match and rank here?".
    Populated only in the natural-language query path (``/listings``); the
    raw filter endpoint leaves it ``None`` because there's no LLM-extracted
    plan to explain against.
    """

    hard_checks: list[HardCheck] = Field(default_factory=list)
    matched_keywords: list[str] = Field(default_factory=list)
    unmatched_keywords: list[str] = Field(default_factory=list)
    soft_facts: list[MatchFact] = Field(default_factory=list)


class RankedListingResult(BaseModel):
    listing_id: str
    score: float
    reason: str
    listing: ListingData
    breakdown: RankingBreakdown | None = None
    match_detail: MatchDetail | None = None


class ListingsResponse(BaseModel):
    listings: list[RankedListingResult]
    meta: dict[str, Any] = Field(default_factory=dict)


class SimilarListing(BaseModel):
    """One row of the DINOv2 "find similar" response.

    ``cosine`` is the max cosine of the query listing's centroid against any
    image of this listing — in [-1, 1], higher is more similar.
    """

    listing_id: str
    cosine: float
    listing: ListingData


class SimilarListingsResponse(BaseModel):
    query_listing_id: str
    results: list[SimilarListing]
    meta: dict[str, Any] = Field(default_factory=dict)


class ImageSearchResponse(BaseModel):
    """Response for ``POST /listings/search/image`` — user uploads an arbitrary
    photo (not an existing listing), and we return visually similar listings
    ranked by max DINOv2 cosine per listing.
    """

    results: list[SimilarListing]
    meta: dict[str, Any] = Field(default_factory=dict)


class HealthResponse(BaseModel):
    status: str


# ---------- Auth + interactions (user system) -------------------------------


_USERNAME_RE = re.compile(r"^[A-Za-z0-9_.\-]{3,32}$")


def _validate_password_strength(value: str) -> str:
    """Reject empty, too-short, letter-only, and digit-only passwords.

    Kept intentionally simple: min-8 chars + must contain both a letter and
    a digit. Anything more elaborate (common-password blocklists, zxcvbn)
    is out of scope for the datathon demo but trivial to bolt on later.
    """
    if not isinstance(value, str) or len(value) < 8:
        raise ValueError("password must be at least 8 characters")
    if not any(c.isalpha() for c in value):
        raise ValueError("password must contain at least one letter")
    if not any(c.isdigit() for c in value):
        raise ValueError("password must contain at least one digit")
    return value


class RegisterRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=32)
    email: EmailStr
    password: str = Field(..., repr=False)

    @field_validator("username")
    @classmethod
    def _validate_username(cls, value: str) -> str:
        if not _USERNAME_RE.match(value):
            raise ValueError(
                "username must be 3-32 chars from [A-Za-z0-9_.-]"
            )
        return value

    @field_validator("password")
    @classmethod
    def _validate_password(cls, value: str) -> str:
        return _validate_password_strength(value)


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=32)
    password: str = Field(..., min_length=1, repr=False)


class ChangePasswordRequest(BaseModel):
    current_password: str = Field(..., min_length=1, repr=False)
    new_password: str = Field(..., repr=False)

    @field_validator("new_password")
    @classmethod
    def _validate_password(cls, value: str) -> str:
        return _validate_password_strength(value)


class DeleteAccountRequest(BaseModel):
    password: str = Field(..., min_length=1, repr=False)


class UserPublic(BaseModel):
    id: int
    username: str
    email: EmailStr
    created_at: str
    last_login_at: str | None = None


class CsrfResponse(BaseModel):
    csrf_token: str


InteractionKind = Literal[
    # Positive preference signals (feed the memory profile).
    # ``save`` / ``unsave`` are kept as legacy aliases for ``like`` / ``unlike``
    # so events written before the split still carry their original meaning.
    "save",
    "unsave",
    "like",
    "unlike",
    # Bookmarks - pure user-facing list, NO memory weight. A user can save an
    # apartment they're curious about without telling the ranker "give me more
    # listings like this".
    "bookmark",
    "unbookmark",
    # Implicit signals.
    "click",
    "dwell",
    # Explicit negative + its undo.
    "dismiss",
    "undismiss",
]


class InteractionRequest(BaseModel):
    listing_id: str = Field(..., min_length=1, max_length=64)
    kind: InteractionKind
    value: float | None = Field(
        default=None,
        description="Optional numeric payload (e.g. dwell seconds).",
    )


class FeatureTaste(BaseModel):
    """One (feature-flag, signed-weight) pair on the learned taste vector.

    ``weight`` lives in ``[-1, +1]`` with the sign convention "positive means
    the user tends to prefer listings that have this feature".
    """

    key: str
    label: str
    weight: float


class PriceRange(BaseModel):
    """Approximate ±1σ CHF band derived from ``exp(log_price_mu ± sigma)``.

    Indicative only; the ranker uses raw ``(mu, sigma)`` on the log scale,
    not this rounded trio.
    """

    low_chf: int
    mid_chf: int
    high_chf: int


class ProfileStats(BaseModel):
    """Counts of distinct listings currently in each drawer.

    ``likes`` matches ``/me/likes`` cardinality; ``bookmarks`` matches
    ``/me/favorites`` cardinality; ``dismissals`` matches ``/me/dismissed``.
    """

    likes: int
    bookmarks: int
    dismissals: int


class UserProfileSummary(BaseModel):
    """Human-readable projection of the learned memory profile for the UI."""

    is_cold_start: bool
    positive_count: int
    liked_features: list[FeatureTaste] = Field(default_factory=list)
    avoided_features: list[FeatureTaste] = Field(default_factory=list)
    price_range_chf: PriceRange | None = None
    stats: ProfileStats


class FavoriteListing(BaseModel):
    """One saved / liked entry, enriched with a compact listing summary so the
    UI can render a thumbnail row without a second round-trip.

    Every enrichment field is optional because the listing might have been
    deleted from ``listings.db`` between the interaction being written and the
    drawer being opened (rare but possible with a re-imported bundle).
    """

    listing_id: str
    saved_at: str
    title: str | None = None
    price_chf: int | None = None
    rooms: float | None = None
    area_sqm: int | None = None
    city: str | None = None
    canton: str | None = None
    object_category: str | None = None
    hero_image_url: str | None = None
    features: list[str] = Field(default_factory=list)


class FavoritesResponse(BaseModel):
    favorites: list[FavoriteListing]
