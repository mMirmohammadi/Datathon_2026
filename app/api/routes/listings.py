from __future__ import annotations

import io
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from PIL import Image, UnidentifiedImageError

from app.api.deps import get_current_user
from app.config import get_settings
from app.core.dinov2_search import (
    dinov2_enabled,
    find_similar_by_image,
    find_similar_listings,
    find_similar_listings_fused,
    is_loaded as dinov2_is_loaded,
)
from app.core.hard_filters import _parse_row
from app.core.landmark_proximity import compute_for_one as compute_nearby_landmarks
from app.db import get_connection
from app.harness.search_service import default_feed, query_from_filters, query_from_text
from app.models.schemas import (
    HealthResponse,
    ImageSearchResponse,
    ListingData,
    ListingsQueryRequest,
    ListingsResponse,
    ListingsSearchRequest,
    SimilarListing,
    SimilarListingsResponse,
)
from app.participant.ranking import _to_listing_data

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok")


@router.get("/listings/default", response_model=ListingsResponse)
def listings_default(
    limit: int = 12,
    user: dict[str, Any] | None = Depends(get_current_user(required=False)),
) -> ListingsResponse:
    """Homepage feed shown before the user has typed a query.

    Personalised by the memory channel when the caller is authenticated and
    past cold-start; anonymous callers and cold-start users see the natural
    pool order. Cheap — no LLM call, no BM25/visual/text-embed run.
    """
    settings = get_settings()
    user_id = int(user["id"]) if user is not None else None
    lim = max(1, min(int(limit), 50))
    return default_feed(
        db_path=settings.db_path,
        users_db_path=settings.users_db_path,
        limit=lim,
        user_id=user_id,
    )


@router.post("/listings", response_model=ListingsResponse)
def listings(
    request: ListingsQueryRequest,
    user: dict[str, Any] | None = Depends(get_current_user(required=False)),
) -> ListingsResponse:
    settings = get_settings()
    user_id = int(user["id"]) if user is not None else None
    # Anonymous callers never trigger personalization regardless of the flag.
    personalize = bool(request.personalize and user_id is not None)
    return query_from_text(
        db_path=settings.db_path,
        query=request.query,
        limit=request.limit,
        offset=request.offset,
        user_id=user_id,
        personalize=personalize,
        users_db_path=settings.users_db_path,
    )


@router.get("/listings/{listing_id}", response_model=ListingData)
def get_listing(listing_id: str) -> ListingData:
    """Return the full ``ListingData`` for one listing by id.

    Used by the Saved-listings drawer to render a detail view on click.
    Reuses ``_parse_row`` + ``_to_listing_data`` so the shape is identical
    to what the search endpoint returns on the hot path.
    """
    settings = get_settings()
    with get_connection(settings.db_path) as conn:
        row = conn.execute(
            """
            SELECT l.*,
                   e.bathroom_count_filled  AS bathroom_count_raw,
                   e.bathroom_shared_filled AS bathroom_shared_raw,
                   e.has_cellar_filled      AS has_cellar_raw,
                   e.kitchen_shared_filled  AS kitchen_shared_raw
            FROM listings l
            LEFT JOIN listings_enriched e ON e.listing_id = l.listing_id
            WHERE l.listing_id = ?
            """,
            (listing_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"listing {listing_id!r} not found",
        )
    parsed = _parse_row(dict(row))
    # Enrich with the top-K nearest landmarks so the detail modal can render
    # the Google-Maps-Directions chip row. One batched DB hit; returns [] for
    # the 1'637 geo-less listings and the detail modal degrades silently.
    parsed["nearby_landmarks"] = compute_nearby_landmarks(
        settings.db_path, str(listing_id),
    )
    return _to_listing_data(parsed)


@router.post("/listings/search/filter", response_model=ListingsResponse)
def listings_search(request: ListingsSearchRequest) -> ListingsResponse:
    settings = get_settings()
    return query_from_filters(
        db_path=settings.db_path,
        hard_facts=request.hard_filters,
    )


# 8 MB body cap (strategy_visual_reverse_search.md §3.4). Large enough for any
# real phone-camera JPEG; small enough that a malicious upload can't OOM the
# encoder. Enforced at the route boundary because starlette's UploadFile itself
# streams to a spool file with no size check.
_MAX_UPLOAD_BYTES = 8 * 1024 * 1024
# Hard cap on decoded pixel dimensions. Prevents the "PIL decompression bomb"
# failure mode where a small compressed PNG expands to multi-gigapixel RGB.
_MAX_IMAGE_PIXELS = 4096 * 4096


def _decode_uploaded_image(raw: bytes) -> Image.Image:
    """Decode uploaded bytes → RGB PIL.Image; enforce size + pixel caps.

    Raises HTTPException(400) with a user-visible detail on any decode
    failure, oversize input, or decompression-bomb attempt. Used by both
    the pure-image ``/listings/search/image`` route and the hybrid
    ``/listings/search/multi`` route so caps are consistent.
    """
    if not raw:
        raise HTTPException(status_code=400, detail="uploaded file is empty")
    if len(raw) > _MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"upload is {len(raw)} bytes, max is {_MAX_UPLOAD_BYTES} "
                "(8 MB); shrink the image and try again."
            ),
        )
    try:
        pil = Image.open(io.BytesIO(raw))
        pil.load()
    except (UnidentifiedImageError, OSError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail=f"could not decode image: {exc!s}",
        ) from exc
    if pil.size[0] * pil.size[1] > _MAX_IMAGE_PIXELS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"decoded image is {pil.size[0]}x{pil.size[1]} pixels, max is "
                f"{_MAX_IMAGE_PIXELS} total pixels (~4096x4096). "
                "Downscale before uploading."
            ),
        )
    if pil.mode != "RGB":
        pil = pil.convert("RGB")
    return pil


@router.post("/listings/search/multi", response_model=ListingsResponse)
async def listings_search_multi(
    query: str | None = Form(default=None),
    file: UploadFile | None = File(default=None),
    limit: int = Form(default=25),
    offset: int = Form(default=0),
    personalize: bool = Form(default=True),
    user: dict[str, Any] | None = Depends(get_current_user(required=False)),
) -> ListingsResponse:
    """Hybrid text + image search — one endpoint, three modes.

    - **Text only** (``query`` set, no ``file``): identical to ``POST /listings``.
    - **Image only** (``file`` set, empty ``query``): DINOv2 image channel dominates
      the RRF; Arctic and BM25 contribute the neutral candidate order.
    - **Text + image**: both channels feed the fusion — text drives hard filters +
      BM25 + semantic; image adds a DINOv2 cosine ranking. The fusion combines
      them so the top results satisfy both the language and the photo.

    Multipart form so the frontend can attach a file with FormData; the JSON
    ``POST /listings`` route stays untouched for non-UI callers.
    """
    q = (query or "").strip()
    if not q and file is None:
        raise HTTPException(
            status_code=400,
            detail="provide at least one of: query text, or an image file",
        )

    image_pil: Image.Image | None = None
    if file is not None:
        if not dinov2_enabled():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=(
                    "image queries require the DINOv2 channel, which is disabled "
                    "(set LISTINGS_DINOV2_ENABLED=1 and restart)."
                ),
            )
        if not dinov2_is_loaded():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=(
                    "DINOv2 index is not loaded. Check startup logs for [WARN] "
                    "dinov2_*."
                ),
            )
        raw = await file.read()
        image_pil = _decode_uploaded_image(raw)

    # Pure image query (no text): avoid the LLM hard-filter call (it would
    # just return empty filters anyway), run the ranker over the full pool.
    # Arctic's "query: " on an empty string is a no-op; BM25 without keywords
    # returns the full candidate pool in natural order; the DINOv2 image
    # channel then drives ranking via RRF.
    effective_query = q if q else ""

    settings = get_settings()
    user_id = int(user["id"]) if user is not None else None
    do_personalize = bool(personalize and user_id is not None)
    return query_from_text(
        db_path=settings.db_path,
        query=effective_query,
        limit=int(limit),
        offset=int(offset),
        user_id=user_id,
        personalize=do_personalize,
        users_db_path=settings.users_db_path,
        image_pil=image_pil,
    )


@router.post(
    "/listings/search/image",
    response_model=ImageSearchResponse,
)
async def listings_search_image(
    file: UploadFile = File(...),
    k: int = 12,
) -> ImageSearchResponse:
    """Arbitrary-photo reverse search — upload a picture, get similar listings.

    Pipeline: decode → DINOv2 ViT-L/14 eval transform → forward → GeM pool →
    L2 → cosine against the 70,548 × 1024 main matrix → max-pool per listing
    → top-K enriched with full :class:`ListingData`. Max-cosine aggregation is
    the same rule the ``/listings/{id}/similar`` endpoint uses for
    consistency.

    Disabled behaviours:
      * 400 when the upload is not a decodable image, empty, or oversize.
      * 503 when the DINOv2 channel is off or the index is not loaded.
    """
    k_clamped = max(1, min(int(k), 50))
    settings = get_settings()

    if not dinov2_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "DINOv2 reverse-image channel is disabled on this server "
                "(set LISTINGS_DINOV2_ENABLED=1 and restart)."
            ),
        )
    if not dinov2_is_loaded():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "DINOv2 index is not loaded. Check the server startup logs "
                "for [WARN] dinov2_*."
            ),
        )

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="uploaded file is empty")
    if len(raw) > _MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"upload is {len(raw)} bytes, max is {_MAX_UPLOAD_BYTES} "
                "(8 MB); shrink the image and try again."
            ),
        )
    try:
        pil = Image.open(io.BytesIO(raw))
        # Force decode now (Image.open is lazy) so a malformed file fails here
        # rather than deep inside the encoder.
        pil.load()
    except (UnidentifiedImageError, OSError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail=f"could not decode image: {exc!s}",
        ) from exc
    if pil.size[0] * pil.size[1] > _MAX_IMAGE_PIXELS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"decoded image is {pil.size[0]}x{pil.size[1]} pixels, max is "
                f"{_MAX_IMAGE_PIXELS} total pixels (~4096x4096). "
                "Downscale before uploading."
            ),
        )
    # RGBA / palette / greyscale all need a composite to RGB before ImageNet
    # normalisation; same rule the indexer's safe_open_image applies.
    if pil.mode != "RGB":
        pil = pil.convert("RGB")

    similar = find_similar_by_image(pil, k=k_clamped)
    if not similar:
        return ImageSearchResponse(
            results=[],
            meta={
                "model": "dinov2_vitl14_reg",
                "k_requested": k_clamped,
                "k_returned": 0,
                "note": "encoder produced no cosine matches (empty corpus?)",
            },
        )

    similar_pids = [pid for pid, _ in similar]
    placeholders = ", ".join("?" for _ in similar_pids)
    with get_connection(settings.db_path) as conn:
        rows = conn.execute(
            f"SELECT * FROM listings WHERE platform_id IN ({placeholders})",
            similar_pids,
        ).fetchall()
    by_pid: dict[str, tuple[str, Any]] = {}
    for row in rows:
        parsed = _parse_row(dict(row))
        by_pid[str(row["platform_id"])] = (
            str(row["listing_id"]),
            _to_listing_data(parsed),
        )

    results: list[SimilarListing] = []
    for pid, cosine in similar:
        resolved = by_pid.get(str(pid))
        if resolved is None:
            continue
        real_listing_id, ld = resolved
        results.append(SimilarListing(
            listing_id=real_listing_id,
            cosine=float(cosine),
            listing=ld,
        ))

    return ImageSearchResponse(
        results=results,
        meta={
            "model": "dinov2_vitl14_reg",
            "embed_dim": 1024,
            "aggregation": "max cosine per candidate listing",
            "k_requested": k_clamped,
            "k_returned": len(results),
            "bytes": len(raw),
            "decoded_size": f"{pil.size[0]}x{pil.size[1]}",
        },
    )


@router.get(
    "/listings/{listing_id}/similar",
    response_model=SimilarListingsResponse,
)
def similar_listings(listing_id: str, k: int = 10) -> SimilarListingsResponse:
    """Look-alike listings fused over image + text + feature channels.

    - **Image**: DINOv2 centroid of the query listing's photos vs every other
      listing's photos (max cosine per listing).
    - **Text**: Arctic description embedding of the query listing vs every
      other listing's description.
    - **Feature**: SQL similarity on canton, object category, rooms, price.

    Works even when the query listing has zero photos in the DINOv2 store —
    the text + feature channels alone produce a useful ranking. Each
    returned listing also has ``image_urls`` re-ordered so the photo that
    best matches the query listing's centroid is first (UX alignment).

    Disabled behaviours:
      * 404 when the query listing doesn't exist in the listings table.
      * 503 when the DINOv2 channel is off (image channel blocked) —
        callers can still rely on the text + feature channels by removing
        the guard in a future patch if that's a hard blocker.
    """
    k_clamped = max(1, min(int(k), 50))
    settings = get_settings()

    # Verify the query listing exists first so callers get a clean 404, not
    # an opaque "no similar results" when the id is wrong.
    with get_connection(settings.db_path) as conn:
        hit = conn.execute(
            "SELECT platform_id FROM listings WHERE listing_id = ? LIMIT 1",
            (listing_id,),
        ).fetchone()
    if hit is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"listing {listing_id!r} not found",
        )
    query_platform_id = hit["platform_id"] or listing_id

    if not dinov2_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "DINOv2 reverse-image channel is disabled on this server "
                "(set LISTINGS_DINOV2_ENABLED=1 and restart)."
            ),
        )
    if not dinov2_is_loaded():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "DINOv2 index is not loaded. Check the server startup logs "
                "for [WARN] dinov2_*."
            ),
        )

    ranked, best_image_ids, image_cosines = find_similar_listings_fused(
        listing_id=listing_id,
        platform_id=query_platform_id,
        db_path=settings.db_path,
        k=k_clamped,
    )
    if not ranked:
        return SimilarListingsResponse(
            query_listing_id=listing_id,
            results=[],
            meta={
                "model": "dinov2_vitl14_reg + arctic-embed-l-v2 + sql-features",
                "note": (
                    "No similar listings produced (all three channels were empty — "
                    "check DINOv2 / Arctic indexes + listings table)."
                ),
            },
        )

    # Enrich with ListingData, keyed on listing_id this time (fused returns
    # listing_ids directly, no platform_id round-trip needed).
    similar_lids = [lid for lid, _ in ranked]
    placeholders = ", ".join("?" for _ in similar_lids)
    with get_connection(settings.db_path) as conn:
        rows = conn.execute(
            f"SELECT * FROM listings WHERE listing_id IN ({placeholders})",
            similar_lids,
        ).fetchall()
    by_lid: dict[str, Any] = {
        str(row["listing_id"]): _to_listing_data(
            _reorder_image_urls(_parse_row(dict(row)), best_image_ids.get(str(row["listing_id"])))
        )
        for row in rows
    }

    # Honour the SimilarListing.cosine schema contract (raw DINOv2 cosine in
    # [-1, 1]). Previously we passed the fused RRF score, which is in the
    # 0.01–0.05 range and renders as "3%" / "4%" in the UI regardless of how
    # visually close the listings actually look. Ordering still comes from the
    # fused score; only the displayed number changes.
    n_with_cosine = 0
    results: list[SimilarListing] = []
    for lid, _fused_score in ranked:
        ld = by_lid.get(str(lid))
        if ld is None:
            continue
        visual_cosine = image_cosines.get(str(lid))
        if visual_cosine is not None:
            n_with_cosine += 1
        results.append(SimilarListing(
            listing_id=lid,
            cosine=float(visual_cosine) if visual_cosine is not None else 0.0,
            listing=ld,
        ))

    if results and n_with_cosine == 0:
        print(
            "[WARN] similar_listings.cosine_unavailable: "
            f"expected=DINOv2 cosine per result for listing_id={listing_id}, "
            f"got=0/{len(results)} results in the image index, "
            "fallback=cosine=0.0 (UI hides the match% chip)",
            flush=True,
        )

    return SimilarListingsResponse(
        query_listing_id=listing_id,
        results=results,
        meta={
            "model": "dinov2_vitl14_reg + arctic-embed-l-v2 + sql-features",
            "embed_dim": 1024,
            "aggregation": "RRF(image, text, feature), k=60",
            "k_requested": k_clamped,
            "k_returned": len(results),
            "cosine_coverage": n_with_cosine,
            "note": (
                "'cosine' is the max DINOv2 visual cosine (in [-1, 1]) between "
                "the query listing and each result; ordering is the fused RRF "
                "score over image + text + features. 0.0 means the result is "
                "outside the image index."
            ),
        },
    )


def _reorder_image_urls(row: dict[str, Any], best_image_id: str | None) -> dict[str, Any]:
    """Re-order ``row['image_urls']`` so the photo matching ``best_image_id``
    is first. Mutates and returns the row for chaining. No-op if the id
    doesn't match any URL (e.g. SRED montage image_ids, which don't map to
    S3 filenames 1:1).

    The DINOv2 ``image_id`` is shaped ``<source>/<platform_id>/<stem>``
    where ``stem`` matches the last path segment of the S3 URL (minus
    extension). Substring match on the stem handles .jpeg / .png / .webp
    / .JPEG variants without a format-specific matcher.
    """
    if not best_image_id:
        return row
    urls = list(row.get("image_urls") or [])
    if not urls:
        return row
    stem = best_image_id.rsplit("/", 1)[-1]
    if not stem:
        return row
    # Find the URL whose path contains the image_id stem (no-extension match).
    target_i = -1
    for i, url in enumerate(urls):
        if stem in str(url):
            target_i = i
            break
    if target_i <= 0:
        return row
    match = urls.pop(target_i)
    urls.insert(0, match)
    row["image_urls"] = urls
    row["hero_image_url"] = match
    return row
