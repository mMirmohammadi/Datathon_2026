"""Image-to-image query against the DINOv2 Tier-1 index.

Takes a query image path, encodes it with DINOv2 ViT-L/14 reg + GeM, runs
cosine top-K against the main index, aggregates per listing with max-pool,
and prints top-K listings with the best matching image per listing.

Usage:
    python -m image_search.scripts.query_dinov2 \
        --data-dir      image_search/data/full/dinov2_store \
        --siglip-index  image_search/data/full/store/index.sqlite \
        --image         /path/to/query.jpg \
        --k-candidates  200 \
        --k-listings    20
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from PIL import Image

from image_search.common.dinov2_model import load as load_dinov2
from image_search.common.dinov2_query import (
    aggregate_per_listing,
    encode_query_image,
    load_dinov2_index,
    search_topk,
)
from image_search.common.io import safe_open_image


def run(
    *,
    data_dir: Path,
    siglip_index: Path,
    image_path: Path,
    k_candidates: int,
    k_listings: int,
    include_floorplan: bool,
    device: str | None,
) -> list[dict]:
    idx = load_dinov2_index(data_dir, siglip_index)
    print(f"[INFO] index loaded: main={idx.main_matrix.shape} "
          f"floor={idx.floor_matrix.shape}", flush=True)

    pil = safe_open_image(image_path)
    if pil is None:
        raise FileNotFoundError(f"could not read query image: {image_path}")

    lm = load_dinov2(device=device)
    qv = encode_query_image(pil, lm)

    main_idx, main_scores = search_topk(qv, idx.main_matrix, top_k=k_candidates)
    main_hits = aggregate_per_listing(
        main_idx, main_scores, idx.main_row_info,
        top_k_listings=k_listings,
    )

    results = [
        {
            "rank": i + 1,
            "score": round(h.score, 6),
            "source": h.info.source,
            "platform_id": h.info.platform_id,
            "image_id": h.info.image_id,
            "path": h.info.path,
            "label": h.info.relevance_label,
            "kind": "main",
        }
        for i, h in enumerate(main_hits)
    ]

    if include_floorplan and idx.floor_matrix.shape[0] > 0:
        fp_idx, fp_scores = search_topk(
            qv, idx.floor_matrix, top_k=k_candidates,
        )
        fp_hits = aggregate_per_listing(
            fp_idx, fp_scores, idx.floor_row_info,
            top_k_listings=k_listings,
        )
        results += [
            {
                "rank": i + 1,
                "score": round(h.score, 6),
                "source": h.info.source,
                "platform_id": h.info.platform_id,
                "image_id": h.info.image_id,
                "path": h.info.path,
                "label": h.info.relevance_label,
                "kind": "floorplan",
            }
            for i, h in enumerate(fp_hits)
        ]

    return results


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", type=Path,
                    default=Path("image_search/data/full/dinov2_store"))
    ap.add_argument("--siglip-index", type=Path,
                    default=Path("image_search/data/full/store/index.sqlite"))
    ap.add_argument("--image", type=Path, required=True)
    ap.add_argument("--k-candidates", type=int, default=200,
                    help="top-K images to consider before per-listing aggregation")
    ap.add_argument("--k-listings", type=int, default=20,
                    help="number of listings to return")
    ap.add_argument("--include-floorplan", action="store_true",
                    help="also query the floorplan index separately")
    ap.add_argument("--device", default=None)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    results = run(
        data_dir=args.data_dir,
        siglip_index=args.siglip_index,
        image_path=args.image,
        k_candidates=args.k_candidates,
        k_listings=args.k_listings,
        include_floorplan=args.include_floorplan,
        device=args.device,
    )
    text = json.dumps(results, indent=2) + "\n"
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text)
        print(f"[OUT] wrote {args.out}")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
