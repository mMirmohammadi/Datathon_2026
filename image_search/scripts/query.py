"""Text-query the main embedding index with max-cosine per listing aggregation.

For a given query:
 1. Encode text with the same SigLIP 2 checkpoint used to build the index.
 2. Compute cosine sim against every main-index image vector.
 3. Group by listing (source, platform_id) and aggregate by max: the best
    image in a listing wins, not the average.
 4. Return top-k listings with the best-matching image_id per listing.

Usage:
    python -m image_search.scripts.query \
        --store-dir image_search/data/full/store \
        --model google/siglip2-giant-opt-patch16-384 \
        --k 20 \
        --query "a big bright modern house"
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

import numpy as np

from image_search.common.embed import encode_text
from image_search.common.model import GIANT_MODEL_ID, load


def run_query(store_dir: Path, model_id: str, queries: list[str], k: int) -> list[dict]:
    db = sqlite3.connect(f"file:{store_dir / 'index.sqlite'}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    main = np.load(store_dir / "embeddings.fp32.npy")
    if main.size == 0:
        raise RuntimeError(f"main embedding matrix is empty at {store_dir}")

    row_info = {
        r["row_idx"]: dict(r)
        for r in db.execute(
            "SELECT row_idx, image_id, source, platform_id, path, "
            "relevance_label, relevance_confidence "
            "FROM images WHERE index_kind='main' ORDER BY row_idx;"
        ).fetchall()
    }
    db.close()

    lm = load(model_id)
    text_feats, _ = encode_text(queries, lm, context="query_batch")  # (Q, D)

    out = []
    for q, t_vec in zip(queries, text_feats):
        sims = main @ t_vec  # (N,)
        # Group by (source, platform_id) and take max similarity
        best_per_listing: dict[tuple[str, str], dict] = {}
        for idx in range(main.shape[0]):
            info = row_info[idx]
            key = (info["source"], info["platform_id"])
            sim = float(sims[idx])
            existing = best_per_listing.get(key)
            if existing is None or sim > existing["sim"]:
                best_per_listing[key] = {
                    "source": info["source"],
                    "platform_id": info["platform_id"],
                    "sim": sim,
                    "image_id": info["image_id"],
                    "label": info["relevance_label"],
                    "path": info["path"],
                }
        ranked = sorted(best_per_listing.values(), key=lambda r: -r["sim"])[:k]
        for i, r in enumerate(ranked):
            r["rank"] = i + 1
        out.append({"query": q, "top_k": ranked})
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--store-dir", type=Path, required=True)
    ap.add_argument("--model", default=GIANT_MODEL_ID)
    ap.add_argument("--k", type=int, default=20)
    ap.add_argument("--query", action="append", required=True,
                    help="Repeatable; encode each query separately and report top-k.")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    results = run_query(args.store_dir, args.model, args.query, args.k)

    text = json.dumps(results, indent=2) + "\n"
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text)
        print(f"[OUT] wrote {args.out}")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
