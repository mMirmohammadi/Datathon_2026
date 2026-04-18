"""Full 42k-image pipeline: enumerate → SRED split → triage → embed → store.

Streams items through the pipeline so memory stays bounded (we never hold more
than `--batch-size` PIL images in memory at once).

Usage:
    python -m image_search.scripts.run_full \
        --model google/siglip2-giant-opt-patch16-384 \
        --batch-size 32 \
        --out-dir image_search/data/full
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path

import numpy as np
import torch
from PIL import Image

from image_search.common import paths
from image_search.common.embed import encode_images
from image_search.common.io import safe_open_image
from image_search.common.model import GIANT_MODEL_ID, load
from image_search.common.prompts import (
    ALL_CLASSES,
    FLOORPLAN_CLASSES,
    MAIN_INDEX_CLASSES,
)
from image_search.common.sred import SRED_MONTAGE_SIZE, split_sred_2x2
from image_search.common.status import step
from image_search.common.store import EmbeddingStore, ImageRow
from image_search.common.triage import (
    SOFTMAX_TEMPERATURE,
    _decide_from_scores,
    build_class_text_bank,
)


def _materialize_ref(ref: paths.ImageRef) -> list[tuple[str, str, str, str, int | None, Image.Image]]:
    """Turn one ImageRef into one or more downstream items, splitting SRED montages."""
    img = safe_open_image(ref.path)
    if img is None:
        return []
    if ref.source == "sred" and img.size == SRED_MONTAGE_SIZE:
        crops = split_sred_2x2(img, parent_image_id=ref.image_id)
        return [
            (f"{ref.source}/{ref.platform_id}/{ref.image_id}#c{c.cell}",
             ref.source, ref.platform_id, str(ref.path), c.cell, c.resized)
            for c in crops
        ]
    return [(f"{ref.source}/{ref.platform_id}/{ref.image_id}",
             ref.source, ref.platform_id, str(ref.path), None, img)]


def _stream_items():
    for ref in paths.iter_all():
        for item in _materialize_ref(ref):
            yield item


def run(*, model_id: str, batch_size: int, out_dir: Path, log_every: int) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    status_log = out_dir / "run_status.jsonl"

    with step("load_model", log_path=status_log) as s:
        lm = load(model_id)
        print(f"[INFO] model={lm.model_id} device={lm.device} dtype={lm.dtype} dim={lm.projection_dim}")
        s["extra"]["model"] = lm.model_id
        s["extra"]["device"] = lm.device
        s["extra"]["dtype"] = str(lm.dtype)
        s["extra"]["projection_dim"] = lm.projection_dim

    with step("build_class_bank", log_path=status_log) as s:
        bank = build_class_text_bank(lm)
        stacked = torch.stack([bank[c] for c in ALL_CLASSES], dim=0).to(torch.float32)
        stacked_np = stacked.detach().cpu().numpy()
        s["count"] = len(ALL_CLASSES)

    t_start = time.time()
    label_dist = Counter()
    per_source = Counter()
    warnings_count = 0
    n_processed = 0

    store = EmbeddingStore(out_dir / "store", projection_dim=lm.projection_dim)
    try:
        batch: list = []
        with step("stream_triage_embed_store", log_path=status_log) as s:
            def flush(batch_items: list) -> None:
                nonlocal warnings_count, n_processed, label_dist, per_source
                if not batch_items:
                    return
                imgs = [b[5] for b in batch_items]
                keys = [b[0] for b in batch_items]
                feats, keep_mask = encode_images(imgs, lm, context="full_triage")
                if not keep_mask.all():
                    warnings_count += int((~keep_mask).sum())
                feats_kept = feats[keep_mask]
                kept_batch = [b for b, keep in zip(batch_items, keep_mask) if keep]
                kept_keys = [k for k, keep in zip(keys, keep_mask) if keep]
                if feats_kept.shape[0] == 0:
                    n_processed += len(batch_items)
                    return

                logits = feats_kept @ stacked_np.T
                probs = torch.softmax(
                    torch.from_numpy(logits).float() * SOFTMAX_TEMPERATURE, dim=-1
                )
                results = _decide_from_scores(probs, parent_ids=kept_keys)

                for it, r, feat in zip(kept_batch, results, feats_kept):
                    row = ImageRow(
                        image_id=it[0], source=it[1], platform_id=it[2], path=it[3],
                        sred_cell=it[4],
                        relevance_label=r.label,
                        relevance_confidence=r.confidence,
                        relevance_margin=r.margin,
                    )
                    store.register_listing(it[2], it[1])
                    if r.label in FLOORPLAN_CLASSES:
                        store.add_floorplan_row(row, feat)
                    elif r.label in MAIN_INDEX_CLASSES:
                        store.add_main_row(row, feat)
                    else:
                        store.add_dropped_row(row)
                    label_dist[r.label] += 1
                    per_source[it[1]] += 1
                store.commit()  # periodic commit so crash ≠ total loss

                n_processed += len(batch_items)
                s["count"] = n_processed
                s["warnings"] = warnings_count

            for item in _stream_items():
                batch.append(item)
                if len(batch) >= batch_size:
                    flush(batch)
                    batch = []
                    if n_processed % log_every == 0:
                        rate = n_processed / max(time.time() - t_start, 1e-6)
                        print(f"[PROGRESS] processed={n_processed} "
                              f"rate={rate:.2f} img/s "
                              f"label_dist={dict(label_dist.most_common(4))}")
            flush(batch)
            s["extra"]["label_dist"] = dict(label_dist)
            s["extra"]["per_source"] = dict(per_source)
            s["extra"]["rate_img_per_s"] = round(
                n_processed / max(time.time() - t_start, 1e-6), 2
            )
    finally:
        store.close()

    duration = time.time() - t_start
    summary = {
        "model_id": model_id,
        "n_processed": n_processed,
        "duration_s": round(duration, 2),
        "rate_img_per_s": round(n_processed / max(duration, 1e-6), 2),
        "label_dist": dict(label_dist),
        "per_source": dict(per_source),
        "warnings_count": warnings_count,
        "kept_main": sum(label_dist[c] for c in MAIN_INDEX_CLASSES),
        "kept_floorplan": sum(label_dist[c] for c in FLOORPLAN_CLASSES),
        "dropped": sum(label_dist[c]
                       for c in ALL_CLASSES
                       if c not in MAIN_INDEX_CLASSES | FLOORPLAN_CLASSES),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--model", default=GIANT_MODEL_ID)
    ap.add_argument("--batch-size", type=int, default=32)
    ap.add_argument("--out-dir", type=Path, default=Path("image_search/data/full"))
    ap.add_argument("--log-every", type=int, default=1024)
    args = ap.parse_args()
    return run(model_id=args.model, batch_size=args.batch_size,
               out_dir=args.out_dir, log_every=args.log_every)


if __name__ == "__main__":
    sys.exit(main())
