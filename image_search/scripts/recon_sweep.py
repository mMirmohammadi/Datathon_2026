"""Stage 0 — class-existence recon sweep.

Samples a small cross-section of raw_data/ images, runs Stage 1 (SRED split) +
Stage 2 (triage only, no embedding storage), and reports the label distribution
across the 7 classes.

Gate: if either `floorplan` or `surroundings-or-view` count is zero, the script
exits with code 2 so the caller knows to stop-and-ask (the orchestrator handles
scaling up or downgrading in response).
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from collections import Counter
from pathlib import Path

from PIL import Image

import numpy as np
import torch

from image_search.common import paths
from image_search.common.embed import encode_images
from image_search.common.io import safe_open_image
from image_search.common.model import TINY_MODEL_ID, load
from image_search.common.prompts import ALL_CLASSES
from image_search.common.sred import SRED_MONTAGE_SIZE, split_sred_2x2
from image_search.common.status import step
from image_search.common.triage import (
    SOFTMAX_TEMPERATURE,
    TriageResult,
    _decide_from_scores,
    build_class_text_bank,
)


def _sample(n_robin: int, n_struct: int, n_sred: int, seed: int) -> list[paths.ImageRef]:
    rng = random.Random(seed)
    robin = list(paths.iter_robinreal())
    struct = list(paths.iter_structured())
    sred = list(paths.iter_sred())
    return (
        rng.sample(robin, min(n_robin, len(robin)))
        + rng.sample(struct, min(n_struct, len(struct)))
        + rng.sample(sred, min(n_sred, len(sred)))
    )


def _batches(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def run(
    *,
    model_id: str,
    n_robin: int,
    n_struct: int,
    n_sred: int,
    seed: int,
    out_dir: Path,
    batch_size: int,
) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)

    with step("recon_load_model"):
        lm = load(model_id)
        print(f"[INFO] model={lm.model_id} device={lm.device} dtype={lm.dtype} dim={lm.projection_dim}")

    with step("recon_build_class_bank") as s:
        bank = build_class_text_bank(lm)
        stacked = torch.stack([bank[c] for c in ALL_CLASSES], dim=0).to(torch.float32)
        stacked_np = stacked.detach().cpu().numpy()
        s["count"] = len(ALL_CLASSES)
        print(f"[INFO] class bank ready — {len(ALL_CLASSES)} classes")

    with step("recon_sample_refs") as s:
        refs = _sample(n_robin, n_struct, n_sred, seed)
        s["count"] = len(refs)

    # Materialize each ref into a list of (parent_id, PIL.Image) pairs.
    # SRED refs become 4 sub-images each; everything else is 1 image.
    items: list[tuple[str, Image.Image, str, str]] = []  # (key, img, source, parent_image_id)
    with step("recon_load_and_split") as s:
        for r in refs:
            img = safe_open_image(r.path)
            if img is None:
                s["warnings"] += 1
                continue
            if r.source == "sred" and img.size == SRED_MONTAGE_SIZE:
                crops = split_sred_2x2(img, parent_image_id=r.image_id)
                for c in crops:
                    key = f"{r.source}/{r.platform_id}/{r.image_id}#c{c.cell}"
                    items.append((key, c.resized, r.source, r.image_id))
            else:
                key = f"{r.source}/{r.platform_id}/{r.image_id}"
                items.append((key, img, r.source, r.image_id))
        s["count"] = len(items)

    with step("recon_triage", total=len(items)) as s:
        labels: list[TriageResult] = []
        keys: list[str] = []
        t0 = time.time()
        for batch in _batches(items, batch_size):
            batch_keys = [it[0] for it in batch]
            batch_imgs = [it[1] for it in batch]
            img_feats, keep_mask = encode_images(batch_imgs, lm, context="recon")
            # any NaN rows are already [WARN]ed; keep what's left.
            if not keep_mask.all():
                s["warnings"] += int((~keep_mask).sum())
            img_feats = img_feats[keep_mask]
            batch_keys = [k for k, keep in zip(batch_keys, keep_mask) if keep]
            if img_feats.shape[0] == 0:
                continue
            logits = img_feats @ stacked_np.T  # (N, C)
            probs = torch.softmax(
                torch.from_numpy(logits).float() * SOFTMAX_TEMPERATURE, dim=-1
            )
            batch_results = _decide_from_scores(probs, parent_ids=batch_keys)
            labels.extend(batch_results)
            keys.extend(batch_keys)
            s["count"] += len(batch)
            if s["count"] % max(batch_size * 4, 1) == 0:
                rate = s["count"] / max(time.time() - t0, 1e-6)
                print(f"[PROGRESS] triage {s['count']}/{len(items)}  {rate:.1f} img/s")

    dist = Counter(r.label for r in labels)
    per_source: dict[str, Counter] = {}
    for (key, _, source, _), r in zip(items, labels):
        per_source.setdefault(source, Counter())[r.label] += 1

    report = {
        "model_id": lm.model_id,
        "device": lm.device,
        "n_refs_sampled": len(refs),
        "n_images_after_split": len(items),
        "class_distribution": dict(dist),
        "per_source": {s: dict(c) for s, c in per_source.items()},
        "kept_count": sum(dist[c] for c in ("interior-room", "building-exterior",
                                             "surroundings-or-view", "floorplan")),
        "dropped_count": sum(dist[c] for c in ("logo-or-banner",
                                                "marketing-or-stock-photo",
                                                "other-uninformative")),
    }

    examples_per_label: dict[str, list[dict]] = {c: [] for c in ALL_CLASSES}
    for (key, _, source, _), r in zip(items, labels):
        bucket = examples_per_label[r.label]
        if len(bucket) < 5:
            bucket.append({
                "key": key,
                "source": source,
                "confidence": round(r.confidence, 4),
                "margin": round(r.margin, 4),
            })

    report["examples_per_label"] = examples_per_label

    report_path = out_dir / f"class_distribution.{model_id.split('/')[-1]}.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(f"[DONE] wrote {report_path}")
    print(f"[DIST] {json.dumps(dist, sort_keys=True)}")
    print(f"[SUMMARY] kept={report['kept_count']} dropped={report['dropped_count']}")

    zero_classes = [c for c in ("floorplan", "surroundings-or-view") if dist.get(c, 0) == 0]
    if zero_classes:
        print(f"[GATE] Zero count for: {zero_classes}. Stopping for user decision.")
        return 2
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--model", default=TINY_MODEL_ID)
    ap.add_argument("--n-robin", type=int, default=70)
    ap.add_argument("--n-struct", type=int, default=70)
    ap.add_argument("--n-sred", type=int, default=15)  # 15 sred × 4 crops = 60 items
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out-dir", type=Path, default=Path("image_search/data/recon"))
    ap.add_argument("--batch-size", type=int, default=8)
    args = ap.parse_args()

    return run(
        model_id=args.model,
        n_robin=args.n_robin,
        n_struct=args.n_struct,
        n_sred=args.n_sred,
        seed=args.seed,
        out_dir=args.out_dir,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    sys.exit(main())
