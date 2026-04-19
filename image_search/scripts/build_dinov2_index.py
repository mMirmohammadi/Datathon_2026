"""Build the DINOv2 Tier-1 GeM index from the existing SigLIP index.

Streams rows from image_search/data/full/store/index.sqlite where
index_kind IN ('main', 'floorplan') --- i.e. every image that SigLIP
kept after triage. For each row:

    - load the image (with safe_open_image)
    - if sred_cell is set, crop the 112x112 SRED cell
    - apply the canonical DINOv2 eval transform (Resize-256 + CenterCrop-224 + Normalize)
    - forward DINOv2 ViT-L/14 reg, GeM-pool, L2-normalize
    - write to the DINOv2 store

Emits artifacts under the output directory:

    main.fp32.npy            shape (N_main, 1024)
    floorplans.fp32.npy      shape (N_floor, 1024)
    index.sqlite             image_id PK + row_idx + index_kind
    build_report.json        rate, warnings, kind counts, skip list
    run_status.jsonl         per-step status lines (for debugging)

Usage:
    python -m image_search.scripts.build_dinov2_index \
        --siglip-index image_search/data/full/store/index.sqlite \
        --out-dir      image_search/data/full/dinov2_store \
        --batch-size   32
        [--limit N]       # smoke-test: process only N rows
        [--random-sample] # pick random rows instead of sorted (for smoke)
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from PIL import Image

from image_search.common.dinov2_embed import encode_images
from image_search.common.dinov2_model import EMBED_DIM, load as load_dinov2
from image_search.common.dinov2_sred import crop_sred_cell
from image_search.common.dinov2_store import Dinov2Row, Dinov2Store
from image_search.common.io import safe_open_image
from image_search.common.status import step
from image_search.common.warn import warn


@dataclass
class SourceRow:
    image_id: str
    source: str
    platform_id: str
    path: str
    sred_cell: int | None
    index_kind: str  # 'main' | 'floorplan'


def _iter_source_rows(
    siglip_db: Path, *, limit: int | None, random_sample: bool
) -> Iterator[SourceRow]:
    conn = sqlite3.connect(f"file:{siglip_db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    order = "RANDOM()" if random_sample else "image_id"
    query = (
        "SELECT image_id, source, platform_id, path, sred_cell, index_kind "
        "FROM images WHERE index_kind IN ('main','floorplan') "
        f"ORDER BY {order}"
    )
    if limit is not None:
        query += f" LIMIT {int(limit)}"
    for r in conn.execute(query):
        yield SourceRow(
            image_id=r["image_id"],
            source=r["source"],
            platform_id=r["platform_id"],
            path=r["path"],
            sred_cell=r["sred_cell"],
            index_kind=r["index_kind"],
        )
    conn.close()


def _materialize(row: SourceRow) -> Image.Image | None:
    """Load + optional SRED crop. Returns PIL RGB image or None on failure."""
    img = safe_open_image(Path(row.path))
    if img is None:
        return None
    if row.sred_cell is not None:
        if img.size != (224, 224):
            warn("sred_size_unexpected", image_id=row.image_id,
                 path=row.path, expected=(224, 224), got=img.size,
                 fallback="skip row")
            return None
        try:
            return crop_sred_cell(img, row.sred_cell,
                                   parent_image_id=row.image_id)
        except ValueError:
            # crop_sred_cell already emitted [WARN] sred_guard
            return None
    return img


def run(*, siglip_db: Path, out_dir: Path, batch_size: int,
        limit: int | None, random_sample: bool, log_every: int,
        device: str | None, entry: str) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    status_log = out_dir / "run_status.jsonl"

    with step("load_dinov2", log_path=status_log) as s:
        lm = load_dinov2(device=device, entry=entry)
        print(f"[INFO] dinov2 entry={lm.entry} device={lm.device} "
              f"dtype={lm.dtype} dim={lm.embed_dim} input={lm.input_size}",
              flush=True)
        s["extra"].update(
            entry=lm.entry, device=lm.device,
            dtype=str(lm.dtype), embed_dim=lm.embed_dim,
            input_size=lm.input_size,
        )

    t_start = time.time()
    n_processed = 0
    n_skipped_load = 0
    n_skipped_nan = 0
    per_kind = Counter()
    skipped_ids: list[tuple[str, str]] = []  # (image_id, reason)

    with Dinov2Store(out_dir, projection_dim=lm.embed_dim) as store:
        with step("stream_encode_store", log_path=status_log) as s:
            batch_rows: list[SourceRow] = []
            batch_imgs: list[Image.Image] = []

            def flush() -> None:
                nonlocal n_processed, n_skipped_nan
                if not batch_imgs:
                    return
                feats, keep = encode_images(batch_imgs, lm,
                                             context="dinov2_index_build")
                # Align with batch_rows (1:1) --- batch_imgs built in same order
                assert len(batch_rows) == len(batch_imgs) == feats.shape[0]
                for src, feat, k in zip(batch_rows, feats, keep):
                    if not k:
                        skipped_ids.append((src.image_id, "nan_embedding"))
                        n_skipped_nan += 1
                        continue
                    row = Dinov2Row(image_id=src.image_id)
                    if src.index_kind == "main":
                        store.add_main_row(row, feat)
                    else:
                        store.add_floorplan_row(row, feat)
                    per_kind[src.index_kind] += 1
                n_processed += len(batch_rows)
                s["count"] = n_processed
                s["warnings"] = n_skipped_load + n_skipped_nan

            for src in _iter_source_rows(siglip_db, limit=limit,
                                          random_sample=random_sample):
                img = _materialize(src)
                if img is None:
                    skipped_ids.append((src.image_id, "load_failure"))
                    n_skipped_load += 1
                    continue
                batch_rows.append(src)
                batch_imgs.append(img)
                if len(batch_imgs) >= batch_size:
                    flush()
                    batch_rows = []
                    batch_imgs = []
                    if n_processed % log_every == 0 and n_processed > 0:
                        rate = n_processed / max(time.time() - t_start, 1e-6)
                        print(f"[PROGRESS] processed={n_processed} "
                              f"rate={rate:.2f} img/s main={per_kind['main']} "
                              f"floor={per_kind['floorplan']} "
                              f"skipped_load={n_skipped_load} "
                              f"skipped_nan={n_skipped_nan}",
                              flush=True)
                    # Note: no periodic store.commit() here. The .npy files
                    # are only written atomically in store.close(), so a
                    # partial sqlite with row_idx pointing to a non-existent
                    # npy would be worse than no progress at all. All-or-
                    # nothing durability is intentional.
            flush()

    duration = time.time() - t_start
    report = {
        "dinov2_entry": lm.entry,
        "embed_dim": lm.embed_dim,
        "input_size": lm.input_size,
        "device": lm.device,
        "dtype": str(lm.dtype),
        "siglip_db": str(siglip_db),
        "out_dir": str(out_dir),
        "duration_s": round(duration, 2),
        "n_processed": n_processed,
        "n_main": int(per_kind["main"]),
        "n_floorplan": int(per_kind["floorplan"]),
        "n_skipped_load": n_skipped_load,
        "n_skipped_nan": n_skipped_nan,
        "rate_img_per_s": round(n_processed / max(duration, 1e-6), 2),
        "skipped_ids": skipped_ids[:500],  # cap for JSON size
        "n_skipped_total": len(skipped_ids),
        "batch_size": batch_size,
        "limit": limit,
        "random_sample": random_sample,
    }
    (out_dir / "build_report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "skipped_ids"},
                     indent=2), flush=True)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--siglip-index", type=Path,
                    default=Path("image_search/data/full/store/index.sqlite"))
    ap.add_argument("--out-dir", type=Path,
                    default=Path("image_search/data/full/dinov2_store"))
    ap.add_argument("--batch-size", type=int, default=32)
    ap.add_argument("--log-every", type=int, default=1024)
    ap.add_argument("--limit", type=int, default=None,
                    help="smoke-test: only process N rows")
    ap.add_argument("--random-sample", action="store_true",
                    help="pick random rows (use with --limit for smoke tests)")
    ap.add_argument("--device", default=None,
                    help="override device (cuda/mps/cpu)")
    ap.add_argument("--entry", default="dinov2_vitl14_reg",
                    help="torch.hub entry (default: dinov2_vitl14_reg)")
    args = ap.parse_args()

    if not args.siglip_index.exists():
        print(f"[ERROR] SigLIP index not found at {args.siglip_index}",
              file=sys.stderr)
        return 2
    return run(
        siglip_db=args.siglip_index,
        out_dir=args.out_dir,
        batch_size=args.batch_size,
        limit=args.limit,
        random_sample=args.random_sample,
        log_every=args.log_every,
        device=args.device,
        entry=args.entry,
    )


if __name__ == "__main__":
    sys.exit(main())
