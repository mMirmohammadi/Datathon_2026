"""Pick N random images per label from the full-run index, render thumbnails
(re-splitting SRED montages on demand), show embedding stats, and write a
markdown with embedded images.
"""
from __future__ import annotations

import argparse
import random
import re
import sqlite3
import sys
from pathlib import Path

import numpy as np
from PIL import Image

from image_search.common.io import safe_open_image
from image_search.common.sred import split_sred_2x2


SRED_KEY_RE = re.compile(r"^sred/([^/]+)/([^/#]+)#c(\d)$")


def _load_image_by_key(image_id: str, path_on_disk_as_stored: str,
                       local_raw_data_root: Path,
                       qolam_raw_data_root: str) -> Image.Image | None:
    """Translate qolam path back to local raw_data path, load, and split SRED."""
    path = Path(path_on_disk_as_stored.replace(qolam_raw_data_root,
                                               str(local_raw_data_root)))
    if not path.exists():
        return None
    img = safe_open_image(path)
    if img is None:
        return None
    m = SRED_KEY_RE.match(image_id)
    if m is not None:
        cell_idx = int(m.group(3))
        if img.size == (224, 224):
            crops = split_sred_2x2(img, parent_image_id=m.group(2))
            return crops[cell_idx].resized
    return img


def _save_thumb(img: Image.Image, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    thumb = img.copy()
    thumb.thumbnail((384, 384))
    if thumb.mode != "RGB":
        thumb = thumb.convert("RGB")
    thumb.save(out_path, "JPEG", quality=85)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--store-dir", type=Path,
                    default=Path("image_search/data/full/store"))
    ap.add_argument("--out-dir", type=Path,
                    default=Path("image_search/data/full/samples"))
    ap.add_argument("--raw-data-root", type=Path,
                    default=Path("raw_data").resolve())
    ap.add_argument("--qolam-raw-root", type=str,
                    default="/home/mahbod/datathon_image_search/raw_data")
    ap.add_argument("--per-label", type=int, default=3)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = random.Random(args.seed)

    db = sqlite3.connect(f"file:{args.store_dir / 'index.sqlite'}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    main_arr = np.load(args.store_dir / "embeddings.fp32.npy")
    floor_arr = np.load(args.store_dir / "floorplans.fp32.npy")

    # Gather rows per label (ORDER BY random)
    labels = [r[0] for r in db.execute(
        "SELECT DISTINCT relevance_label FROM images ORDER BY relevance_label;"
    )]
    per_label_rows: dict[str, list[dict]] = {}
    for label in labels:
        rows = [dict(r) for r in db.execute(
            "SELECT image_id, source, platform_id, path, sred_cell, "
            "relevance_label, relevance_confidence, relevance_margin, "
            "index_kind, row_idx "
            "FROM images WHERE relevance_label=?;",
            (label,)).fetchall()]
        rng.shuffle(rows)
        per_label_rows[label] = rows[:args.per_label]

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    thumbs_dir = out_dir / "thumbs"
    thumbs_dir.mkdir(parents=True, exist_ok=True)

    lines = [
        "# Sample images — kept vs dropped",
        "",
        f"Random (seed={args.seed}) sample of **{args.per_label} images per label** drawn "
        f"from the full-run index `{args.store_dir}`. For each: the rendered "
        "image (with SRED sub-crops regenerated on demand), the triage label + "
        "confidence, and the first 8 components of the 1536-d L2-normalized "
        "embedding vector stored in `embeddings.fp32.npy` or `floorplans.fp32.npy`.",
        "",
    ]

    for label in labels:
        lines.append(f"## `{label}`")
        lines.append("")
        for row in per_label_rows[label]:
            image_id = row["image_id"]
            img = _load_image_by_key(
                image_id, row["path"], args.raw_data_root, args.qolam_raw_root
            )
            if img is None:
                lines.append(f"- (image not found on disk: `{image_id}`)")
                continue
            thumb_name = image_id.replace("/", "__").replace("#", "_")
            thumb_rel = Path("thumbs") / f"{thumb_name}.jpg"
            _save_thumb(img, out_dir / thumb_rel)

            # Retrieve the stored embedding
            if row["index_kind"] == "main":
                vec = main_arr[row["row_idx"]]
                index_name = "embeddings.fp32.npy"
            elif row["index_kind"] == "floorplan":
                vec = floor_arr[row["row_idx"]]
                index_name = "floorplans.fp32.npy"
            else:
                vec = None
                index_name = "— (dropped, no embedding stored)"

            lines.append(f"### `{image_id}`")
            lines.append("")
            lines.append(f"![{label}]({thumb_rel})")
            lines.append("")
            lines.append(f"- **label**: `{label}`  **kind**: `{row['index_kind']}`")
            lines.append(
                f"- **confidence**: {row['relevance_confidence']:.4f}  "
                f"**margin**: {row['relevance_margin']:.4f}"
            )
            lines.append(f"- **source**: `{row['source']}`  "
                         f"**platform_id**: `{row['platform_id']}`")
            if vec is not None:
                norm = float(np.linalg.norm(vec))
                head = ", ".join(f"{v: .4f}" for v in vec[:8])
                lines.append(f"- **embedding** (row {row['row_idx']} in `{index_name}`): "
                             f"dim=1536 L2={norm:.4f} dtype={vec.dtype}")
                lines.append(f"- first 8 values: `[{head}, ...]`")
            else:
                lines.append(f"- **embedding**: {index_name}")
            lines.append("")

    report_path = out_dir / "samples.md"
    report_path.write_text("\n".join(lines) + "\n")
    print(f"[OUT] wrote {report_path}")
    print(f"[OUT] thumbs: {thumbs_dir} ({len(list(thumbs_dir.iterdir()))} files)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
