"""Deep cross-check of a visual query's top-k results.

Given an already-run queries.json (top-k per query with image_id/source/platform_id/path):
  - For each rank, look up the listing metadata in the matching source CSV.
  - Render a thumbnail (regenerating SRED sub-crops on demand).
  - Emit a markdown that places image, label, listing attributes, and description
    side-by-side so the user can judge each match.

Usage:
  python -m image_search.scripts.crosscheck_query_topk \
      --queries-json image_search/data/full/queries.json \
      --query-index 0 \
      --k 20 \
      --out-dir image_search/data/full/query_crosscheck
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path

from PIL import Image

from image_search.common.io import safe_open_image
from image_search.common.sred import split_sred_2x2


QOLAM_ROOT = "/home/mahbod/datathon_image_search/raw_data"
LOCAL_ROOT = Path("raw_data").resolve()
SRED_RE = re.compile(r"^sred/([^/]+)/([^/#]+)#c(\d)$")


def _load_csv_index(path: Path, key_col: str = "platform_id") -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not path.exists():
        return out
    with path.open() as f:
        for row in csv.DictReader(f):
            k = (row.get(key_col) or "").strip().strip('"')
            if k:
                out[k] = row
    return out


def _load_image(image_id: str, qolam_path: str) -> Image.Image | None:
    local = Path(qolam_path.replace(QOLAM_ROOT, str(LOCAL_ROOT)))
    if not local.exists():
        return None
    img = safe_open_image(local)
    if img is None:
        return None
    m = SRED_RE.match(image_id)
    if m is not None and img.size == (224, 224):
        crops = split_sred_2x2(img, parent_image_id=m.group(2))
        return crops[int(m.group(3))].resized
    return img


def _save_thumb(img: Image.Image, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    t = img.copy()
    t.thumbnail((384, 384))
    if t.mode != "RGB":
        t = t.convert("RGB")
    t.save(out_path, "JPEG", quality=85)


def _parse_address(raw: str) -> dict:
    if not raw or not raw.startswith("{"):
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _attrs_from_row(row: dict) -> dict:
    loc = _parse_address(row.get("location_address", ""))
    return {
        "title": (row.get("title") or "").strip('"'),
        "city": loc.get("city") or loc.get("object_city") or "",
        "canton": (loc.get("object_state") or "").upper(),
        "zip": loc.get("object_zip") or "",
        "rooms": row.get("number_of_rooms", "").strip(),
        "area": row.get("area", "").strip(),
        "price": row.get("price", "").strip(),
        "object_type": row.get("object_type_text", "") or row.get("object_type", ""),
        "object_category": row.get("object_category", ""),
        "status": row.get("status", ""),
        "year_built": row.get("year_built", ""),
        "floor": row.get("floor", ""),
        "desc": re.sub(r"\s+", " ",
                       (row.get("object_description", "") or "")).strip()[:320],
    }


def run(queries_path: Path, query_idx: int, k: int, out_dir: Path) -> int:
    queries = json.loads(queries_path.read_text())
    if query_idx >= len(queries):
        raise IndexError(f"query_idx {query_idx} out of range (len={len(queries)})")

    entry = queries[query_idx]
    query = entry["query"]
    top_k = entry["top_k"][:k]

    idx_struct = _load_csv_index(
        Path("raw_data/structured_data_withimages_updated.csv"))
    idx_robin = _load_csv_index(
        Path("raw_data/robinreal_data_withimages-1776461278845.csv"))
    # SRED CSV's primary key is the image filename stem (no platform_id column)
    sred_rows: dict[str, dict] = {}
    sred_csv = Path("raw_data/sred_data_withmontageimages_latlong.csv")
    if sred_csv.exists():
        with sred_csv.open() as f:
            for r in csv.DictReader(f):
                # sred uses `id` as stem e.g. 1154156
                pid = (r.get("id") or "").strip()
                if pid:
                    sred_rows[pid] = r

    out_dir.mkdir(parents=True, exist_ok=True)
    thumbs_dir = out_dir / "thumbs"
    thumbs_dir.mkdir(exist_ok=True)
    report = out_dir / f"query_{query_idx}_top{k}.md"

    lines = [
        f"# Cross-check: `{query}` — top-{k}",
        "",
        f"Deep inspection of the top-{k} images returned by the full index "
        f"for the query above. Each entry shows the thumbnail, the image's "
        "triage label, and the source listing's raw CSV attributes so you can "
        "verify the match.",
        "",
    ]

    for rank_item in top_k:
        rnk = rank_item["rank"]
        sim = rank_item["sim"]
        src = rank_item["source"]
        pid = rank_item["platform_id"]
        iid = rank_item["image_id"]
        path = rank_item["path"]
        label = rank_item["label"]

        img = _load_image(iid, path)
        thumb_md = "(image not on disk)"
        if img is not None:
            thumb_name = f"r{rnk:02d}_" + hashlib.md5(iid.encode()).hexdigest()[:10] + ".jpg"
            _save_thumb(img, thumbs_dir / thumb_name)
            thumb_md = f"![r{rnk}](thumbs/{thumb_name})"

        if src == "structured":
            row = idx_struct.get(pid)
            csv_name = "structured_data_withimages_updated.csv"
        elif src == "robinreal":
            row = idx_robin.get(pid)
            csv_name = "robinreal_data_withimages-1776461278845.csv"
        elif src == "sred":
            row = sred_rows.get(pid)
            csv_name = "sred_data_withmontageimages_latlong.csv"
        else:
            row = None
            csv_name = "?"

        if row is not None and (src in ("structured", "robinreal")):
            a = _attrs_from_row(row)
            listing_block = (
                f"- **title**: `{a['title']}`\n"
                f"- **city**: {a['city']}  **canton**: {a['canton']}  **zip**: {a['zip']}\n"
                f"- **rooms**: {a['rooms']}  **area**: {a['area']} m²  **price**: CHF {a['price']}  "
                f"**object_type**: {a['object_type']}\n"
                f"- **status**: {a['status']}  **year_built**: {a['year_built']}  "
                f"**floor**: {a['floor']}\n"
                f"- **description_snippet**: {a['desc'] or '(empty)'}"
            )
        elif row is not None and src == "sred":
            # SRED columns differ — minimal attrs
            listing_block = (
                f"- **sred id**: `{pid}`  "
                f"**lat**: {row.get('latitude','')}  **lng**: {row.get('longitude','')}  "
                f"**price**: CHF {row.get('price','')}  **rooms**: {row.get('number_of_rooms','')}"
            )
        else:
            listing_block = f"- (listing row not found in `{csv_name}`)"

        lines.append(f"## #{rnk}  sim={sim:.4f}  label=`{label}`")
        lines.append("")
        lines.append(thumb_md)
        lines.append("")
        lines.append(f"- **source**: `{src}`  **platform_id**: `{pid}`")
        lines.append(f"- **image_id**: `{iid}`")
        lines.append(listing_block)
        lines.append("")

    report.write_text("\n".join(lines) + "\n")
    print(f"wrote {report}")
    print(f"thumbs: {thumbs_dir} ({len(list(thumbs_dir.iterdir()))} files)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--queries-json", type=Path,
                    default=Path("image_search/data/full/queries.json"))
    ap.add_argument("--query-index", type=int, default=0)
    ap.add_argument("--k", type=int, default=20)
    ap.add_argument("--out-dir", type=Path,
                    default=Path("image_search/data/full/query_crosscheck"))
    args = ap.parse_args()
    return run(args.queries_json, args.query_index, args.k, args.out_dir)


if __name__ == "__main__":
    main()
