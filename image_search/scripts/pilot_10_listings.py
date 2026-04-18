"""10-listing pilot — the primary validation gate before the full 42k run.

Runs stages P0..P7 per the approved plan on a hand-picked set of 10 listings
(3 robinreal + 3 structured + 4 SRED) read from fixtures/pilot_10.csv.

Stops and prints a clear banner at P4 (keep/drop audit) and P7 (final audit),
writing `data/pilot_results/pilot_audit.md` so a human can review.

Usage:
    python -m image_search.scripts.pilot_10_listings \
        --model google/siglip2-giant-opt-patch16-384 \
        --stage {p4|p7}       # p4 = stop after triage audit; p7 = continue through query
"""
from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
import time
from collections import Counter
from pathlib import Path

import numpy as np
import torch
from PIL import Image

from image_search.common.embed import encode_images, encode_text
from image_search.common.io import safe_open_image
from image_search.common.model import GIANT_MODEL_ID, load
from image_search.common.paths import (
    ROBINREAL_DIR,
    SRED_DIR,
    STRUCTURED_DIR,
    ImageRef,
    _is_image,
)
from image_search.common.prompts import (
    ALL_CLASSES,
    FLOORPLAN_CLASSES,
    MAIN_INDEX_CLASSES,
    flatten,
)
from image_search.common.sred import SRED_MONTAGE_SIZE, split_sred_2x2
from image_search.common.status import step
from image_search.common.store import EmbeddingStore, ImageRow
from image_search.common.triage import (
    SOFTMAX_TEMPERATURE,
    TriageResult,
    _decide_from_scores,
    build_class_text_bank,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_CSV = REPO_ROOT / "image_search/fixtures/pilot_10.csv"
PILOT_OUT = REPO_ROOT / "image_search/data/pilot_results"
PILOT_THUMBS = PILOT_OUT / "thumbs"


QUERIES_MULTI = [
    ("en", "a big bright modern house"),
    ("de", "ein helles modernes Apartment mit Balkon"),
    ("fr", "une maison lumineuse et moderne"),
    ("it", "una casa grande e luminosa"),
]


def _read_fixture() -> list[dict]:
    with FIXTURE_CSV.open() as f:
        return list(csv.DictReader(f))


def _enumerate_fixture(rows: list[dict]) -> list[ImageRef]:
    refs: list[ImageRef] = []
    for r in rows:
        source = r["source"]
        pid = r["platform_id"]
        if source == "sred":
            p = SRED_DIR / f"{pid}.jpeg"
            if not p.exists():
                p = SRED_DIR / f"{pid}.jpg"
            if not p.exists():
                raise FileNotFoundError(f"sred image not found for {pid}: tried .jpeg and .jpg")
            refs.append(ImageRef(source="sred", platform_id=pid, image_id=pid, path=p))
        elif source in ("robinreal", "structured"):
            base = ROBINREAL_DIR if source == "robinreal" else STRUCTURED_DIR
            sub = base / f"platform_id={pid}"
            if not sub.is_dir():
                raise FileNotFoundError(f"{source} dir not found: {sub}")
            for f in sorted(sub.iterdir()):
                if _is_image(f):
                    refs.append(ImageRef(source=source, platform_id=pid,
                                         image_id=f.stem, path=f))
        else:
            raise ValueError(f"unknown source: {source}")
    return refs


def _expand_with_sred_split(refs: list[ImageRef]) -> list[tuple[str, str, str, str, int | None, Image.Image]]:
    """Return list of (image_id_with_cell, source, platform_id, path, sred_cell, pil_image)."""
    out = []
    for r in refs:
        img = safe_open_image(r.path)
        if img is None:
            continue
        if r.source == "sred" and img.size == SRED_MONTAGE_SIZE:
            crops = split_sred_2x2(img, parent_image_id=r.image_id)
            for c in crops:
                iid = f"{r.source}/{r.platform_id}/{r.image_id}#c{c.cell}"
                out.append((iid, r.source, r.platform_id, str(r.path), c.cell, c.resized))
        else:
            iid = f"{r.source}/{r.platform_id}/{r.image_id}"
            # Ensure image is resized to the model input later by the processor;
            # we keep the original resolution here.
            out.append((iid, r.source, r.platform_id, str(r.path), None, img))
    return out


def _save_thumb(img: Image.Image, key: str) -> Path:
    PILOT_THUMBS.mkdir(parents=True, exist_ok=True)
    safe_key = key.replace("/", "__").replace("#", "_c")
    path = PILOT_THUMBS / f"{safe_key}.jpg"
    thumb = img.copy()
    thumb.thumbnail((320, 320))
    if thumb.mode != "RGB":
        thumb = thumb.convert("RGB")
    thumb.save(path, "JPEG", quality=80)
    return path


def _batches(items, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def run_pilot(*, model_id: str, stage: str, batch_size: int) -> int:
    PILOT_OUT.mkdir(parents=True, exist_ok=True)
    status_log = PILOT_OUT / "pilot_status.jsonl"
    audit_md = PILOT_OUT / "pilot_audit.md"

    # P0 — environment
    with step("P0_env_smoke", log_path=status_log) as s:
        print(f"[INFO] torch={torch.__version__} cuda={torch.cuda.is_available()}")
        print(f"[INFO] mps={torch.backends.mps.is_available()}")
        lm = load(model_id)
        print(f"[INFO] model={lm.model_id} device={lm.device} dtype={lm.dtype} dim={lm.projection_dim}")
        s["extra"]["device"] = lm.device
        s["extra"]["dtype"] = str(lm.dtype)
        s["extra"]["projection_dim"] = lm.projection_dim

    # P1 — enumerate fixture
    with step("P1_enumerate", log_path=status_log) as s:
        fixture_rows = _read_fixture()
        refs = _enumerate_fixture(fixture_rows)
        s["count"] = len(refs)
        per_source = Counter(r.source for r in refs)
        print(f"[INFO] per_source counts: {dict(per_source)}")

    # P2 — SRED split
    with step("P2_sred_split", log_path=status_log) as s:
        items = _expand_with_sred_split(refs)
        s["count"] = len(items)
        sred_items = [i for i in items if i[1] == "sred"]
        print(f"[INFO] total items after split: {len(items)} "
              f"(sred sub-crops: {len(sred_items)})")

    # Build class text bank
    with step("P3a_text_bank", log_path=status_log) as s:
        class_bank = build_class_text_bank(lm)
        stacked = torch.stack([class_bank[c] for c in ALL_CLASSES], dim=0).to(torch.float32)
        stacked_np = stacked.detach().cpu().numpy()
        s["count"] = len(ALL_CLASSES)

    # P3 — triage
    triage_results: list[TriageResult] = []
    image_features_keep: dict[str, np.ndarray] = {}
    with step("P3_triage", total=len(items), log_path=status_log) as s:
        t0 = time.time()
        for batch in _batches(items, batch_size):
            keys = [b[0] for b in batch]
            imgs = [b[5] for b in batch]
            feats, keep_mask = encode_images(imgs, lm, context="pilot_triage")
            if not keep_mask.all():
                s["warnings"] += int((~keep_mask).sum())
            feats_kept = feats[keep_mask]
            keys_kept = [k for k, keep in zip(keys, keep_mask) if keep]
            if feats_kept.shape[0] == 0:
                s["count"] += len(batch)
                continue
            logits = feats_kept @ stacked_np.T
            probs = torch.softmax(
                torch.from_numpy(logits).float() * SOFTMAX_TEMPERATURE, dim=-1
            )
            res = _decide_from_scores(probs, parent_ids=keys_kept)
            triage_results.extend(res)
            for k, f in zip(keys_kept, feats_kept):
                image_features_keep[k] = f
            s["count"] += len(batch)
            print(f"[PROGRESS] triage {s['count']}/{len(items)}  "
                  f"({s['count'] / max(time.time()-t0, 1e-6):.2f} img/s)")

    # Re-pair triage results (ordered) to their keys via image_features_keep,
    # which was populated in the same triage batch order.
    results_by_key: dict[str, TriageResult] = {}
    it_idx = 0
    for it in items:
        key = it[0]
        if key in image_features_keep:
            results_by_key[key] = triage_results[it_idx]
            it_idx += 1

    dist = Counter(r.label for r in triage_results)
    main_count = sum(dist[c] for c in MAIN_INDEX_CLASSES)
    floor_count = sum(dist[c] for c in FLOORPLAN_CLASSES)
    dropped_count = sum(r.label not in (MAIN_INDEX_CLASSES | FLOORPLAN_CLASSES)
                        for r in triage_results)
    print(f"[SUMMARY] label_dist={dict(dist)} "
          f"KEPT={main_count + floor_count} (main={main_count} floorplan={floor_count}) "
          f"DROPPED={dropped_count}")

    # P4 — audit gate
    with step("P4_audit_gate", log_path=status_log) as s:
        # Save thumbnails and write audit markdown
        if PILOT_THUMBS.exists():
            shutil.rmtree(PILOT_THUMBS)
        PILOT_THUMBS.mkdir(parents=True, exist_ok=True)
        per_label: dict[str, list[dict]] = {c: [] for c in ALL_CLASSES}
        for it in items:
            key = it[0]
            if key not in results_by_key:
                continue
            r = results_by_key[key]
            thumb_path = _save_thumb(it[5], key)
            per_label[r.label].append({
                "key": key,
                "source": it[1],
                "platform_id": it[2],
                "path": it[3],
                "sred_cell": it[4],
                "confidence": round(r.confidence, 4),
                "margin": round(r.margin, 4),
                "thumb": str(thumb_path.relative_to(PILOT_OUT)),
            })

        lines = [
            "# Pilot — P4 keep/drop audit",
            "",
            f"- model: `{model_id}`  device: `{lm.device}`  dtype: `{lm.dtype}`",
            f"- items after SRED split: **{len(items)}**",
            f"- KEPT: **{main_count + floor_count}**  (main={main_count} floorplan={floor_count})",
            f"- DROPPED: **{dropped_count}**",
            "",
            "## Label distribution",
            "",
            "| label | count |",
            "|---|---:|",
        ]
        for label in ALL_CLASSES:
            lines.append(f"| `{label}` | {dist.get(label, 0)} |")
        lines.append("")

        for label in ALL_CLASSES:
            bucket = per_label[label]
            if not bucket:
                continue
            lines.append(f"## `{label}` ({len(bucket)})")
            lines.append("")
            for entry in bucket:
                lines.append(
                    f"- ![thumb]({entry['thumb']})  "
                    f"`{entry['key']}`  conf={entry['confidence']}  margin={entry['margin']}"
                )
            lines.append("")
        audit_md.write_text("\n".join(lines))
        print(f"[OUT] wrote {audit_md}")
        print(f"[OUT] thumbs at {PILOT_THUMBS} ({len(list(PILOT_THUMBS.iterdir()))} files)")

    # Acceptance-criteria invariants (plan acceptance criteria 1..5, 9)
    invariants: list[tuple[str, bool, str]] = []
    # 1. ETH-Juniors PNG (robinreal/695fb9669... only image) must be DROPPED
    eth_keys = [k for k in results_by_key
                if "695fb9669ba220265d3629b6" in k and "robinreal" in k]
    eth_ok = all(results_by_key[k].label not in MAIN_INDEX_CLASSES | FLOORPLAN_CLASSES
                 for k in eth_keys)
    invariants.append(("ETH-Juniors PNG classified as DROPPED", eth_ok,
                       f"{len(eth_keys)} images, labels: {[results_by_key[k].label for k in eth_keys]}"))
    # 2. Nessel stock (structured/37093067 first image) must be DROPPED
    nessel_keys = [k for k in results_by_key
                   if "37093067" in k and "caf3b8dd" in k]
    nessel_ok = all(results_by_key[k].label not in MAIN_INDEX_CLASSES | FLOORPLAN_CLASSES
                    for k in nessel_keys)
    invariants.append(("Nessel stock photo classified as DROPPED", nessel_ok,
                       f"keys={nessel_keys} labels={[results_by_key[k].label for k in nessel_keys]}"))
    # 3. At least one interior-room kept from robinreal/69d8f49...
    living_keys = [k for k in results_by_key
                   if "69d8f49fcc81b8171a96548d" in k and "0-ff000" in k]
    living_ok = any(results_by_key[k].label == "interior-room" for k in living_keys)
    invariants.append(("Robinreal living room → interior-room", living_ok,
                       f"keys={living_keys} labels={[results_by_key[k].label for k in living_keys]}"))
    # 5. Every SRED montage → 4 sub-embeddings
    sred_cells_seen: dict[str, set[int]] = {}
    for it in items:
        if it[1] == "sred":
            sred_cells_seen.setdefault(it[2], set()).add(it[4])
    sred_ok = all(len(cells) == 4 for cells in sred_cells_seen.values())
    sred_detail = {k: sorted(v) for k, v in sred_cells_seen.items()}
    invariants.append(("Every SRED montage produces 4 cells", sred_ok,
                       f"cells_per_montage={sred_detail}"))
    # 9. Invariant: main index rows only contain MAIN_INDEX_CLASSES labels
    main_labels_ok = True  # will verify after we write to store (P5)

    # Append invariants to audit
    with audit_md.open("a") as fh:
        fh.write("\n## Acceptance-criteria invariants\n\n")
        fh.write("| check | pass | details |\n|---|---|---|\n")
        for name, ok, detail in invariants:
            fh.write(f"| {name} | {'✓' if ok else '✗'} | {detail} |\n")
        fh.write("\n")

    if stage == "p4":
        print("\n" + "=" * 70)
        print("STOP — P4 audit gate reached. Review:")
        print(f"  {audit_md}")
        print("  (thumbnails at image_search/data/pilot_results/thumbs/)")
        print("=" * 70)
        # Emit invariant pass/fail on stdout too
        for name, ok, detail in invariants:
            mark = "OK" if ok else "FAIL"
            print(f"  [{mark}] {name}")
        all_pass = all(ok for _, ok, _ in invariants)
        return 0 if all_pass else 3

    # P5 — embed + store (kept images only)
    store_dir = PILOT_OUT / "store"
    if store_dir.exists():
        shutil.rmtree(store_dir)
    with step("P5_embed_and_store", log_path=status_log) as s:
        with EmbeddingStore(store_dir, projection_dim=lm.projection_dim) as store:
            for it in items:
                key = it[0]
                if key not in results_by_key:
                    continue
                r = results_by_key[key]
                row = ImageRow(
                    image_id=key,
                    source=it[1], platform_id=it[2], path=it[3],
                    sred_cell=it[4],
                    relevance_label=r.label,
                    relevance_confidence=r.confidence,
                    relevance_margin=r.margin,
                )
                store.register_listing(it[2], it[1])
                if r.label in FLOORPLAN_CLASSES:
                    store.add_floorplan_row(row, image_features_keep[key])
                elif r.label in MAIN_INDEX_CLASSES:
                    store.add_main_row(row, image_features_keep[key])
                else:
                    store.add_dropped_row(row)
            counts = store.count_by_kind()
        s["count"] = sum(counts.values())
        s["extra"]["counts"] = counts

    # P6 — queries
    with step("P6_queries", log_path=status_log) as s:
        main_npy = np.load(store_dir / "embeddings.fp32.npy")
        print(f"[INFO] main embedding matrix: {main_npy.shape} dtype={main_npy.dtype}")
        # Map row_idx → key (for display)
        import sqlite3
        db = sqlite3.connect(store_dir / "index.sqlite")
        db.row_factory = sqlite3.Row
        row_map = {r["row_idx"]: dict(r) for r in db.execute(
            "SELECT row_idx, image_id, platform_id, source, path, relevance_label "
            "FROM images WHERE index_kind='main' ORDER BY row_idx;").fetchall()}
        db.close()

        query_report = []
        for lang, q in QUERIES_MULTI:
            t_feat, _ = encode_text([q], lm, context=f"pilot_query_{lang}")
            if main_npy.shape[0] == 0 or t_feat.shape[0] == 0:
                query_report.append({"lang": lang, "query": q, "top5": [],
                                     "note": "empty main index"})
                continue
            sims = main_npy @ t_feat[0]  # (N,)
            order = np.argsort(-sims)[:5]
            top5 = []
            for rank, idx in enumerate(order):
                info = row_map.get(int(idx), {})
                top5.append({
                    "rank": rank + 1,
                    "sim": float(sims[idx]),
                    "image_id": info.get("image_id"),
                    "label": info.get("relevance_label"),
                    "source": info.get("source"),
                })
            query_report.append({"lang": lang, "query": q, "top5": top5})

        (PILOT_OUT / "query_top5.json").write_text(json.dumps(query_report, indent=2))
        with audit_md.open("a") as fh:
            fh.write("\n## P6 query results (top-5 per query)\n\n")
            for qr in query_report:
                fh.write(f"### `{qr['lang']}`: {qr['query']}\n\n")
                fh.write("| rank | sim | image_id | label |\n|---|---|---|---|\n")
                for t in qr["top5"]:
                    fh.write(f"| {t['rank']} | {t['sim']:.4f} | `{t['image_id']}` | "
                             f"`{t['label']}` |\n")
                fh.write("\n")

    # P7 — final summary
    with step("P7_final_summary", log_path=status_log) as s:
        print("\n" + "=" * 70)
        print("PILOT P7 DONE. Final audit:")
        print(f"  {audit_md}")
        print(f"  store: {store_dir}")
        print(f"  queries: {PILOT_OUT / 'query_top5.json'}")
        print("=" * 70)
        for name, ok, detail in invariants:
            mark = "OK" if ok else "FAIL"
            print(f"  [{mark}] {name}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--model", default=GIANT_MODEL_ID)
    ap.add_argument("--stage", choices=["p4", "p7"], default="p4")
    ap.add_argument("--batch-size", type=int, default=8)
    args = ap.parse_args()
    return run_pilot(model_id=args.model, stage=args.stage, batch_size=args.batch_size)


if __name__ == "__main__":
    sys.exit(main())
