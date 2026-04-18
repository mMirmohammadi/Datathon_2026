"""Cross-check the full-run store on disk.

Runs nine invariant checks that would fail loud rather than silently ship
bad data. Exit 0 if all pass, non-zero if any fail.

Checks:
 1. SQLite `images` table row count == enumerated images on disk (after SRED expansion).
 2. Main memmap shape[0] == COUNT(index_kind='main').
 3. Floorplan memmap shape[0] == COUNT(index_kind='floorplan').
 4. Every main-index row has `relevance_label IN MAIN_INDEX_CLASSES`.
 5. Every floorplan-index row has `relevance_label == 'floorplan'`.
 6. Every dropped row has `relevance_label IN DROPPED_CLASSES`.
 7. No NaN/inf in either memmap.
 8. All vectors have L2 norm in [0.99, 1.01].
 9. No duplicate image_ids in images table.

Plus a label-distribution sanity report.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

import numpy as np

from image_search.common import paths
from image_search.common.prompts import (
    DROPPED_CLASSES,
    FLOORPLAN_CLASSES,
    MAIN_INDEX_CLASSES,
)
from image_search.common.sred import SRED_MONTAGE_SIZE


def _count_disk_items() -> int:
    from PIL import Image
    total = 0
    for ref in paths.iter_all():
        if ref.source == "sred":
            # Assume all sred files are 224x224 montages (verified). Each → 4 items.
            total += 4
        else:
            total += 1
    return total


def verify(store_dir: Path, *, check_disk_count: bool) -> int:
    db_path = store_dir / "index.sqlite"
    main_path = store_dir / "embeddings.fp32.npy"
    floor_path = store_dir / "floorplans.fp32.npy"

    for p in (db_path, main_path, floor_path):
        if not p.exists():
            print(f"[FAIL] missing: {p}")
            return 2

    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    main = np.load(main_path)
    floor = np.load(floor_path)

    failures: list[str] = []
    passes: list[str] = []

    # 1. DB row count vs disk
    n_db = db.execute("SELECT COUNT(*) FROM images;").fetchone()[0]
    if check_disk_count:
        n_disk = _count_disk_items()
        if n_db == n_disk:
            passes.append(f"DB row count matches disk ({n_db})")
        else:
            failures.append(f"DB row count {n_db} != disk {n_disk}")
    else:
        passes.append(f"DB row count = {n_db} (disk cross-check skipped)")

    # 2. main memmap shape
    n_main_db = db.execute(
        "SELECT COUNT(*) FROM images WHERE index_kind='main';"
    ).fetchone()[0]
    if main.shape[0] == n_main_db:
        passes.append(f"main memmap rows = {main.shape[0]} = DB count")
    else:
        failures.append(
            f"main memmap rows {main.shape[0]} != DB count {n_main_db}"
        )

    # 3. floorplan memmap shape
    n_floor_db = db.execute(
        "SELECT COUNT(*) FROM images WHERE index_kind='floorplan';"
    ).fetchone()[0]
    if floor.shape[0] == n_floor_db:
        passes.append(f"floorplan memmap rows = {floor.shape[0]} = DB count")
    else:
        failures.append(
            f"floorplan memmap rows {floor.shape[0]} != DB count {n_floor_db}"
        )

    # 4-6. Label set invariants
    main_labels = {r[0] for r in db.execute(
        "SELECT DISTINCT relevance_label FROM images WHERE index_kind='main';"
    )}
    if main_labels.issubset(MAIN_INDEX_CLASSES):
        passes.append(f"main labels ⊆ MAIN_INDEX_CLASSES ({sorted(main_labels)})")
    else:
        failures.append(f"main has forbidden labels: {main_labels - MAIN_INDEX_CLASSES}")

    floor_labels = {r[0] for r in db.execute(
        "SELECT DISTINCT relevance_label FROM images WHERE index_kind='floorplan';"
    )}
    if floor_labels.issubset(FLOORPLAN_CLASSES):
        passes.append(f"floorplan labels ⊆ FLOORPLAN_CLASSES ({sorted(floor_labels)})")
    else:
        failures.append(f"floorplan has forbidden labels: {floor_labels - FLOORPLAN_CLASSES}")

    dropped_labels = {r[0] for r in db.execute(
        "SELECT DISTINCT relevance_label FROM images WHERE index_kind='dropped';"
    )}
    if dropped_labels.issubset(DROPPED_CLASSES):
        passes.append(f"dropped labels ⊆ DROPPED_CLASSES ({sorted(dropped_labels)})")
    else:
        failures.append(f"dropped has forbidden labels: {dropped_labels - DROPPED_CLASSES}")

    # 7. NaN/inf
    if main.size and not np.isfinite(main).all():
        failures.append(f"main memmap has {(~np.isfinite(main)).sum()} non-finite values")
    else:
        passes.append(f"main memmap all finite ({main.size} values)")
    if floor.size and not np.isfinite(floor).all():
        failures.append(f"floorplan memmap has {(~np.isfinite(floor)).sum()} non-finite values")
    else:
        passes.append(f"floorplan memmap all finite ({floor.size} values)")

    # 8. L2 norms
    for name, arr in [("main", main), ("floorplan", floor)]:
        if arr.size == 0:
            passes.append(f"{name} memmap empty (L2 check skipped)")
            continue
        norms = np.linalg.norm(arr, axis=-1)
        bad = (norms < 0.99) | (norms > 1.01)
        if bad.any():
            failures.append(
                f"{name} has {bad.sum()} vectors with L2 norm outside [0.99, 1.01]; "
                f"min={norms.min():.4f} max={norms.max():.4f}"
            )
        else:
            passes.append(
                f"{name} L2 norms ∈ [{norms.min():.4f}, {norms.max():.4f}]"
            )

    # 9. Unique image_ids
    total, unique = db.execute(
        "SELECT COUNT(*), COUNT(DISTINCT image_id) FROM images;"
    ).fetchone()
    if total == unique:
        passes.append(f"all image_ids unique ({unique})")
    else:
        failures.append(f"{total - unique} duplicate image_ids in DB")

    # Bonus — label distribution
    dist = {r[0]: r[1] for r in db.execute(
        "SELECT relevance_label, COUNT(*) FROM images GROUP BY relevance_label ORDER BY 2 DESC;"
    )}
    per_source = {r[0]: r[1] for r in db.execute(
        "SELECT source, COUNT(*) FROM images GROUP BY source ORDER BY 2 DESC;"
    )}
    by_kind = {r[0]: r[1] for r in db.execute(
        "SELECT index_kind, COUNT(*) FROM images GROUP BY index_kind;"
    )}
    db.close()

    print("\n=== Verification Report ===")
    print(f"store:        {store_dir}")
    print(f"db rows:      {n_db}")
    print(f"main memmap:  {main.shape} {main.dtype}")
    print(f"floor memmap: {floor.shape} {floor.dtype}")
    print(f"by_kind:      {by_kind}")
    print(f"per_source:   {per_source}")
    print(f"label_dist:   {dist}")

    print("\n=== Checks ===")
    for p in passes:
        print(f"  [OK]   {p}")
    for f in failures:
        print(f"  [FAIL] {f}")

    report = {
        "store_dir": str(store_dir),
        "db_rows": n_db,
        "main_shape": list(main.shape),
        "floor_shape": list(floor.shape),
        "by_kind": by_kind,
        "per_source": per_source,
        "label_dist": dist,
        "passes": passes,
        "failures": failures,
    }
    (store_dir / "verification.json").write_text(json.dumps(report, indent=2) + "\n")

    return 0 if not failures else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--store-dir", type=Path, required=True)
    ap.add_argument("--skip-disk-count", action="store_true",
                    help="Skip the expensive disk enumeration count check")
    args = ap.parse_args()
    return verify(args.store_dir, check_disk_count=not args.skip_disk_count)


if __name__ == "__main__":
    sys.exit(main())
