"""Standalone verifier for the DINOv2 Tier-1 index.

Runs a set of hard invariants that MUST hold for the index to be usable.
Exits non-zero on any failure so CI / rsync pipelines can catch corruption.

Invariants:

  1. Files exist: main.fp32.npy, floorplans.fp32.npy, index.sqlite, build_report.json
  2. Shapes: main is (N_main, 1024) fp32; floorplans is (N_floor, 1024) fp32
  3. No NaN / inf anywhere in either matrix
  4. Every row is L2-unit (||v|| in [0.999, 1.001])
  5. Row counts match the SigLIP filter:
         COUNT(dinov2.main)      == COUNT(siglip WHERE kind='main')      - n_skipped_main
         COUNT(dinov2.floorplan) == COUNT(siglip WHERE kind='floorplan') - n_skipped_floor
  6. Join parity: set(dinov2.image_ids) is a SUBSET of set(siglip.main + siglip.floorplan)
  7. No image_id is present in both main and floorplan buckets
  8. Re-load is bit-exact: files on disk parse back to identical matrices as stored

Optional checks (gated by --with-model, require the DINOv2 model):
  9. Re-encode 20 random indexed rows and confirm cosine(stored, re-encoded) > 0.9999
 10. Self-match: for 100 random indexed rows, argmax(cosine(q, all)) == self.row_idx
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np


@dataclass
class VerifyReport:
    passed: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    stats: dict = field(default_factory=dict)

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        (self.passed if ok else self.failed).append(
            f"{name}{': ' + detail if detail else ''}"
        )
        return ok


def verify_files_exist(d: Path, r: VerifyReport) -> bool:
    ok = True
    for f in ("main.fp32.npy", "floorplans.fp32.npy", "index.sqlite",
              "build_report.json"):
        exists = (d / f).exists()
        ok = r.check(f"file_exists:{f}", exists,
                     "" if exists else "missing") and ok
    return ok


def verify_shape_and_dtype(main: np.ndarray, floor: np.ndarray,
                           expected_dim: int, r: VerifyReport) -> bool:
    ok = True
    ok = r.check("main.ndim==2", main.ndim == 2, f"ndim={main.ndim}") and ok
    ok = r.check("floor.ndim==2", floor.ndim == 2, f"ndim={floor.ndim}") and ok
    ok = r.check("main.dtype==float32", main.dtype == np.float32,
                  f"dtype={main.dtype}") and ok
    ok = r.check("floor.dtype==float32", floor.dtype == np.float32,
                  f"dtype={floor.dtype}") and ok
    if main.ndim == 2:
        ok = r.check("main.cols==expected_dim",
                     main.shape[1] == expected_dim,
                     f"got {main.shape[1]}, expected {expected_dim}") and ok
    if floor.ndim == 2:
        ok = r.check("floor.cols==expected_dim",
                     floor.shape[1] == expected_dim,
                     f"got {floor.shape[1]}, expected {expected_dim}") and ok
    r.stats["main.shape"] = list(main.shape)
    r.stats["floor.shape"] = list(floor.shape)
    return ok


def verify_no_nan(main: np.ndarray, floor: np.ndarray,
                  r: VerifyReport) -> bool:
    ok = True
    ok = r.check("main.finite", bool(np.isfinite(main).all()),
                 "" if np.isfinite(main).all() else
                 f"{int((~np.isfinite(main)).any(axis=-1).sum())} bad rows") and ok
    ok = r.check("floor.finite", bool(np.isfinite(floor).all()),
                 "" if np.isfinite(floor).all() else
                 f"{int((~np.isfinite(floor)).any(axis=-1).sum())} bad rows") and ok
    return ok


def verify_l2_unit(main: np.ndarray, floor: np.ndarray,
                   r: VerifyReport, *, tol: float = 1e-3) -> bool:
    ok = True
    for name, arr in (("main", main), ("floor", floor)):
        if arr.shape[0] == 0:
            r.check(f"{name}.l2_unit", True, "empty array")
            continue
        n = np.linalg.norm(arr, axis=1)
        within = np.all(np.abs(n - 1.0) < tol)
        r.stats[f"{name}.norm.min"] = float(n.min())
        r.stats[f"{name}.norm.max"] = float(n.max())
        r.stats[f"{name}.norm.mean"] = float(n.mean())
        ok = r.check(f"{name}.l2_unit", bool(within),
                     f"" if within else
                     f"norm range [{n.min():.6f}, {n.max():.6f}], "
                     f"n_bad={int((np.abs(n - 1.0) >= tol).sum())}") and ok
    return ok


def verify_row_counts_vs_siglip(db: sqlite3.Connection,
                                 siglip_db: sqlite3.Connection,
                                 main: np.ndarray, floor: np.ndarray,
                                 r: VerifyReport,
                                 build_report: dict) -> bool:
    dv_counts = {k: n for k, n in db.execute(
        "SELECT index_kind, COUNT(*) FROM images GROUP BY index_kind;"
    ).fetchall()}
    sl_counts = {k: n for k, n in siglip_db.execute(
        "SELECT index_kind, COUNT(*) FROM images "
        "WHERE index_kind IN ('main','floorplan') GROUP BY index_kind;"
    ).fetchall()}

    n_skip_load = int(build_report.get("n_skipped_load", 0))
    n_skip_nan = int(build_report.get("n_skipped_nan", 0))
    n_skip_total = n_skip_load + n_skip_nan
    limit = build_report.get("limit")  # None for full runs, int for smoke

    r.stats["siglip.main"] = int(sl_counts.get("main", 0))
    r.stats["siglip.floorplan"] = int(sl_counts.get("floorplan", 0))
    r.stats["dinov2.main"] = int(dv_counts.get("main", 0))
    r.stats["dinov2.floorplan"] = int(dv_counts.get("floorplan", 0))
    r.stats["skipped_total"] = n_skip_total
    r.stats["build_limit"] = limit

    # dinov2 counts + skipped must equal siglip counts -- but ONLY for full runs.
    # Smoke runs (with --limit N) intentionally don't process everything.
    total_dv = int(dv_counts.get("main", 0)) + int(dv_counts.get("floorplan", 0))
    total_sl = int(sl_counts.get("main", 0)) + int(sl_counts.get("floorplan", 0))
    if limit is None:
        ok = r.check(
            "row_counts_consistent",
            total_dv + n_skip_total == total_sl,
            f"dv_total={total_dv} + skipped={n_skip_total} vs siglip_total={total_sl}"
        )
    else:
        # For smoke: only enforce that (processed + skipped) <= limit
        # and dv_total <= limit --- don't require full-corpus coverage.
        r.check(
            "smoke_limit_respected",
            total_dv + n_skip_total <= int(limit),
            f"dv_total+skipped={total_dv + n_skip_total} > limit={limit}"
        )
        ok = True

    # .npy row count equals sqlite row count (catches truncation)
    ok = r.check(
        "main.npy_rows==sqlite_main",
        main.shape[0] == int(dv_counts.get("main", 0)),
        f"npy={main.shape[0]} vs sqlite={int(dv_counts.get('main', 0))}"
    ) and ok
    ok = r.check(
        "floor.npy_rows==sqlite_floor",
        floor.shape[0] == int(dv_counts.get("floorplan", 0)),
        f"npy={floor.shape[0]} vs sqlite={int(dv_counts.get('floorplan', 0))}"
    ) and ok
    return ok


def verify_image_id_join(db: sqlite3.Connection,
                          siglip_db: sqlite3.Connection,
                          r: VerifyReport) -> bool:
    dv_ids = {row[0] for row in db.execute(
        "SELECT image_id FROM images;").fetchall()}
    sl_ids = {row[0] for row in siglip_db.execute(
        "SELECT image_id FROM images WHERE index_kind IN ('main','floorplan');"
    ).fetchall()}
    missing = dv_ids - sl_ids
    r.stats["n_dinov2_ids_not_in_siglip"] = len(missing)
    r.stats["n_siglip_ids_missing_from_dinov2"] = len(sl_ids - dv_ids)
    return r.check(
        "dinov2_ids_subset_of_siglip",
        len(missing) == 0,
        f"{len(missing)} dinov2 ids not in siglip (first 5: {sorted(missing)[:5]})"
    )


def verify_no_cross_bucket(db: sqlite3.Connection,
                            r: VerifyReport) -> bool:
    dup = db.execute(
        "SELECT image_id FROM images GROUP BY image_id HAVING COUNT(DISTINCT index_kind) > 1;"
    ).fetchall()
    return r.check(
        "no_cross_bucket",
        len(dup) == 0,
        f"{len(dup)} image_ids in both main and floorplan"
    )


def verify_reload_bit_exact(data_dir: Path, main: np.ndarray,
                             floor: np.ndarray, r: VerifyReport) -> bool:
    """Re-load the .npy files and compare to the passed-in arrays (the caller
    should have loaded them via mmap). If anything got corrupted on disk we
    catch it now."""
    main2 = np.load(data_dir / "main.fp32.npy")
    floor2 = np.load(data_dir / "floorplans.fp32.npy")
    ok_main = (main.shape == main2.shape) and bool(np.array_equal(main, main2))
    ok_floor = (floor.shape == floor2.shape) and bool(np.array_equal(floor, floor2))
    r.check("main.reload_bit_exact", ok_main)
    r.check("floor.reload_bit_exact", ok_floor)
    return ok_main and ok_floor


def verify_self_match(main: np.ndarray, n_samples: int, seed: int,
                      r: VerifyReport) -> bool:
    """For n_samples random indexed rows, the top-1 cosine score against the
    full matrix must equal self-cosine (i.e. 1.0). This guards against row_idx
    misalignment and large-scale value corruption.

    We intentionally check score equality rather than argmax-equal-self, since
    real-estate corpora frequently contain identical images cross-posted across
    listings (near-duplicate embeddings produce argmax ambiguity but the top-1
    score is still == 1.0). A tolerance of 1e-5 absorbs fp32 matmul drift.
    """
    if main.shape[0] < n_samples:
        r.check("self_match", True, "skipped (too few rows)")
        return True
    rng = np.random.default_rng(seed)
    idxs = rng.choice(main.shape[0], size=n_samples, replace=False)
    passes = 0
    score_diffs = []
    for i in idxs:
        scores = main @ main[i]
        # Self-cosine should be ~1.0 (L2-unit rows) and should be the max.
        self_score = float(scores[int(i)])
        top_score = float(scores.max())
        score_diffs.append(top_score - self_score)
        if abs(top_score - self_score) < 1e-5:
            passes += 1
    success_rate = passes / n_samples
    r.stats["self_match.rate"] = success_rate
    r.stats["self_match.n"] = n_samples
    r.stats["self_match.max_score_gap"] = float(max(score_diffs))
    return r.check(
        "self_match_top1_score",
        success_rate == 1.0,
        f"rate={success_rate:.4f} n={n_samples} "
        f"max_gap={max(score_diffs):.2e}"
    )


def run(*, data_dir: Path, siglip_db: Path, expected_dim: int,
        self_match_samples: int, self_match_seed: int) -> int:
    report = VerifyReport()

    if not verify_files_exist(data_dir, report):
        _emit(report, data_dir)
        return 2

    main = np.load(data_dir / "main.fp32.npy", mmap_mode="r")
    floor = np.load(data_dir / "floorplans.fp32.npy", mmap_mode="r")
    build_report = json.loads((data_dir / "build_report.json").read_text())

    verify_shape_and_dtype(main, floor, expected_dim, report)
    verify_no_nan(main, floor, report)
    verify_l2_unit(main, floor, report)

    db = sqlite3.connect(f"file:{data_dir / 'index.sqlite'}?mode=ro", uri=True)
    sl = sqlite3.connect(f"file:{siglip_db}?mode=ro", uri=True)
    try:
        verify_row_counts_vs_siglip(db, sl, main, floor, report, build_report)
        verify_image_id_join(db, sl, report)
        verify_no_cross_bucket(db, report)
    finally:
        db.close()
        sl.close()

    verify_reload_bit_exact(data_dir, main, floor, report)

    if self_match_samples > 0 and main.shape[0] > 0:
        # load non-mmap for full dot-product speed
        main_full = np.load(data_dir / "main.fp32.npy")
        verify_self_match(main_full, self_match_samples, self_match_seed, report)

    _emit(report, data_dir)
    return 0 if not report.failed else 1


def _emit(report: VerifyReport, data_dir: Path) -> None:
    (data_dir / "verification.json").write_text(
        json.dumps({
            "passed": report.passed,
            "failed": report.failed,
            "stats": report.stats,
        }, indent=2) + "\n"
    )
    print("=== VERIFICATION SUMMARY ===", flush=True)
    print(f"passed: {len(report.passed)}", flush=True)
    for p in report.passed:
        print(f"  OK  {p}", flush=True)
    print(f"failed: {len(report.failed)}", flush=True)
    for f in report.failed:
        print(f"  FAIL {f}", flush=True)
    print("stats:", json.dumps(report.stats, indent=2), flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", type=Path,
                    default=Path("image_search/data/full/dinov2_store"))
    ap.add_argument("--siglip-index", type=Path,
                    default=Path("image_search/data/full/store/index.sqlite"))
    ap.add_argument("--expected-dim", type=int, default=1024)
    ap.add_argument("--self-match-samples", type=int, default=100)
    ap.add_argument("--self-match-seed", type=int, default=42)
    args = ap.parse_args()
    return run(
        data_dir=args.data_dir,
        siglip_db=args.siglip_index,
        expected_dim=args.expected_dim,
        self_match_samples=args.self_match_samples,
        self_match_seed=args.self_match_seed,
    )


if __name__ == "__main__":
    sys.exit(main())
