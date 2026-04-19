"""Install the teammate-built dataset bundle into the repo's ``data/`` tree.

Idempotent:
- Copies ``datathon2026_dataset/data/listings.db`` to ``data/listings.db``
  if the destination is absent.
- Copies ``embeddings.fp16.npy`` / ``embeddings_ids.json`` / ``landmarks.json``
  / ``landmarks_mined_candidates.json`` from ``datathon2026_dataset/data/ranking/``
  to ``data/ranking/`` when missing.
- Returns a dict describing what it did so callers can emit structured logs.

Run directly (``python -m scripts.install_dataset``) to install on demand, or
import ``ensure_installed(db_path, ranking_dir)`` from the bootstrap.
"""
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
BUNDLE_DIR = REPO_ROOT / "datathon2026_dataset"
DEFAULT_DB_PATH = REPO_ROOT / "data" / "listings.db"
DEFAULT_RANKING_DIR = REPO_ROOT / "data" / "ranking"

_RANKING_FILES = (
    "embeddings.fp16.npy",
    "embeddings_ids.json",
    "landmarks.json",
    "landmarks_mined_candidates.json",
)


def ensure_installed(
    db_path: Path = DEFAULT_DB_PATH,
    ranking_dir: Path = DEFAULT_RANKING_DIR,
    bundle_dir: Path = BUNDLE_DIR,
) -> dict[str, Any]:
    """Install the bundle if ``db_path`` is missing. Returns a report dict.

    Raises FileNotFoundError if the bundle itself is absent and the DB is
    also missing, so callers can decide between legacy-CSV fallback and
    failing hard.
    """
    report: dict[str, Any] = {
        "db_installed": False,
        "ranking_files_installed": [],
        "db_path": str(db_path),
    }
    bundled_db = bundle_dir / "data" / "listings.db"
    if not db_path.exists():
        if not bundled_db.exists():
            raise FileNotFoundError(
                f"install_dataset: neither {db_path} nor {bundled_db} exists. "
                "Place the dataset bundle at datathon2026_dataset/ (repo-relative "
                "layout: data/listings.db + data/ranking/*) or fall back to the "
                "legacy CSV import."
            )
        db_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(bundled_db, db_path)
        report["db_installed"] = True
        print(
            f"[INFO] install_dataset: copied {bundled_db} -> {db_path}",
            flush=True,
        )

    ranking_dir.mkdir(parents=True, exist_ok=True)
    bundled_ranking = bundle_dir / "data" / "ranking"
    for name in _RANKING_FILES:
        dst = ranking_dir / name
        if dst.exists():
            continue
        src = bundled_ranking / name
        if not src.exists():
            print(
                f"[WARN] install_dataset: expected={src}, got=missing, "
                f"fallback=skip (downstream ranking features may be disabled)",
                flush=True,
            )
            continue
        shutil.copy2(src, dst)
        report["ranking_files_installed"].append(name)
        print(f"[INFO] install_dataset: copied {src.name} -> {dst}", flush=True)
    return report


def _main() -> int:
    report = ensure_installed()
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
