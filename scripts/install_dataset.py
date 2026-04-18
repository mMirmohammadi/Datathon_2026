"""Install the teammate-built dataset bundle into the repo's ``data/`` tree.

Idempotent:
- Decompresses ``datathon2026_dataset/listings.db.gz`` to ``data/listings.db``
  if the destination is absent.
- Copies ``embeddings.fp16.npy`` / ``embeddings_ids.json`` / ``landmarks.json``
  from the bundle to ``data/ranking/`` when missing.
- Returns a dict describing what it did so callers can emit structured logs.

Run directly (``python -m scripts.install_dataset``) to install on demand, or
import ``ensure_installed(db_path, ranking_dir)`` from the bootstrap.
"""
from __future__ import annotations

import gzip
import shutil
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
BUNDLE_DIR = REPO_ROOT / "datathon2026_dataset"
DEFAULT_DB_PATH = REPO_ROOT / "data" / "listings.db"
DEFAULT_RANKING_DIR = REPO_ROOT / "data" / "ranking"

_RANKING_FILES = ("embeddings.fp16.npy", "embeddings_ids.json", "landmarks.json")


def _gunzip(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(src, "rb") as fin, dst.open("wb") as fout:
        shutil.copyfileobj(fin, fout)


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
    db_gz = bundle_dir / "listings.db.gz"
    if not db_path.exists():
        if not db_gz.exists():
            raise FileNotFoundError(
                f"install_dataset: neither {db_path} nor {db_gz} exists. "
                "Place the dataset bundle at datathon2026_dataset/ or fall "
                "back to the legacy CSV import."
            )
        _gunzip(db_gz, db_path)
        report["db_installed"] = True
        print(
            f"[INFO] install_dataset: decompressed {db_gz} -> {db_path}",
            flush=True,
        )

    ranking_dir.mkdir(parents=True, exist_ok=True)
    for name in _RANKING_FILES:
        dst = ranking_dir / name
        if dst.exists():
            continue
        src = bundle_dir / name
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
