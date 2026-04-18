from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _find_default_raw_data_dir() -> Path:
    root = _project_root()
    configured = os.getenv("LISTINGS_RAW_DATA_DIR")
    if configured:
        return Path(configured)
    return root / "raw_data"


def _default_db_path() -> Path:
    configured = os.getenv("LISTINGS_DB_PATH")
    if configured:
        return Path(configured)
    return _project_root() / "data" / "listings.db"


@dataclass(slots=True)
class Settings:
    raw_data_dir: Path
    db_path: Path
    s3_bucket: str
    s3_region: str
    s3_prefix: str


def get_settings() -> Settings:
    return Settings(
        raw_data_dir=_find_default_raw_data_dir(),
        db_path=_default_db_path(),
        s3_bucket=os.getenv(
            "LISTINGS_S3_BUCKET",
            "crawl-data-951752554117-eu-central-2-an",
        ),
        s3_region=os.getenv("LISTINGS_S3_REGION", "eu-central-2"),
        s3_prefix=os.getenv("LISTINGS_S3_PREFIX", "prod"),
    )
