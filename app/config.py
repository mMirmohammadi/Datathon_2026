from __future__ import annotations

import os
import secrets
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")


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


def _default_users_db_path() -> Path:
    configured = os.getenv("LISTINGS_USERS_DB_PATH")
    if configured:
        return Path(configured)
    return _project_root() / "data" / "users.db"


_SESSION_SECRET_FILE = _project_root() / "data" / ".session_secret"


@lru_cache(maxsize=1)
def _resolve_session_secret() -> str:
    """Read ``LISTINGS_SESSION_SECRET`` from env, else persist a generated one.

    Production should set the env var explicitly. When unset we auto-generate a
    256-bit secret and persist it to ``data/.session_secret`` (chmod 600) so
    sessions survive restarts. A loud [WARN] goes to stderr so operators can
    see this is the fallback path, per CLAUDE.md §5 (no silent fallbacks).
    """
    configured = os.getenv("LISTINGS_SESSION_SECRET")
    if configured:
        return configured
    path = _SESSION_SECRET_FILE
    if path.exists():
        secret = path.read_text(encoding="utf-8").strip()
        if secret:
            print(
                "[WARN] session_secret: expected=LISTINGS_SESSION_SECRET env var, "
                f"got=unset, fallback=loaded persisted secret from {path}. "
                "Set LISTINGS_SESSION_SECRET in production.",
                flush=True,
            )
            return secret
    path.parent.mkdir(parents=True, exist_ok=True)
    secret = secrets.token_urlsafe(32)
    path.write_text(secret + "\n", encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        pass
    print(
        "[WARN] session_secret: expected=LISTINGS_SESSION_SECRET env var, "
        f"got=unset, fallback=generated fresh secret and persisted to {path}. "
        "Set LISTINGS_SESSION_SECRET in production.",
        flush=True,
    )
    return secret


def _cookie_secure() -> bool:
    raw = os.getenv("LISTINGS_COOKIE_SECURE", "0").strip().lower()
    return raw in ("1", "true", "yes", "on")


@dataclass(slots=True)
class Settings:
    raw_data_dir: Path
    db_path: Path
    users_db_path: Path
    session_secret: str
    cookie_secure: bool
    s3_bucket: str
    s3_region: str
    s3_prefix: str


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        raw_data_dir=_find_default_raw_data_dir(),
        db_path=_default_db_path(),
        users_db_path=_default_users_db_path(),
        session_secret=_resolve_session_secret(),
        cookie_secure=_cookie_secure(),
        s3_bucket=os.getenv(
            "LISTINGS_S3_BUCKET",
            "crawl-data-951752554117-eu-central-2-an",
        ),
        s3_region=os.getenv("LISTINGS_S3_REGION", "eu-central-2"),
        s3_prefix=os.getenv("LISTINGS_S3_PREFIX", "prod"),
    )
