from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _disable_visual_by_default(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Force visual search off for every test so no test accidentally pulls
    the 3.7 GB SigLIP checkpoint or the 433 MB memmap. Individual tests that
    need the hybrid path opt in explicitly by setting the env var back to
    "1" and monkeypatching the loader / encoder.

    Also isolates each test's users.db and session secret so auth-aware tests
    don't collide. Settings cache is cleared so the next ``get_settings()``
    call picks up the freshly-overridden env.
    """
    monkeypatch.setenv("LISTINGS_VISUAL_ENABLED", "0")
    # Keep the unit suite fast: per-test tmp DBs should fall through to the
    # legacy 500-row CSV importer instead of decompressing the 417 MB bundle.
    monkeypatch.setenv("LISTINGS_SKIP_BUNDLE_INSTALL", "1")
    monkeypatch.setenv("LISTINGS_TEXT_EMBED_ENABLED", "0")
    # Per-test scratch users DB so auth state never leaks between tests.
    users_dir = tmp_path_factory.mktemp("users_db")
    monkeypatch.setenv("LISTINGS_USERS_DB_PATH", str(users_dir / "users.db"))
    monkeypatch.setenv(
        "LISTINGS_SESSION_SECRET",
        "test-secret-not-for-production-use-0123456789",
    )
    from app.config import get_settings, _resolve_session_secret
    get_settings.cache_clear()
    _resolve_session_secret.cache_clear()
    # Drop any in-memory login-attempt counters from previous tests.
    from app.auth import ratelimit
    ratelimit.reset_for_tests()
