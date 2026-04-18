from __future__ import annotations

import os

import pytest


@pytest.fixture(autouse=True)
def _disable_visual_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force visual search off for every test so no test accidentally pulls
    the 3.7 GB SigLIP checkpoint or the 433 MB memmap. Individual tests that
    need the hybrid path opt in explicitly by setting the env var back to
    "1" and monkeypatching the loader / encoder.
    """
    monkeypatch.setenv("LISTINGS_VISUAL_ENABLED", "0")
