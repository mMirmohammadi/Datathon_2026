"""Smoke-suite conftest — unwinds the unit-suite's env guards.

Smoke tests want the real teammate bundle + optional ML models to load, so
we clear the ``LISTINGS_SKIP_BUNDLE_INSTALL`` / ``LISTINGS_VISUAL_ENABLED`` /
``LISTINGS_TEXT_EMBED_ENABLED`` overrides set by ``tests/conftest.py``.
"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _allow_real_bundle(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LISTINGS_SKIP_BUNDLE_INSTALL", raising=False)
    # Per-test default is still disabled; individual tests flip them back on
    # when they want to exercise the model paths.
    yield
