"""Canonical-query smoke: real SigLIP + Arctic-Embed + real DB.

Skipped unless the heavy ML dependencies are installed AND the teammate
bundle is on disk. Exercises the full pipeline end-to-end for queries drawn
from the NEXT_STEPS doc. Assertions are intentionally lenient: we check
that each activated signal contributes *something* to the top-10, not that
the ranking is perfect.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest


pytestmark = pytest.mark.smoke

torch = pytest.importorskip("torch")
pytest.importorskip("sentence_transformers")
pytest.importorskip("transformers")

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def live_db() -> Path:
    """Install + migrate into the shared ``data/listings.db`` if missing."""
    db_path = REPO_ROOT / "data" / "listings.db"
    if not db_path.exists():
        from scripts.install_dataset import ensure_installed
        from scripts.migrate_db_to_app_schema import migrate

        ensure_installed(db_path=db_path)
        migrate(db_path)

    # Verify the migration ran — column presence is enough.
    with sqlite3.connect(db_path) as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(listings)")}
    if "city_slug" not in cols:
        from scripts.migrate_db_to_app_schema import migrate as _migrate
        _migrate(db_path)
    return db_path


@pytest.fixture(scope="module")
def app_client(live_db: Path):
    """FastAPI TestClient with real SigLIP + Arctic loaded."""
    import os
    os.environ["LISTINGS_DB_PATH"] = str(live_db)
    os.environ["LISTINGS_SKIP_BUNDLE_INSTALL"] = "0"
    os.environ["LISTINGS_VISUAL_ENABLED"] = "1"
    os.environ["LISTINGS_TEXT_EMBED_ENABLED"] = "1"

    from fastapi.testclient import TestClient
    from app.main import app

    with TestClient(app) as client:
        yield client


def _post(client, query: str, **extra):
    body = {"query": query, "limit": 10, **extra}
    response = client.post("/listings", json=body)
    assert response.status_code == 200, response.text
    return response.json()


def test_canonical_zurich_eth_cheap_quiet(app_client) -> None:
    body = _post(app_client, "günstige 2.5-Zimmer-Wohnung in Zürich, ruhig, nahe ETH")
    listings = body["listings"]
    assert listings
    # At least one soft signal should be reflected in the reasons.
    reasons = " ".join(item["reason"] for item in listings)
    assert "soft preferences" in reasons or "text match" in reasons


def test_canonical_lausanne_epfl_furnished(app_client) -> None:
    body = _post(app_client, "meublé 1.5 pièces à Lausanne près de l'EPFL, quiet")
    listings = body["listings"]
    assert listings
    # At least one should be in Vaud (canton VD) or city Lausanne.
    cities = {(item["listing"].get("city") or "").lower() for item in listings}
    assert any("lausanne" in c for c in cities), cities


def test_canonical_zurich_hb_commute(app_client) -> None:
    body = _post(app_client,
                 "Wohnung in Zürich, max 25 Min zum HB, mit Balkon, modern")
    listings = body["listings"]
    assert listings
    # Expect the LLM to emit commute_target=zurich_hb + near_public_transport
    # which in turn activates the corresponding soft-signal rankings.
    scores = [item["score"] for item in listings]
    assert any(s > 0 for s in scores), scores
