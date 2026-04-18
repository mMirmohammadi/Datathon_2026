"""Latency smoke: p50 < 500 ms after warmup on MPS, p95 < 1500 ms.

Skipped on CPU-only hosts (model load alone is tens of seconds).
"""
from __future__ import annotations

import os
import time
from pathlib import Path

import pytest


pytestmark = pytest.mark.smoke

torch = pytest.importorskip("torch")
pytest.importorskip("sentence_transformers")

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_p50_and_p95_latency_after_warmup() -> None:
    if not torch.backends.mps.is_available() and not torch.cuda.is_available():
        pytest.skip("CPU-only host; latency smoke only meaningful on MPS/CUDA")

    db_path = REPO_ROOT / "data" / "listings.db"
    if not db_path.exists():
        pytest.skip(f"DB not installed at {db_path}")

    os.environ["LISTINGS_DB_PATH"] = str(db_path)
    os.environ["LISTINGS_SKIP_BUNDLE_INSTALL"] = "0"
    os.environ["LISTINGS_VISUAL_ENABLED"] = "1"
    os.environ["LISTINGS_TEXT_EMBED_ENABLED"] = "1"

    from fastapi.testclient import TestClient
    from app.main import app

    queries = [
        "bright 3-room apartment in Zurich",
        "ruhige Wohnung nahe EPFL",
        "family-friendly flat in Bern with garden",
        "cheap studio Winterthur",
        "modern Dachwohnung Basel am Rhein",
    ]

    with TestClient(app) as client:
        # Warm up the encoders.
        client.post("/listings", json={"query": queries[0], "limit": 3})

        timings: list[float] = []
        for _ in range(2):
            for q in queries:
                t0 = time.perf_counter()
                r = client.post("/listings", json={"query": q, "limit": 5})
                assert r.status_code == 200
                timings.append(time.perf_counter() - t0)

    timings.sort()
    p50 = timings[len(timings) // 2]
    p95 = timings[int(len(timings) * 0.95)]
    print(f"[INFO] latency_smoke: p50={p50*1000:.0f}ms p95={p95*1000:.0f}ms "
          f"n={len(timings)}")
    assert p50 < 0.5, f"p50 {p50:.2f}s > 500 ms target"
    assert p95 < 1.5, f"p95 {p95:.2f}s > 1500 ms target"
