from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.staticfiles import StaticFiles

from app.api.routes.listings import router as listings_router
from app.config import get_settings
from app.core.visual_search import load_visual_index, visual_enabled
from app.harness.bootstrap import bootstrap_database


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    bootstrap_database(db_path=settings.db_path, raw_data_dir=settings.raw_data_dir)
    if visual_enabled():
        load_visual_index()
    else:
        print(
            "[WARN] visual_disabled_by_env: LISTINGS_VISUAL_ENABLED=0, "
            "skipping SigLIP load, expected=visual re-ranker, "
            "fallback=BM25-only ranking",
            flush=True,
        )
    yield


app = FastAPI(
    title="Datathon 2026 Listings Harness",
    lifespan=lifespan,
)
app.include_router(listings_router)

_sred_images_dir = get_settings().raw_data_dir / "sred_images"
if _sred_images_dir.exists():
    app.mount(
        "/raw-data-images",
        StaticFiles(directory=str(_sred_images_dir)),
        name="raw-data-images",
    )
