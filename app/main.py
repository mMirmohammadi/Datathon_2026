from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from starlette.staticfiles import StaticFiles

from app.api.routes.listings import router as listings_router
from app.config import get_settings
from app.core.text_embed_search import load_text_embed_index, text_embed_enabled
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
            "fallback=BM25+text-only ranking",
            flush=True,
        )
    if text_embed_enabled():
        load_text_embed_index()
    else:
        print(
            "[WARN] text_embed_disabled_by_env: LISTINGS_TEXT_EMBED_ENABLED=0, "
            "skipping Arctic-Embed load, expected=text re-ranker, "
            "fallback=BM25+visual-only ranking",
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


# Demo frontend: single HTML page at /demo backed by assets served from
# /demo-assets. Uses the same-origin /listings API; no CORS needed.
_DEMO_DIR = Path(__file__).resolve().parent / "static"
if _DEMO_DIR.exists() and (_DEMO_DIR / "demo.html").exists():
    app.mount(
        "/demo-assets",
        StaticFiles(directory=str(_DEMO_DIR)),
        name="demo-assets",
    )

    @app.get("/demo", include_in_schema=False)
    def demo_page() -> FileResponse:
        return FileResponse(str(_DEMO_DIR / "demo.html"))
else:
    print(
        f"[WARN] demo_page_missing: expected=demo.html in {_DEMO_DIR}, "
        f"got=not found, fallback=/demo route disabled",
        flush=True,
    )
