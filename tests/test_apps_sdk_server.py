from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from apps_sdk.server.main import (
    PublicWidgetStaticFiles,
    SEARCH_TOOL_NAME,
    WIDGET_TEMPLATE_URI,
    build_resource_contents_meta,
    build_search_tool_result,
    build_tool_descriptor,
    load_widget_html,
)
from apps_sdk.server.widget import WIDGET_MIME_TYPE


def test_load_widget_html_uses_manifest_assets(tmp_path: Path) -> None:
    dist_dir = tmp_path / "dist"
    assets_dir = dist_dir / "assets"
    assets_dir.mkdir(parents=True)
    (dist_dir / ".vite").mkdir()
    (dist_dir / ".vite" / "manifest.json").write_text(
        '{"src/main.tsx":{"file":"assets/main-abc.js","css":["assets/main-abc.css"]}}',
        encoding="utf-8",
    )

    html = load_widget_html(
        dist_dir=dist_dir,
        public_base_url="https://example.com",
    )

    assert "https://example.com/widget-assets/assets/main-abc.js" in html
    assert "https://example.com/widget-assets/assets/main-abc.css" in html
    assert 'id="root"' in html


def test_load_widget_html_accepts_vite_index_manifest_entry(tmp_path: Path) -> None:
    dist_dir = tmp_path / "dist"
    (dist_dir / ".vite").mkdir(parents=True)
    (dist_dir / ".vite" / "manifest.json").write_text(
        '{"index.html":{"file":"assets/index-abc.js","css":["assets/index-abc.css"]}}',
        encoding="utf-8",
    )

    html = load_widget_html(
        dist_dir=dist_dir,
        public_base_url="https://example.com",
    )

    assert "https://example.com/widget-assets/assets/index-abc.js" in html
    assert "https://example.com/widget-assets/assets/index-abc.css" in html


def test_build_tool_descriptor_points_to_widget() -> None:
    descriptor = build_tool_descriptor()

    assert descriptor.name == SEARCH_TOOL_NAME
    assert "query" in descriptor.inputSchema["properties"]
    assert "limit" in descriptor.inputSchema["properties"]
    assert descriptor.meta["ui"]["resourceUri"] == WIDGET_TEMPLATE_URI
    assert descriptor.meta["openai/outputTemplate"] == WIDGET_TEMPLATE_URI


def test_build_search_tool_result_wraps_results() -> None:
    result = build_search_tool_result(
        query="3 room apartment",
        payload={
            "listings": [
                {
                    "listing_id": "1",
                    "score": 1.0,
                    "reason": "stub",
                    "listing": {
                        "id": "1",
                        "title": "Example",
                        "city": "Zurich",
                        "latitude": 47.37,
                        "longitude": 8.54,
                        "price_chf": 2500,
                        "rooms": 3.0,
                    },
                }
            ],
            "meta": {},
        },
    )

    assert result.structuredContent["listings"][0]["listing_id"] == "1"
    assert result.structuredContent["meta"] == {}
    assert result.meta["openai/outputTemplate"] == WIDGET_TEMPLATE_URI
    assert "3 room apartment" in result.content[0].text


def test_resource_contents_meta_uses_mcp_apps_csp_domains() -> None:
    meta = build_resource_contents_meta(public_base_url="https://example.com")

    assert "https://example.com" in meta["ui"]["csp"]["connectDomains"]
    assert "https://example.com" in meta["ui"]["csp"]["resourceDomains"]
    assert "connectSrc" not in meta["ui"]["csp"]
    assert "resourceSrc" not in meta["ui"]["csp"]


def test_widget_uses_mcp_apps_mime_type() -> None:
    assert WIDGET_MIME_TYPE == "text/html;profile=mcp-app"


def test_widget_assets_include_cors_headers(tmp_path: Path) -> None:
    dist_dir = tmp_path / "dist"
    assets_dir = dist_dir / "assets"
    assets_dir.mkdir(parents=True)
    asset_path = assets_dir / "index-test.js"
    asset_path.write_text("console.log('ok');", encoding="utf-8")

    app = FastAPI()
    app.mount("/widget-assets", PublicWidgetStaticFiles(directory=str(dist_dir)), name="widget-assets")

    with TestClient(app) as client:
        response = client.get("/widget-assets/assets/index-test.js")

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "*"
