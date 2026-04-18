from __future__ import annotations

import os
from typing import Any

import mcp.types as types
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from starlette.responses import Response
from starlette.staticfiles import StaticFiles

from apps_sdk.server.client import get_listings_api_client
from apps_sdk.server.widget import (
    WIDGET_MIME_TYPE,
    WIDGET_TEMPLATE_URI,
    get_public_base_url,
    get_widget_dist_dir,
    load_widget_html,
)

SEARCH_TOOL_NAME = "search_listings"
MAP_RESOURCE_ORIGINS = [
    "https://a.basemaps.cartocdn.com",
    "https://b.basemaps.cartocdn.com",
    "https://c.basemaps.cartocdn.com",
    "https://d.basemaps.cartocdn.com",
    "https://assets.comparis.ch",
    "https://assets-comparis.b-cdn.net",
]


class SearchListingsInput(BaseModel):
    query: str = Field(..., description="Natural-language property search query.")
    limit: int = Field(default=25, ge=1, le=100)
    offset: int = Field(default=0, ge=0)

    model_config = ConfigDict(extra="forbid")


class PublicWidgetStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope: dict[str, Any]) -> Response:
        response = await super().get_response(path, scope)
        if response.status_code < 400:
            response.headers.setdefault("Access-Control-Allow-Origin", "*")
            response.headers.setdefault("Access-Control-Allow-Methods", "GET, OPTIONS")
            response.headers.setdefault("Access-Control-Allow-Headers", "*")
            response.headers.setdefault("Cross-Origin-Resource-Policy", "cross-origin")
        return response


def _split_env_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _transport_security_settings() -> TransportSecuritySettings:
    allowed_hosts = _split_env_list(os.getenv("MCP_ALLOWED_HOSTS"))
    allowed_origins = _split_env_list(os.getenv("MCP_ALLOWED_ORIGINS"))
    if not allowed_hosts and not allowed_origins:
        return TransportSecuritySettings(enable_dns_rebinding_protection=False)
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=allowed_hosts,
        allowed_origins=allowed_origins,
    )


def build_tool_descriptor() -> types.Tool:
    return types.Tool(
        name=SEARCH_TOOL_NAME,
        title="Search listings",
        description="Search Swiss real-estate listings from the harness and render a ranked list with map pins.",
        inputSchema=SearchListingsInput.model_json_schema(),
        annotations=types.ToolAnnotations(
            readOnlyHint=True,
            destructiveHint=False,
            openWorldHint=False,
        ),
        _meta=build_tool_meta(),
    )


def build_search_tool_result(
    *,
    query: str,
    payload: dict[str, Any],
) -> types.CallToolResult:
    count = len(payload.get("listings", []))
    summary = f"Showing {count} listing{'s' if count != 1 else ''} for “{query}”."
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=summary)],
        structuredContent=payload,
        _meta=build_tool_result_meta(),
    )


def build_tool_meta() -> dict[str, Any]:
    return {
        "securitySchemes": [{"type": "noauth"}],
        "ui": {
            "resourceUri": WIDGET_TEMPLATE_URI,
            "visibility": ["model", "app"],
        },
        "openai/outputTemplate": WIDGET_TEMPLATE_URI,
        "openai/toolInvocation/invoking": "Searching listings…",
        "openai/toolInvocation/invoked": "Listings ready",
        "openai/widgetAccessible": True,
    }


def build_tool_result_meta() -> dict[str, Any]:
    return {"openai/outputTemplate": WIDGET_TEMPLATE_URI}


def build_resource_contents_meta(*, public_base_url: str | None = None) -> dict[str, Any]:
    base_url = public_base_url or get_public_base_url()
    return {
        "ui": {
            "prefersBorder": False,
            "csp": {
                "connectDomains": [base_url, *MAP_RESOURCE_ORIGINS],
                "resourceDomains": [base_url, *MAP_RESOURCE_ORIGINS],
            },
        },
        "openai/widgetAccessible": True,
    }


mcp = FastMCP(
    name="datathon2026-listings-app",
    stateless_http=True,
    transport_security=_transport_security_settings(),
)


@mcp._mcp_server.list_tools()
async def _list_tools() -> list[types.Tool]:
    return [build_tool_descriptor()]


@mcp._mcp_server.list_resources()
async def _list_resources() -> list[types.Resource]:
    return [
        types.Resource(
            name="Listings map and ranked list",
            title="Listings map and ranked list",
            uri=WIDGET_TEMPLATE_URI,
            description="Combined ranked list and map widget for listing search results.",
            mimeType=WIDGET_MIME_TYPE,
            _meta=build_resource_contents_meta(),
        )
    ]


async def _handle_read_resource(req: types.ReadResourceRequest) -> types.ServerResult:
    if str(req.params.uri) != WIDGET_TEMPLATE_URI:
        raise ValueError(f"Unknown resource: {req.params.uri}")

    html = load_widget_html(
        dist_dir=get_widget_dist_dir(),
        public_base_url=get_public_base_url(),
    )
    return types.ServerResult(
        types.ReadResourceResult(
            contents=[
                types.TextResourceContents(
                    uri=WIDGET_TEMPLATE_URI,
                    mimeType=WIDGET_MIME_TYPE,
                    text=html,
                    _meta=build_resource_contents_meta(),
                )
            ]
        )
    )


async def _handle_call_tool(req: types.CallToolRequest) -> types.ServerResult:
    if req.params.name != SEARCH_TOOL_NAME:
        return types.ServerResult(
            types.CallToolResult(
                content=[types.TextContent(type="text", text=f"Unknown tool: {req.params.name}")],
                isError=True,
            )
        )

    try:
        search_input = SearchListingsInput.model_validate(req.params.arguments or {})
    except ValidationError as exc:
        return types.ServerResult(
            types.CallToolResult(
                content=[types.TextContent(type="text", text=f"Invalid input: {exc.errors()}")],
                isError=True,
            )
        )

    response_payload = await get_listings_api_client().search_listings(
        query=search_input.query,
        limit=search_input.limit,
        offset=search_input.offset,
    )
    return types.ServerResult(
        build_search_tool_result(query=search_input.query, payload=response_payload)
    )


mcp._mcp_server.request_handlers[types.ReadResourceRequest] = _handle_read_resource
mcp._mcp_server.request_handlers[types.CallToolRequest] = _handle_call_tool

app = mcp.streamable_http_app()
_widget_dist_dir = get_widget_dist_dir()
_widget_dist_dir.mkdir(parents=True, exist_ok=True)
app.mount(
    "/widget-assets",
    PublicWidgetStaticFiles(directory=str(_widget_dist_dir)),
    name="widget-assets",
)


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("APPS_SDK_PORT", "8001"))
    uvicorn.run(app, host="0.0.0.0", port=port)
