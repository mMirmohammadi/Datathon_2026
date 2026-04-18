from __future__ import annotations

import argparse
import asyncio
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.types import Resource, TextResourceContents, Tool

from apps_sdk.server.main import SEARCH_TOOL_NAME, WIDGET_TEMPLATE_URI
from apps_sdk.server.widget import WIDGET_MIME_TYPE


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def validate_tool_descriptor_payload(tool: Tool) -> None:
    meta = tool.meta or {}
    ui_meta = meta.get("ui") or {}

    _require(tool.name == SEARCH_TOOL_NAME, f"Unexpected tool name: {tool.name}")
    _require(ui_meta.get("resourceUri") == WIDGET_TEMPLATE_URI, "Tool ui.resourceUri mismatch.")
    _require(ui_meta.get("visibility") == ["model", "app"], "Tool ui.visibility mismatch.")
    _require(meta.get("openai/outputTemplate") == WIDGET_TEMPLATE_URI, "Missing output template.")


def validate_resource_descriptor(resource: Resource) -> None:
    _require(str(resource.uri) == WIDGET_TEMPLATE_URI, f"Unexpected resource URI: {resource.uri}")
    _require(resource.mimeType == WIDGET_MIME_TYPE, f"Unexpected resource mimeType: {resource.mimeType}")


def validate_resource_contents_payload(content: TextResourceContents) -> None:
    meta = content.meta or {}
    ui_meta = meta.get("ui") or {}
    csp_meta = ui_meta.get("csp") or {}

    _require(str(content.uri) == WIDGET_TEMPLATE_URI, f"Unexpected content URI: {content.uri}")
    _require(content.mimeType == WIDGET_MIME_TYPE, f"Unexpected content mimeType: {content.mimeType}")
    _require("id=\"root\"" in content.text or "id='root'" in content.text, "Widget HTML missing root node.")
    _require(
        isinstance(csp_meta.get("connectDomains"), list) and bool(csp_meta["connectDomains"]),
        "Resource CSP connectDomains missing.",
    )
    _require(
        isinstance(csp_meta.get("resourceDomains"), list) and bool(csp_meta["resourceDomains"]),
        "Resource CSP resourceDomains missing.",
    )


async def run_smoke(*, base_url: str) -> None:
    async with streamable_http_client(base_url) as (read_stream, write_stream, _):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()

            tools_result = await session.list_tools()
            tool = next((tool for tool in tools_result.tools if tool.name == SEARCH_TOOL_NAME), None)
            _require(tool is not None, f"Tool {SEARCH_TOOL_NAME!r} not found.")
            validate_tool_descriptor_payload(tool)

            resources_result = await session.list_resources()
            resource = next(
                (resource for resource in resources_result.resources if str(resource.uri) == WIDGET_TEMPLATE_URI),
                None,
            )
            _require(resource is not None, f"Resource {WIDGET_TEMPLATE_URI!r} not found.")
            validate_resource_descriptor(resource)

            resource_result = await session.read_resource(WIDGET_TEMPLATE_URI)
            _require(bool(resource_result.contents), "Resource returned no contents.")
            first_content = resource_result.contents[0]
            _require(
                isinstance(first_content, TextResourceContents),
                f"Unexpected resource content type: {type(first_content).__name__}",
            )
            validate_resource_contents_payload(first_content)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke-test the live MCP server by checking initialize, tools/list, resources/list, and resources/read."
    )
    parser.add_argument(
        "--url",
        default="http://localhost:8001/mcp",
        help="Base MCP streamable HTTP endpoint.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        asyncio.run(run_smoke(base_url=args.url))
    except Exception as exc:
        print(f"MCP smoke test failed: {exc}")
        return 1

    print(f"MCP smoke test passed for {args.url}")
    return 0
