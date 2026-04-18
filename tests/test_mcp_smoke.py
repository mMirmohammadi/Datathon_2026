from apps_sdk.server.main import (
    WIDGET_TEMPLATE_URI,
    build_resource_contents_meta,
    build_tool_descriptor,
)
from apps_sdk.server.smoke import (
    validate_resource_contents_payload,
    validate_resource_descriptor,
    validate_tool_descriptor_payload,
)
from mcp.types import Resource, TextResourceContents


def test_validate_tool_descriptor_payload_accepts_current_tool() -> None:
    validate_tool_descriptor_payload(build_tool_descriptor())


def test_validate_resource_descriptor_accepts_current_resource() -> None:
    resource = Resource(
        name="Listings map and ranked list",
        title="Listings map and ranked list",
        uri=WIDGET_TEMPLATE_URI,
        description="Combined ranked list and map widget for listing search results.",
        mimeType="text/html;profile=mcp-app",
        _meta=build_resource_contents_meta(public_base_url="https://example.com"),
    )

    validate_resource_descriptor(resource)


def test_validate_resource_contents_payload_accepts_current_content() -> None:
    content = TextResourceContents(
        uri=WIDGET_TEMPLATE_URI,
        mimeType="text/html;profile=mcp-app",
        text="<html><body><div id='root'></div></body></html>",
        _meta=build_resource_contents_meta(public_base_url="https://example.com"),
    )

    validate_resource_contents_payload(content)
