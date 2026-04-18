# Widget Change Notes

Before making changes in `apps_sdk/web`, check these sources first:

- `../server/main.py`
- `../server/widget.py`
- `../server/smoke.py`
- `../../README.md`
- Context7 OpenAI Apps SDK docs and OpenAI Apps sdk examples (mcp apps)
- Context7 MCP Apps / Model Context Protocol Apps docs

Priorities:

- Keep the widget compatible and in sync with the existing MCP tool result shape.
- Prefer thin, local UI changes over introducing new framework layers.
- Preserve the split list + map layout unless there is a strong reason to change it.
