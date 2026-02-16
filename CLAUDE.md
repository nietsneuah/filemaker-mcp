# FileMaker MCP Server — Project Context

## What This Is
A Python MCP server connecting AI clients (Claude Desktop, Claude Code) to
FileMaker databases via OData v4. Provides read-only query, schema discovery,
and pandas-based analytics tools.

## Architecture
```
Claude Desktop / Claude Code (MCP Client)
    ↕ stdio
FileMaker MCP Server (Python / FastMCP)
    ↕ HTTPS
FileMaker Server (OData v4 API)
```

## Tech Stack
- **Python 3.12+** with `uv` for package management
- **FastMCP** — MCP framework
- **httpx** — async HTTP client
- **pandas** — analytics/aggregation engine

## Key Modules
- `src/filemaker_mcp/server.py` — FastMCP entry point, tool registration
- `src/filemaker_mcp/auth.py` — OData HTTP client
- `src/filemaker_mcp/tools/query.py` — Query tools + field quoting
- `src/filemaker_mcp/tools/schema.py` — Schema discovery + DDL bootstrap
- `src/filemaker_mcp/tools/analytics.py` — pandas load/analyze tools
- `src/filemaker_mcp/ddl.py` — Schema cache + FieldDef types
- `src/filemaker_mcp/config.py` — Settings from env vars

## Commands
```bash
uv sync                    # Install dependencies
uv run filemaker-mcp       # Start MCP server
uv run pytest              # Run tests
uv run ruff check .        # Lint
uv run ruff format .       # Format
```

## FM OData Patterns
- Field names with spaces must be double-quoted in OData URLs
- Dates use bare ISO format: `2026-02-14` (no quotes)
- `$count` returns as `@count` (not `@odata.count`)
- `$top=0` breaks count — use `$top=1` instead

## Coding Standards
- Type hints on all functions
- Docstrings on public functions (become MCP tool descriptions)
- async/await throughout
- Environment variables for all config
