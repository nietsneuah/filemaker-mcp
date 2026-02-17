# Changelog

## [Unreleased]

### Fixed
- **Settings rejects extra environment variables** — `Settings` (pydantic-settings) now includes `"extra": "ignore"` in `model_config`. Previously, any unrecognized environment variable (e.g., `ANTHROPIC_API_KEY`, `GHL_API_KEY`) caused a `ValidationError` at import time. This was invisible when running as a standalone MCP server (only FM vars present), but broke when imported as a library dependency in projects with their own env vars. ([#1](https://github.com/nietsneuah/filemaker-mcp/pull/1))

## [0.1.0] — 2026-02-16

### Added
- Initial release — FileMaker MCP server for Claude
- OData v4 read-only access: query, get, count, list tables, get schema
- Pandas-powered analytics: load dataset, analyze (groupby/sum/count/mean/min/max)
- Auto-discovers tables from OData service document
- Optional `GetTableDDL` script integration for richer schema
- Claude Desktop and Claude Code configuration support
