# Changelog

## [Unreleased]

## [0.1.2] — 2026-02-21

### Added
- **Release automation** — `scripts/release.sh` handles version bump, CHANGELOG update, tests, commit, tag, push, and GitHub release creation. Supports `patch`, `minor`, `major`, explicit `X.Y.Z`, and `--dry-run`.
- **Single version source** — hatch dynamic version reads from `src/filemaker_mcp/__init__.py`. Removed static `version` from `pyproject.toml`.

### Fixed
- Release script hardened — version format validation, `gh` CLI preflight check, reject multiple args, push only specific tag, `uv` PATH fix for non-interactive shells.

## [0.1.1] — 2026-02-21

### Fixed
- **Silent empty table list on connection failure** — `list_tables()` now shows the actual error when bootstrap fails (bad credentials, unreachable host, missing `FM_DATABASE`) instead of silently returning an empty list. Clears stale errors when bootstrap subsequently succeeds. ([#2](https://github.com/nietsneuah/filemaker-mcp/issues/2))
- **Zero tables discovered when DDL script is missing** — `bootstrap_ddl()` now falls through to OData service document discovery when the `SCR_DDL_GetTableDDL` script is not found on the FM server. Previously, the "script not found" branch returned immediately and relied on static DDL, which is empty in this repo — resulting in zero tables. ([#3](https://github.com/nietsneuah/filemaker-mcp/pull/3))
- **Settings rejects extra environment variables** — `Settings` (pydantic-settings) now includes `"extra": "ignore"` in `model_config`. Previously, any unrecognized environment variable (e.g., `ANTHROPIC_API_KEY`, `GHL_API_KEY`) caused a `ValidationError` at import time. This was invisible when running as a standalone MCP server (only FM vars present), but broke when imported as a library dependency in projects with their own env vars. ([#1](https://github.com/nietsneuah/filemaker-mcp/pull/1))

## [0.1.0] — 2026-02-16

### Added
- Initial release — FileMaker MCP server for Claude
- OData v4 read-only access: query, get, count, list tables, get schema
- Pandas-powered analytics: load dataset, analyze (groupby/sum/count/mean/min/max)
- Auto-discovers tables from OData service document
- Optional `GetTableDDL` script integration for richer schema
- Claude Desktop and Claude Code configuration support
