# Changelog

## [Unreleased]

## [0.1.3] — 2026-02-22

### Developer Tools & Context Management

- **CLI tooling** — New `mcp-tools` CLI with `schema`, `query`, and `list-tables` subcommands for terminal-based FileMaker access without an MCP client
- **Field classifier** — Universal FM field classification engine (key, stored, internal, calculated, summary, global) with 8 priority-ordered rules, confidence levels, and per-field/per-table/tenant-wide overrides
- **DDL Context Builder** — Training tool that bootstraps FM, classifies all fields via the classifier, writes delta to DDL Context, and produces a JSON report. Supports `--dry-run` and `--metadata` enrichment
- **Data fidelity diagnostic** — Live FM diagnostic for PK integrity, FK round-trips, type consistency, field completeness, and metadata key validation across all exposed tables
- **Shared script bootstrap** — Extracted `bootstrap_tenant()` into `scripts/_common.py`, eliminating duplicated boilerplate. All scripts accept `--tenant` for explicit tenant selection
- **Date period arithmetic** — Generic `ReportDates` class for Daily, WTD, MTD, QTD, YTD + all comparative periods. Schema-agnostic, pure stdlib, importable by downstream consumers

### Caching & Analytics

- **Opportunistic date-range caching** — Auto-caches query results as DataFrames per table keyed by date range. Gap-fetching for incremental growth
- **Today-refresh** — Queries including today always re-fetch from FM even on cache hit
- **New aggregations** — `median`, `nunique`, `std` added to `fmrug_analyze`
- **Time-series mode** — `period` parameter for week/month/quarter resampling
- **Pivot cross-tabulation** — `pivot_column` parameter for pivot tables
- **Analytics enforcement** — MCP instructions mandate `fmrug_analyze` for all totals, counts, and comparisons

### Bug Fixes

- Date cache bypass — Non-date filters on cached tables no longer trigger unbounded full-table fetches
- `eq` date filter — Treats `eq X` as `ge X and le X` instead of ignoring the operator
- `$select` honored on cache-hit path — Previously returned all columns regardless
- `get_record` metadata filtering — Strips `@id`, `@editLink` and all `@`-prefixed keys
- `count_records` — Uses `get_pk_field(table)` instead of hardcoded `"PrimaryKey"`
- DRY: `auth.py` error handling — Extracted shared handler from 4 identical exception blocks

### Added

- **CONTRIBUTING.md** — Community contribution guide
- **Diagnostic scripts organized** — `scripts/diagnostics/` for all dev tools

## [0.1.2] — 2026-02-21

### Added

- **Release automation** — `scripts/release.sh` for version bump, tests, commit, tag, push, and GitHub release
- **Single version source** — hatch dynamic version reads from `__init__.py`

### Fixed

- `uv` not found in release script — added `$HOME/.local/bin` to PATH

## [0.1.1] — 2026-02-21

### Fixed

- **Surface bootstrap errors** — `list_tables()` now shows actual OData errors instead of silently returning empty
