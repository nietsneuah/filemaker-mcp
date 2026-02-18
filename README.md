# filemaker-mcp

Connect Claude (or any MCP client) to a FileMaker database — read-only queries,
schema discovery, and pandas-powered analytics.

## What It Does

filemaker-mcp is an MCP server that gives AI assistants live access to your
FileMaker data via OData v4. Load it in Claude Desktop or Claude Code and ask
questions about your data in plain English.

**Tools provided:**
- `fm_query_records` — Search and filter records with OData expressions
- `fm_get_record` — Fetch a single record by primary key
- `fm_count_records` — Count records with optional filters
- `fm_list_tables` — List available tables
- `fm_get_schema` — Discover field names, types, and keys
- `fm_load_dataset` — Pull records into memory for analytics
- `fm_analyze` — Run groupby/sum/count/mean/min/max on loaded data
- `fm_list_datasets` — See what datasets are loaded

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- FileMaker Server with OData v4 enabled
- An FM account with `fmodata` extended privilege

### Install

```bash
git clone https://github.com/nietsneuah/filemaker-mcp.git
cd filemaker-mcp
cp .env.example .env
# Edit .env with your FileMaker server details
uv sync
```

### Configure Claude Desktop

Add to your Claude Desktop MCP config
(`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "filemaker": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/filemaker-mcp", "filemaker-mcp"],
      "env": {
        "FM_HOST": "your-server.example.com",
        "FM_DATABASE": "your_database",
        "FM_USERNAME": "mcp_agent",
        "FM_PASSWORD": "your_password"
      }
    }
  }
}
```

### Run

```bash
uv run filemaker-mcp
```

## Schema Discovery — Important Setup Step

On startup, the server auto-discovers your table **names** from the OData
service document. However, for Claude to query effectively it needs field
names, types, and primary keys — this requires the `SCR_DDL_GetTableDDL`
FileMaker script.

**Without the DDL script:** Claude can see which tables exist but won't know
field names or types. Queries will be limited since Claude can't construct
proper filters or selects without knowing the schema.

**With the DDL script:** Full schema discovery — field names, types, primary
keys, foreign keys, and field tier classification. This is the intended setup.

See [FM Account Setup](docs/FM_ACCOUNT_SETUP.md) for the script definition
and installation steps. The script uses `GetTableDDL()` (FileMaker 22+) and
takes ~5 minutes to create.

## Analytics

For reports and summaries, use the analytics tools instead of raw queries:

1. `fm_load_dataset` — Fetch records into a pandas DataFrame (auto-paginates)
2. `fm_analyze` — Run aggregations instantly (no additional FM round trips)

This returns ~200 tokens instead of ~400K for raw records — much more efficient
for dashboards and trend analysis.

## Documentation

- [Architecture](docs/ARCHITECTURE.md) — System design and security model
- [FM Account Setup](docs/FM_ACCOUNT_SETUP.md) — FileMaker privilege configuration
- [Tool Reference](docs/TOOL_REFERENCE.md) — Detailed parameter docs

## License

GPL-3.0 — see [LICENSE](LICENSE)

## Author

Doug Hauenstein / [FM Rug Software](https://github.com/nietsneuah)
