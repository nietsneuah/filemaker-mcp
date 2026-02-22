"""FileMaker MCP Server — Entry point.

Registers all tools with FastMCP and handles lifecycle.
Run via: uv run filemaker-mcp
"""

import datetime
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastmcp import FastMCP

from filemaker_mcp.auth import odata_client
from filemaker_mcp.config import settings
from filemaker_mcp.tools.analytics import analyze as analytics_analyze
from filemaker_mcp.tools.analytics import flush_datasets as analytics_flush_datasets
from filemaker_mcp.tools.analytics import list_datasets as analytics_list_datasets
from filemaker_mcp.tools.analytics import load_dataset as analytics_load_dataset
from filemaker_mcp.tools.context import delete_context as context_delete_context
from filemaker_mcp.tools.context import save_context as context_save_context
from filemaker_mcp.tools.query import count_records, get_record, list_tables, query_records
from filemaker_mcp.tools.schema import bootstrap_ddl, get_schema
from filemaker_mcp.tools.tenant import (
    get_active_tenant,
    init_tenants,
)
from filemaker_mcp.tools.tenant import (
    list_tenants as tenant_list_tenants,
)
from filemaker_mcp.tools.tenant import (
    use_tenant as tenant_use_tenant,
)

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastMCP) -> AsyncIterator[None]:
    """Server lifecycle: load tenants, bootstrap default, close client on shutdown."""
    from filemaker_mcp.auth import reset_client

    default_name = init_tenants()
    tenant = get_active_tenant()
    if tenant:
        await reset_client(tenant)
        await bootstrap_ddl()
        logger.info("Connected to default tenant '%s' (%s)", default_name, tenant.host)
    else:
        logger.warning("No tenants configured — server starting without FM connection")
    try:
        yield
    finally:
        await odata_client.close()


# --- Initialize FastMCP Server ---
mcp = FastMCP(
    "FileMaker",
    lifespan=lifespan,
    instructions=(
        "You are connected to a FileMaker database via the FileMaker MCP server. "
        "\n\n"
        "CRITICAL WORKFLOW — follow this order:\n"
        "1. ALWAYS call fm_get_schema(table='TableName') BEFORE querying any table\n"
        "2. Use the EXACT field names returned by get_schema in your filters and selects\n"
        "3. Field names vary by table — some use spaces ('Customer Name'), "
        "some use underscores ('Date_of_Service'). "
        "The ONLY source of truth for field names is get_schema.\n"
        "\n"
        "QUERY TIPS:\n"
        f"- Today's date: {datetime.date.today().isoformat()}\n"
        "- Date filters: bare ISO dates, NO quotes (e.g., Date_of_Service ge 2026-02-14)\n"
        "- Use count_records before large queries to gauge result size\n"
        "\n"
        "ANALYTICS (for reports, summaries, aggregation):\n"
        "- Use fm_load_dataset to pull records into memory (fast, one-time FM call)\n"
        "- Use fm_analyze to run groupby/sum/count/mean/min/max (instant, no FM call)\n"
        "- Use fm_list_datasets to see what's loaded\n"
        "- Preferred over raw queries for any question involving totals, trends, or comparisons\n"
    ),
)


# --- Register Tools ---
# Each function's docstring becomes the tool description that Claude sees.
# Type hints become the parameter schema.


@mcp.tool()
async def fm_query_records(
    table: str,
    filter: str = "",
    select: str = "",
    top: int = 20,
    skip: int = 0,
    orderby: str = "",
    count: bool = True,
) -> str:
    """Query FileMaker records from a FileMaker table using OData v4.

    Use this tool to search, filter, and retrieve records from your
    FileMaker database.

    Args:
        table: Table name (use fm_list_tables to see available tables).
        filter: OData $filter expression.
            ALWAYS call fm_get_schema(table) first — field names vary by table.
            Examples (use exact names from get_schema):
            - "City eq 'Springfield'"
            - "ServiceDate ge 2026-01-01"
            - "Amount gt 500"
            - "Region eq 'A' and Status eq 'Open'"
        select: Comma-separated field names to return. Leave empty for all fields.
            Example: "Company Name,Phone,City,Email"
        top: Maximum records to return (default 20, max 10000).
        skip: Number of records to skip (for pagination).
        orderby: OData $orderby expression.
            Example: "ServiceDate desc" or "Company Name asc"
        count: Include total record count in response (default True).

    Returns:
        Formatted text with matching records and field values.
    """
    return await query_records(
        table=table,
        filter=filter,
        select=select,
        top=top,
        skip=skip,
        orderby=orderby,
        count=count,
    )


@mcp.tool()
async def fm_get_record(table: str, record_id: str, id_field: str = "") -> str:
    """Get a single FileMaker record by its primary key.

    Use this when you know the specific record ID and want full details.

    Args:
        table: Table name (use fm_list_tables for available tables).
        record_id: The primary key value to look up.
        id_field: The primary key field name (use fm_get_schema to find PKs).

    Returns:
        Formatted text with all fields for the matching record.
    """
    return await get_record(table=table, record_id=record_id, id_field=id_field)


@mcp.tool()
async def fm_count_records(table: str, filter: str = "") -> str:
    """Get the total record count for an FileMaker table, optionally filtered.

    Quick way to check data volume or validate filter expressions
    before running a full query.

    Args:
        table: Table name (see fm_query_records for available tables).
        filter: Optional OData $filter expression to count matching records.

    Returns:
        The record count as a text message.
    """
    return await count_records(table=table, filter=filter)


@mcp.tool()
async def fm_list_tables() -> str:
    """List all available FileMaker tables and their descriptions.

    Use this to understand what data is available before querying.
    Always start here if unsure which table to query.

    Returns:
        List of table names with descriptions of what each contains.
    """
    return await list_tables()


@mcp.tool()
async def fm_get_schema(table: str = "", refresh: bool = False, show_all: bool = False) -> str:
    """Get the database schema (field names and types) from FileMaker.

    Use this to discover exact field names, their types, and primary keys
    before constructing queries. Essential for building accurate filter
    and select expressions.

    IMPORTANT: Always call this with a specific table name before querying
    that table for the first time — many field names contain spaces.

    Schema is cached in memory for the session. If you need a table not in
    the standard list, just request it — the server will auto-discover it
    from FileMaker.

    Args:
        table: Table name to get fields for (e.g., "Customers", "Orders").
            Leave empty to list all available tables.
            You can request any table that exists in FileMaker — not just
            the standard list. Unknown tables are auto-discovered.
        refresh: Force re-fetch from live FM server. Use when you suspect
            the schema has changed (e.g., new fields added in FileMaker).
            Default uses cached DDL (instant, no API call).
        show_all: Show all fields including internal/system fields.
            Default hides internal fields (globals, speed fields, etc.)
            to keep schema output concise.

    Returns:
        Formatted listing of fields with names, types, and annotations.
    """
    return await get_schema(table=table, refresh=refresh, show_all=show_all)


@mcp.tool()
async def fm_load_dataset(
    name: str,
    table: str,
    filter: str = "",
    select: str = "",
) -> str:
    """Load FileMaker records into a named dataset for fast analytics.

    Fetches records from FM and stores them as a pandas DataFrame in session
    memory. Load once, then run multiple analyses with fm_analyze — no
    additional FM round trips needed.

    Auto-paginates if more than 10,000 records match.
    Loading a dataset with an existing name replaces it (refresh).

    IMPORTANT: Call fm_get_schema(table) first to discover field names.

    Args:
        name: Your chosen identifier for this dataset (e.g., "inv25", "customers").
        table: FM table to query (see fm_list_tables for available tables).
        filter: OData $filter expression. Use exact field names from get_schema.
            Example: "ServiceDate ge 2025-01-01 and ServiceDate lt 2026-01-01"
        select: Comma-separated fields to fetch. Leave empty for all fields.
            TIP: Select only the fields you need — reduces memory and speeds loading.
            Example: "Technician,Region,Amount,ServiceDate"

    Returns:
        Summary with row count, columns, and memory usage.
    """
    return await analytics_load_dataset(name=name, table=table, filter=filter, select=select)


@mcp.tool()
async def fm_analyze(
    dataset: str,
    groupby: str = "",
    aggregate: str = "",
    filter: str = "",
    sort: str = "",
    limit: int = 50,
    period: str = "",
    pivot_column: str = "",
) -> str:
    """Analyze a loaded dataset with groupby/aggregation. No FM round trip.

    Runs pandas aggregation on a previously loaded dataset OR a table from
    the auto-populated table cache (from query_records). Returns compact
    summary tables instead of raw records — ~200 tokens vs ~400K tokens.

    Behavior by parameter combination:
    - groupby + aggregate: Grouped aggregation (most common)
    - aggregate only: Scalar aggregation across all rows
    - groupby only: Group counts (value_counts)
    - neither: Summary statistics (describe)
    - period: Time-series resampling (week/month/quarter)
    - pivot_column: Cross-tabulation pivot table

    Supported aggregate functions: sum, count, mean, min, max, median, nunique, std

    Args:
        dataset: Name of a previously loaded dataset (from fm_load_dataset),
            or a table name from the auto-populated table cache.
        groupby: Comma-separated field names to group by.
            Example: "Technician,Region"
        aggregate: Comma-separated function:field pairs.
            Example: "sum:Amount,count:Amount,mean:Amount"
        filter: Pandas query expression to narrow data before aggregating.
            Example: "Region == 'A'" or "Amount > 500"
        sort: Sort result by column name with optional direction.
            Example: "Amount_sum desc"
        limit: Maximum rows in output (default 50).
        period: Time-series resampling — "week", "month", or "quarter".
            First groupby field must be a datetime column.
            Example: groupby="ServiceDate", period="month"
        pivot_column: Cross-tabulate by this column (pivot table).
            Requires groupby for row index and aggregate for values.
            Example: groupby="Technician", pivot_column="Region", aggregate="sum:Amount"

    Returns:
        Formatted summary table with aggregation results.
    """
    return await analytics_analyze(
        dataset=dataset,
        groupby=groupby,
        aggregate=aggregate,
        filter=filter,
        sort=sort,
        limit=limit,
        period=period,
        pivot_column=pivot_column,
    )


@mcp.tool()
async def fm_list_datasets() -> str:
    """List all datasets currently loaded in session memory.

    Shows what's available for analysis with fm_analyze.
    Includes dataset name, source table, row count, columns, and load time.

    Returns:
        Formatted list of loaded datasets, or message if none loaded.
    """
    return await analytics_list_datasets()


@mcp.tool()
async def fm_flush_datasets(table: str = "") -> str:
    """Flush cached table data from session memory.

    The MCP server auto-caches query results per table for fast repeat access.
    Use this to force a fresh fetch from FileMaker — for example, after data
    has been modified, or to free memory.

    Args:
        table: Specific table to flush (e.g., "Invoices").
            Leave empty to flush ALL cached tables.

    Returns:
        Confirmation with number of rows/tables flushed.
    """
    return await analytics_flush_datasets(table=table)


@mcp.tool()
async def fm_use_tenant(name: str) -> str:
    """Switch to a different FileMaker tenant.

    Connects to the named tenant's FM server and discovers
    its schema. First switch triggers full bootstrap (may take
    a few seconds). All subsequent queries go to this tenant.

    Args:
        name: Tenant name as configured (e.g., "production", "staging").
            Case-insensitive. Use fm_list_tenants() to see available names.

    Returns:
        Connection summary with host, database, and table count.
    """
    return await tenant_use_tenant(name=name)


@mcp.tool()
async def fm_list_tenants() -> str:
    """List all configured FileMaker tenants and show which is active.

    Shows tenant names, hosts, and databases. Use fm_use_tenant()
    to switch to a different tenant.

    Returns:
        Formatted list of tenants with the active one marked.
    """
    return tenant_list_tenants()


@mcp.tool()
async def fm_save_context(
    table_name: str,
    context: str,
    field_name: str = "",
    context_type: str = "field_values",
    source: str = "auto",
) -> str:
    """Save an operational learning about a FileMaker field or table.

    Call this when you discover useful information during queries:
    - Field value mappings (e.g., Commercial field uses "1" not "Yes")
    - OData syntax rules (e.g., "ne" operator not supported)
    - Query patterns (e.g., how to join two tables)
    - Relationships (e.g., FK between Invoices and Customers)
    - Value normalization maps (e.g., "Jake" and "Jacob Owens" are the same person)

    The learning is saved to FM and loaded automatically at next startup,
    so future sessions benefit immediately.

    Args:
        table_name: Table this applies to (e.g., "Invoices").
        context: What you learned. For most types, free text
            (e.g., "Boolean: 1=yes, empty/0=no"). For "value_map" type,
            MUST be a JSON object mapping variant values to their canonical
            form, e.g. '{"Jake": "Jacob Owens", "Bob": "Robert Smith"}'.
        field_name: Specific field name, or empty for table-level context.
        context_type: Category — "field_values", "syntax_rule",
            "query_pattern", "relationship", or "value_map". Use "value_map"
            when the user identifies that two field values represent the same
            entity (e.g., nicknames, abbreviations, data entry variants).
            Value maps are applied automatically during fm_analyze groupby.
        source: How this was discovered — "auto", "auto:filter_discovery", "manual".

    Returns:
        Confirmation message or error description.
    """
    return await context_save_context(
        table_name=table_name,
        context=context,
        field_name=field_name,
        context_type=context_type,
        source=source,
    )


@mcp.tool()
async def fm_delete_context(
    table_name: str,
    field_name: str = "",
    context_type: str = "field_values",
) -> str:
    """Delete an operational learning about a FileMaker field or table.

    Call this to remove stale or incorrect context entries — for example,
    when a table or field has been renamed/deleted, or when a previously
    saved hint is no longer accurate.

    The record is deleted from FM and removed from the local cache immediately.

    Args:
        table_name: Table this applies to (e.g., "Invoices").
        field_name: Specific field name, or empty for table-level context.
        context_type: Category — "field_values", "syntax_rule", "query_pattern", "relationship".

    Returns:
        Confirmation message or error description.
    """
    return await context_delete_context(
        table_name=table_name,
        field_name=field_name,
        context_type=context_type,
    )


def main() -> None:
    """Entry point for the MCP server."""
    logger.info("Starting FileMaker MCP Server")
    logger.info("FM Host: %s", settings.fm_host)
    logger.info("FM Database: %s", settings.fm_database)
    logger.info("FM User: %s", settings.fm_username)
    mcp.run()


if __name__ == "__main__":
    main()
