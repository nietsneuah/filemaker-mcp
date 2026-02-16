"""Analytics tools — session-persistent DataFrames with pandas aggregation.

Load data from FM into named DataFrames, then run groupby/aggregate
queries without additional FM round trips. Results are compact summary
tables (~200 tokens) instead of raw records (~400K tokens).
"""

import logging
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from filemaker_mcp.auth import odata_client
from filemaker_mcp.ddl import TABLES
from filemaker_mcp.tools.query import (
    EXPOSED_TABLES,
    normalize_dates_in_filter,
    quote_fields_in_filter,
    quote_fields_in_select,
)

logger = logging.getLogger(__name__)


@dataclass
class DatasetEntry:
    """A named DataFrame with metadata about its source."""

    df: pd.DataFrame
    table: str
    filter: str
    select: str
    loaded_at: datetime
    row_count: int


# Session-persistent cache — keys are Claude-chosen dataset names.
# Persists for MCP server process lifetime. No eviction needed —
# business datasets are typically 1-5 MB each.
_datasets: dict[str, DatasetEntry] = {}


async def list_datasets() -> str:
    """List all datasets currently loaded in session memory.

    Returns:
        Formatted list of datasets with name, source, row count, and columns.
    """
    if not _datasets:
        return "No datasets loaded. Use fm_load_dataset to load data from a table."

    lines = ["Loaded datasets:", ""]
    for name, entry in _datasets.items():
        cols = ", ".join(entry.df.columns.tolist())
        lines.append(f"  {name}: {entry.row_count} rows from {entry.table}")
        lines.append(f"    Filter: {entry.filter or '(none)'}")
        lines.append(f"    Columns: {cols}")
        lines.append(f"    Loaded: {entry.loaded_at.isoformat()}")
        lines.append("")
    return "\n".join(lines)


async def load_dataset(
    name: str,
    table: str,
    filter: str = "",
    select: str = "",
) -> str:
    """Fetch records from FM and store as a named DataFrame.

    Args:
        name: Claude-chosen identifier for the dataset (e.g., "inv25").
        table: FM table to query.
        filter: OData $filter expression (date normalization + field quoting applied).
        select: Comma-separated fields to fetch. Empty = all fields.

    Returns:
        Summary of what was loaded (row count, columns, memory usage).
    """
    # Validate table
    if table not in EXPOSED_TABLES:
        available = ", ".join(EXPOSED_TABLES.keys())
        return f"Error: Unknown table '{table}'. Available tables: {available}"

    # Build OData params — reuse the query pipeline for filter/select processing
    params: dict[str, str] = {"$top": "10000"}
    if filter:
        params["$filter"] = quote_fields_in_filter(normalize_dates_in_filter(filter))
    if select:
        params["$select"] = quote_fields_in_select(select)

    try:
        # Fetch with auto-pagination
        all_records: list[dict] = []
        skip = 0
        while True:
            page_params = {**params}
            if skip > 0:
                page_params["$skip"] = str(skip)

            data = await odata_client.get(table, params=page_params)
            records = data.get("value", [])
            all_records.extend(records)

            if len(records) < 10000:
                break
            skip += 10000

        if not all_records:
            return f"0 records matched filter for '{table}'. Dataset '{name}' not created."

        # Build DataFrame
        df = pd.DataFrame(all_records)

        # Convert date columns using DDL type info
        table_ddl = TABLES.get(table, {})
        for field_name, field_def in table_ddl.items():
            if field_def.get("type") in ("date", "datetime") and field_name in df.columns:
                df[field_name] = pd.to_datetime(df[field_name], errors="coerce")

        # Store in session cache
        entry = DatasetEntry(
            df=df,
            table=table,
            filter=filter,
            select=select,
            loaded_at=datetime.now(),
            row_count=len(df),
        )
        _datasets[name] = entry

        # Build summary
        cols = ", ".join(df.columns.tolist())
        mem = df.memory_usage(deep=True).sum()
        mem_str = f"{mem / 1024:.0f} KB" if mem < 1024 * 1024 else f"{mem / (1024 * 1024):.1f} MB"
        return (
            f"Dataset '{name}': {len(df)} rows x {len(df.columns)} columns ({mem_str})\n"
            f"Source: {table}" + (f" | Filter: {filter}" if filter else "") + f"\nColumns: {cols}"
        )

    except ConnectionError as e:
        return f"Connection error loading dataset: {e}"
    except PermissionError as e:
        return f"Authentication error: {e}"
    except ValueError as e:
        return f"Query error loading dataset: {e}"
    except Exception as e:
        logger.exception("Error loading dataset '%s' from %s", name, table)
        return f"Error loading dataset: {type(e).__name__}: {e}"


_SUPPORTED_AGGS = {"sum", "count", "mean", "min", "max"}


def _parse_aggregates(
    aggregate_str: str, available_columns: list[str]
) -> dict[str, list[str]] | str:
    """Parse 'sum:Field,count:Field' into {Field: [sum, count]}.

    Returns a dict on success, or an error string on failure.
    """
    if not aggregate_str:
        return {}

    agg_dict: dict[str, list[str]] = {}
    for pair in aggregate_str.split(","):
        pair = pair.strip()
        if ":" not in pair:
            return (
                f"Invalid aggregate format: '{pair}'. "
                "Expected 'function:field' (e.g., 'sum:InvoiceTotal')."
            )
        func, field = pair.split(":", 1)
        func = func.strip().lower()
        field = field.strip()

        if func not in _SUPPORTED_AGGS:
            return f"Unknown function '{func}'. Supported: {', '.join(sorted(_SUPPORTED_AGGS))}"
        if field not in available_columns:
            return f"Field '{field}' not in dataset. Available: {', '.join(available_columns)}"

        if field not in agg_dict:
            agg_dict[field] = []
        agg_dict[field].append(func)

    return agg_dict


async def analyze(
    dataset: str,
    groupby: str = "",
    aggregate: str = "",
    filter: str = "",
    sort: str = "",
    limit: int = 50,
) -> str:
    """Run aggregation on a stored dataset. No FM round trip -- pure pandas.

    Args:
        dataset: Name of a previously loaded dataset.
        groupby: Comma-separated field names (e.g., "Driver,Zone").
        aggregate: Comma-separated function:field pairs
            (e.g., "sum:InvoiceTotal,count:InvoiceTotal").
        filter: Pandas query expression to narrow data before aggregating (e.g., "Zone == 'A'").
        sort: Sort result by column (e.g., "InvoiceTotal_sum desc").
        limit: Max rows in output (default 50).

    Returns:
        Formatted text table with results.
    """
    if dataset not in _datasets:
        available = ", ".join(_datasets.keys()) if _datasets else "(none)"
        return (
            f"Dataset '{dataset}' not found. Loaded datasets: {available}. "
            "Use fm_load_dataset to load data first."
        )

    entry = _datasets[dataset]
    df = entry.df.copy()

    # Apply pandas filter
    if filter:
        try:
            df = df.query(filter)
        except Exception as e:
            return f"Filter error: {e}"

    if not groupby and not aggregate:
        # describe() -- summary statistics
        result_df = df.describe(include="all")
        return f"Summary statistics for '{dataset}' ({len(df)} records):\n\n{result_df.to_string()}"

    # Parse groupby fields
    groupby_fields = [f.strip() for f in groupby.split(",") if f.strip()] if groupby else []

    # Validate groupby fields
    for field in groupby_fields:
        if field not in df.columns:
            return f"Field '{field}' not in dataset. Available: {', '.join(df.columns.tolist())}"

    if groupby and not aggregate:
        # Value counts per group
        if len(groupby_fields) == 1:
            counts = df[groupby_fields[0]].value_counts()
            result_str = counts.head(limit).to_string()
        else:
            counts = df.groupby(groupby_fields).size().reset_index(name="count")
            counts = counts.sort_values("count", ascending=False).head(limit)
            result_str = counts.to_string(index=False)
        return (
            f"Group counts for '{dataset}' ({len(df)} records):\n\n"
            f"{result_str}\n\n"
            f"({len(counts)} groups)"
        )

    # Parse aggregate spec
    agg_dict = _parse_aggregates(aggregate, df.columns.tolist())
    if isinstance(agg_dict, str):
        return agg_dict  # Error message

    if groupby_fields:
        # Grouped aggregation
        try:
            result_df = df.groupby(groupby_fields).agg(agg_dict)
        except Exception as e:
            return f"Aggregation error: {e}"

        # Flatten multi-level column names: (InvoiceTotal, sum) -> InvoiceTotal_sum
        result_df.columns = [f"{col}_{func}" for col, func in result_df.columns]
        result_df = result_df.reset_index()
    else:
        # Scalar aggregation -- apply agg across all rows
        results = {}
        for field, funcs in agg_dict.items():
            for func in funcs:
                col_name = f"{field}_{func}"
                results[col_name] = [getattr(df[field], func)()]
        result_df = pd.DataFrame(results)

    # Sort
    if sort:
        parts = sort.strip().split()
        sort_col = parts[0]
        ascending = not (len(parts) > 1 and parts[1].lower() == "desc")
        if sort_col in result_df.columns:
            result_df = result_df.sort_values(sort_col, ascending=ascending)

    # Limit
    total_groups = len(result_df)
    result_df = result_df.head(limit)

    # Format output
    result_str = result_df.to_string(index=False)
    return (
        f"Analysis of '{dataset}' ({len(df)} records aggregated):\n\n"
        f"{result_str}\n\n"
        f"({total_groups} groups shown, {len(entry.df)} total records in dataset)"
    )
