"""Analytics tools — session-persistent DataFrames with pandas aggregation.

Load data from FM into named DataFrames, then run groupby/aggregate
queries without additional FM round trips. Results are compact summary
tables (~200 tokens) instead of raw records (~400K tokens).

Table-level caching: query_records auto-populates _table_cache with one
DataFrame per table, keyed by date range. Subsequent queries for the same
table serve from cache if the date range is covered.
"""

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pandas as pd  # type: ignore[import-untyped]

from filemaker_mcp.auth import odata_client
from filemaker_mcp.ddl import TABLES, get_context_value
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
    date_field: str = ""  # from cache_config, "" for cache_all/named
    date_min: date | None = None  # earliest date in DataFrame
    date_max: date | None = None  # latest date in DataFrame
    pk_field: str = "PrimaryKey"  # from DDL, for dedup on merge


# Session-persistent cache — keys are Claude-chosen dataset names.
# Persists for MCP server process lifetime. No eviction needed —
# business datasets are typically 1-5 MB each.
_datasets: dict[str, DatasetEntry] = {}

# Table-level cache — one DataFrame per table, keyed by table name.
# Populated automatically by query_records when cache_config exists.
_table_cache: dict[str, DatasetEntry] = {}

MAX_ROWS_PER_TABLE = 50_000


# --- Table cache management ---


async def flush_datasets(table: str = "") -> str:
    """Flush cached table DataFrames.

    Args:
        table: Specific table to flush. Empty = flush all.

    Returns:
        Confirmation message.
    """
    if table:
        if table in _table_cache:
            rows = _table_cache[table].row_count
            del _table_cache[table]
            return f"Flushed '{table}' ({rows} rows)."
        return f"No cached data found for '{table}'."
    count = len(_table_cache)
    _table_cache.clear()
    return f"Flushed {count} table cache(s)."


def compute_date_gaps(
    existing_min: str | None,
    existing_max: str | None,
    requested_min: str | None,
    requested_max: str | None,
) -> list[tuple[str | None, str | None]]:
    """Compute date ranges that need fetching to satisfy the request.

    Compares existing cached date range with requested range and returns
    a list of (min, max) gap tuples that need fetching from FM.

    Args:
        existing_min: Cached range start (ISO string) or None if no cache.
        existing_max: Cached range end (ISO string) or None if no cache.
        requested_min: Requested range start or None (open-ended).
        requested_max: Requested range end or None (open-ended).

    Returns:
        List of (min_date, max_date) tuples to fetch. Empty list = fully cached.
    """
    if existing_min is None or existing_max is None:
        # No existing cache — fetch the full requested range
        return [(requested_min, requested_max)]

    e_min = date.fromisoformat(existing_min)
    e_max = date.fromisoformat(existing_max)

    r_min = date.fromisoformat(requested_min) if requested_min else None
    r_max = date.fromisoformat(requested_max) if requested_max else None

    gaps: list[tuple[str | None, str | None]] = []

    # Gap before existing range
    if r_min is not None and r_min < e_min:
        gap_end = (e_min - timedelta(days=1)).isoformat()
        gaps.append((r_min.isoformat(), gap_end))
    elif r_min is None:
        # Open-ended left — need everything before existing
        gap_end = (e_min - timedelta(days=1)).isoformat()
        gaps.append((None, gap_end))

    # Gap after existing range
    if r_max is not None and r_max > e_max:
        gap_start = (e_max + timedelta(days=1)).isoformat()
        gaps.append((gap_start, r_max.isoformat()))
    elif r_max is None:
        # Open-ended right — need everything after existing
        gap_start = (e_max + timedelta(days=1)).isoformat()
        gaps.append((gap_start, None))

    return gaps


def _enforce_row_limit(df: pd.DataFrame, date_field: str, table: str) -> pd.DataFrame:
    """Truncate DataFrame to MAX_ROWS_PER_TABLE, keeping most recent rows."""
    if len(df) <= MAX_ROWS_PER_TABLE:
        return df
    before = len(df)
    if date_field and date_field in df.columns:
        df = df.sort_values(date_field, ascending=False).head(MAX_ROWS_PER_TABLE)
    else:
        df = df.tail(MAX_ROWS_PER_TABLE)
    logger.warning(
        "Table cache for '%s' exceeded %d rows (%d) — truncated to %d most recent",
        table,
        MAX_ROWS_PER_TABLE,
        before,
        len(df),
    )
    return df.reset_index(drop=True)


def merge_into_table_cache(
    table: str,
    new_df: pd.DataFrame,
    date_field: str,
    pk_field: str,
    date_min: str | None,
    date_max: str | None,
) -> None:
    """Merge new records into the table cache, deduplicating on PK.

    If no cache exists for the table, creates a new entry.
    If cache exists, concatenates and deduplicates on pk_field,
    then updates date bounds to the union of old and new ranges.

    Args:
        table: Table name (key in _table_cache).
        new_df: New records to merge.
        date_field: Date field name for this table.
        pk_field: Primary key field for deduplication.
        date_min: New range lower bound (ISO string or None).
        date_max: New range upper bound (ISO string or None).
    """
    d_min = date.fromisoformat(date_min) if date_min else None
    d_max = date.fromisoformat(date_max) if date_max else None

    if table not in _table_cache:
        df = _enforce_row_limit(new_df, date_field, table)
        _table_cache[table] = DatasetEntry(
            df=df,
            table=table,
            filter="",
            select="",
            loaded_at=datetime.now(),
            row_count=len(df),
            date_field=date_field,
            date_min=d_min,
            date_max=d_max,
            pk_field=pk_field,
        )
        return

    existing = _table_cache[table]
    combined = pd.concat([existing.df, new_df], ignore_index=True)

    # Deduplicate on PK — keep last (new data wins)
    if pk_field in combined.columns:
        combined = combined.drop_duplicates(subset=[pk_field], keep="last")

    combined = _enforce_row_limit(combined, date_field, table)

    # Update date bounds to union
    new_min = d_min
    new_max = d_max
    if existing.date_min and new_min:
        new_min = min(existing.date_min, new_min)
    elif existing.date_min:
        new_min = existing.date_min
    if existing.date_max and new_max:
        new_max = max(existing.date_max, new_max)
    elif existing.date_max:
        new_max = existing.date_max

    existing.df = combined
    existing.row_count = len(combined)
    existing.date_min = new_min
    existing.date_max = new_max
    existing.loaded_at = datetime.now()


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
        all_records: list[dict[str, object]] = []
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

        # Build DataFrame, dropping OData metadata columns
        df = pd.DataFrame(all_records)
        meta_cols = [c for c in df.columns if c.startswith("@")]
        if meta_cols:
            df = df.drop(columns=meta_cols)

        # Convert date columns using DDL type info
        table_ddl = TABLES.get(table, {})
        for field_name, field_def in table_ddl.items():
            if field_def.get("type") in ("date", "datetime") and field_name in df.columns:
                df[field_name] = pd.to_datetime(df[field_name], format="mixed", errors="coerce")

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


_SUPPORTED_AGGS = {"sum", "count", "mean", "min", "max", "median", "nunique", "std"}


def _parse_value_maps(context_str: str | None) -> dict[str, str]:
    """Parse a value_map context string (JSON dict) into a replacement dict.

    Returns empty dict on None, empty string, or invalid JSON.
    """
    if not context_str:
        return {}
    try:
        parsed = json.loads(context_str)
    except (json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {str(k): str(v) for k, v in parsed.items()}


def _apply_normalization(
    df: pd.DataFrame, field_mappings: dict[str, dict[str, str]]
) -> tuple[pd.DataFrame, list[str]]:
    """Apply value mappings to DataFrame columns, returning a copy.

    Args:
        df: Source DataFrame (not mutated).
        field_mappings: {field_name: {old_value: new_value, ...}, ...}

    Returns:
        Tuple of (normalized DataFrame copy, list of inline note strings).
        Notes are empty if no values were actually changed.
    """
    if not field_mappings:
        return df, []

    df = df.copy()
    notes: list[str] = []

    for field, mapping in field_mappings.items():
        if field not in df.columns:
            continue
        before = df[field].copy()
        df[field] = df[field].replace(mapping)
        changed_counts: list[str] = []
        for old_val, new_val in mapping.items():
            count = int((before == old_val).sum())
            if count > 0:
                changed_counts.append(f"{old_val}\u2192{new_val}: {count}")
        if changed_counts:
            notes.append(f"{field} ({', '.join(changed_counts)})")

    return df, notes


def _collect_value_maps(table: str, fields: list[str]) -> dict[str, dict[str, str]]:
    """Collect value_map DDL Context entries for the given fields.

    Args:
        table: FM table name.
        fields: List of field names to check for value_map entries.

    Returns:
        Dict of {field: {old_val: new_val}} for fields that have mappings.
        Fields with no mapping or malformed JSON are silently skipped.
    """
    mappings: dict[str, dict[str, str]] = {}
    for field in fields:
        raw = get_context_value(table, "value_map", field)
        parsed = _parse_value_maps(raw)
        if parsed:
            mappings[field] = parsed
    return mappings


def _format_norm_note(notes: list[str]) -> str:
    """Format normalization notes as an inline appendix."""
    if not notes:
        return ""
    return f"\nNormalized: {', '.join(notes)}"


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
                "Expected 'function:field' (e.g., 'sum:Amount')."
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


_PERIOD_FREQS = {"week": "W", "month": "ME", "quarter": "QE"}


async def analyze(
    dataset: str,
    groupby: str = "",
    aggregate: str = "",
    filter: str = "",
    sort: str = "",
    limit: int = 50,
    period: str = "",
    pivot_column: str = "",
) -> str:
    """Run aggregation on a stored dataset. No FM round trip -- pure pandas.

    Args:
        dataset: Name of a previously loaded dataset, or a table name
            from the table cache (auto-populated by query_records).
        groupby: Comma-separated field names (e.g., "Technician,Region").
        aggregate: Comma-separated function:field pairs
            (e.g., "sum:Amount,count:Amount").
        filter: Pandas query expression to narrow data before aggregating (e.g., "Region == 'A'").
        sort: Sort result by column (e.g., "Amount_sum desc").
        limit: Max rows in output (default 50).
        period: Time-series resampling — "week", "month", or "quarter".
            Requires groupby to include a datetime column.
        pivot_column: Cross-tabulate by this column (creates a pivot table).
            Requires groupby for row index and aggregate for values.

    Returns:
        Formatted text table with results.
    """
    # Resolve dataset — named datasets take precedence, then table cache
    if dataset in _datasets:
        entry = _datasets[dataset]
    elif dataset in _table_cache:
        entry = _table_cache[dataset]
    else:
        available = ", ".join(list(_datasets.keys()) + list(_table_cache.keys())) or "(none)"
        return (
            f"Dataset '{dataset}' not found. Available: {available}. "
            "Use fm_load_dataset to load data, or query a cached table first."
        )

    df = entry.df.copy()

    # Apply pandas filter
    if filter:
        try:
            df = df.query(filter)
        except Exception as e:
            return f"Filter error: {e}"

    # --- Collect and apply value_map normalization ---
    norm_notes: list[str] = []
    if groupby:
        groupby_fields_for_norm = [f.strip() for f in groupby.split(",") if f.strip()]
        norm_fields = list(groupby_fields_for_norm)
        if pivot_column:
            norm_fields.append(pivot_column)
        field_mappings = _collect_value_maps(entry.table, norm_fields)
        if field_mappings:
            df, norm_notes = _apply_normalization(df, field_mappings)

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

    # --- Time-series mode ---
    if period:
        if period not in _PERIOD_FREQS:
            return f"Invalid period '{period}'. Supported: {', '.join(_PERIOD_FREQS.keys())}"
        freq = _PERIOD_FREQS[period]
        if not groupby_fields:
            return "Period requires a groupby field (the date column)."
        date_col = groupby_fields[0]
        if date_col not in df.columns or not pd.api.types.is_datetime64_any_dtype(df[date_col]):
            return f"Field '{date_col}' must be a datetime column for period grouping."

        agg_dict = _parse_aggregates(aggregate, df.columns.tolist())
        if isinstance(agg_dict, str):
            return agg_dict

        grouper: list[pd.Grouper | str] = [pd.Grouper(key=date_col, freq=freq)]
        if len(groupby_fields) > 1:
            grouper.extend(groupby_fields[1:])

        try:
            result_df = df.groupby(grouper).agg(agg_dict)
        except Exception as e:
            return f"Time-series aggregation error: {e}"

        result_df.columns = [f"{col}_{func}" for col, func in result_df.columns]
        result_df = result_df.reset_index()

        # Format date column for readability
        if date_col in result_df.columns:
            result_df[date_col] = result_df[date_col].dt.strftime("%Y-%m")

        if sort:
            parts = sort.strip().split()
            sort_col = parts[0]
            ascending = not (len(parts) > 1 and parts[1].lower() == "desc")
            if sort_col in result_df.columns:
                result_df = result_df.sort_values(sort_col, ascending=ascending)

        total_groups = len(result_df)
        result_df = result_df.head(limit)
        result_str = result_df.to_string(index=False)
        return (
            f"Time-series analysis of '{dataset}' ({len(df)} records, {period}ly):\n\n"
            f"{result_str}\n\n"
            f"({total_groups} periods)" + _format_norm_note(norm_notes)
        )

    # --- Pivot mode ---
    if pivot_column:
        if pivot_column not in df.columns:
            cols = ", ".join(df.columns.tolist())
            return f"Pivot column '{pivot_column}' not in dataset. Available: {cols}"
        if not groupby_fields:
            return "Pivot requires a groupby field for row index."
        agg_dict = _parse_aggregates(aggregate, df.columns.tolist())
        if isinstance(agg_dict, str):
            return agg_dict
        if not agg_dict:
            return "Pivot requires an aggregate (e.g., 'count:Amount')."

        # Use first agg field and function for pivot
        agg_field = next(iter(agg_dict.keys()))
        agg_func = agg_dict[agg_field][0]

        try:
            result_df = pd.pivot_table(
                df,
                index=groupby_fields,
                columns=pivot_column,
                values=agg_field,
                aggfunc=agg_func,
                fill_value=0,
            )
        except Exception as e:
            return f"Pivot error: {e}"

        result_df = result_df.reset_index()
        total_groups = len(result_df)
        result_df = result_df.head(limit)
        result_str = result_df.to_string(index=False)
        return (
            f"Pivot analysis of '{dataset}' ({len(df)} records):\n"
            f"Rows: {', '.join(groupby_fields)} | Columns: {pivot_column} "
            f"| Values: {agg_func}({agg_field})\n\n"
            f"{result_str}\n\n"
            f"({total_groups} rows)" + _format_norm_note(norm_notes)
        )

    # --- Standard groupby/aggregate ---
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
            f"({len(counts)} groups)" + _format_norm_note(norm_notes)
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

        # Flatten multi-level column names: (Amount, sum) -> Amount_sum
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
        + _format_norm_note(norm_notes)
    )
