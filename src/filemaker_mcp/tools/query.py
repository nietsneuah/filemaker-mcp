"""Read-only query tools for FileMaker data via OData v4.

These tools are exposed to AI clients via MCP. The docstrings serve as
tool descriptions — Claude uses them to understand when/how to invoke each tool.

OData query reference:
  $filter   — WHERE clause (e.g., "City eq ''")
  $select   — Column list (e.g., "Customer Name,Phone,City")
  $top      — LIMIT (e.g., 20)
  $skip     — OFFSET for pagination
  $orderby  — ORDER BY (e.g., "Customer Name asc")
  $count    — Include total count in response

FM OData field name notes:
  - All field names are automatically double-quoted ("Field Name") before URL encoding
  - This is required for fields with spaces and harmless for others
  - Use exact field names from the schema (case-sensitive)
"""

import logging
import re
from datetime import date, datetime
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from filemaker_mcp.auth import odata_client
from filemaker_mcp.ddl import TABLES, get_cache_config, get_field_context, get_pk_field

logger = logging.getLogger(__name__)

# --- Date normalization for FM OData filters ---
# FM OData requires bare ISO dates: 2026-02-14 (no quotes, no timestamp).
# LLM clients may generate quoted dates, US format, or timestamps.

# ISO timestamp suffix: T00:00:00, T14:30:00Z, T14:30:00-05:00, etc.
_ISO_TIMESTAMP_RE = re.compile(r"(\d{4}-\d{2}-\d{2})T\d{2}:\d{2}:\d{2}[Z\d:.+\-]*")

# US date with optional time: M/D/YYYY or MM/DD/YYYY, optional HH:MM:SS AM/PM
_US_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})(?:\s+\d{1,2}:\d{2}:\d{2}\s*(?:AM|PM)?)?")


def normalize_dates_in_filter(filter_str: str) -> str:
    """Normalize date formats in an OData $filter string for FM compatibility.

    FM OData requires bare ISO dates (2026-02-14). This catches common
    wrong formats from LLM clients and FM JSON output.

    Args:
        filter_str: Raw OData $filter expression.

    Returns:
        Filter with dates normalized to bare ISO format.
    """
    if not filter_str:
        return filter_str

    original = filter_str

    # 1. Strip quotes around ISO dates: '2026-02-14' or "2026-02-14" -> 2026-02-14
    filter_str = re.sub(
        r"""['"](\d{4}-\d{2}-\d{2})(?:T[^'"]*)?['"]""",
        r"\1",
        filter_str,
    )

    # 2. Strip ISO timestamp suffixes: 2026-02-14T00:00:00Z -> 2026-02-14
    filter_str = _ISO_TIMESTAMP_RE.sub(r"\1", filter_str)

    # 3. Convert US dates: MM/DD/YYYY or M/D/YYYY (with optional time) -> YYYY-MM-DD
    def _us_to_iso(m: re.Match[str]) -> str:
        month, day, year = m.group(1), m.group(2), m.group(3)
        return f"{year}-{int(month):02d}-{int(day):02d}"

    filter_str = _US_DATE_RE.sub(_us_to_iso, filter_str)

    # 4. Strip quotes that may still surround converted ISO dates
    filter_str = re.sub(
        r"""['"](\d{4}-\d{2}-\d{2})['"]""",
        r"\1",
        filter_str,
    )

    if filter_str != original:
        logger.warning("Normalized dates in filter: %r → %r", original, filter_str)

    return filter_str


# --- Field name quoting for FM OData ---
# FM OData requires field names containing spaces to be wrapped in double quotes.
# We quote ALL field names unconditionally — FM accepts quoted names regardless of spaces.
# Table names in URL paths must NOT be quoted (FM rejects them).


def quote_fields_in_select(select: str) -> str:
    """Wrap each field name in a $select list with double quotes.

    Input:  "Customer Name,City,Zone"
    Output: '"Customer Name","City","Zone"'
    """
    if not select:
        return select
    fields = []
    for field in select.split(","):
        field = field.strip()
        if not field:
            continue
        if not field.startswith('"'):
            field = f'"{field}"'
        fields.append(field)
    return ",".join(fields)


def quote_fields_in_orderby(orderby: str) -> str:
    """Wrap field names in an $orderby expression with double quotes.

    Handles: "Customer Name asc", "City desc", "Field1 asc,Field2 desc"

    Input:  "Customer Name asc,City desc"
    Output: '"Customer Name" asc,"City" desc'
    """
    if not orderby:
        return orderby
    parts = []
    for clause in orderby.split(","):
        clause = clause.strip()
        if not clause:
            continue
        # Check for trailing asc/desc direction
        direction = ""
        for suffix in (" asc", " desc"):
            if clause.lower().endswith(suffix):
                direction = clause[-(len(suffix) - 1) :]  # preserve original case
                clause = clause[: -(len(suffix) - 1)].strip()
                direction = " " + direction.strip()
                break
        if not clause.startswith('"'):
            clause = f'"{clause}"'
        parts.append(f"{clause}{direction}")
    return ",".join(parts)


def quote_fields_in_filter(filter_str: str) -> str:
    """Wrap field names in an OData $filter expression with double quotes.

    Identifies field names by position (before comparison operators) and
    inside OData functions, then wraps them in double quotes. Leaves string
    literals, numbers, dates, operators, and already-quoted names untouched.

    Input:  "Customer Name eq 'Smith' and ServiceDate ge 2026-02-14"
    Output: '"Customer Name" eq \'Smith\' and "ServiceDate" ge 2026-02-14'
    """
    if not filter_str:
        return filter_str

    # Handle OData functions: contains(Field Name,'value') → contains("Field Name",'value')
    def _quote_func_field(m: re.Match[str]) -> str:
        func = m.group(1)
        field = m.group(2).strip()
        rest = m.group(3)
        if not field.startswith('"'):
            field = f'"{field}"'
        return f"{func}({field},{rest})"

    filter_str = re.sub(
        r"(contains|startswith|endswith)\(([^,]+),(.*?)\)",
        _quote_func_field,
        filter_str,
    )

    # Split on logical operators (and/or) while preserving them,
    # then process each comparison clause independently
    # Pattern: split on ' and ' or ' or ' (case-sensitive per OData spec)
    clauses = re.split(r"(\s+(?:and|or)\s+)", filter_str)

    result_parts = []
    for part in clauses:
        stripped = part.strip()
        if stripped in ("and", "or"):
            result_parts.append(part)
            continue

        # Check if this clause has an OData comparison operator
        op_match = re.match(
            r"^(.*?)\s+(eq|ne|gt|ge|lt|le)\s+(.*)$",
            part.strip(),
        )
        if op_match:
            field_name = op_match.group(1).strip()
            op = op_match.group(2)
            value = op_match.group(3).strip()

            if not field_name.startswith('"'):
                field_name = f'"{field_name}"'

            # Reconstruct with original spacing
            leading_space = part[: len(part) - len(part.lstrip())]
            result_parts.append(f"{leading_space}{field_name} {op} {value}")
        else:
            result_parts.append(part)

    return "".join(result_parts)


# --- Date range extraction for cache logic ---

_DATE_RANGE_RE = re.compile(r'"?(\w+)"?\s+(ge|gt|le|lt|eq)\s+(\d{4}-\d{2}-\d{2})')


def extract_date_range(filter_str: str, date_field: str) -> tuple[str | None, str | None]:
    """Extract date bounds for a specific field from an OData filter.

    Scans for ge/gt (lower bound) and le/lt (upper bound) comparisons
    on the named date field. Returns (min_date, max_date) as ISO strings,
    or None for each bound not found.

    Args:
        filter_str: OData $filter expression.
        date_field: The date field name to look for.

    Returns:
        Tuple of (min_date, max_date) as ISO date strings or None.
    """
    if not filter_str or not date_field:
        return (None, None)

    lower: str | None = None
    upper: str | None = None

    for match in _DATE_RANGE_RE.finditer(filter_str):
        field = match.group(1)
        op = match.group(2)
        val = match.group(3)

        if field != date_field:
            continue

        if op == "eq":
            lower = val
            upper = val
        elif op in ("ge", "gt"):
            lower = val
        elif op in ("le", "lt"):
            upper = val

    return (lower, upper)


# Dynamically populated by OData discovery during bootstrap.
# Keys are table names, values are description strings.
EXPOSED_TABLES: dict[str, str] = {}

# Stores error message if bootstrap failed, surfaced by list_tables().
_bootstrap_error: str | None = None


def set_bootstrap_error(error: str | None) -> None:
    """Store a bootstrap failure message for later surfacing."""
    global _bootstrap_error
    _bootstrap_error = error


def merge_discovered_tables(table_names: list[str]) -> None:
    """Add FM-discovered tables to EXPOSED_TABLES if not already present.

    Preserves existing curated descriptions. New tables get a generic
    description indicating they were auto-discovered from FM OData.

    Args:
        table_names: Table names discovered from the OData service document.
    """
    for name in table_names:
        if name not in EXPOSED_TABLES:
            EXPOSED_TABLES[name] = "Auto-discovered from FileMaker OData."


def clear_exposed_tables() -> None:
    """Clear all exposed table descriptions.

    Called during tenant switching to remove stale table list.
    """
    EXPOSED_TABLES.clear()


def _enrich_results(
    formatted: str,
    table: str,
    result_fields: list[str],
    cache_info: str = "",
) -> str:
    """Append DDL Context hints and cache info to formatted query results.

    Args:
        formatted: Already-formatted record text from _format_records.
        table: Table name for context lookup.
        result_fields: Field names present in the result set.
        cache_info: Optional cache notification string.

    Returns:
        Enriched text with context hints appended.
    """
    hints: list[str] = []
    for field in result_fields:
        ctx = get_field_context(table, field)
        if ctx:
            hints.append(f"  {field}: {ctx}")

    sections: list[str] = []
    if hints:
        sections.append("--- Context ---\n" + "\n".join(hints))
    if cache_info:
        sections.append("--- Cache ---\n  " + cache_info)

    if not sections:
        return formatted

    return formatted.rstrip() + "\n\n" + "\n\n".join(sections) + "\n"


def _format_value(value: Any) -> str:
    """Format a field value for display, handling FM quirks."""
    if value is None:
        return ""
    if isinstance(value, str) and len(value) > 500:
        return value[:500] + "... [truncated]"
    return str(value)


def _format_records(data: dict[str, Any], table: str) -> str:
    """Format OData response into readable text for the AI client.

    Returns a structured text representation that Claude can reason about.
    Limits output size to avoid overwhelming the context window.
    """
    records = data.get("value", [])
    # FM OData uses @count (not @odata.count per the OData v4 spec)
    count = data.get("@odata.count") or data.get("@count")

    if not records:
        if count is not None and count > 0:
            return f"Found {count} total records in {table} (none returned — check $top/$skip)."
        return f"No records found in {table} matching your query."

    lines = []

    # Header with count info
    if count is not None:
        lines.append(f"Found {count} total records in {table} (showing {len(records)}):")
    else:
        lines.append(f"Showing {len(records)} records from {table}:")

    lines.append("")

    # Format each record
    for i, record in enumerate(records, 1):
        lines.append(f"--- Record {i} ---")
        for key, value in record.items():
            # Skip OData metadata fields (@id, @editLink, @odata.*)
            if key.startswith("@"):
                continue
            formatted = _format_value(value)
            if formatted:  # Only show non-empty fields
                lines.append(f"  {key}: {formatted}")
        lines.append("")

    return "\n".join(lines)


# --- Non-date filter extraction for in-memory filtering ---

_NON_DATE_FILTER_RE = re.compile(
    r'"?(\w[\w\s]*\w|\w+)"?\s+(eq|ne|gt|ge|lt|le)\s+'
    r"(?:'([^']*)'|(\d+(?:\.\d+)?))"
)


def _extract_non_date_filters(filter_str: str, date_field: str) -> list[tuple[str, str, str]]:
    """Extract non-date comparison clauses from an OData filter.

    Returns list of (field_name, operator, value) tuples, excluding
    comparisons on the date field (those are handled by date range logic).
    """
    results = []
    for m in _NON_DATE_FILTER_RE.finditer(filter_str):
        field = m.group(1).strip()
        if field == date_field:
            continue
        op = m.group(2)
        value = m.group(3) if m.group(3) is not None else m.group(4)
        results.append((field, op, value))
    return results


def _apply_filters_to_df(
    df: pd.DataFrame,
    filter_str: str,
    date_field: str,
    req_min: str | None,
    req_max: str | None,
) -> pd.DataFrame:
    """Apply date range + non-date OData filters to a DataFrame.

    Args:
        df: Source DataFrame.
        filter_str: Original OData filter string.
        date_field: Date field name for date range filtering.
        req_min: Requested minimum date (ISO string or None).
        req_max: Requested maximum date (ISO string or None).

    Returns:
        Filtered DataFrame.
    """
    if not filter_str:
        return df

    # Apply date filters
    if req_min and date_field in df.columns:
        df = df[df[date_field] >= pd.Timestamp(req_min)]
    if req_max and date_field in df.columns:
        df = df[df[date_field] <= pd.Timestamp(req_max)]

    # Apply non-date OData filters
    non_date_parts = _extract_non_date_filters(filter_str, date_field)
    for field_name, op, value in non_date_parts:
        if field_name not in df.columns:
            continue
        if op == "eq":
            df = df[df[field_name].astype(str) == value]
        elif op == "ne":
            df = df[df[field_name].astype(str) != value]
        elif op in ("gt", "ge", "lt", "le"):
            try:
                num_val = float(value)
                numeric_col = pd.to_numeric(df[field_name], errors="coerce")
                if op == "gt":
                    df = df[numeric_col > num_val]
                elif op == "ge":
                    df = df[numeric_col >= num_val]
                elif op == "lt":
                    df = df[numeric_col < num_val]
                elif op == "le":
                    df = df[numeric_col <= num_val]
            except (ValueError, TypeError):
                pass  # Skip non-numeric comparisons
    return df


def _apply_orderby_to_df(df: pd.DataFrame, orderby: str) -> pd.DataFrame:
    """Apply OData $orderby to a DataFrame."""
    if not orderby:
        return df
    for clause in reversed(orderby.split(",")):
        clause = clause.strip()
        asc = True
        for suffix in (" asc", " desc"):
            if clause.lower().endswith(suffix):
                asc = suffix.strip() == "asc"
                clause = clause[: -(len(suffix) - 1)].strip()
                break
        clause = clause.strip('"')
        if clause in df.columns:
            df = df.sort_values(clause, ascending=asc)
    return df


def _apply_select_to_df(df: pd.DataFrame, select: str) -> pd.DataFrame:
    """Restrict DataFrame columns to those in a $select expression."""
    if not select:
        return df
    cols = [c.strip() for c in select.split(",")]
    cols = [c for c in cols if c in df.columns]
    return df[cols] if cols else df


async def _fetch_and_cache_gap(
    table: str,
    date_field: str,
    pk_field: str,
    gap_min: str | None,
    gap_max: str | None,
) -> bool:
    """Fetch a date range gap from FM and merge into table cache.

    Returns True on success, False on failure.
    """
    from filemaker_mcp.tools.analytics import merge_into_table_cache

    gap_filter_parts: list[str] = []
    if gap_min:
        gap_filter_parts.append(f'"{date_field}" ge {gap_min}')
    if gap_max:
        gap_filter_parts.append(f'"{date_field}" le {gap_max}')
    gap_filter = " and ".join(gap_filter_parts) if gap_filter_parts else ""

    gap_params: dict[str, str] = {"$top": "10000"}
    if gap_filter:
        gap_params["$filter"] = gap_filter

    try:
        all_records: list[dict[str, object]] = []
        skip_offset = 0
        while True:
            page_params = {**gap_params}
            if skip_offset > 0:
                page_params["$skip"] = str(skip_offset)
            data = await odata_client.get(table, params=page_params)
            records = data.get("value", [])
            all_records.extend(records)
            if len(records) < 10000:
                break
            skip_offset += 10000

        if all_records:
            gap_df = pd.DataFrame(all_records)
            meta_cols = [c for c in gap_df.columns if c.startswith("@")]
            if meta_cols:
                gap_df = gap_df.drop(columns=meta_cols)
            # Convert date columns using DDL type info
            table_ddl = TABLES.get(table, {})
            for fname, fdef in table_ddl.items():
                if fdef.get("type") in ("date", "datetime") and fname in gap_df.columns:
                    gap_df[fname] = pd.to_datetime(gap_df[fname], format="mixed", errors="coerce")
            merge_into_table_cache(
                table=table,
                new_df=gap_df,
                date_field=date_field,
                pk_field=pk_field,
                date_min=gap_min,
                date_max=gap_max,
            )
        return True
    except (ConnectionError, PermissionError, ValueError) as e:
        logger.warning("Cache fetch failed for %s: %s", table, e)
        return False


async def query_records(
    table: str,
    filter: str = "",
    select: str = "",
    top: int = 20,
    skip: int = 0,
    orderby: str = "",
    count: bool = True,
) -> str:
    """Query FileMaker records from a FileMaker table using OData v4.

    Use this tool to search, filter, and retrieve records from the FileMaker ERP system.
    This is the primary tool for answering questions about customers, invoices,
    orders, drivers, and service history.

    Args:
        table: Table name (use list_tables() to see available tables).
        filter: OData $filter expression.
            ALWAYS call get_schema(table) first — field names vary by table.
            Examples (use exact names from get_schema):
            - "City eq ''"
            - "ServiceDate ge 2026-01-01"
            - "Amount gt 500"
            - "Zone eq 'A' and Status eq 'Open'"
        select: Comma-separated field names to return. Leave empty for all fields.
            Example: "Customer Name,Phone,City,Email"
        top: Maximum records to return (default 20, max 100).
        skip: Number of records to skip (for pagination).
        orderby: OData $orderby expression.
            Example: "ServiceDate desc" or "Customer Name asc"
        count: Include total record count in response (default True).

    Returns:
        Formatted text with matching records and field values.
    """
    # Validate table name
    if table not in EXPOSED_TABLES:
        available = ", ".join(EXPOSED_TABLES.keys())
        return f"Error: Unknown table '{table}'. Available tables: {available}"

    # Cap results — FM OData supports up to 10,000 per request
    top = min(top, 10000)

    # --- Cache check ---
    from filemaker_mcp.tools.analytics import _table_cache, compute_date_gaps

    cache_config = get_cache_config(table)

    use_date_cache = False
    if cache_config and cache_config["mode"] == "date_range":
        date_field = cache_config["date_field"]
        pk_field = get_pk_field(table)
        normalized_filter = normalize_dates_in_filter(filter) if filter else ""
        req_min, req_max = extract_date_range(normalized_filter, date_field)
        existing = _table_cache.get(table)

        # Only use cache if filter references the date field OR we already have data cached.
        # Without this guard, a filter on a non-date field triggers an unbounded
        # full-table fetch (no $filter, no $select) which times out on large tables.
        if req_min is not None or req_max is not None or existing:
            use_date_cache = True
        else:
            logger.info(
                "Skipping date_range cache for %s — filter has no '%s' range",
                table,
                date_field,
            )

    if use_date_cache:
        if existing:
            gaps = compute_date_gaps(
                existing_min=existing.date_min.isoformat() if existing.date_min else None,
                existing_max=existing.date_max.isoformat() if existing.date_max else None,
                requested_min=req_min,
                requested_max=req_max,
            )
        else:
            gaps = [(req_min, req_max)]

        # Today-refresh: if the range touches today, always re-fetch today's
        # data so newly booked/cancelled jobs appear immediately.
        today_str = date.today().isoformat()
        touches_today = (
            (req_max is None)  # open-ended right bound
            or (req_max >= today_str)
        )
        if existing and touches_today:
            # Remove any gap that already covers today, replace with exact today
            gaps = [g for g in gaps if g != (today_str, today_str)]
            gaps.append((today_str, today_str))

        # Fetch any gaps from FM
        all_ok = True
        for gap_min, gap_max in gaps:
            ok = await _fetch_and_cache_gap(table, date_field, pk_field, gap_min, gap_max)
            if not ok:
                all_ok = False
                break

        # Serve from cache if we have data
        cached = _table_cache.get(table)
        if cached is not None and all_ok:
            result_df = _apply_filters_to_df(
                cached.df.copy(), normalized_filter, date_field, req_min, req_max
            )
            result_df = _apply_orderby_to_df(result_df, orderby)
            total_count = len(result_df)
            result_df = result_df.iloc[skip : skip + top]
            result_df = _apply_select_to_df(result_df, select)
            records_list = result_df.to_dict("records")
            data = {"value": records_list, "@count": total_count}
            field_names = (
                [k for k in records_list[0] if not str(k).startswith("@")] if records_list else []
            )
            formatted = _format_records(data, table)
            c_info = f"{cached.row_count} rows cached for {table}"
            if cached.date_min and cached.date_max:
                c_info += f" ({cached.date_min.isoformat()} → {cached.date_max.isoformat()})"
            c_info += ". Use fm_analyze for aggregation — no FM call needed."
            return _enrich_results(formatted, table, field_names, cache_info=c_info)

    if not use_date_cache and cache_config and cache_config["mode"] == "cache_all":
        pk_field = get_pk_field(table)
        if table not in _table_cache:
            try:
                all_records: list[dict[str, object]] = []
                skip_offset = 0
                while True:
                    p: dict[str, str] = {"$top": "10000"}
                    if skip_offset > 0:
                        p["$skip"] = str(skip_offset)
                    data = await odata_client.get(table, params=p)
                    records = data.get("value", [])
                    all_records.extend(records)
                    if len(records) < 10000:
                        break
                    skip_offset += 10000
                if all_records:
                    df = pd.DataFrame(all_records)
                    meta_cols = [c for c in df.columns if c.startswith("@")]
                    if meta_cols:
                        df = df.drop(columns=meta_cols)
                    from filemaker_mcp.tools.analytics import DatasetEntry

                    _table_cache[table] = DatasetEntry(
                        df=df,
                        table=table,
                        filter="",
                        select="",
                        loaded_at=datetime.now(),
                        row_count=len(df),
                        date_field="",
                        date_min=None,
                        date_max=None,
                        pk_field=pk_field,
                    )
            except (ConnectionError, PermissionError, ValueError):
                pass  # Fall through to normal query

        cached = _table_cache.get(table)
        if cached is not None:
            result_df = cached.df.copy()
            # Apply non-date filters (cache_all has no date field)
            non_date_parts = _extract_non_date_filters(filter, "") if filter else []
            for field_name, op, value in non_date_parts:
                if field_name in result_df.columns:
                    if op == "eq":
                        result_df = result_df[result_df[field_name].astype(str) == value]
                    elif op == "ne":
                        result_df = result_df[result_df[field_name].astype(str) != value]
            result_df = _apply_orderby_to_df(result_df, orderby)
            total_count = len(result_df)
            result_df = result_df.iloc[skip : skip + top]
            result_df = _apply_select_to_df(result_df, select)
            records_list = result_df.to_dict("records")
            data = {"value": records_list, "@count": total_count}
            field_names = (
                [k for k in records_list[0] if not str(k).startswith("@")] if records_list else []
            )
            formatted = _format_records(data, table)
            c_info = (
                f"{cached.row_count} rows cached for {table}. "
                "Use fm_analyze for aggregation — no FM call needed."
            )
            return _enrich_results(formatted, table, field_names, cache_info=c_info)

    # --- No cache / fallback: original FM query path ---
    params: dict[str, str] = {"$top": str(top)}

    if filter:
        params["$filter"] = quote_fields_in_filter(normalize_dates_in_filter(filter))
    if select:
        params["$select"] = quote_fields_in_select(select)
    if skip > 0:
        params["$skip"] = str(skip)
    if orderby:
        params["$orderby"] = quote_fields_in_orderby(orderby)
    if count:
        params["$count"] = "true"

    try:
        data = await odata_client.get(table, params=params)
        formatted = _format_records(data, table)
        records = data.get("value", [])
        field_names = [k for k in records[0] if not str(k).startswith("@")] if records else []
        return _enrich_results(formatted, table, field_names)

    except ConnectionError as e:
        return f"Connection error: {e}"
    except PermissionError as e:
        return f"Authentication error: {e}"
    except ValueError as e:
        error_msg = str(e)
        if any(kw in error_msg.lower() for kw in ["property", "field", "column", "not found"]):
            return (
                f"Query error: {error_msg}\n\n"
                f"TIP: This may be caused by incorrect field names. "
                f"Call fm_get_schema(table='{table}') to discover exact "
                f"field names — they vary by table (some use spaces, "
                f"some use underscores). The schema is the only source of truth."
            )
        return f"Query error: {error_msg}"
    except Exception as e:
        logger.exception("Unexpected error querying %s", table)
        return f"Error querying {table}: {type(e).__name__}: {e}"


async def get_record(table: str, record_id: str, id_field: str = "") -> str:
    """Get a single record from FileMaker by its primary key.

    Use this when you know the specific record ID and want full details.

    Args:
        table: Table name (use list_tables() to see available tables).
        record_id: The primary key value to look up.
        id_field: The primary key field name. Use get_schema(table) to find PKs.
            Defaults to "PrimaryKey" if not specified.

    Returns:
        Formatted text with all fields for the matching record.
    """
    if table not in EXPOSED_TABLES:
        available = ", ".join(EXPOSED_TABLES.keys())
        return f"Error: Unknown table '{table}'. Available tables: {available}"

    pk_field = id_field or "PrimaryKey"

    # Build filter for exact match
    # Try numeric first, fall back to string
    quoted_pk = f'"{pk_field}"'
    try:
        int(record_id)
        filter_expr = f"{quoted_pk} eq {record_id}"
    except ValueError:
        filter_expr = f"{quoted_pk} eq '{record_id}'"

    params = {"$filter": filter_expr, "$top": "1"}

    try:
        data = await odata_client.get(table, params=params)
        records = data.get("value", [])

        if not records:
            return f"No record found in {table} where {pk_field} = {record_id}"

        # Format single record with all fields
        record = records[0]
        lines = [f"Record from {table} ({pk_field} = {record_id}):", ""]
        for key, value in record.items():
            if key.startswith("@"):
                continue
            lines.append(f"  {key}: {_format_value(value)}")

        return "\n".join(lines)

    except Exception as e:
        logger.exception("Error getting record from %s", table)
        return f"Error: {type(e).__name__}: {e}"


async def count_records(table: str, filter: str = "") -> str:
    """Get the total record count for a table, optionally filtered.

    Quick way to check data volume or validate filter expressions
    before running a full query.

    Args:
        table: Table name (see query_records for available tables).
        filter: Optional OData $filter expression to count matching records.

    Returns:
        The record count as a text message.
    """
    if table not in EXPOSED_TABLES:
        available = ", ".join(EXPOSED_TABLES.keys())
        return f"Error: Unknown table '{table}'. Available tables: {available}"

    # FM OData returns 0 count when $top=0, so use $top=1 with $select
    # on a small field to minimize data transfer.
    pk = get_pk_field(table)
    params: dict[str, str] = {"$count": "true", "$top": "1", "$select": f'"{pk}"'}
    if filter:
        params["$filter"] = quote_fields_in_filter(normalize_dates_in_filter(filter))

    try:
        data = await odata_client.get(table, params=params)
        # FM OData uses @count (not @odata.count per OData v4 spec)
        count = data.get("@odata.count") or data.get("@count", "unknown")

        if filter:
            return f"{table}: {count} records matching filter '{filter}'"
        else:
            return f"{table}: {count} total records"

    except Exception as e:
        logger.exception("Error counting records in %s", table)
        return f"Error: {type(e).__name__}: {e}"


async def list_tables() -> str:
    """List all available FileMaker tables and their descriptions.

    Use this to understand what data is available before querying.

    Returns:
        List of table names with descriptions of what each contains.
    """
    if not EXPOSED_TABLES and _bootstrap_error:
        return (
            "No tables available. Connection failed during startup:\n\n"
            f"  {_bootstrap_error}\n\n"
            "Check your .env file: FM_HOST, FM_DATABASE, FM_USERNAME, FM_PASSWORD.\n"
            "If using a self-signed certificate, set FM_VERIFY_SSL=false."
        )

    # Separate tables with curated descriptions from auto-discovered ones
    curated = {t: d for t, d in EXPOSED_TABLES.items() if "Auto-discovered" not in d}
    discovered = sorted(t for t, d in EXPOSED_TABLES.items() if "Auto-discovered" in d)

    lines = [f"Available tables ({len(EXPOSED_TABLES)} total):", ""]

    if curated:
        for table, description in curated.items():
            lines.append(f"  {table}: {description}")
        lines.append("")

    if discovered:
        lines.append(f"  + {len(discovered)} discovered tables: {', '.join(discovered)}")

    return "\n".join(lines)
