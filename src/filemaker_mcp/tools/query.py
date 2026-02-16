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
from typing import Any

from filemaker_mcp.auth import odata_client

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
    def _us_to_iso(m: re.Match) -> str:
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

    Input:  "Customer Name eq 'Smith' and Date_of_Service ge 2026-02-14"
    Output: '"Customer Name" eq \'Smith\' and "Date_of_Service" ge 2026-02-14'
    """
    if not filter_str:
        return filter_str

    # Handle OData functions: contains(Field Name,'value') → contains("Field Name",'value')
    def _quote_func_field(m: re.Match) -> str:
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


# Tables available in Phase 1 (read-only)
# Auto-populated by bootstrap_ddl() at server startup.
# Tables are discovered from the FileMaker OData service document.
EXPOSED_TABLES: dict[str, str] = {}


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
            # Skip OData metadata fields
            if key.startswith("@odata"):
                continue
            formatted = _format_value(value)
            if formatted:  # Only show non-empty fields
                lines.append(f"  {key}: {formatted}")
        lines.append("")

    return "\n".join(lines)


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
        table: Table name. Available tables:
            - Location: Customer service locations (primary customer record)
            - Customers: Parent customer entities
            - InHomeInvoiceHeader: In-home service invoices/work orders
            - InHomeLineItem: Invoice line items (rooms, furniture, services)
            - Orders: Orders
            - Drivers IH: In-home service drivers
        filter: OData $filter expression.
            ALWAYS call get_schema(table) first — field names vary by table.
            Examples (use exact names from get_schema):
            - "City eq ''"
            - "Date_of_Service ge 2026-01-01"
            - "InvoiceTotal gt 500"
            - "Zone eq 'A' and Status eq 'Open'"
        select: Comma-separated field names to return. Leave empty for all fields.
            Example: "Customer Name,Phone,City,Email"
        top: Maximum records to return (default 20, max 100).
        skip: Number of records to skip (for pagination).
        orderby: OData $orderby expression.
            Example: "Date_of_Service desc" or "Customer Name asc"
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

    # Build OData query parameters
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
        return _format_records(data, table)

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
        table: Table name (see query_records for available tables).
        record_id: The primary key value to look up.
        id_field: The primary key field name. Defaults per table:
            - Location: "_kp_CustLoc"
            - Customers: "Customer_id"
            - InHomeInvoiceHeader: "PrimaryKey"
            - Orders: (uses default FM record ID)

    Returns:
        Formatted text with all fields for the matching record.
    """
    if table not in EXPOSED_TABLES:
        available = ", ".join(EXPOSED_TABLES.keys())
        return f"Error: Unknown table '{table}'. Available tables: {available}"

    # Default primary key fields per table
    default_id_fields: dict[str, str] = {}

    pk_field = id_field or default_id_fields.get(table, "PrimaryKey")

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
            if key.startswith("@odata"):
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
    params: dict[str, str] = {"$count": "true", "$top": "1", "$select": '"PrimaryKey"'}
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
    lines = ["Available FileMaker tables:", ""]
    for table, description in EXPOSED_TABLES.items():
        lines.append(f"  {table}: {description}")
    return "\n".join(lines)
