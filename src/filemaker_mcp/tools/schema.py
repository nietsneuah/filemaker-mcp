"""Schema and metadata tools for FileMaker database discovery.

Default behavior: infer schema by querying one record per table (fast, ~100ms).
Fallback with refresh=True: fetch full $metadata XML from FM Server (slow, ~5MB).
"""

import asyncio
import logging
import re
import xml.etree.ElementTree as ET
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

from filemaker_mcp.auth import odata_client
from filemaker_mcp.ddl import TABLES, is_script_available, set_script_available, update_tables
from filemaker_mcp.ddl_parser import parse_ddl
from filemaker_mcp.tools.query import EXPOSED_TABLES

logger = logging.getLogger(__name__)

# Errors that indicate transient failures — worth retrying
_RETRYABLE_ERRORS = (ConnectionError, httpx.TimeoutException)


async def _retry_with_backoff(
    fn: Callable[[], Awaitable[Any]],
    *,
    max_retries: int = 3,
    base_delay: float = 1.0,
) -> Any | None:
    """Call an async function with exponential backoff on transient errors.

    Args:
        fn: Zero-arg async callable to invoke.
        max_retries: Maximum number of retries after initial attempt.
        base_delay: Base delay in seconds (doubled each retry).

    Returns:
        The return value of fn, or None if all retries exhausted.

    Raises:
        PermissionError: On 401 (not retryable).
        ValueError: On 404 (not retryable).
    """
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            return await fn()
        except _RETRYABLE_ERRORS as e:
            last_error = e
            if attempt < max_retries:
                delay = base_delay * (2**attempt)
                logger.warning(
                    "Retry %d/%d after %s (%.1fs delay): %s",
                    attempt + 1,
                    max_retries,
                    type(e).__name__,
                    delay,
                    e,
                )
                await asyncio.sleep(delay)
        except (PermissionError, ValueError):
            raise
    logger.error("All %d retries exhausted: %s", max_retries, last_error)
    return None


async def _discover_tables_from_odata() -> list[str]:
    """Discover available table names from the OData service document.

    Calls GET /fmi/odata/v4/{database}?$format=JSON and extracts table names
    from the service document response.

    Returns:
        List of table names, or empty list on failure.
    """
    try:
        data = await odata_client.get("", params={"$format": "JSON"})
        entries = data.get("value", [])
        return [entry["name"] for entry in entries if "name" in entry]
    except Exception:
        logger.exception("Failed to discover tables from OData service document")
        return []


# Per-table cache: table_name -> {field_name: type_str}
_schema_cache: dict[str, dict[str, str]] = {}

# Full $metadata XML cache (only populated when refresh=True)
_metadata_cache: str | None = None

# Patterns for date/datetime detection in string values
_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _infer_field_type(value: Any) -> str:
    """Infer a simplified field type from a JSON value.

    Args:
        value: A value from a FileMaker OData JSON response.

    Returns:
        Simplified type name: text, number, decimal, boolean, datetime, date, or unknown.
    """
    if value is None:
        return "unknown"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "number"
    if isinstance(value, float):
        return "decimal"
    if isinstance(value, str):
        if _DATETIME_RE.match(value):
            return "datetime"
        if _DATE_RE.match(value):
            return "date"
        return "text"
    return "unknown"


def _format_ddl_schema(table: str, fields: dict, show_all: bool = False) -> str:
    """Format DDL fields into readable schema text.

    Args:
        table: Table name.
        fields: DDL field definitions from TABLES.
        show_all: If False, hide internal-tier fields.

    Returns:
        Formatted schema text with tier markers.
    """
    total = len(fields)
    internal_count = sum(1 for f in fields.values() if f.get("tier") == "internal")
    hidden = 0 if show_all else internal_count

    lines: list[str] = []
    header = f"Table: {table} ({total} fields"
    if hidden > 0:
        header += f", {hidden} internal hidden"
    header += ")"
    lines.append(header)
    lines.append("-" * len(header))

    for field_name, field_def in fields.items():
        tier = field_def.get("tier", "standard")

        if not show_all and tier == "internal":
            continue

        markers: list[str] = []
        if field_def.get("pk"):
            markers.append("PK")
        if field_def.get("fk"):
            markers.append("FK")
        if tier == "key":
            markers.append("key")
        if tier == "internal":
            markers.append("internal")

        field_type = field_def.get("type", "unknown")
        marker_str = f" [{', '.join(markers)}]" if markers else ""
        date_hint = (
            "  (filter as: YYYY-MM-DD, no quotes)" if field_type in ("datetime", "date") else ""
        )
        lines.append(f"  {field_name}: {field_type}{marker_str}{date_hint}")

    lines.append("")
    lines.append(f"{total} fields total")
    if hidden > 0:
        lines.append(
            f"Tip: Use get_schema(table='{table}', show_all=True) to see all {total} fields."
        )

    return "\n".join(lines)


def _format_inferred_schema(table: str, field_types: dict[str, str]) -> str:
    """Format an inferred schema into readable text.

    Args:
        table: Table name.
        field_types: Mapping of field_name -> type_str.

    Returns:
        Formatted text matching the style of _parse_metadata_xml output.
    """
    lines: list[str] = []
    lines.append(f"Table: {table}")
    lines.append("-" * (len(table) + 7))

    null_count = 0
    for field_name, field_type in field_types.items():
        markers: list[str] = []
        if field_name.startswith("_kp_"):
            markers.append("PK")
        elif field_name.startswith("_kf_"):
            markers.append("FK")

        marker_str = f" [{', '.join(markers)}]" if markers else ""
        date_hint = (
            "  (filter as: YYYY-MM-DD, no quotes)" if field_type in ("datetime", "date") else ""
        )
        lines.append(f"  {field_name}: {field_type}{marker_str}{date_hint}")

        if field_type == "unknown":
            null_count += 1

    lines.append("")
    lines.append(f"{len(field_types)} fields total")
    if null_count > 0:
        lines.append(f"Note: {null_count} fields were null in sample — types shown as 'unknown'.")

    return "\n".join(lines)


async def _infer_table_schema(table: str) -> dict[str, str]:
    """Infer table schema by querying one record via OData.

    Args:
        table: Table name to query.

    Returns:
        Dict mapping field_name -> inferred type string.

    Raises:
        ValueError: If the table returns no records.
    """
    data = await odata_client.get(table, params={"$top": "1"})
    records = data.get("value", [])

    if not records:
        raise ValueError(f"Table '{table}' returned no records for schema inference.")

    record = records[0]
    field_types: dict[str, str] = {}
    for key, value in record.items():
        # Skip OData metadata fields
        if key.startswith("@"):
            continue
        field_types[key] = _infer_field_type(value)

    return field_types


async def _get_schema_from_metadata(table_filter: str = "") -> str:
    """Fetch schema from $metadata XML endpoint (slow, authoritative).

    This is the original implementation, kept as a fallback for refresh=True.

    Args:
        table_filter: If provided, only show fields for this table.

    Returns:
        Formatted text listing tables and their fields with types.
    """
    global _metadata_cache
    data = await odata_client.get("$metadata")
    xml_text = data.get("metadata_xml", "")
    if xml_text:
        _metadata_cache = xml_text

    if not xml_text:
        return "Error: Empty metadata response from FileMaker Server."

    return _parse_metadata_xml(xml_text, table_filter=table_filter)


def _parse_metadata_xml(xml_text: str, table_filter: str = "") -> str:
    """Parse OData $metadata XML into readable field listing.

    Args:
        xml_text: Raw XML from $metadata endpoint
        table_filter: If provided, only show fields for this table

    Returns:
        Formatted text listing tables and their fields with types
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        return f"Error parsing metadata XML: {e}"

    # OData metadata uses edmx namespace
    namespaces = {
        "edmx": "http://docs.oasis-open.org/odata/ns/edmx",
        "edm": "http://docs.oasis-open.org/odata/ns/edm",
    }

    lines: list[str] = []
    entity_types = root.findall(".//edm:EntityType", namespaces)

    if not entity_types:
        # Try without namespace prefix (some FM versions differ)
        entity_types = root.findall(".//{http://docs.oasis-open.org/odata/ns/edm}EntityType")

    for entity in entity_types:
        table_name = entity.get("Name", "Unknown")

        # Apply table filter if specified
        if table_filter and table_name.lower() != table_filter.lower():
            continue

        lines.append(f"Table: {table_name}")
        lines.append("-" * (len(table_name) + 7))

        # Get key fields
        key_elem = entity.find("edm:Key", namespaces)
        if key_elem is None:
            key_elem = entity.find("{http://docs.oasis-open.org/odata/ns/edm}Key")

        key_fields = set()
        if key_elem is not None:
            for prop_ref in key_elem:
                key_name = prop_ref.get("Name", "")
                if key_name:
                    key_fields.add(key_name)

        # List properties
        properties = entity.findall("edm:Property", namespaces)
        if not properties:
            properties = entity.findall("{http://docs.oasis-open.org/odata/ns/edm}Property")

        for prop in properties:
            field_name = prop.get("Name", "Unknown")
            field_type = prop.get("Type", "Unknown")
            nullable = prop.get("Nullable", "true")

            # Simplify Edm types for readability
            type_map = {
                "Edm.String": "text",
                "Edm.Int32": "number",
                "Edm.Int64": "number",
                "Edm.Decimal": "decimal",
                "Edm.Double": "decimal",
                "Edm.Boolean": "boolean",
                "Edm.DateTimeOffset": "datetime",
                "Edm.Date": "date",
                "Edm.Binary": "binary/container",
                "Edm.Stream": "binary/container",
            }
            simple_type = type_map.get(field_type, field_type)

            # Build field description
            markers = []
            if field_name in key_fields:
                markers.append("PK")
            if nullable == "false":
                markers.append("required")

            # Check for description annotation
            annotations = prop.findall("edm:Annotation", namespaces)
            if not annotations:
                annotations = prop.findall("{http://docs.oasis-open.org/odata/ns/edm}Annotation")
            description = ""
            for ann in annotations:
                if "Description" in (ann.get("Term", "")):
                    description = ann.get("String", "")

            marker_str = f" [{', '.join(markers)}]" if markers else ""
            desc_str = f"  -- {description}" if description else ""
            lines.append(f"  {field_name}: {simple_type}{marker_str}{desc_str}")

        lines.append("")

    if not lines:
        if table_filter:
            return f"No table named '{table_filter}' found in metadata."
        return "No tables found in metadata response."

    return "\n".join(lines)


_DDL_SCRIPT_NAME = "SCR_DDL_GetTableDDL"


async def _refresh_ddl_via_script(table_names: list[str]) -> bool:
    """Attempt to refresh DDL by calling the FM GetTableDDL script via OData.

    Args:
        table_names: List of table names to fetch DDL for.

    Returns:
        True if script succeeded and TABLES was updated, False if script
        is unavailable and caller should fall back to $metadata.
    """
    # Skip if we already know the script isn't available
    if is_script_available() is False:
        return False

    try:
        param = str(table_names).replace("'", '"')
        result = await odata_client.post(
            f"Script.{_DDL_SCRIPT_NAME}",
            json_body={"scriptParameterValue": param},
        )

        # Extract DDL text from script result
        # FM OData returns: {"scriptResult": {"code": 0, "resultParameter": "CREATE TABLE..."}}
        ddl_text = ""
        if isinstance(result, dict):
            script_result = result.get("scriptResult", "")
            if isinstance(script_result, dict):
                ddl_text = script_result.get("resultParameter", "")
            elif isinstance(script_result, str):
                ddl_text = script_result
            if not ddl_text:
                ddl_text = result.get("value", "")

        if not ddl_text or not isinstance(ddl_text, str):
            logger.warning("DDL script returned empty result")
            return False

        # Check for error JSON from the script
        if ddl_text.strip().startswith("{"):
            logger.warning("DDL script returned error: %s", ddl_text[:200])
            return False

        # Parse and update cache
        parsed = parse_ddl(ddl_text)
        if parsed:
            update_tables(parsed)
            set_script_available(True)
            logger.info(
                "Refreshed DDL via script: %d tables, %d fields",
                len(parsed),
                sum(len(f) for f in parsed.values()),
            )
            return True

        logger.warning("DDL script returned unparseable result")
        return False

    except ValueError as e:
        if "not found" in str(e).lower():
            logger.info("DDL script not available on this tenant, will use $metadata fallback")
            set_script_available(False)
        else:
            logger.warning("DDL script error: %s", e)
        return False
    except (ConnectionError, PermissionError):
        raise  # Let these propagate
    except Exception:
        logger.exception("Unexpected error calling DDL script")
        return False


async def bootstrap_ddl() -> None:
    """Bootstrap DDL for all FM-visible tables on server startup.

    Three-step sequence:
    1. Probe: Check if the DDL script exists on FM.
    2. Discover: Get table names from OData service document.
    3. Fetch: Call DDL script with discovered tables + exponential backoff.

    If any step fails, the system degrades gracefully to static DDL.
    This is called from the server lifespan hook — before accepting connections.
    """
    # Step 1: Probe — does the DDL script exist?
    if is_script_available() is False:
        logger.info("DDL bootstrap: script previously marked unavailable, skipping")
        return

    try:
        await odata_client.post(
            f"Script.{_DDL_SCRIPT_NAME}",
            json_body={"scriptParameterValue": "[]"},
        )
        set_script_available(True)
        logger.info("DDL bootstrap step 1: script probe succeeded")
    except ValueError as e:
        if "not found" in str(e).lower():
            logger.info("DDL bootstrap step 1: script not found, using static DDL")
            set_script_available(False)
            return
        # Other ValueError — script exists but errored, continue
        set_script_available(True)
        logger.warning("DDL bootstrap step 1: probe returned error, continuing: %s", e)
    except PermissionError:
        logger.error("DDL bootstrap step 1: authentication failed, skipping bootstrap")
        return
    except ConnectionError:
        logger.warning("DDL bootstrap step 1: FM unreachable, using static DDL")
        return

    # Step 2: Discover tables from OData service document
    discovered = await _retry_with_backoff(
        _discover_tables_from_odata,
        max_retries=3,
        base_delay=1.0,
    )
    if discovered:
        table_names = discovered
        logger.info("DDL bootstrap step 2: discovered %d tables from FM", len(table_names))
        # Merge discovered tables into EXPOSED_TABLES
        from filemaker_mcp.tools.query import merge_discovered_tables

        merge_discovered_tables(table_names)
    else:
        # Fallback to hardcoded EXPOSED_TABLES
        table_names = list(EXPOSED_TABLES.keys())
        logger.warning(
            "DDL bootstrap step 2: discovery failed, falling back to %d hardcoded tables",
            len(table_names),
        )

    # Step 3: Fetch DDL for all tables via script
    async def _fetch_ddl() -> bool:
        return await _refresh_ddl_via_script(table_names)

    result = await _retry_with_backoff(_fetch_ddl, max_retries=3, base_delay=1.0)

    if result:
        missing = [t for t in table_names if t not in TABLES]
        if missing:
            logger.warning("DDL bootstrap step 3: missing tables after refresh: %s", missing)
        else:
            logger.info("DDL bootstrap complete: all %d tables populated", len(table_names))
    else:
        logger.warning(
            "DDL bootstrap step 3: DDL fetch failed after retries, "
            "using static DDL fallback (%d of %d tables have static DDL)",
            sum(1 for t in table_names if t in TABLES),
            len(table_names),
        )


async def get_schema(table: str = "", refresh: bool = False, show_all: bool = False) -> str:
    """Get the database schema (tables and fields) from FileMaker.

    Uses static DDL by default (instant, no API call). On cache miss or
    refresh=True, tries live DDL refresh via FM script, falls back to
    $metadata XML parsing.

    Args:
        table: Optional table name to get fields for just that table.
            Leave empty to list all available tables.
        refresh: Force re-fetch from live FM server.
        show_all: Show all fields including internal/system fields.
            Default hides internal fields to keep output concise.

    Returns:
        Formatted listing of tables with field names, types, and annotations.
    """
    try:
        # Specific table requested
        if table:
            # Cache hit (no refresh forced)
            if not refresh and table in TABLES and TABLES[table]:
                return _format_ddl_schema(table, TABLES[table], show_all=show_all)

            # Cache miss or refresh — try live DDL
            script_ok = await _refresh_ddl_via_script([table])

            # If script worked and table is now cached, return it
            if script_ok and table in TABLES and TABLES[table]:
                return _format_ddl_schema(table, TABLES[table], show_all=show_all)

            # Fallback: $metadata
            return await _get_schema_from_metadata(table)

        # No table specified — list available tables
        lines = [
            "Available FileMaker tables (use get_schema(table='X') for field details):",
            "",
        ]
        for tbl, desc in EXPOSED_TABLES.items():
            lines.append(f"  {tbl}: {desc}")
        lines.append("")
        lines.append("Tip: Call get_schema(table='Location') to see all fields for a table.")
        return "\n".join(lines)

    except ConnectionError as e:
        return f"Connection error: {e}"
    except PermissionError as e:
        return f"Authentication error: {e}"
    except Exception as e:
        logger.exception("Error fetching schema")
        return f"Error fetching schema: {type(e).__name__}: {e}"
