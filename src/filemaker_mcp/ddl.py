"""DDL (Data Definition Layer) cache for FileMaker tables.

Populated dynamically from the SCR_DDL_GetTableDDL script at bootstrap.
Falls back to $metadata if the script isn't available.

Types: varchar(255) -> text, int -> number, datetime -> datetime,
varbinary(4096) -> binary. PK/FK from DDL PRIMARY KEY / FOREIGN KEY.

Tiers:
  key      — Essential for queries, joins, and business logic. Always shown.
  standard — Useful fields Claude may need. Shown by default.
  internal — FM system/calc/global fields. Hidden by default.

Tier assignment priority (first match wins):
  1. Name: _kp_* / _kf_*           → key (primary/foreign key)
  2. Annotation: Calculation=true   → internal (from $metadata)
  3. Annotation: Summary=true       → internal (from $metadata)
  4. Annotation: Global=true        → internal (from $metadata)
  5. Name: _sp_*                    → internal (speed/UI cache)
  6. Name: g+Upper / G_*           → internal (global fields)
  7. Default                        → standard

Annotations come from FM OData $metadata (com.filemaker.odata.* terms).
When unavailable, name heuristics alone are used (graceful degradation).
"""

from typing import TypedDict


class FieldDef(TypedDict, total=False):
    """Schema definition for a single FileMaker field."""

    type: str  # text, number, datetime, binary
    tier: str  # "key" | "standard" | "internal"
    pk: bool  # True if primary key
    fk: bool  # True if foreign key
    description: str  # Optional human-readable note


class FieldAnnotations(TypedDict, total=False):
    """OData $metadata annotations for a FileMaker field."""

    calculation: bool  # Field is a calculation
    summary: bool  # Field is a summary
    global_: bool  # Field contains a global value
    comment: str  # FMComment text


TableSchema = dict[str, FieldDef]

# Dynamically populated by DDL script or $metadata during bootstrap.
TABLES: dict[str, TableSchema] = {}

# Populated from $metadata during bootstrap.
# Structure: {table_name: {field_name: FieldAnnotations}}
FIELD_ANNOTATIONS: dict[str, dict[str, FieldAnnotations]] = {}

# FM table name for operational context records
CONTEXT_TABLE = "TBL_DDL_Context"

# Operational context loaded from TBL_DDL_Context at bootstrap.
# Key: (TableName, FieldName, ContextType) — triple key avoids collisions
#   when multiple context types exist for the same table+field.
# Value: {"context": str}
DDL_CONTEXT: dict[tuple[str, str, str], dict[str, str]] = {}

# --- Runtime cache management ---

# None = not checked yet, True = available, False = unavailable (404)
_script_available: bool | None = None


def update_tables(new_tables: dict[str, TableSchema]) -> None:
    """Update the in-memory TABLES dict with new/refreshed table definitions.

    Args:
        new_tables: Dict mapping table_name -> field definitions.
            Overwrites existing entries for the same table name.
    """
    TABLES.update(new_tables)


def is_script_available() -> bool | None:
    """Check if SCR_DDL_GetTableDDL is available on this tenant.

    Returns:
        None if not yet checked, True if available, False if 404'd.
    """
    return _script_available


def set_script_available(available: bool | None) -> None:
    """Cache whether the DDL script is available on this tenant."""
    global _script_available
    _script_available = available


def clear_tables() -> None:
    """Clear all cached DDL tables, annotations, context, and reset script availability.

    Called during tenant switching to remove stale schema data.
    """
    global _script_available
    TABLES.clear()
    FIELD_ANNOTATIONS.clear()
    DDL_CONTEXT.clear()
    _script_available = None


def update_annotations(new_annotations: dict[str, dict[str, FieldAnnotations]]) -> None:
    """Update the in-memory FIELD_ANNOTATIONS dict."""
    FIELD_ANNOTATIONS.update(new_annotations)


def clear_annotations() -> None:
    """Clear all cached field annotations. Called during tenant switching."""
    FIELD_ANNOTATIONS.clear()


def update_context(records: list[dict[str, str]]) -> None:
    """Update DDL_CONTEXT from raw OData records.

    Args:
        records: List of dicts with keys: TableName, FieldName, ContextType, Context.
    """
    for rec in records:
        key = (
            rec.get("TableName", ""),
            rec.get("FieldName", ""),
            rec.get("ContextType", ""),
        )
        DDL_CONTEXT[key] = {
            "context": rec.get("Context", ""),
        }


def clear_context() -> None:
    """Clear all cached DDL context. Called during tenant switching."""
    DDL_CONTEXT.clear()


def remove_context(table: str, field: str, context_type: str = "") -> bool:
    """Remove context entries from the local cache.

    If context_type is specified, removes only that specific entry.
    If context_type is empty, removes ALL entries for table+field.

    Returns:
        True if any entries were removed, False if none found.
    """
    if context_type:
        return DDL_CONTEXT.pop((table, field, context_type), None) is not None
    # Remove all context_types for this table+field
    keys = [k for k in DDL_CONTEXT if k[0] == table and k[1] == field]
    for k in keys:
        del DDL_CONTEXT[k]
    return len(keys) > 0


def get_field_context(table: str, field: str) -> str | None:
    """Get context hint for a specific field, or None.

    If multiple context types exist for the same field, joins them.
    """
    hints = [v["context"] for k, v in DDL_CONTEXT.items() if k[0] == table and k[1] == field]
    return "; ".join(hints) if hints else None


def get_table_context(table: str) -> list[dict[str, str]]:
    """Get all context entries for a table (field-level and table-level)."""
    return [
        {"field": k[1], "context_type": k[2], **v} for k, v in DDL_CONTEXT.items() if k[0] == table
    ]


def get_context_value(table: str, context_type: str, field: str = "") -> str | None:
    """Look up a single DDL Context value by table and context type.

    Args:
        table: FM table name.
        context_type: Context category (e.g., "report_select", "cache_config").
        field: Field name. Empty string for table-level context.

    Returns:
        The context string, or None if no matching entry exists.
    """
    key = (table, field, context_type)
    entry = DDL_CONTEXT.get(key)
    return entry["context"] if entry else None


def get_date_fields(table: str) -> list[str]:
    """Return field names with type 'datetime' or 'date' for a table.

    Reads from TABLES (populated at bootstrap). Returns empty list
    if table not found or has no date fields.
    """
    schema = TABLES.get(table, {})
    return [
        name for name, field_def in schema.items() if field_def.get("type") in ("datetime", "date")
    ]


def get_all_date_fields() -> dict[str, list[str]]:
    """Return all tables that have datetime/date fields.

    Returns {table_name: [field_name, ...]} for tables with at least
    one date field. Only includes tables in TABLES (populated at bootstrap).
    """
    result = {}
    for table_name, schema in TABLES.items():
        date_fields = [
            name
            for name, field_def in schema.items()
            if field_def.get("type") in ("datetime", "date")
        ]
        if date_fields:
            result[table_name] = date_fields
    return result


def get_cache_config(table: str) -> dict[str, str] | None:
    """Get caching configuration for a table from DDL_CONTEXT.

    Looks for a cache_config entry. Returns:
    - {"mode": "date_range", "date_field": "FieldName"} if context is "date_key"
    - {"mode": "cache_all", "date_field": ""} if context is "cache_all"
    - None if no cache_config entry exists

    Cache config is stored in TBL_DDL_Context with ContextType='cache_config'.
    """
    for key, value in DDL_CONTEXT.items():
        if key[0] == table and key[2] == "cache_config":
            ctx = value.get("context", "")
            if ctx == "date_key":
                return {"mode": "date_range", "date_field": key[1]}
            elif ctx == "cache_all":
                return {"mode": "cache_all", "date_field": ""}
    return None


def get_pk_field(table: str) -> str:
    """Get the primary key field name for a table from DDL.

    Scans TABLES for a field with pk=True. Falls back to "PrimaryKey"
    if no PK is marked (the most common FM PK field name).

    Args:
        table: FM table name.

    Returns:
        Primary key field name string.
    """
    table_ddl = TABLES.get(table, {})
    for field_name, field_def in table_ddl.items():
        if field_def.get("pk"):
            return field_name
    return "PrimaryKey"
