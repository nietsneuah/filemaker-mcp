"""Static DDL (Data Definition Layer) for FileMaker tables.

Generated from native FileMaker GetTableDDL() output, then manually reviewed.
Types are authoritative: varchar(255) -> text, int -> number, datetime -> datetime,
varbinary(4096) -> binary. PK/FK constraints come from the DDL PRIMARY KEY and
FOREIGN KEY declarations.

Tiers:
  key      — Essential for queries, joins, and business logic. Always shown.
  standard — Useful fields Claude may need. Shown by default.
  internal — FM system/calc/global fields. Hidden by default.

Tier heuristics applied:
  _kp_* -> key + pk (primary key)
  _kf_* -> key + fk (foreign key)
  _sp_* -> internal (speed/UI cache fields)
  g + uppercase / G_* -> internal (global fields)
  c prefix (unstored calcs) -> standard unless promoted
  s prefix (summaries) -> standard unless promoted
  x prefix (filter/exclude globals) -> standard
"""

from typing import TypedDict


class FieldDef(TypedDict, total=False):
    """Schema definition for a single FileMaker field."""

    type: str  # text, number, datetime, binary
    tier: str  # "key" | "standard" | "internal"
    pk: bool  # True if primary key
    fk: bool  # True if foreign key
    description: str  # Optional human-readable note


TableSchema = dict[str, FieldDef]

# Generated from native FileMaker GetTableDDL() output — all 8 tables, all fields.
# PK/FK constraints are authoritative from the DDL.
# Auto-populated by bootstrap_ddl() at server startup.
# Users can also define static schemas here as a fallback.
# See FieldDef TypedDict above for the schema format.
TABLES: dict[str, TableSchema] = {}

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
