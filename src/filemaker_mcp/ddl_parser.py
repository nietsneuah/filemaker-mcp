"""Parse FileMaker GetTableDDL() output into FieldDef dicts.

Converts raw SQL DDL (CREATE TABLE statements) into the structured
FieldDef format used by the schema tools. Handles FM-specific type
mapping and applies tier classification for fields.

Type mapping:
  varchar(255)     -> text
  int              -> number
  datetime         -> datetime
  varbinary(4096)  -> binary

Tier assignment priority (first match wins):
  1. Name: _kp_* / _kf_*           -> key (primary/foreign key)
  2. Annotation: Calculation=true   -> internal (from $metadata)
  3. Annotation: Summary=true       -> internal (from $metadata)
  4. Annotation: Global=true        -> internal (from $metadata)
  5. Name: _sp_*                    -> internal (speed/UI cache)
  6. Name: g+Upper / G_*           -> internal (global)
  7. Default                        -> standard

When annotations are unavailable, only name heuristics are used.
"""

import re
from typing import Any

from filemaker_mcp.ddl import FieldDef, TableSchema

# Regex patterns for DDL parsing
_CREATE_TABLE_RE = re.compile(
    r'CREATE\s+TABLE\s+"([^"]+)"\s*\((.*?)\);',
    re.DOTALL | re.IGNORECASE,
)
_FIELD_RE = re.compile(
    r'"([^"]+)"\s+(varchar\(\d+\)|int|datetime|varbinary\(\d+\))',
    re.IGNORECASE,
)
_PK_RE = re.compile(r"PRIMARY\s+KEY\s*\(([^)]+)\)", re.IGNORECASE)
_FK_RE = re.compile(r"FOREIGN\s+KEY\s*\(([^)]+)\)", re.IGNORECASE)

# FM SQL type -> our type system
_TYPE_MAP: dict[str, str] = {
    "varchar": "text",
    "int": "number",
    "datetime": "datetime",
    "varbinary": "binary",
}


def _map_type(sql_type: str) -> str:
    """Map FM SQL type to our simplified type system."""
    base = sql_type.split("(")[0].lower()
    return _TYPE_MAP.get(base, "text")


def _assign_tier(field_name: str, annotations: dict[str, Any] | None = None) -> str:
    """Apply tier classification: annotations first, then name heuristics.

    Args:
        field_name: The field name to classify.
        annotations: Optional FieldAnnotations dict for this specific field.
            If present, Calculation/Summary/Global override name heuristics.

    Returns:
        Tier string: "key", "internal", or "standard".
    """
    # Name-based key detection always wins (PK/FK fields are always key)
    if field_name.startswith("_kp_") or field_name.startswith("_kf_"):
        return "key"

    # Annotation-based classification (highest priority after key)
    if annotations and (
        annotations.get("calculation") or annotations.get("summary") or annotations.get("global_")
    ):
        return "internal"

    # Name-based heuristics (fallback)
    if field_name.startswith("_sp_"):
        return "internal"
    # g + uppercase letter = global (e.g., gGlobal, gDate)
    if len(field_name) > 1 and field_name[0] == "g" and field_name[1].isupper():
        return "internal"
    if field_name.startswith("G_"):
        return "internal"
    return "standard"


def parse_ddl(
    ddl_text: str,
    annotations: dict[str, dict[str, Any]] | None = None,
) -> dict[str, TableSchema]:
    """Parse DDL text into structured table/field definitions.

    Args:
        ddl_text: Raw DDL from GetTableDDL() — one or more CREATE TABLE statements.
        annotations: Optional per-table, per-field annotation dicts from $metadata.
            Structure: {table_name: {field_name: FieldAnnotations}}.
            Used for tier classification and description population.

    Returns:
        Dict mapping table_name -> {field_name: FieldDef}.
    """
    if not ddl_text.strip():
        return {}

    tables: dict[str, TableSchema] = {}

    for match in _CREATE_TABLE_RE.finditer(ddl_text):
        table_name = match.group(1)
        body = match.group(2)

        # Extract PRIMARY KEY fields
        pk_fields: set[str] = set()
        for pk_match in _PK_RE.finditer(body):
            for pk_name in pk_match.group(1).split(","):
                pk_fields.add(pk_name.strip().strip('"'))

        # Extract FOREIGN KEY fields
        fk_fields: set[str] = set()
        for fk_match in _FK_RE.finditer(body):
            for fk_name in fk_match.group(1).split(","):
                fk_fields.add(fk_name.strip().strip('"'))

        # Parse field definitions
        table_ann = (annotations or {}).get(table_name, {})
        fields: TableSchema = {}
        for field_match in _FIELD_RE.finditer(body):
            field_name = field_match.group(1)
            sql_type = field_match.group(2)

            field_ann = table_ann.get(field_name)

            field_def: FieldDef = {
                "type": _map_type(sql_type),
                "tier": _assign_tier(field_name, field_ann),
            }

            # Populate description from FMComment annotation
            if field_ann and field_ann.get("comment"):
                field_def["description"] = field_ann["comment"]

            # Apply PK/FK from constraints
            if field_name in pk_fields:
                field_def["pk"] = True
            if field_name in fk_fields:
                field_def["fk"] = True

            # _kp_ fields always get pk=True even without PK constraint
            if field_name.startswith("_kp_") and "pk" not in field_def:
                field_def["pk"] = True
            # _kf_ fields always get fk=True even without FK constraint
            if field_name.startswith("_kf_") and "fk" not in field_def:
                field_def["fk"] = True

            fields[field_name] = field_def

        tables[table_name] = fields

    return tables
