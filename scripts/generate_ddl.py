"""Generate DDL from live FileMaker OData data.

Queries $top=1 per table, infers types, assigns tiers, and outputs
Python source code for src/filemaker_mcp/ddl.py TABLES dict.

Usage:
    uv run python scripts/generate_ddl.py > /tmp/ddl_output.py

Then review the output and paste into src/filemaker_mcp/ddl.py.
"""

import asyncio
import re
import sys
from typing import Any

import httpx

from filemaker_mcp.config import settings

# Add your table names here, or leave empty to use all
# tables discovered from the OData service document.
EXPOSED_TABLES: list[str] = []

DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def infer_type(value: Any) -> str:
    """Infer field type from a JSON value."""
    if value is None:
        return "unknown"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "number"
    if isinstance(value, float):
        return "decimal"
    if isinstance(value, str):
        if DATETIME_RE.match(value):
            return "datetime"
        if DATE_RE.match(value):
            return "date"
        return "text"
    return "unknown"


def assign_tier(field_name: str) -> tuple[str, dict[str, bool]]:
    """Assign tier and key markers based on field name patterns."""
    extras: dict[str, bool] = {}

    if field_name.startswith("_kp_"):
        extras["pk"] = True
        return "key", extras
    if field_name.startswith("_kf_"):
        extras["fk"] = True
        return "key", extras
    if field_name.startswith(("g_", "G_", "_sp_")):
        return "internal", extras
    if len(field_name) > 1 and field_name[0] == "g" and field_name[1].isupper():
        return "internal", extras

    return "standard", extras


async def fetch_table_schema(table: str) -> dict[str, dict[str, Any]]:
    """Fetch one record from table and build field definitions."""
    async with httpx.AsyncClient(
        base_url=settings.odata_base_url,
        auth=settings.basic_auth,
        verify=settings.fm_verify_ssl,
        timeout=120,
        headers={"Accept": "application/json"},
    ) as client:
        response = await client.get(f"/{table}", params={"$top": "1"})
        response.raise_for_status()
        data = response.json()

    records = data.get("value", [])
    if not records:
        print(f"WARNING: {table} returned no records", file=sys.stderr)
        return {}

    record = records[0]
    fields: dict[str, dict[str, Any]] = {}

    for field_name, value in record.items():
        if field_name.startswith("@"):
            continue
        field_type = infer_type(value)
        tier, extras = assign_tier(field_name)
        field_def: dict[str, Any] = {"type": field_type, "tier": tier}
        field_def.update(extras)
        fields[field_name] = field_def

    return fields


def format_field_def(field_def: dict[str, Any]) -> str:
    """Format a FieldDef dict as Python source code."""
    parts = [f'"type": "{field_def["type"]}"', f'"tier": "{field_def["tier"]}"']
    if field_def.get("pk"):
        parts.append('"pk": True')
    if field_def.get("fk"):
        parts.append('"fk": True')
    if field_def.get("description"):
        parts.append(f'"description": "{field_def["description"]}"')
    return "{" + ", ".join(parts) + "}"


async def main() -> None:
    """Generate DDL output for all tables."""
    print("TABLES: dict[str, TableSchema] = {")

    for table in EXPOSED_TABLES:
        print(f"    # --- {table} ---", file=sys.stderr)
        try:
            fields = await fetch_table_schema(table)
        except Exception as e:
            print(f"ERROR: {table}: {e}", file=sys.stderr)
            continue

        print(f'    "{table}": {{')
        for field_name, field_def in fields.items():
            escaped_name = field_name.replace('"', '\\"')
            print(f'        "{escaped_name}": {format_field_def(field_def)},')
        print("    },")

        tier_counts: dict[str, int] = {}
        for fd in fields.values():
            t = fd["tier"]
            tier_counts[t] = tier_counts.get(t, 0) + 1
        print(
            f"    # {table}: {len(fields)} fields — "
            + ", ".join(f"{v} {k}" for k, v in sorted(tier_counts.items())),
            file=sys.stderr,
        )

    print("}")


if __name__ == "__main__":
    asyncio.run(main())
