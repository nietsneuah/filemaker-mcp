"""Context persistence — write operational learnings to FM.

Provides save_context() which writes field-value hints, syntax rules,
and query patterns to TBL_DDL_Context in FileMaker. Records are loaded
at next bootstrap to improve future query efficiency.
"""

import logging

from filemaker_mcp.auth import odata_client
from filemaker_mcp.ddl import CONTEXT_TABLE, remove_context, update_context

logger = logging.getLogger(__name__)


def _odata_escape(value: str) -> str:
    """Escape single quotes for OData string literals."""
    return value.replace("'", "''")


def _build_context_filter(table_name: str, field_name: str, context_type: str) -> str:
    """Build an OData $filter for context dedup lookup.

    FM OData rejects ``eq ''`` for empty strings, so we use
    ``length(Field) eq 0`` instead.
    """
    parts: list[str] = []
    if table_name:
        parts.append(f"TableName eq '{_odata_escape(table_name)}'")
    else:
        parts.append("length(TableName) eq 0")
    if field_name:
        parts.append(f"FieldName eq '{_odata_escape(field_name)}'")
    else:
        parts.append("length(FieldName) eq 0")
    parts.append(f"ContextType eq '{_odata_escape(context_type)}'")
    return " and ".join(parts)


async def save_context(
    table_name: str,
    context: str,
    field_name: str = "",
    context_type: str = "field_values",
    source: str = "auto",
) -> str:
    """Save an operational learning to TBL_DDL_Context in FileMaker.

    Deduplicates: if a record with the same TableName + FieldName + ContextType
    already exists, it PATCHes instead of creating a duplicate.

    Also updates the local DDL_CONTEXT cache so the hint takes effect
    immediately in the current session.

    Args:
        table_name: FM table this context applies to, e.g. "Invoices".
        context: The learning text, e.g. "Boolean field: 1=yes, empty/0=no".
        field_name: Specific field (empty string for table-level context).
        context_type: Category — field_values, syntax_rule, query_pattern, relationship.
        source: Origin tag — auto, auto:filter_discovery, manual, etc.

    Returns:
        Success or error message string.
    """
    try:
        # Check for existing record (deduplication)
        existing = await odata_client.get(
            CONTEXT_TABLE,
            params={
                "$filter": _build_context_filter(table_name, field_name, context_type),
                "$top": "1",
            },
        )
        records = existing.get("value", [])

        if records:
            # PATCH existing record
            record_id = records[0].get("PrimaryKey", "")
            await odata_client.patch(
                f"{CONTEXT_TABLE}('{_odata_escape(str(record_id))}')",
                json_body={"Context": context, "Source": source},
            )
            # Update local cache
            update_context(
                [
                    {
                        "TableName": table_name,
                        "FieldName": field_name,
                        "ContextType": context_type,
                        "Context": context,
                    }
                ]
            )
            logger.info("Updated context: %s.%s (%s)", table_name, field_name or "*", context_type)
            return f"Updated context for {table_name}.{field_name or '(table)'}: {context}"
        else:
            # POST new record
            await odata_client.post(
                CONTEXT_TABLE,
                json_body={
                    "TableName": table_name,
                    "FieldName": field_name,
                    "ContextType": context_type,
                    "Context": context,
                    "Source": source,
                    "CreatedBy": "mcp_agent",
                },
            )
            # Update local cache
            update_context(
                [
                    {
                        "TableName": table_name,
                        "FieldName": field_name,
                        "ContextType": context_type,
                        "Context": context,
                    }
                ]
            )
            logger.info("Created context: %s.%s (%s)", table_name, field_name or "*", context_type)
            return f"Created context for {table_name}.{field_name or '(table)'}: {context}"

    except PermissionError as e:
        msg = (
            f"Error: No write access to {CONTEXT_TABLE}. "
            f"Grant OData write permission to mcp_agent. ({e})"
        )
        logger.warning(msg)
        return msg
    except ValueError as e:
        if "not found" in str(e).lower():
            return f"Error: {CONTEXT_TABLE} table not found in FM. Create it first."
        return f"Error saving context: {e}"
    except ConnectionError as e:
        return f"Error: Cannot reach FM server — {e}"
    except Exception as e:
        logger.exception("Unexpected error saving context")
        return f"Error saving context: {type(e).__name__}: {e}"


async def delete_context(
    table_name: str,
    field_name: str = "",
    context_type: str = "field_values",
) -> str:
    """Delete an operational learning from TBL_DDL_Context in FileMaker.

    Finds the matching record by TableName + FieldName + ContextType,
    deletes it from FM, and removes it from the local cache.

    Args:
        table_name: FM table this context applies to.
        field_name: Specific field (empty string for table-level context).
        context_type: Category — field_values, syntax_rule, query_pattern, relationship.

    Returns:
        Success or error message string.
    """
    try:
        # Find the record to delete
        existing = await odata_client.get(
            CONTEXT_TABLE,
            params={
                "$filter": _build_context_filter(table_name, field_name, context_type),
                "$top": "1",
            },
        )
        records = existing.get("value", [])

        if not records:
            return (
                f"No context found for {table_name}.{field_name or '(table)'} "
                f"({context_type}) — nothing to delete."
            )

        record_id = records[0].get("PrimaryKey", "")
        await odata_client.delete(f"{CONTEXT_TABLE}('{_odata_escape(str(record_id))}')")

        # Remove from local cache
        remove_context(table_name, field_name, context_type)

        logger.info(
            "Deleted context: %s.%s (%s)",
            table_name,
            field_name or "*",
            context_type,
        )
        return f"Deleted context for {table_name}.{field_name or '(table)'} ({context_type})"

    except PermissionError as e:
        msg = (
            f"Error: No delete access to {CONTEXT_TABLE}. "
            f"Grant OData delete permission to mcp_agent. ({e})"
        )
        logger.warning(msg)
        return msg
    except ValueError as e:
        if "not found" in str(e).lower():
            return f"Error: {CONTEXT_TABLE} table not found in FM. Create it first."
        return f"Error deleting context: {e}"
    except ConnectionError as e:
        return f"Error: Cannot reach FM server — {e}"
    except Exception as e:
        logger.exception("Unexpected error deleting context")
        return f"Error deleting context: {type(e).__name__}: {e}"
