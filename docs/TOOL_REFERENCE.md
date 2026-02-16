# FileMaker MCP — Tool Reference

## Phase 1 Tools (Read Only)

### fm_query_records

**Purpose:** Search and retrieve records from any exposed FileMaker table.

**Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| table | string | required | Table name (Location, Customers, InHomeInvoiceHeader, InHomeLineItem, Orders, OrderLine, Pickups, Drivers IH) |
| filter | string | "" | OData $filter expression |
| select | string | "" | Comma-separated field names |
| top | int | 20 | Max records (capped at 100) |
| skip | int | 0 | Records to skip (pagination) |
| orderby | string | "" | Sort expression |
| count | bool | true | Include total count |

**OData Filter Examples:**

**IMPORTANT:** Always call `fm_get_schema(table='TableName')` first to discover exact field names. Field names vary by table — some use spaces, some use underscores.

```
# Exact match
City eq ''

# Date comparison (use exact field name from get_schema)
Date_of_Service ge 2026-01-01

# Numeric comparison
InvoiceTotal gt 500

# Combined
City eq '' and InvoiceTotal gt 200

# Null check
Email ne null
```

---

### fm_get_record

**Purpose:** Retrieve a single record by primary key.

**Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| table | string | required | Table name |
| record_id | string | required | Primary key value |
| id_field | string | auto | Override PK field name |

**Default PK fields:** Location→_kp_CustLoc, Customers→Customer_id, InHomeInvoiceHeader→PrimaryKey

---

### fm_count_records

**Purpose:** Get record count, optionally filtered. Use before large queries.

**Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| table | string | required | Table name |
| filter | string | "" | OData filter to count matching records |

---

### fm_list_tables

**Purpose:** List all available tables with descriptions. No parameters.

---

### fm_get_schema

**Purpose:** Get field names, types, and annotations for a table. Uses static DDL by default (instant, no API call).

**Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| table | string | "" | Table name (empty = all tables) |
| refresh | bool | false | Force live fetch from FM server |
| show_all | bool | false | Include internal/system fields |

**Always call this before querying a table for the first time** to get exact field names. Field naming varies by table — some use spaces (`Customer Name`), some use underscores (`Date_of_Service`). The schema is the only source of truth.
