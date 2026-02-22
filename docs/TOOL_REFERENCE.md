# FileMaker MCP — Tool Reference

## Query Tools

### fm_query_records

**Purpose:** Search and retrieve records from any exposed FileMaker table. Auto-caches results per table for fast repeat queries.

**Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| table | string | required | Table name (use fm_list_tables to discover) |
| filter | string | "" | OData $filter expression |
| select | string | "" | Comma-separated field names |
| top | int | 20 | Max records (capped at 10,000) |
| skip | int | 0 | Records to skip (pagination) |
| orderby | string | "" | Sort expression |
| count | bool | true | Include total count |

**Auto-caching:** Tables with `cache_config` entries in TBL_DDL_Context are automatically cached as DataFrames. Date-range tables grow incrementally; cache-all tables fetch once. Cached tables are available in `fm_analyze` by table name.

**OData Filter Examples:**

**IMPORTANT:** Always call `fm_get_schema(table='TableName')` first to discover exact field names. Field names vary by table — some use spaces, some use underscores.

```
# Exact match
City eq 'Springfield'

# Date comparison (use exact field name from get_schema)
ServiceDate ge 2026-01-01

# Numeric comparison
Amount gt 500

# Combined
City eq 'Springfield' and Amount gt 200

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

**Purpose:** Get field names, types, and annotations for a table. Uses static DDL by default (instant, no API call). Includes DDL Context hints when available.

**Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| table | string | "" | Table name (empty = all tables) |
| refresh | bool | false | Force live fetch from FM server |
| show_all | bool | false | Include internal/system fields |

**Always call this before querying a table for the first time** to get exact field names.

---

## Analytics Tools

### fm_load_dataset

**Purpose:** Load records from FM into a named DataFrame for fast analytics. Auto-paginates.

**Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| name | string | required | Your chosen dataset name (e.g., "inv25") |
| table | string | required | FM table to query |
| filter | string | "" | OData $filter expression |
| select | string | "" | Comma-separated fields (empty = all) |

---

### fm_analyze

**Purpose:** Run pandas aggregation on a loaded dataset or auto-cached table. No FM round trip.

**Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| dataset | string | required | Dataset name or cached table name |
| groupby | string | "" | Comma-separated group fields |
| aggregate | string | "" | `function:field` pairs (e.g., `sum:Amount,count:Amount`) |
| filter | string | "" | Pandas query expression |
| sort | string | "" | Sort column + direction (e.g., `Amount_sum desc`) |
| limit | int | 50 | Max rows in output |
| period | string | "" | Time-series: `week`, `month`, or `quarter` |
| pivot_column | string | "" | Cross-tabulation column |

**Supported agg functions:** sum, count, mean, min, max, median, nunique, std

**Modes:**
- `groupby + aggregate` → Grouped aggregation
- `aggregate only` → Scalar across all rows
- `groupby only` → Value counts
- `neither` → Summary statistics (describe)
- `period` → Time-series resampling (first groupby field must be datetime)
- `pivot_column` → Pivot table (requires groupby for rows, aggregate for values)

---

### fm_list_datasets

**Purpose:** List all named datasets in session memory. No parameters.

---

### fm_flush_datasets

**Purpose:** Flush auto-cached table data from memory. Use after data changes.

**Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| table | string | "" | Table to flush (empty = flush all) |

---

## Learning Tools

### fm_save_context

**Purpose:** Save an operational learning to TBL_DDL_Context in FM. Loaded at next bootstrap.

**Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| table_name | string | required | Table this applies to |
| context | string | required | The learning text |
| field_name | string | "" | Specific field (empty = table-level) |
| context_type | string | "field_values" | Category: field_values, syntax_rule, query_pattern, relationship, cache_config |
| source | string | "auto" | Origin: auto, manual, etc. |

---

### fm_delete_context

**Purpose:** Remove a stale or incorrect context entry from FM and local cache.

**Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| table_name | string | required | Table this applies to |
| field_name | string | "" | Specific field (empty = table-level) |
| context_type | string | "field_values" | Category to delete |

---

## Tenant Tools

### fm_use_tenant

**Purpose:** Switch to a different FM tenant. Bootstraps schema on first switch.

**Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| name | string | required | Tenant name (case-insensitive) |

---

### fm_list_tenants

**Purpose:** List configured tenants and show which is active. No parameters.
