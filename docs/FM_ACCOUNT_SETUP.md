# FileMaker Account Setup for MCP

## Create the `mcp_agent` Account

### In FileMaker Pro:

1. **File → Manage → Security**
2. **New Account**:
   - Account Name: `mcp_agent`
   - Password: (use a strong password, store in .env)
   - Privilege Set: Create a new one called `MCP_ReadOnly`

### Configure Privilege Set: `MCP_ReadOnly`

**Data Access:**
- Records: Custom privileges
  - All tables: View Only (no create, edit, delete)
  - Or scope to specific tables you want to expose

**Layout Access:**
- All layouts required by OData: View Only
- Note: OData accesses tables via their base table name, but the Data API
  accesses via layouts. Ensure layouts exist for each table you want to expose.

**Extended Privileges — REQUIRED:**
- ☑ `fmrest` — FileMaker Data API access
- ☑ `fmodata` — OData API access

**Scripts:**
- ☑ Grant access to `SCR_DDL_GetTableDDL` (required for schema discovery — see below)
- Phase 2: Grant access to additional scripts for AI execution

### Verify Access

Test OData from a browser or curl:

```bash
curl -u mcp_agent:YOUR_PASSWORD \
  "https://your-server.example.com/fmi/odata/v4/YOUR_DATABASE/YourTable?\$top=1" \
  -H "Accept: application/json"
```

You should see a JSON response with one record.

Test metadata:

```bash
curl -u mcp_agent:YOUR_PASSWORD \
  "https://your-server.example.com/fmi/odata/v4/YOUR_DATABASE/\$metadata"
```

You should see XML with EntityType definitions for your tables.

---

## Required: `SCR_DDL_GetTableDDL` Script

The MCP server auto-discovers table **names** from the OData service document,
but without this script Claude won't know field names, types, or keys —
making effective queries impossible. Install this script in every FileMaker
database you connect to the MCP server.

### What It Does

On startup and when `fm_get_schema` is called, the server calls this script via
the OData script endpoint. If the script isn't found, the server falls back to
`$metadata` (field names only, no types or keys).

### Script Specification

- **Name:** `SCR_DDL_GetTableDDL`
- **Parameter:** JSON array of table names — `["Customers", "Invoices", "LineItems"]`
- **Result:** SQL DDL text — one or more `CREATE TABLE` statements

### Expected Output Format

The script should return DDL like this:

```sql
CREATE TABLE "Customers" (
  "CustomerID" int,
  "First Name" varchar(255),
  "Last Name" varchar(255),
  "Email" varchar(255),
  "Created" datetime,
  PRIMARY KEY ("CustomerID")
);
CREATE TABLE "Invoices" (
  "InvoiceID" int,
  "CustomerID" int,
  "Total" int,
  "Invoice Date" datetime,
  PRIMARY KEY ("InvoiceID"),
  FOREIGN KEY ("CustomerID")
);
```

### Supported SQL Types

| FM SQL Type | Maps To | Description |
|-------------|---------|-------------|
| `varchar(N)` | text | Text fields |
| `int` | number | Number fields |
| `datetime` | datetime | Date, time, timestamp fields |
| `varbinary(N)` | binary | Container fields |

### Field Tier Heuristics

The server auto-classifies fields by name convention:

| Pattern | Tier | Meaning |
|---------|------|---------|
| `_kp_*` | key | Primary key |
| `_kf_*` | key | Foreign key |
| `_sp_*` | internal | Speed/cache fields (hidden from AI) |
| `g` + uppercase, `G_*` | internal | Global fields (hidden from AI) |
| Everything else | standard | Normal fields |

### FileMaker Script Definition

Create this script in your FileMaker file. It uses the native `GetTableDDL()`
function introduced in FileMaker 22:

```
Script: SCR_DDL_GetTableDDL
Run with full access: Off

# ============================================
# Validate parameter (expects JSON array)
# ============================================
Set Variable [ $param ; Value: Get ( ScriptParameter ) ]

If [ IsEmpty ( $param ) or Left ( $param ; 1 ) ≠ "[" ]
  Exit Script [ JSONSetElement ( "{}" ;
    [ "error" ; True ; JSONBoolean ] ;
    [ "message" ; "Parameter must be a JSON array" ; JSONString ]
  ) ]
End If

# ============================================
# Generate DDL using native FM 22 function
# ============================================
Set Variable [ $ddl ; Value: GetTableDDL ( $param ; True ) ]

If [ $ddl = "?" ]
  Exit Script [ JSONSetElement ( "{}" ;
    [ "error" ; True ; JSONBoolean ] ;
    [ "message" ; "GetTableDDL returned error" ; JSONString ]
  ) ]
End If

# ============================================
# Return DDL text
# ============================================
Exit Script [ $ddl ]
```

**Notes:**
- `GetTableDDL( jsonArray ; ignoreError )` — the second parameter (`True`)
  returns partial results if some tables are inaccessible
- The script is called headless via OData — no UI dialogs
- Grant `mcp_agent` permission to run this script in the privilege set

### Calling Convention

The server calls via OData:

```
POST /fmi/odata/v4/{database}/Script.SCR_DDL_GetTableDDL
Content-Type: application/json

{"scriptParameterValue": "[\"Customers\", \"Invoices\"]"}
```

Response:

```json
{
  "scriptResult": {
    "code": 0,
    "resultParameter": "CREATE TABLE \"Customers\" (\"CustomerID\" int, ...); ..."
  }
}
```

### Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| 401 Unauthorized | Bad credentials | Verify fmrest + fmodata |
| 404 Not Found | Wrong database/table | Check name, verify layout exists |
| 403 Forbidden | No table access | Check privilege set |
| Connection refused | OData not enabled | Admin Console → Connectors |
| No types in schema | DDL script missing | Create script or use $metadata |
