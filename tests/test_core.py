"""Tests that run without a live FM server connection.

These validate configuration, parameter validation, and formatting logic.
Integration tests against a live server are in test_integration.py (Phase 2).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from filemaker_mcp.auth import FMODataClient
from filemaker_mcp.config import Settings
from filemaker_mcp.ddl import (
    TABLES,
    FieldDef,  # noqa: F401 — import verifies FieldDef is exportable
    is_script_available,
    set_script_available,
    update_tables,
)
from filemaker_mcp.ddl_parser import parse_ddl
from filemaker_mcp.tools.query import (
    EXPOSED_TABLES,
    _format_records,
    _format_value,
    merge_discovered_tables,
    normalize_dates_in_filter,
    quote_fields_in_filter,
    quote_fields_in_orderby,
    quote_fields_in_select,
)
from filemaker_mcp.tools.schema import (
    _discover_tables_from_odata,
    _format_ddl_schema,
    _format_inferred_schema,
    _infer_field_type,
    _parse_metadata_xml,
    _retry_with_backoff,
    bootstrap_ddl,
)


class TestConfig:
    """Test configuration loading."""

    def test_default_host(self) -> None:
        s = Settings(fm_database="test", fm_password="test")
        assert s.fm_host == "your-server.example.com"

    def test_odata_url(self) -> None:
        s = Settings(fm_host="example.com", fm_database="MyDB", fm_password="test")
        assert s.odata_base_url == "https://example.com/fmi/odata/v4/MyDB"

    def test_data_api_url(self) -> None:
        s = Settings(fm_host="example.com", fm_database="MyDB", fm_password="test")
        assert s.data_api_base_url == "https://example.com/fmi/data/vLatest/databases/MyDB"

    def test_basic_auth_tuple(self) -> None:
        s = Settings(fm_username="user1", fm_password="pass1", fm_database="test")
        assert s.basic_auth == ("user1", "pass1")


class TestFormatting:
    """Test response formatting helpers."""

    def test_format_value_none(self) -> None:
        assert _format_value(None) == ""

    def test_format_value_string(self) -> None:
        assert _format_value("hello") == "hello"

    def test_format_value_number(self) -> None:
        assert _format_value(42) == "42"

    def test_format_value_truncates_long_strings(self) -> None:
        long_text = "x" * 600
        result = _format_value(long_text)
        assert len(result) < 600
        assert result.endswith("... [truncated]")

    def test_format_records_empty(self) -> None:
        data: dict = {"value": []}
        result = _format_records(data, "Location")
        assert "No records found" in result

    def test_format_records_with_data(self) -> None:
        data = {
            "value": [{"Customer Name": "Smith", "City": "", "@odata.etag": "skip_this"}],
            "@odata.count": 1,
        }
        result = _format_records(data, "Location")
        assert "Smith" in result
        assert "" in result
        assert "@odata.etag" not in result  # Metadata fields filtered out
        assert "1 total records" in result


class TestExposedTables:
    """Verify table configuration is consistent."""

    def test_all_tables_have_descriptions(self) -> None:
        for table, desc in EXPOSED_TABLES.items():
            assert len(desc) > 5, f"Table '{table}' needs a description"


class TestSchemaInference:
    """Test query-based schema inference (type detection and formatting)."""

    # --- Type inference ---

    def test_infer_type_none(self) -> None:
        assert _infer_field_type(None) == "unknown"

    def test_infer_type_string(self) -> None:
        assert _infer_field_type("hello") == "text"

    def test_infer_type_empty_string(self) -> None:
        assert _infer_field_type("") == "text"

    def test_infer_type_int(self) -> None:
        assert _infer_field_type(42) == "number"
        assert _infer_field_type(0) == "number"

    def test_infer_type_float(self) -> None:
        assert _infer_field_type(3.14) == "decimal"

    def test_infer_type_bool(self) -> None:
        # bool must be detected before int (bool is subclass of int)
        assert _infer_field_type(True) == "boolean"
        assert _infer_field_type(False) == "boolean"

    def test_infer_type_datetime(self) -> None:
        assert _infer_field_type("2025-01-15T14:30:00Z") == "datetime"
        assert _infer_field_type("2024-11-19T22:32:56Z") == "datetime"

    def test_infer_type_date(self) -> None:
        assert _infer_field_type("2025-01-15") == "date"
        assert _infer_field_type("1900-01-01") == "date"

    def test_infer_type_date_like_string_not_date(self) -> None:
        # String that looks like a date prefix but has extra chars
        assert _infer_field_type("2025-01-15 extra") == "text"

    # --- Schema formatting ---

    def test_format_schema_basic(self) -> None:
        field_types = {"Customer Name": "text", "City": "text", "Zip": "text"}
        result = _format_inferred_schema("Location", field_types)
        assert "Table: Location" in result
        assert "Customer Name: text" in result
        assert "3 fields total" in result

    def test_format_schema_pk_marker(self) -> None:
        field_types = {"_kp_CustLoc": "number", "Customer Name": "text"}
        result = _format_inferred_schema("Location", field_types)
        assert "[PK]" in result
        assert "_kp_CustLoc: number [PK]" in result

    def test_format_schema_fk_marker(self) -> None:
        field_types = {"_kf_Customer": "number", "Name": "text"}
        result = _format_inferred_schema("Test", field_types)
        assert "[FK]" in result

    def test_format_schema_null_count(self) -> None:
        field_types = {"Name": "text", "Phone": "unknown", "Email": "unknown"}
        result = _format_inferred_schema("Location", field_types)
        assert "2 fields were null in sample" in result


class TestMetadataXmlParsing:
    """Test $metadata XML parsing (used by refresh=True fallback)."""

    def test_parse_metadata_with_filter(self) -> None:
        xml = """<?xml version="1.0" encoding="utf-8"?>
        <edmx:Edmx Version="4.01" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
        <edmx:DataServices>
        <Schema Namespace="test" xmlns="http://docs.oasis-open.org/odata/ns/edm">
        <EntityType Name="Location">
            <Key><PropertyRef Name="_kp_CustLoc"/></Key>
            <Property Name="_kp_CustLoc" Type="Edm.Int32" Nullable="false"/>
            <Property Name="Customer Name" Type="Edm.String"/>
            <Property Name="City" Type="Edm.String"/>
        </EntityType>
        <EntityType Name="Orders">
            <Key><PropertyRef Name="PrimaryKey"/></Key>
            <Property Name="PrimaryKey" Type="Edm.Int32" Nullable="false"/>
        </EntityType>
        </Schema>
        </edmx:DataServices>
        </edmx:Edmx>"""

        result = _parse_metadata_xml(xml, table_filter="Location")
        assert "Location" in result
        assert "Orders" not in result
        assert "_kp_CustLoc" in result
        assert "Customer Name" in result

    def test_parse_metadata_without_filter(self) -> None:
        xml = """<?xml version="1.0" encoding="utf-8"?>
        <edmx:Edmx Version="4.01" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
        <edmx:DataServices>
        <Schema Namespace="test" xmlns="http://docs.oasis-open.org/odata/ns/edm">
        <EntityType Name="Location">
            <Key><PropertyRef Name="_kp_CustLoc"/></Key>
            <Property Name="_kp_CustLoc" Type="Edm.Int32" Nullable="false"/>
        </EntityType>
        <EntityType Name="Orders">
            <Key><PropertyRef Name="PrimaryKey"/></Key>
            <Property Name="PrimaryKey" Type="Edm.Int32" Nullable="false"/>
        </EntityType>
        </Schema>
        </edmx:DataServices>
        </edmx:Edmx>"""

        result = _parse_metadata_xml(xml)
        assert "Location" in result
        assert "Orders" in result


class TestDDL:
    """Test static DDL structure and content."""

    def test_tables_dict_exists(self) -> None:
        assert isinstance(TABLES, dict)

    def test_static_ddl_tables_are_exposed(self) -> None:
        """Every table with static DDL should be in EXPOSED_TABLES."""
        from filemaker_mcp.tools.query import EXPOSED_TABLES

        for table in TABLES:
            assert table in EXPOSED_TABLES, f"Static DDL for '{table}' but not in EXPOSED_TABLES"

    def test_field_has_type(self) -> None:
        for table_name, fields in TABLES.items():
            for field_name, field_def in fields.items():
                assert "type" in field_def, f"{table_name}.{field_name} missing 'type'"

    def test_field_has_tier(self) -> None:
        for table_name, fields in TABLES.items():
            for field_name, field_def in fields.items():
                assert "tier" in field_def, f"{table_name}.{field_name} missing 'tier'"
                assert field_def["tier"] in ("key", "standard", "internal"), (
                    f"{table_name}.{field_name} invalid tier: {field_def['tier']}"
                )

    def test_pk_fields_marked(self) -> None:
        for table_name, fields in TABLES.items():
            for field_name, field_def in fields.items():
                if field_name.startswith("_kp_"):
                    assert field_def.get("pk") is True, (
                        f"{table_name}.{field_name} should have pk=True"
                    )

    def test_fk_fields_marked(self) -> None:
        for table_name, fields in TABLES.items():
            for field_name, field_def in fields.items():
                if field_name.startswith("_kf_"):
                    assert field_def.get("fk") is True, (
                        f"{table_name}.{field_name} should have fk=True"
                    )


class TestDDLSchemaFormatting:
    """Test DDL-based schema output formatting."""

    def test_format_ddl_hides_internal(self) -> None:
        fields = {
            "_kp_ID": {"type": "text", "tier": "key", "pk": True},
            "Name": {"type": "text", "tier": "standard"},
            "g_Global": {"type": "text", "tier": "internal"},
        }
        result = _format_ddl_schema("TestTable", fields, show_all=False)
        assert "_kp_ID" in result
        assert "Name" in result
        assert "g_Global" not in result
        assert "internal hidden" in result

    def test_format_ddl_show_all(self) -> None:
        fields = {
            "_kp_ID": {"type": "text", "tier": "key", "pk": True},
            "g_Global": {"type": "text", "tier": "internal"},
        }
        result = _format_ddl_schema("TestTable", fields, show_all=True)
        assert "g_Global" in result
        assert "[internal]" in result

    def test_format_ddl_pk_fk_markers(self) -> None:
        fields = {
            "_kp_ID": {"type": "text", "tier": "key", "pk": True},
            "_kf_Parent": {"type": "text", "tier": "key", "fk": True},
        }
        result = _format_ddl_schema("TestTable", fields, show_all=False)
        assert "[PK, key]" in result
        assert "[FK, key]" in result

    def test_format_ddl_field_counts(self) -> None:
        fields = {
            "_kp_ID": {"type": "text", "tier": "key", "pk": True},
            "Name": {"type": "text", "tier": "standard"},
            "g_X": {"type": "text", "tier": "internal"},
        }
        result = _format_ddl_schema("TestTable", fields, show_all=False)
        assert "3 fields total" in result
        assert "1 internal hidden" in result


class TestODataClientPost:
    """Test FMODataClient.post() method."""

    @pytest.mark.asyncio
    async def test_post_returns_json(self) -> None:
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"scriptResult": "OK"}
        mock_response.raise_for_status = MagicMock()

        mock_http = MagicMock()
        mock_http.post = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        result = await client.post("Script.MyScript", json_body={"scriptParameterValue": "x"})
        assert result == {"scriptResult": "OK"}
        mock_http.post.assert_awaited_once_with(
            "/Script.MyScript", json={"scriptParameterValue": "x"}
        )

    @pytest.mark.asyncio
    async def test_post_404_raises_valueerror(self) -> None:
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not Found",
            request=MagicMock(),
            response=mock_response,
        )

        mock_http = MagicMock()
        mock_http.post = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        with pytest.raises(ValueError, match="not found"):
            await client.post("Script.Missing")

    @pytest.mark.asyncio
    async def test_post_401_raises_permissionerror(self) -> None:
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Unauthorized",
            request=MagicMock(),
            response=mock_response,
        )

        mock_http = MagicMock()
        mock_http.post = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        with pytest.raises(PermissionError):
            await client.post("Script.Test")


class TestDDLCache:
    """Test runtime DDL cache management."""

    def test_update_tables_adds_new_table(self) -> None:
        new_table = {"Field1": {"type": "text", "tier": "standard"}}
        update_tables({"NewTestTable": new_table})
        assert "NewTestTable" in TABLES
        assert TABLES["NewTestTable"]["Field1"]["type"] == "text"
        # Clean up
        del TABLES["NewTestTable"]

    def test_update_tables_overwrites_existing(self) -> None:
        original = dict(TABLES.get("Location", {}))
        update_tables({"Location": {"_kp_CustLoc": {"type": "number", "tier": "key", "pk": True}}})
        assert len(TABLES["Location"]) == 1  # Overwritten
        # Restore
        TABLES["Location"] = original

    def test_script_available_default_none(self) -> None:
        set_script_available(None)
        assert is_script_available() is None

    def test_script_available_set_true(self) -> None:
        set_script_available(True)
        assert is_script_available() is True
        set_script_available(None)  # Reset

    def test_script_available_set_false(self) -> None:
        set_script_available(False)
        assert is_script_available() is False
        set_script_available(None)  # Reset


class TestDDLParser:
    """Test DDL text parsing into FieldDef dicts."""

    def test_parse_simple_table(self) -> None:
        ddl = """CREATE TABLE "Location" (
"_kp_CustLoc" int,
"Customer Name" varchar(255),
"Map" varbinary(4096),
"Timestamp_Create" datetime,
PRIMARY KEY (_kp_CustLoc)
);"""
        result = parse_ddl(ddl)
        assert "Location" in result
        loc = result["Location"]
        assert loc["_kp_CustLoc"]["type"] == "number"
        assert loc["_kp_CustLoc"]["pk"] is True
        assert loc["_kp_CustLoc"]["tier"] == "key"
        assert loc["Customer Name"]["type"] == "text"
        assert loc["Map"]["type"] == "binary"
        assert loc["Timestamp_Create"]["type"] == "datetime"

    def test_parse_foreign_key(self) -> None:
        ddl = """CREATE TABLE "Orders" (
"PrimaryKey" varchar(255),
"_kf_CustLoc" varchar(255),
PRIMARY KEY (PrimaryKey),
FOREIGN KEY (_kf_CustLoc) REFERENCES Location(_kp_CustLoc)
);"""
        result = parse_ddl(ddl)
        assert result["Orders"]["_kf_CustLoc"]["fk"] is True
        assert result["Orders"]["_kf_CustLoc"]["tier"] == "key"
        assert result["Orders"]["PrimaryKey"]["pk"] is True

    def test_parse_tier_heuristics(self) -> None:
        ddl = """CREATE TABLE "Test" (
"_kp_ID" int,
"_kf_Parent" varchar(255),
"_sp_cache" int,
"gGlobal" varchar(255),
"G_Flag" varchar(255),
"Name" varchar(255),
"cCalcField" varchar(255),
"sSum" int,
PRIMARY KEY (_kp_ID)
);"""
        result = parse_ddl(ddl)
        t = result["Test"]
        assert t["_kp_ID"]["tier"] == "key"
        assert t["_kf_Parent"]["tier"] == "key"
        assert t["_sp_cache"]["tier"] == "internal"
        assert t["gGlobal"]["tier"] == "internal"
        assert t["G_Flag"]["tier"] == "internal"
        assert t["Name"]["tier"] == "standard"
        assert t["cCalcField"]["tier"] == "standard"
        assert t["sSum"]["tier"] == "standard"

    def test_parse_multiple_tables(self) -> None:
        ddl = """CREATE TABLE "A" (
"id" int,
PRIMARY KEY (id)
);

CREATE TABLE "B" (
"id" varchar(255),
PRIMARY KEY (id)
);"""
        result = parse_ddl(ddl)
        assert len(result) == 2
        assert "A" in result
        assert "B" in result

    def test_parse_field_with_comment(self) -> None:
        ddl = """CREATE TABLE "Test" (
"Field1" varchar(255), /*This is a comment*/
PRIMARY KEY (Field1)
);"""
        result = parse_ddl(ddl)
        assert result["Test"]["Field1"]["type"] == "text"

    def test_parse_empty_string(self) -> None:
        result = parse_ddl("")
        assert result == {}


class TestODataURLEncoding:
    """Test FM OData URL encoding quirks.

    FM OData rejects '+' for spaces (must use %20), and requires
    literal $ and , characters in query strings.
    """

    @pytest.mark.asyncio
    async def test_spaces_encoded_as_percent20(self) -> None:
        """FM OData rejects '+' for spaces — must use %20."""
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"value": []}
        mock_response.raise_for_status = MagicMock()

        mock_http = MagicMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        await client.get("Location", params={"$filter": "City eq ''"})

        called_url = mock_http.get.call_args[0][0]
        assert "+" not in called_url, f"URL contains '+' for spaces: {called_url}"
        assert "%20" in called_url, f"URL missing %20 encoding: {called_url}"

    @pytest.mark.asyncio
    async def test_dollar_signs_preserved(self) -> None:
        """OData param keys ($filter, $top) must not be percent-encoded."""
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"value": []}
        mock_response.raise_for_status = MagicMock()

        mock_http = MagicMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        await client.get("Location", params={"$filter": "City eq 'X'", "$top": "10"})

        called_url = mock_http.get.call_args[0][0]
        assert "$filter=" in called_url, f"$filter encoded: {called_url}"
        assert "$top=" in called_url, f"$top encoded: {called_url}"
        assert "%24" not in called_url, f"$ encoded as %24: {called_url}"

    @pytest.mark.asyncio
    async def test_commas_preserved_in_select(self) -> None:
        """$select comma-separated lists must preserve literal commas."""
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"value": []}
        mock_response.raise_for_status = MagicMock()

        mock_http = MagicMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        await client.get("Location", params={"$select": "Name,City,Zone"})

        called_url = mock_http.get.call_args[0][0]
        assert "Name,City,Zone" in called_url, f"Commas encoded: {called_url}"
        assert "%2C" not in called_url, f"Commas as %2C: {called_url}"

    @pytest.mark.asyncio
    async def test_no_params_skips_encoding(self) -> None:
        """GET with no params should not append query string."""
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"metadata_xml": "<xml/>"}
        mock_response.raise_for_status = MagicMock()
        mock_response.text = "<xml/>"

        mock_http = MagicMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        await client.get("$metadata")

        called_url = mock_http.get.call_args[0][0]
        assert called_url == "/$metadata"

    @pytest.mark.asyncio
    async def test_single_quotes_preserved_in_filter(self) -> None:
        """String literals in OData filters use single quotes."""
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"value": []}
        mock_response.raise_for_status = MagicMock()

        mock_http = MagicMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        await client.get("Location", params={"$filter": "City eq ''"})

        called_url = mock_http.get.call_args[0][0]
        assert "'" in called_url, f"Single quotes encoded: {called_url}"


class TestCountKeyCompatibility:
    """Test FM OData @count vs @odata.count handling."""

    def test_format_records_uses_at_count(self) -> None:
        """FM OData returns @count, not @odata.count."""
        data = {
            "value": [{"Name": "Smith"}],
            "@count": 42,
        }
        result = _format_records(data, "Location")
        assert "42 total records" in result

    def test_format_records_uses_odata_count(self) -> None:
        """Standard OData @odata.count still works."""
        data = {
            "value": [{"Name": "Smith"}],
            "@odata.count": 42,
        }
        result = _format_records(data, "Location")
        assert "42 total records" in result

    def test_format_records_prefers_odata_count(self) -> None:
        """If both keys present, @odata.count takes precedence."""
        data = {
            "value": [{"Name": "Smith"}],
            "@odata.count": 10,
            "@count": 99,
        }
        result = _format_records(data, "Location")
        assert "10 total records" in result

    def test_format_records_empty_with_count(self) -> None:
        """Empty value list but count > 0 gives helpful message."""
        data = {
            "value": [],
            "@count": 50,
        }
        result = _format_records(data, "Location")
        assert "50 total records" in result
        assert "$top/$skip" in result

    def test_format_records_no_count_key(self) -> None:
        """No count key at all — shows record count only."""
        data = {
            "value": [{"Name": "Smith"}],
        }
        result = _format_records(data, "Location")
        assert "Showing 1 records" in result
        assert "total" not in result.split("\n")[0]  # No "X total" in header


class TestCountRecordsParams:
    """Test count_records uses $top=1 with $select to work around FM $top=0 bug."""

    @pytest.mark.asyncio
    async def test_count_uses_top_1_not_0(self) -> None:
        """FM returns @count=0 when $top=0 — we must use $top=1."""
        from filemaker_mcp.tools.query import count_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"@count": 42, "value": [{"PrimaryKey": "x"}]})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            result = await count_records("Location")

        # count_records passes params as kwarg to odata_client.get(table, params=...)
        params = mock_client.get.call_args[1].get("params", {})
        assert params.get("$top") == "1", f"Expected $top=1 but got: {params}"
        assert "42" in result

    @pytest.mark.asyncio
    async def test_count_selects_primarykey(self) -> None:
        """Count query should $select=PrimaryKey to minimize payload."""
        from filemaker_mcp.tools.query import count_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"@count": 5, "value": [{"PrimaryKey": "x"}]})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await count_records("InHomeInvoiceHeader")

        params = mock_client.get.call_args[1].get("params", {})
        assert params.get("$select") == '"PrimaryKey"'

    @pytest.mark.asyncio
    async def test_count_with_filter(self) -> None:
        """Count with filter includes $filter in request."""
        from filemaker_mcp.tools.query import count_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"@count": 6, "value": [{"PrimaryKey": "x"}]})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            result = await count_records(
                "InHomeInvoiceHeader",
                filter="Date_of_Service eq 2026-02-14",
            )

        params = mock_client.get.call_args[1].get("params", {})
        assert "Date_of_Service" in params.get("$filter", "")
        assert "6 records matching" in result

    @pytest.mark.asyncio
    async def test_count_reads_at_count_key(self) -> None:
        """count_records reads FM's @count key correctly."""
        from filemaker_mcp.tools.query import count_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"@count": 100, "value": [{"PrimaryKey": "x"}]})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            result = await count_records("Location")

        assert "100 total records" in result

    @pytest.mark.asyncio
    async def test_count_unknown_table_returns_error(self) -> None:
        """count_records rejects unknown table names."""
        from filemaker_mcp.tools.query import count_records

        result = await count_records("FakeTable")
        assert "Error" in result
        assert "FakeTable" in result


class TestLiveDDLRefresh:
    """Test live DDL refresh via script execution."""

    @pytest.mark.asyncio
    async def test_refresh_via_script_success(self) -> None:
        from filemaker_mcp.ddl import TABLES, set_script_available
        from filemaker_mcp.tools.schema import _refresh_ddl_via_script

        ddl_response = """CREATE TABLE "TestRefresh" (
"_kp_ID" int,
"Name" varchar(255),
PRIMARY KEY (_kp_ID)
);"""
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(
            return_value={"scriptResult": {"code": 0, "resultParameter": ddl_response}}
        )

        set_script_available(None)  # Reset

        with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
            result = await _refresh_ddl_via_script(["TestRefresh"])

        assert result is True
        assert "TestRefresh" in TABLES
        assert TABLES["TestRefresh"]["_kp_ID"]["type"] == "number"
        assert TABLES["TestRefresh"]["_kp_ID"]["pk"] is True
        # Clean up
        del TABLES["TestRefresh"]
        set_script_available(None)

    @pytest.mark.asyncio
    async def test_refresh_via_script_404_falls_through(self) -> None:
        from filemaker_mcp.ddl import is_script_available, set_script_available
        from filemaker_mcp.tools.schema import _refresh_ddl_via_script

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=ValueError("Resource not found"))

        set_script_available(None)

        with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
            result = await _refresh_ddl_via_script(["SomeTable"])

        assert result is False
        assert is_script_available() is False
        set_script_available(None)  # Reset

    @pytest.mark.asyncio
    async def test_refresh_skips_script_when_cached_unavailable(self) -> None:
        from filemaker_mcp.ddl import set_script_available
        from filemaker_mcp.tools.schema import _refresh_ddl_via_script

        mock_client = AsyncMock()
        set_script_available(False)  # Script already known unavailable

        with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
            result = await _refresh_ddl_via_script(["SomeTable"])

        assert result is False
        mock_client.post.assert_not_called()  # Should skip entirely
        set_script_available(None)  # Reset


class TestQueryRecords:
    """Test query_records — the primary workhorse tool."""

    @pytest.mark.asyncio
    async def test_unknown_table_returns_error(self) -> None:
        from filemaker_mcp.tools.query import query_records

        result = await query_records("NonexistentTable")
        assert "Error" in result
        assert "NonexistentTable" in result
        assert "Available tables" in result

    @pytest.mark.asyncio
    async def test_top_capped_at_10000(self) -> None:
        from filemaker_mcp.tools.query import query_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": [], "@count": 0})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await query_records("Location", top=99999)

        params = mock_client.get.call_args[1]["params"]
        assert params["$top"] == "10000"

    @pytest.mark.asyncio
    async def test_builds_all_odata_params(self) -> None:
        from filemaker_mcp.tools.query import query_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": [], "@count": 0})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await query_records(
                "Location",
                filter="City eq ''",
                select="Name,City",
                top=10,
                skip=5,
                orderby="Name asc",
                count=True,
            )

        params = mock_client.get.call_args[1]["params"]
        assert params["$filter"] == "\"City\" eq ''"
        assert params["$select"] == '"Name","City"'
        assert params["$top"] == "10"
        assert params["$skip"] == "5"
        assert params["$orderby"] == '"Name" asc'
        assert params["$count"] == "true"

    @pytest.mark.asyncio
    async def test_omits_empty_optional_params(self) -> None:
        from filemaker_mcp.tools.query import query_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": [], "@count": 0})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await query_records("Location")

        params = mock_client.get.call_args[1]["params"]
        assert "$filter" not in params
        assert "$select" not in params
        assert "$skip" not in params
        assert "$orderby" not in params

    @pytest.mark.asyncio
    async def test_count_false_omits_param(self) -> None:
        from filemaker_mcp.tools.query import query_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": []})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await query_records("Location", count=False)

        params = mock_client.get.call_args[1]["params"]
        assert "$count" not in params

    @pytest.mark.asyncio
    async def test_connection_error_returns_message(self) -> None:
        from filemaker_mcp.tools.query import query_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=ConnectionError("Server unreachable"))

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            result = await query_records("Location")

        assert "Connection error" in result
        assert "Server unreachable" in result

    @pytest.mark.asyncio
    async def test_permission_error_returns_message(self) -> None:
        from filemaker_mcp.tools.query import query_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=PermissionError("Auth failed"))

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            result = await query_records("Location")

        assert "Authentication error" in result

    @pytest.mark.asyncio
    async def test_field_not_found_error_shows_hint(self) -> None:
        from filemaker_mcp.tools.query import query_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(
            side_effect=ValueError("The field named 'BadField' does not exist")
        )

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            result = await query_records("Location", filter="BadField eq 'x'")

        assert "TIP" in result
        assert "fm_get_schema" in result

    @pytest.mark.asyncio
    async def test_generic_value_error_no_hint(self) -> None:
        from filemaker_mcp.tools.query import query_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=ValueError("Some other OData error"))

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            result = await query_records("Location")

        assert "Query error" in result
        assert "TIP" not in result

    @pytest.mark.asyncio
    async def test_unexpected_exception_returns_type_and_message(self) -> None:
        from filemaker_mcp.tools.query import query_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=RuntimeError("Something broke"))

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            result = await query_records("Location")

        assert "RuntimeError" in result
        assert "Something broke" in result

    @pytest.mark.asyncio
    async def test_returns_formatted_records(self) -> None:
        from filemaker_mcp.tools.query import query_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(
            return_value={
                "value": [
                    {"Name": "Smith", "City": ""},
                    {"Name": "Jones", "City": ""},
                ],
                "@count": 2,
            }
        )

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            result = await query_records("Location", top=5)

        assert "Smith" in result
        assert "Jones" in result
        assert "2 total records" in result


class TestGetRecord:
    """Test get_record — single record lookup by primary key."""

    @pytest.mark.asyncio
    async def test_unknown_table_returns_error(self) -> None:
        from filemaker_mcp.tools.query import get_record

        result = await get_record("FakeTable", "123")
        assert "Error" in result
        assert "FakeTable" in result

    @pytest.mark.asyncio
    async def test_uses_default_pk_field(self) -> None:
        from filemaker_mcp.tools.query import get_record

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": [{"_kp_CustLoc": 123, "Name": "Smith"}]})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await get_record("Location", "123")

        params = mock_client.get.call_args[1]["params"]
        assert "PrimaryKey" in params["$filter"]

    @pytest.mark.asyncio
    async def test_uses_custom_id_field(self) -> None:
        from filemaker_mcp.tools.query import get_record

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": [{"MyField": "abc"}]})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await get_record("Location", "abc", id_field="MyField")

        params = mock_client.get.call_args[1]["params"]
        assert "MyField" in params["$filter"]
        assert "'abc'" in params["$filter"]

    @pytest.mark.asyncio
    async def test_numeric_id_no_quotes(self) -> None:
        from filemaker_mcp.tools.query import get_record

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": [{"_kp_CustLoc": 42}]})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await get_record("Location", "42")

        params = mock_client.get.call_args[1]["params"]
        # Numeric: no quotes around value
        assert "eq 42" in params["$filter"]
        assert "eq '42'" not in params["$filter"]

    @pytest.mark.asyncio
    async def test_string_id_has_quotes(self) -> None:
        from filemaker_mcp.tools.query import get_record

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": [{"PrimaryKey": "ABC-123"}]})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await get_record("InHomeInvoiceHeader", "ABC-123")

        params = mock_client.get.call_args[1]["params"]
        assert "'ABC-123'" in params["$filter"]

    @pytest.mark.asyncio
    async def test_not_found_returns_message(self) -> None:
        from filemaker_mcp.tools.query import get_record

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": []})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            result = await get_record("Location", "99999")

        assert "No record found" in result
        assert "99999" in result

    @pytest.mark.asyncio
    async def test_found_record_formats_fields(self) -> None:
        from filemaker_mcp.tools.query import get_record

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(
            return_value={
                "value": [
                    {
                        "_kp_CustLoc": 100,
                        "Customer Name": "",
                        "City": "",
                        "@odata.etag": "skip",
                    }
                ]
            }
        )

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            result = await get_record("Location", "100")

        assert "" in result
        assert "" in result
        assert "@odata.etag" not in result


class TestODataClientGetErrors:
    """Test FMODataClient.get() error handling paths."""

    @pytest.mark.asyncio
    async def test_get_401_raises_permission_error(self) -> None:
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Unauthorized", request=MagicMock(), response=mock_response
        )

        mock_http = MagicMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        with pytest.raises(PermissionError, match="Authentication failed"):
            await client.get("Location")

    @pytest.mark.asyncio
    async def test_get_404_raises_value_error(self) -> None:
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not Found", request=MagicMock(), response=mock_response
        )

        mock_http = MagicMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        with pytest.raises(ValueError, match="not found"):
            await client.get("BadTable")

    @pytest.mark.asyncio
    async def test_get_connection_error(self) -> None:
        client = FMODataClient()

        mock_http = MagicMock()
        mock_http.get = AsyncMock(side_effect=httpx.ConnectError("Connection refused"))
        mock_http.is_closed = False
        client._client = mock_http

        with pytest.raises(ConnectionError, match="Cannot connect"):
            await client.get("Location")

    @pytest.mark.asyncio
    async def test_get_400_extracts_fm_error_json(self) -> None:
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"error": {"message": "The field 'Bad' does not exist"}}
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Bad Request", request=MagicMock(), response=mock_response
        )

        mock_http = MagicMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        with pytest.raises(ValueError, match="The field 'Bad' does not exist"):
            await client.get("Location", params={"$filter": "Bad eq 1"})

    @pytest.mark.asyncio
    async def test_get_500_with_non_json_body(self) -> None:
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.side_effect = ValueError("Not JSON")
        mock_response.text = "Internal Server Error"
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Server Error", request=MagicMock(), response=mock_response
        )

        mock_http = MagicMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        with pytest.raises(ValueError, match="Internal Server Error"):
            await client.get("Location")


class TestListTables:
    """Test list_tables output."""

    @pytest.mark.asyncio
    async def test_lists_all_exposed_tables(self) -> None:
        from filemaker_mcp.tools.query import list_tables

        result = await list_tables()
        for table in EXPOSED_TABLES:
            assert table in result

    @pytest.mark.asyncio
    async def test_includes_descriptions(self) -> None:
        from filemaker_mcp.tools.query import list_tables

        result = await list_tables()
        assert "customer" in result.lower()
        assert "invoice" in result.lower()


class TestListTablesBootstrapError:
    """Test list_tables surfaces bootstrap errors when tables are empty."""

    @pytest.mark.asyncio
    async def test_shows_error_when_bootstrap_failed(self) -> None:
        from filemaker_mcp.tools.query import (
            EXPOSED_TABLES,
            list_tables,
            set_bootstrap_error,
        )

        saved = dict(EXPOSED_TABLES)
        EXPOSED_TABLES.clear()
        set_bootstrap_error("ConnectionError: Cannot connect to FM Server")
        try:
            result = await list_tables()
            assert "No tables available" in result
            assert "ConnectionError" in result
            assert "FM_HOST" in result
        finally:
            EXPOSED_TABLES.update(saved)
            set_bootstrap_error(None)

    @pytest.mark.asyncio
    async def test_no_error_when_tables_present(self) -> None:
        from filemaker_mcp.tools.query import (
            EXPOSED_TABLES,
            list_tables,
            set_bootstrap_error,
        )

        saved = dict(EXPOSED_TABLES)
        EXPOSED_TABLES["TestTable"] = "Test description"
        set_bootstrap_error("some old error")
        try:
            result = await list_tables()
            assert "No tables available" not in result
        finally:
            EXPOSED_TABLES.clear()
            EXPOSED_TABLES.update(saved)
            set_bootstrap_error(None)


class TestNormalizeDatesInFilter:
    """Tests for normalize_dates_in_filter() — FM OData date format safety net."""

    # --- Passthrough (already correct) ---

    def test_bare_iso_date_unchanged(self) -> None:
        f = "Date_of_Service eq 2026-02-14"
        assert normalize_dates_in_filter(f) == f

    def test_bare_iso_date_range_unchanged(self) -> None:
        f = "Date_of_Service ge 2026-01-01 and Date_of_Service lt 2026-02-01"
        assert normalize_dates_in_filter(f) == f

    def test_non_date_string_unchanged(self) -> None:
        f = "City eq ''"
        assert normalize_dates_in_filter(f) == f

    def test_empty_filter_unchanged(self) -> None:
        assert normalize_dates_in_filter("") == ""

    # --- Quoted ISO dates ---

    def test_single_quoted_iso_date(self) -> None:
        assert (
            normalize_dates_in_filter("Date_of_Service eq '2026-02-14'")
            == "Date_of_Service eq 2026-02-14"
        )

    def test_double_quoted_iso_date(self) -> None:
        assert (
            normalize_dates_in_filter('Date_of_Service eq "2026-02-14"')
            == "Date_of_Service eq 2026-02-14"
        )

    # --- ISO timestamps stripped to date ---

    def test_iso_timestamp_stripped(self) -> None:
        assert (
            normalize_dates_in_filter("Date_of_Service eq 2026-02-14T00:00:00")
            == "Date_of_Service eq 2026-02-14"
        )

    def test_iso_timestamp_with_utc_stripped(self) -> None:
        assert (
            normalize_dates_in_filter("Date_of_Service ge 2026-02-14T00:00:00Z")
            == "Date_of_Service ge 2026-02-14"
        )

    def test_iso_timestamp_with_offset_stripped(self) -> None:
        assert (
            normalize_dates_in_filter("Date_of_Service eq 2026-02-14T14:30:00-05:00")
            == "Date_of_Service eq 2026-02-14"
        )

    # --- US format dates ---

    def test_us_date_mm_dd_yyyy(self) -> None:
        assert (
            normalize_dates_in_filter("Date_of_Service eq 02/15/2026")
            == "Date_of_Service eq 2026-02-15"
        )

    def test_us_date_m_d_yyyy(self) -> None:
        assert (
            normalize_dates_in_filter("Date_of_Service eq 2/5/2026")
            == "Date_of_Service eq 2026-02-05"
        )

    def test_us_date_with_time(self) -> None:
        assert (
            normalize_dates_in_filter("Date_of_Service eq 2/15/2026 3:45:00 PM")
            == "Date_of_Service eq 2026-02-15"
        )

    def test_quoted_us_date(self) -> None:
        assert (
            normalize_dates_in_filter("Date_of_Service eq '02/15/2026'")
            == "Date_of_Service eq 2026-02-15"
        )

    # --- Combined filters ---

    def test_mixed_date_and_string(self) -> None:
        assert (
            normalize_dates_in_filter("Date_of_Service ge '2026-02-01' and City eq ''")
            == "Date_of_Service ge 2026-02-01 and City eq ''"
        )

    def test_two_dates_in_range(self) -> None:
        assert (
            normalize_dates_in_filter(
                "Date_of_Service ge '2026-01-01' and Date_of_Service lt '2026-02-01'"
            )
            == "Date_of_Service ge 2026-01-01 and Date_of_Service lt 2026-02-01"
        )


class TestSchemaDateHints:
    """Tests for date format hints in schema output."""

    def test_ddl_schema_datetime_field_has_hint(self) -> None:
        fields = {
            "Date_of_Service": {"type": "datetime", "tier": "key"},
        }
        result = _format_ddl_schema("Test", fields)
        assert "(filter as: YYYY-MM-DD, no quotes)" in result

    def test_ddl_schema_date_field_has_hint(self) -> None:
        fields = {
            "PostDate": {"type": "date", "tier": "standard"},
        }
        result = _format_ddl_schema("Test", fields)
        assert "(filter as: YYYY-MM-DD, no quotes)" in result

    def test_ddl_schema_text_field_no_hint(self) -> None:
        fields = {
            "City": {"type": "text", "tier": "standard"},
        }
        result = _format_ddl_schema("Test", fields)
        assert "(filter as:" not in result

    def test_inferred_schema_datetime_has_hint(self) -> None:
        field_types = {"Date_of_Service": "datetime", "City": "text"}
        result = _format_inferred_schema("Test", field_types)
        assert "Date_of_Service: datetime" in result
        assert "(filter as: YYYY-MM-DD, no quotes)" in result
        assert "City: text" in result
        # text field should NOT have the hint
        lines = result.split("\n")
        city_line = next(line for line in lines if "City:" in line)
        assert "(filter as:" not in city_line


class TestNormalizeDatesEdgeCases:
    """Additional date normalizer tests from code review feedback."""

    def test_quoted_iso_datetime(self) -> None:
        """Quoted ISO datetime — most likely LLM output format."""
        assert (
            normalize_dates_in_filter("Date_of_Service eq '2026-02-14T14:30:00Z'")
            == "Date_of_Service eq 2026-02-14"
        )

    def test_positive_timezone_offset(self) -> None:
        assert (
            normalize_dates_in_filter("Date_of_Service eq 2026-02-14T14:30:00+05:30")
            == "Date_of_Service eq 2026-02-14"
        )

    def test_fractional_seconds(self) -> None:
        """JavaScript toISOString() format."""
        assert (
            normalize_dates_in_filter("Date_of_Service eq 2026-02-14T14:30:00.123Z")
            == "Date_of_Service eq 2026-02-14"
        )


class TestRetryWithBackoff:
    """Test exponential backoff retry helper."""

    @pytest.mark.asyncio
    async def test_succeeds_first_try(self) -> None:
        """No retries needed when function succeeds immediately."""
        fn = AsyncMock(return_value="ok")
        result = await _retry_with_backoff(fn, max_retries=3, base_delay=0.01)
        assert result == "ok"
        assert fn.call_count == 1

    @pytest.mark.asyncio
    async def test_retries_on_connection_error(self) -> None:
        """Retries on ConnectionError, succeeds on second attempt."""
        fn = AsyncMock(side_effect=[ConnectionError("down"), "ok"])
        result = await _retry_with_backoff(fn, max_retries=3, base_delay=0.01)
        assert result == "ok"
        assert fn.call_count == 2

    @pytest.mark.asyncio
    async def test_retries_on_timeout(self) -> None:
        """Retries on httpx.ReadTimeout."""
        fn = AsyncMock(side_effect=[httpx.ReadTimeout("slow"), "ok"])
        result = await _retry_with_backoff(fn, max_retries=3, base_delay=0.01)
        assert result == "ok"
        assert fn.call_count == 2

    @pytest.mark.asyncio
    async def test_returns_none_after_max_retries(self) -> None:
        """Returns None when all retries exhausted."""
        fn = AsyncMock(side_effect=ConnectionError("down"))
        result = await _retry_with_backoff(fn, max_retries=2, base_delay=0.01)
        assert result is None
        assert fn.call_count == 3  # 1 initial + 2 retries

    @pytest.mark.asyncio
    async def test_no_retry_on_permission_error(self) -> None:
        """PermissionError (401) is not retryable — raises immediately."""
        fn = AsyncMock(side_effect=PermissionError("bad creds"))
        with pytest.raises(PermissionError):
            await _retry_with_backoff(fn, max_retries=3, base_delay=0.01)
        assert fn.call_count == 1

    @pytest.mark.asyncio
    async def test_no_retry_on_value_error(self) -> None:
        """ValueError (404) is not retryable — raises immediately."""
        fn = AsyncMock(side_effect=ValueError("not found"))
        with pytest.raises(ValueError):
            await _retry_with_backoff(fn, max_retries=3, base_delay=0.01)
        assert fn.call_count == 1


class TestDiscoverTables:
    """Test OData service document table discovery."""

    @pytest.mark.asyncio
    async def test_parses_service_document(self) -> None:
        """Extracts table names from OData service document JSON."""
        service_doc = {
            "value": [
                {"name": "Location", "url": "Location"},
                {"name": "Customers", "url": "Customers"},
                {"name": "Orders", "url": "Orders"},
            ]
        }
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=service_doc)

        with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
            result = await _discover_tables_from_odata()

        assert result == ["Location", "Customers", "Orders"]
        mock_client.get.assert_called_once_with("", params={"$format": "JSON"})

    @pytest.mark.asyncio
    async def test_empty_value_returns_empty_list(self) -> None:
        """Returns empty list when service document has no tables."""
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": []})

        with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
            result = await _discover_tables_from_odata()

        assert result == []

    @pytest.mark.asyncio
    async def test_missing_value_key_returns_empty(self) -> None:
        """Returns empty list when response has no 'value' key."""
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"@odata.context": "..."})

        with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
            result = await _discover_tables_from_odata()

        assert result == []

    @pytest.mark.asyncio
    async def test_connection_error_returns_empty(self) -> None:
        """Returns empty list on connection failure (caller handles fallback)."""
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=ConnectionError("down"))

        with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
            result = await _discover_tables_from_odata()

        assert result == []


class TestBootstrapDDL:
    """Test 3-step DDL bootstrap: probe script -> discover tables -> fetch DDL."""

    @pytest.mark.asyncio
    async def test_step1_script_not_found_falls_through_to_discovery(self) -> None:
        """If DDL script doesn't exist (404), bootstrap falls through to OData discovery."""
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=ValueError("not found"))
        mock_client.get = AsyncMock(return_value={"value": [{"name": "Customers"}]})

        with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
            set_script_available(None)
            await bootstrap_ddl()

        # Should have called get (step 2 OData discovery)
        mock_client.get.assert_called()
        assert is_script_available() is False

    @pytest.mark.asyncio
    async def test_step1_auth_failure_stops_early(self) -> None:
        """If auth fails (401), bootstrap stops after step 1."""
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=PermissionError("bad creds"))

        with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
            set_script_available(None)
            await bootstrap_ddl()

        mock_client.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_step2_discovers_tables_from_fm(self) -> None:
        """Step 2 calls OData service document and uses discovered tables."""
        service_doc = {
            "value": [
                {"name": "Location", "url": "Location"},
                {"name": "NewTable", "url": "NewTable"},
            ]
        }
        ddl_response = """CREATE TABLE "Location" (
"_kp_CustLoc" int,
PRIMARY KEY (_kp_CustLoc)
);"""
        mock_client = AsyncMock()
        # Step 1 probe succeeds
        mock_client.post = AsyncMock(
            return_value={"scriptResult": {"code": 0, "resultParameter": ddl_response}}
        )
        # Step 2 discovery
        mock_client.get = AsyncMock(return_value=service_doc)

        original_exposed = dict(EXPOSED_TABLES)
        try:
            with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
                set_script_available(None)
                await bootstrap_ddl()

            # NewTable should have been merged into EXPOSED_TABLES
            assert "NewTable" in EXPOSED_TABLES
        finally:
            EXPOSED_TABLES.clear()
            EXPOSED_TABLES.update(original_exposed)
            set_script_available(None)

    @pytest.mark.asyncio
    async def test_step2_fallback_to_exposed_tables_on_failure(self) -> None:
        """If discovery fails, falls back to hardcoded EXPOSED_TABLES keys."""
        ddl_response = """CREATE TABLE "Location" (
"_kp_CustLoc" int,
PRIMARY KEY (_kp_CustLoc)
);"""
        mock_client = AsyncMock()
        # Step 1 probe succeeds
        probe_response = {"scriptResult": {"code": 0, "resultParameter": "ok"}}
        # Step 2 discovery fails, Step 3 DDL succeeds
        mock_client.post = AsyncMock(
            side_effect=[
                probe_response,  # step 1 probe
                {"scriptResult": {"code": 0, "resultParameter": ddl_response}},  # step 3
            ]
        )
        mock_client.get = AsyncMock(side_effect=ConnectionError("down"))

        with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
            set_script_available(None)
            await bootstrap_ddl()

        # Step 3 should have been called with EXPOSED_TABLES keys as fallback
        step3_call = mock_client.post.call_args_list[1]
        param_str = step3_call[1].get("json_body", {}).get("scriptParameterValue", "")
        for table in EXPOSED_TABLES:
            assert table in param_str

    @pytest.mark.asyncio
    async def test_step3_retries_on_transient_failure(self) -> None:
        """Step 3 retries with backoff on connection errors."""
        ddl_response = """CREATE TABLE "RetryTest" (
"_kp_ID" int,
PRIMARY KEY (_kp_ID)
);"""
        service_doc = {"value": [{"name": "RetryTest", "url": "RetryTest"}]}

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(
            side_effect=[
                # Step 1 probe succeeds
                {"scriptResult": {"code": 0, "resultParameter": "ok"}},
                # Step 3 fails then succeeds
                ConnectionError("transient"),
                {"scriptResult": {"code": 0, "resultParameter": ddl_response}},
            ]
        )
        mock_client.get = AsyncMock(return_value=service_doc)

        original_exposed = dict(EXPOSED_TABLES)
        try:
            with (
                patch("filemaker_mcp.tools.schema.odata_client", mock_client),
                patch("filemaker_mcp.tools.schema.asyncio.sleep", new_callable=AsyncMock),
            ):
                set_script_available(None)
                await bootstrap_ddl()

            assert "RetryTest" in TABLES
        finally:
            if "RetryTest" in TABLES:
                del TABLES["RetryTest"]
            EXPOSED_TABLES.clear()
            EXPOSED_TABLES.update(original_exposed)
            set_script_available(None)

    @pytest.mark.asyncio
    async def test_full_failure_does_not_raise(self) -> None:
        """If everything fails, bootstrap logs but doesn't raise."""
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=ConnectionError("down"))
        mock_client.get = AsyncMock(side_effect=ConnectionError("down"))

        with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
            set_script_available(None)
            # Should not raise
            await bootstrap_ddl()

    @pytest.mark.asyncio
    async def test_script_available_skips_probe(self) -> None:
        """If script_available is already False, skip all steps."""
        mock_client = AsyncMock()

        set_script_available(False)
        try:
            with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
                await bootstrap_ddl()

            mock_client.post.assert_not_called()
            mock_client.get.assert_not_called()
        finally:
            set_script_available(None)


class TestFieldNameQuoting:
    """Tests for FM OData field name quoting — wraps all field names in double quotes."""

    # --- $select quoting ---

    def test_select_single_field_no_spaces(self) -> None:
        assert quote_fields_in_select("City") == '"City"'

    def test_select_single_field_with_spaces(self) -> None:
        assert quote_fields_in_select("Customer Name") == '"Customer Name"'

    def test_select_multiple_fields(self) -> None:
        assert quote_fields_in_select("Customer Name,City,Zone") == '"Customer Name","City","Zone"'

    def test_select_already_quoted(self) -> None:
        assert quote_fields_in_select('"Customer Name"') == '"Customer Name"'

    def test_select_mixed_quoted_unquoted(self) -> None:
        assert quote_fields_in_select('"Customer Name",City') == '"Customer Name","City"'

    def test_select_empty(self) -> None:
        assert quote_fields_in_select("") == ""

    def test_select_preserves_whitespace_trim(self) -> None:
        assert quote_fields_in_select("City , Zone") == '"City","Zone"'

    # --- $orderby quoting ---

    def test_orderby_single_field(self) -> None:
        assert quote_fields_in_orderby("City asc") == '"City" asc'

    def test_orderby_field_with_spaces(self) -> None:
        assert quote_fields_in_orderby("Customer Name asc") == '"Customer Name" asc'

    def test_orderby_no_direction(self) -> None:
        assert quote_fields_in_orderby("City") == '"City"'

    def test_orderby_desc(self) -> None:
        assert quote_fields_in_orderby("Date_of_Service desc") == '"Date_of_Service" desc'

    def test_orderby_multiple(self) -> None:
        assert (
            quote_fields_in_orderby("Customer Name asc,City desc")
            == '"Customer Name" asc,"City" desc'
        )

    def test_orderby_empty(self) -> None:
        assert quote_fields_in_orderby("") == ""

    def test_orderby_already_quoted(self) -> None:
        assert quote_fields_in_orderby('"Customer Name" asc') == '"Customer Name" asc'

    # --- $filter quoting ---

    def test_filter_simple_eq(self) -> None:
        assert quote_fields_in_filter("City eq ''") == "\"City\" eq ''"

    def test_filter_field_with_spaces(self) -> None:
        assert quote_fields_in_filter("Customer Name eq 'Smith'") == "\"Customer Name\" eq 'Smith'"

    def test_filter_date_comparison(self) -> None:
        assert (
            quote_fields_in_filter("Date_of_Service ge 2026-02-14")
            == '"Date_of_Service" ge 2026-02-14'
        )

    def test_filter_numeric_comparison(self) -> None:
        assert quote_fields_in_filter("InvoiceTotal gt 500") == '"InvoiceTotal" gt 500'

    def test_filter_and_compound(self) -> None:
        assert (
            quote_fields_in_filter("Zone eq 'A' and Status eq 'Open'")
            == "\"Zone\" eq 'A' and \"Status\" eq 'Open'"
        )

    def test_filter_or_compound(self) -> None:
        assert (
            quote_fields_in_filter("City eq '' or City eq ''")
            == "\"City\" eq '' or \"City\" eq ''"
        )

    def test_filter_range_two_dates(self) -> None:
        assert (
            quote_fields_in_filter(
                "Date_of_Service ge 2026-01-01 and Date_of_Service lt 2026-02-01"
            )
            == '"Date_of_Service" ge 2026-01-01 and "Date_of_Service" lt 2026-02-01'
        )

    def test_filter_empty(self) -> None:
        assert quote_fields_in_filter("") == ""

    def test_filter_already_quoted(self) -> None:
        assert quote_fields_in_filter("\"City\" eq ''") == "\"City\" eq ''"

    def test_filter_pk_field_with_underscore(self) -> None:
        assert quote_fields_in_filter("_kp_CustLoc eq 12345") == '"_kp_CustLoc" eq 12345'

    def test_filter_ne_operator(self) -> None:
        assert quote_fields_in_filter("Status ne 'Closed'") == "\"Status\" ne 'Closed'"

    def test_filter_le_ge_lt_gt_operators(self) -> None:
        assert quote_fields_in_filter("InvoiceTotal le 1000") == '"InvoiceTotal" le 1000'
        assert quote_fields_in_filter("InvoiceTotal lt 1000") == '"InvoiceTotal" lt 1000'

    def test_filter_contains_function(self) -> None:
        """OData contains() function — field name is first arg."""
        assert (
            quote_fields_in_filter("contains(Customer Name,'Smith')")
            == "contains(\"Customer Name\",'Smith')"
        )

    def test_filter_startswith_function(self) -> None:
        assert quote_fields_in_filter("startswith(City,'Cin')") == "startswith(\"City\",'Cin')"


class TestFieldQuotingWiring:
    """Verify quoting is wired into query tools — params sent to odata_client have quoted fields."""

    @pytest.mark.asyncio
    async def test_query_records_quotes_filter(self) -> None:
        from filemaker_mcp.tools.query import query_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": [], "@count": 0})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await query_records("Location", filter="Customer Name eq 'Smith'")

        params = mock_client.get.call_args[1]["params"]
        assert params["$filter"] == "\"Customer Name\" eq 'Smith'"

    @pytest.mark.asyncio
    async def test_query_records_quotes_select(self) -> None:
        from filemaker_mcp.tools.query import query_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": [], "@count": 0})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await query_records("Location", select="Customer Name,City,Zone")

        params = mock_client.get.call_args[1]["params"]
        assert params["$select"] == '"Customer Name","City","Zone"'

    @pytest.mark.asyncio
    async def test_query_records_quotes_orderby(self) -> None:
        from filemaker_mcp.tools.query import query_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": [], "@count": 0})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await query_records("Location", orderby="Customer Name asc")

        params = mock_client.get.call_args[1]["params"]
        assert params["$orderby"] == '"Customer Name" asc'

    @pytest.mark.asyncio
    async def test_count_records_quotes_filter(self) -> None:
        from filemaker_mcp.tools.query import count_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"@count": 5, "value": [{"PrimaryKey": "x"}]})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await count_records("Location", filter="Customer Name eq 'Smith'")

        params = mock_client.get.call_args[1]["params"]
        assert params["$filter"] == "\"Customer Name\" eq 'Smith'"

    @pytest.mark.asyncio
    async def test_get_record_quotes_pk_field(self) -> None:
        from filemaker_mcp.tools.query import get_record

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": [{"_kp_CustLoc": 123}]})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await get_record("Location", "123")

        params = mock_client.get.call_args[1]["params"]
        assert '"PrimaryKey"' in params["$filter"]

    @pytest.mark.asyncio
    async def test_query_records_date_normalization_before_quoting(self) -> None:
        """Date normalization should run BEFORE field quoting."""
        from filemaker_mcp.tools.query import query_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": [], "@count": 0})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await query_records(
                "InHomeInvoiceHeader",
                filter="Date_of_Service eq '2026-02-14'",
            )

        params = mock_client.get.call_args[1]["params"]
        # Date should be normalized (no quotes around date) AND field should be quoted
        assert params["$filter"] == '"Date_of_Service" eq 2026-02-14'


class TestMergeDiscoveredTables:
    """Test dynamic EXPOSED_TABLES merging."""

    def test_adds_new_table(self) -> None:
        """New table gets added with auto-discovered description."""
        original = dict(EXPOSED_TABLES)  # snapshot
        try:
            merge_discovered_tables(["BrandNewTable"])
            assert "BrandNewTable" in EXPOSED_TABLES
            assert "Auto-discovered" in EXPOSED_TABLES["BrandNewTable"]
        finally:
            # Restore original state
            EXPOSED_TABLES.clear()
            EXPOSED_TABLES.update(original)

    def test_preserves_existing_description(self) -> None:
        """Existing curated descriptions are not overwritten."""
        original = dict(EXPOSED_TABLES)
        original_desc = EXPOSED_TABLES["Location"]
        try:
            merge_discovered_tables(["Location", "BrandNewTable"])
            assert EXPOSED_TABLES["Location"] == original_desc
            assert "BrandNewTable" in EXPOSED_TABLES
        finally:
            EXPOSED_TABLES.clear()
            EXPOSED_TABLES.update(original)

    def test_empty_list_is_noop(self) -> None:
        """Empty list doesn't change EXPOSED_TABLES."""
        original = dict(EXPOSED_TABLES)
        merge_discovered_tables([])
        assert original == EXPOSED_TABLES

    def test_multiple_new_tables(self) -> None:
        """Multiple new tables all get added."""
        original = dict(EXPOSED_TABLES)
        try:
            merge_discovered_tables(["TableA", "TableB", "TableC"])
            assert "TableA" in EXPOSED_TABLES
            assert "TableB" in EXPOSED_TABLES
            assert "TableC" in EXPOSED_TABLES
        finally:
            EXPOSED_TABLES.clear()
            EXPOSED_TABLES.update(original)
