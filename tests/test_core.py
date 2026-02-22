"""Tests that run without a live FM server connection.

These validate configuration, parameter validation, and formatting logic.
Integration tests against a live server are in test_integration.py (Phase 2).
"""

import os
from datetime import date, datetime, timedelta
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
    extract_date_range,
    merge_discovered_tables,
    normalize_dates_in_filter,
    quote_fields_in_filter,
    quote_fields_in_orderby,
    quote_fields_in_select,
)
from filemaker_mcp.tools.schema import (
    _discover_tables_from_odata,
    _extract_field_annotations,
    _format_ddl_schema,
    _format_inferred_schema,
    _infer_field_type,
    _parse_metadata_xml,
    _retry_with_backoff,
    bootstrap_ddl,
)


@pytest.fixture()
def populate_exposed_tables():
    """Temporarily populate EXPOSED_TABLES for tests that need table validation to pass."""
    saved = dict(EXPOSED_TABLES)
    EXPOSED_TABLES.update(
        {
            "Location": "Customer locations.",
            "Invoices": "Service invoices.",
            "Customers": "Customer records.",
            "Orders": "Order records.",
            "Drivers": "Service drivers.",
        }
    )
    yield
    EXPOSED_TABLES.clear()
    EXPOSED_TABLES.update(saved)


class TestConfig:
    """Test configuration loading."""

    def test_default_host(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("FM_HOST", raising=False)
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


class TestTenantConfig:
    """Test tenant configuration loading."""

    def _clear_tenant_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Remove any stray *_FM_HOST env vars and prevent .env reload."""
        for key in list(os.environ):
            if key.endswith("_FM_HOST") and key != "FM_HOST":
                monkeypatch.delenv(key, raising=False)
        # Prevent load_dotenv() from reloading .env vars we just cleared
        monkeypatch.setattr("dotenv.load_dotenv", lambda: None)

    def test_load_tenants_from_prefixed_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Prefixed env vars create named tenants."""
        self._clear_tenant_env(monkeypatch)
        monkeypatch.setenv("ACME_FM_HOST", "your-server.example.com")
        monkeypatch.setenv("ACME_FM_DATABASE", "FileMaker")
        monkeypatch.setenv("ACME_FM_USERNAME", "mcp_agent")
        monkeypatch.setenv("ACME_FM_PASSWORD", "secret1")
        monkeypatch.setenv("STAGING_FM_HOST", "staging.example.com")
        monkeypatch.setenv("STAGING_FM_DATABASE", "StagingDB")
        monkeypatch.setenv("STAGING_FM_USERNAME", "mcp_agent")
        monkeypatch.setenv("STAGING_FM_PASSWORD", "secret2")
        monkeypatch.delenv("FM_HOST", raising=False)

        from filemaker_mcp.config import load_tenants

        tenants = load_tenants()
        assert "acme" in tenants
        assert "staging" in tenants
        assert tenants["acme"].host == "your-server.example.com"
        assert tenants["acme"].database == "FileMaker"
        assert tenants["staging"].host == "staging.example.com"
        assert tenants["staging"].database == "StagingDB"

    def test_load_tenants_fallback_to_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When no prefixed vars, fall back to FM_HOST etc. as 'default' tenant."""
        self._clear_tenant_env(monkeypatch)
        monkeypatch.setenv("FM_HOST", "fallback.example.com")
        monkeypatch.setenv("FM_DATABASE", "FallbackDB")
        monkeypatch.setenv("FM_USERNAME", "user")
        monkeypatch.setenv("FM_PASSWORD", "pass")

        from filemaker_mcp.config import load_tenants

        tenants = load_tenants()
        assert "default" in tenants
        assert tenants["default"].host == "fallback.example.com"

    def test_load_tenants_uses_defaults_for_missing_fields(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Missing username/password/ssl/timeout get defaults."""
        self._clear_tenant_env(monkeypatch)
        monkeypatch.setenv("TESTCO_FM_HOST", "test.example.com")
        monkeypatch.setenv("TESTCO_FM_DATABASE", "TestDB")
        monkeypatch.delenv("FM_HOST", raising=False)

        from filemaker_mcp.config import load_tenants

        tenants = load_tenants()
        assert "testco" in tenants
        t = tenants["testco"]
        assert t.username == "mcp_agent"
        assert t.password == ""
        assert t.verify_ssl is True
        assert t.timeout == 60

    def test_default_tenant_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """FM_DEFAULT_TENANT selects the default."""
        self._clear_tenant_env(monkeypatch)
        monkeypatch.setenv("ACME_FM_HOST", "your-server.example.com")
        monkeypatch.setenv("ACME_FM_DATABASE", "FileMaker")
        monkeypatch.setenv("STAGING_FM_HOST", "staging.example.com")
        monkeypatch.setenv("STAGING_FM_DATABASE", "StagingDB")
        monkeypatch.setenv("FM_DEFAULT_TENANT", "staging")
        monkeypatch.delenv("FM_HOST", raising=False)

        from filemaker_mcp.config import get_default_tenant_name, load_tenants

        tenants = load_tenants()
        default = get_default_tenant_name(tenants)
        assert default == "staging"


class TestCredentialProvider:
    """Test CredentialProvider protocol and EnvCredentialProvider."""

    def test_env_provider_loads_prefixed_tenants(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """EnvCredentialProvider discovers tenants from prefixed env vars."""
        # Clear any existing tenant env vars
        for key in list(os.environ):
            if key.endswith("_FM_HOST") and key != "FM_HOST":
                monkeypatch.delenv(key, raising=False)
        monkeypatch.setattr("dotenv.load_dotenv", lambda: None)

        monkeypatch.setenv("ACME_FM_HOST", "your-server.example.com")
        monkeypatch.setenv("ACME_FM_DATABASE", "FileMaker")
        monkeypatch.setenv("ACME_FM_USERNAME", "mcp_agent")
        monkeypatch.setenv("ACME_FM_PASSWORD", "secret1")
        monkeypatch.setenv("FM_DEFAULT_TENANT", "acme")
        monkeypatch.delenv("FM_HOST", raising=False)

        from filemaker_mcp.credential_provider import EnvCredentialProvider

        provider = EnvCredentialProvider()
        assert "acme" in provider.get_tenant_names()
        creds = provider.get_credentials("acme")
        assert creds.host == "your-server.example.com"
        assert creds.database == "FileMaker"
        assert provider.get_default_tenant() == "acme"

    def test_env_provider_fallback_single_tenant(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Falls back to FM_HOST as 'default' tenant when no prefixed vars."""
        for key in list(os.environ):
            if key.endswith("_FM_HOST") and key != "FM_HOST":
                monkeypatch.delenv(key, raising=False)
        monkeypatch.setattr("dotenv.load_dotenv", lambda: None)

        monkeypatch.setenv("FM_HOST", "fallback.example.com")
        monkeypatch.setenv("FM_DATABASE", "FallbackDB")
        monkeypatch.setenv("FM_USERNAME", "user")
        monkeypatch.setenv("FM_PASSWORD", "pass")

        from filemaker_mcp.credential_provider import EnvCredentialProvider

        provider = EnvCredentialProvider()
        assert "default" in provider.get_tenant_names()
        creds = provider.get_credentials("default")
        assert creds.host == "fallback.example.com"

    def test_env_provider_unknown_tenant_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Requesting unknown tenant raises KeyError."""
        for key in list(os.environ):
            if key.endswith("_FM_HOST") and key != "FM_HOST":
                monkeypatch.delenv(key, raising=False)
        monkeypatch.setattr("dotenv.load_dotenv", lambda: None)
        monkeypatch.delenv("FM_HOST", raising=False)

        from filemaker_mcp.credential_provider import EnvCredentialProvider

        provider = EnvCredentialProvider()
        with pytest.raises(KeyError):
            provider.get_credentials("nonexistent")

    def test_env_provider_no_env_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No FM env vars at all returns empty tenant list."""
        for key in list(os.environ):
            if key.endswith("_FM_HOST"):
                monkeypatch.delenv(key, raising=False)
        monkeypatch.setattr("dotenv.load_dotenv", lambda: None)
        monkeypatch.delenv("FM_HOST", raising=False)

        from filemaker_mcp.credential_provider import EnvCredentialProvider

        provider = EnvCredentialProvider()
        assert provider.get_tenant_names() == []
        assert provider.get_default_tenant() == ""


class TestStateClear:
    """Test state clearing functions for tenant switching."""

    def test_clear_tables(self) -> None:
        from filemaker_mcp.ddl import (
            FIELD_ANNOTATIONS,
            TABLES,
            clear_tables,
            is_script_available,
            set_script_available,
            update_annotations,
        )

        # Save original state
        saved_tables = dict(TABLES)
        saved_script = is_script_available()
        saved_ann = dict(FIELD_ANNOTATIONS)

        try:
            TABLES["TestTable"] = {"field": {"type": "text", "tier": "standard"}}
            update_annotations({"TestTable": {"field": {"calculation": True}}})
            set_script_available(True)
            assert "TestTable" in TABLES
            assert "TestTable" in FIELD_ANNOTATIONS
            assert is_script_available() is True

            clear_tables()
            assert len(TABLES) == 0
            assert len(FIELD_ANNOTATIONS) == 0
            assert is_script_available() is None
        finally:
            # Restore original state
            TABLES.clear()
            TABLES.update(saved_tables)
            FIELD_ANNOTATIONS.clear()
            FIELD_ANNOTATIONS.update(saved_ann)
            set_script_available(saved_script)

    def test_clear_exposed_tables(self) -> None:
        from filemaker_mcp.tools.query import EXPOSED_TABLES, clear_exposed_tables

        # Save original state
        saved = dict(EXPOSED_TABLES)

        try:
            EXPOSED_TABLES["TestTable"] = "A test table."
            assert "TestTable" in EXPOSED_TABLES

            clear_exposed_tables()
            assert len(EXPOSED_TABLES) == 0
        finally:
            # Restore original state
            EXPOSED_TABLES.clear()
            EXPOSED_TABLES.update(saved)

    def test_clear_schema_cache(self) -> None:
        from filemaker_mcp.tools.schema import clear_schema_cache

        # Just verify it runs without error — internal cache is private
        clear_schema_cache()


class TestAuthReset:
    """Test OData client credential reset."""

    @pytest.mark.asyncio
    async def test_reset_client_changes_base_url(self) -> None:
        from filemaker_mcp.auth import odata_client, reset_client
        from filemaker_mcp.config import TenantConfig

        new_tenant = TenantConfig(
            name="test",
            host="new-host.example.com",
            database="NewDB",
            username="new_user",
            password="new_pass",
        )
        await reset_client(new_tenant)

        # The internal client should now use the new base URL
        client = await odata_client._get_client()
        assert "new-host.example.com" in str(client.base_url)
        assert "NewDB" in str(client.base_url)

        # Cleanup — restore default client
        await odata_client.close()


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
            "value": [{"Company Name": "Smith", "City": "Springfield", "@odata.etag": "skip_this"}],
            "@odata.count": 1,
        }
        result = _format_records(data, "Location")
        assert "Smith" in result
        assert "Springfield" in result
        assert "@odata.etag" not in result  # Metadata fields filtered out
        assert "1 total records" in result


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
        field_types = {"Company Name": "text", "City": "text", "Zip": "text"}
        result = _format_inferred_schema("Location", field_types)
        assert "Table: Location" in result
        assert "Company Name: text" in result
        assert "3 fields total" in result

    def test_format_schema_pk_marker(self) -> None:
        field_types = {"_kp_LocationID": "number", "Company Name": "text"}
        result = _format_inferred_schema("Location", field_types)
        assert "[PK]" in result
        assert "_kp_LocationID: number [PK]" in result

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
            <Key><PropertyRef Name="_kp_LocationID"/></Key>
            <Property Name="_kp_LocationID" Type="Edm.Int32" Nullable="false"/>
            <Property Name="Company Name" Type="Edm.String"/>
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
        assert "_kp_LocationID" in result
        assert "Company Name" in result

    def test_parse_metadata_without_filter(self) -> None:
        xml = """<?xml version="1.0" encoding="utf-8"?>
        <edmx:Edmx Version="4.01" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
        <edmx:DataServices>
        <Schema Namespace="test" xmlns="http://docs.oasis-open.org/odata/ns/edm">
        <EntityType Name="Location">
            <Key><PropertyRef Name="_kp_LocationID"/></Key>
            <Property Name="_kp_LocationID" Type="Edm.Int32" Nullable="false"/>
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


class TestFieldAnnotations:
    """Test FIELD_ANNOTATIONS store and accessors."""

    def test_update_and_get_annotations(self) -> None:
        from filemaker_mcp.ddl import FIELD_ANNOTATIONS, clear_annotations, update_annotations

        clear_annotations()
        assert FIELD_ANNOTATIONS == {}

        annotations = {
            "Location": {
                "cCalc": {"calculation": True},
                "sSum": {"summary": True},
                "gGlobal": {"global_": True},
                "Name": {"comment": "Customer name"},
            }
        }
        update_annotations(annotations)
        assert "Location" in FIELD_ANNOTATIONS
        assert FIELD_ANNOTATIONS["Location"]["cCalc"]["calculation"] is True
        assert FIELD_ANNOTATIONS["Location"]["Name"].get("comment") == "Customer name"

        clear_annotations()
        assert FIELD_ANNOTATIONS == {}


class TestExtractFieldAnnotations:
    """Test _extract_field_annotations() parsing of $metadata XML."""

    def test_extracts_calculation_annotation(self) -> None:
        xml = """<?xml version="1.0" encoding="utf-8"?>
        <edmx:Edmx Version="4.01" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
        <edmx:DataServices>
        <Schema Namespace="test" xmlns="http://docs.oasis-open.org/odata/ns/edm">
        <EntityType Name="Orders">
            <Key><PropertyRef Name="PK"/></Key>
            <Property Name="PK" Type="Edm.String" Nullable="false"/>
            <Property Name="cTotal" Type="Edm.Int32">
                <Annotation Term="com.filemaker.odata.Calculation" Bool="true"/>
            </Property>
            <Property Name="sBalance" Type="Edm.Int32">
                <Annotation Term="com.filemaker.odata.Summary" Bool="true"/>
            </Property>
            <Property Name="gDate" Type="Edm.DateTimeOffset">
                <Annotation Term="com.filemaker.odata.Global" Bool="true"/>
            </Property>
            <Property Name="Name" Type="Edm.String">
                <Annotation Term="com.filemaker.odata.FMComment" String="Customer name"/>
            </Property>
            <Property Name="Street" Type="Edm.String"/>
        </EntityType>
        </Schema>
        </edmx:DataServices>
        </edmx:Edmx>"""

        result = _extract_field_annotations(xml)
        assert "Orders" in result
        assert result["Orders"]["cTotal"]["calculation"] is True
        assert result["Orders"]["sBalance"]["summary"] is True
        assert result["Orders"]["gDate"]["global_"] is True
        assert result["Orders"]["Name"]["comment"] == "Customer name"
        # Street has no annotations — should not be in result
        assert "Street" not in result["Orders"]

    def test_empty_xml_returns_empty(self) -> None:
        result = _extract_field_annotations("")
        assert result == {}

    def test_malformed_xml_returns_empty(self) -> None:
        result = _extract_field_annotations("<broken xml without closing")
        assert result == {}

    def test_bool_false_annotation_ignored(self) -> None:
        """Bool='false' should not mark field as calculation/summary/global."""
        xml = """<?xml version="1.0" encoding="utf-8"?>
        <edmx:Edmx Version="4.01" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
        <edmx:DataServices>
        <Schema Namespace="test" xmlns="http://docs.oasis-open.org/odata/ns/edm">
        <EntityType Name="Test">
            <Property Name="Name" Type="Edm.String">
                <Annotation Term="com.filemaker.odata.Calculation" Bool="false"/>
            </Property>
        </EntityType>
        </Schema>
        </edmx:DataServices>
        </edmx:Edmx>"""

        result = _extract_field_annotations(xml)
        # Bool="false" should not create an annotation entry
        assert result.get("Test", {}).get("Name") is None

    def test_no_annotations_returns_empty_table(self) -> None:
        xml = """<?xml version="1.0" encoding="utf-8"?>
        <edmx:Edmx Version="4.01" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
        <edmx:DataServices>
        <Schema Namespace="test" xmlns="http://docs.oasis-open.org/odata/ns/edm">
        <EntityType Name="Simple">
            <Key><PropertyRef Name="ID"/></Key>
            <Property Name="ID" Type="Edm.Int32" Nullable="false"/>
            <Property Name="Name" Type="Edm.String"/>
        </EntityType>
        </Schema>
        </edmx:DataServices>
        </edmx:Edmx>"""

        result = _extract_field_annotations(xml)
        assert result.get("Simple", {}) == {}

    def test_multiple_tables(self) -> None:
        xml = """<?xml version="1.0" encoding="utf-8"?>
        <edmx:Edmx Version="4.01" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
        <edmx:DataServices>
        <Schema Namespace="test" xmlns="http://docs.oasis-open.org/odata/ns/edm">
        <EntityType Name="TableA">
            <Property Name="calcA" Type="Edm.String">
                <Annotation Term="com.filemaker.odata.Calculation" Bool="true"/>
            </Property>
        </EntityType>
        <EntityType Name="TableB">
            <Property Name="sumB" Type="Edm.Int32">
                <Annotation Term="com.filemaker.odata.Summary" Bool="true"/>
            </Property>
        </EntityType>
        </Schema>
        </edmx:DataServices>
        </edmx:Edmx>"""

        result = _extract_field_annotations(xml)
        assert result["TableA"]["calcA"]["calculation"] is True
        assert result["TableB"]["sumB"]["summary"] is True

    def test_realistic_fm_xml_with_extra_annotations(self) -> None:
        """Real FM $metadata includes FieldID, Permissions, Index etc. alongside our targets."""
        xml = """<?xml version="1.0" encoding="utf-8"?>
        <edmx:Edmx Version="4.01" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
        <edmx:DataServices>
        <Schema Namespace="test" xmlns="http://docs.oasis-open.org/odata/ns/edm">
        <EntityType Name="Customers_">
            <Key><PropertyRef Name="ROWID"/></Key>
            <Property Name="BalanceConsolidated" Type="Edm.Decimal">
                <Annotation Term="com.filemaker.odata.FieldID" String="FMFID:326418579648"/>
                <Annotation Term="com.filemaker.odata.Calculation" Bool="true"/>
                <Annotation Term="Org.OData.Core.V1.Permissions">
                    <EnumMember>Org.OData.Core.V1.Permission/Read</EnumMember>
                </Annotation>
                <Annotation Term="com.filemaker.odata.FMComment" String="Sum of balances"/>
            </Property>
            <Property Name="Name" Type="Edm.String">
                <Annotation Term="com.filemaker.odata.FieldID" String="FMFID:100"/>
                <Annotation Term="com.filemaker.odata.Index" Bool="true"/>
                <Annotation Term="Org.OData.Core.V1.Permissions">
                    <EnumMember>Org.OData.Core.V1.Permission/Read</EnumMember>
                </Annotation>
            </Property>
            <Property Name="gDriver_ID" Type="Edm.Decimal">
                <Annotation Term="com.filemaker.odata.FieldID" String="FMFID:200"/>
                <Annotation Term="com.filemaker.odata.Global" Bool="true"/>
                <Annotation Term="Org.OData.Core.V1.Permissions">
                    <EnumMember>Org.OData.Core.V1.Permission/Read</EnumMember>
                </Annotation>
                <Annotation Term="com.filemaker.odata.FMComment" String="Global used to search"/>
            </Property>
        </EntityType>
        </Schema>
        </edmx:DataServices>
        </edmx:Edmx>"""

        result = _extract_field_annotations(xml)
        # EntityType Name="Customers_" gets stripped to "Customers"
        cust = result["Customers"]
        # BalanceConsolidated: calculation + comment, ignores FieldID/Permissions
        assert cust["BalanceConsolidated"]["calculation"] is True
        assert cust["BalanceConsolidated"]["comment"] == "Sum of balances"
        assert "summary" not in cust["BalanceConsolidated"]
        # Name: only has Index (not in our map) — should NOT appear
        assert "Name" not in cust
        # gDriver_ID: global + comment
        assert cust["gDriver_ID"]["global_"] is True
        assert cust["gDriver_ID"]["comment"] == "Global used to search"

    def test_strips_trailing_underscore_from_entity_name(self) -> None:
        """FM $metadata uses 'Orders_' but DDL uses 'Orders'. Must strip."""
        xml = """<?xml version="1.0" encoding="utf-8"?>
        <edmx:Edmx Version="4.01" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
        <edmx:DataServices>
        <Schema Namespace="test" xmlns="http://docs.oasis-open.org/odata/ns/edm">
        <EntityType Name="Orders_">
            <Property Name="cTotal" Type="Edm.Int32">
                <Annotation Term="com.filemaker.odata.Calculation" Bool="true"/>
            </Property>
        </EntityType>
        </Schema>
        </edmx:DataServices>
        </edmx:Edmx>"""

        result = _extract_field_annotations(xml)
        # Should be "Orders" (stripped), not "Orders_"
        assert "Orders" in result
        assert "Orders_" not in result
        assert result["Orders"]["cTotal"]["calculation"] is True


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


class TestODataPatch:
    """Test FMODataClient.patch() method."""

    @pytest.mark.asyncio
    async def test_patch_sends_request(self) -> None:
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_response.raise_for_status = MagicMock()

        mock_http = MagicMock()
        mock_http.patch = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        result = await client.patch(
            "TBL_DDL_Context('123')",
            json_body={"Context": "updated hint"},
        )
        mock_http.patch.assert_awaited_once_with(
            "/TBL_DDL_Context('123')", json={"Context": "updated hint"}
        )
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_patch_204_returns_empty_dict(self) -> None:
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_response.raise_for_status = MagicMock()

        mock_http = MagicMock()
        mock_http.patch = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        result = await client.patch("TBL_DDL_Context('456')", json_body={"Context": "x"})
        assert result == {}

    @pytest.mark.asyncio
    async def test_patch_401_raises_permissionerror(self) -> None:
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Unauthorized",
            request=MagicMock(),
            response=mock_response,
        )

        mock_http = MagicMock()
        mock_http.patch = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        with pytest.raises(PermissionError):
            await client.patch("TBL_DDL_Context('123')", json_body={})

    @pytest.mark.asyncio
    async def test_patch_404_raises_valueerror(self) -> None:
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not Found",
            request=MagicMock(),
            response=mock_response,
        )

        mock_http = MagicMock()
        mock_http.patch = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        with pytest.raises(ValueError, match="not found"):
            await client.patch("TBL_DDL_Context('bad')", json_body={})


class TestODataDelete:
    """Test FMODataClient.delete() method."""

    @pytest.mark.asyncio
    async def test_delete_sends_request(self) -> None:
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_response.raise_for_status = MagicMock()

        mock_http = MagicMock()
        mock_http.delete = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        result = await client.delete("TBL_DDL_Context('123')")
        mock_http.delete.assert_awaited_once_with("/TBL_DDL_Context('123')")
        assert result == {}

    @pytest.mark.asyncio
    async def test_delete_401_raises_permissionerror(self) -> None:
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Unauthorized",
            request=MagicMock(),
            response=mock_response,
        )

        mock_http = MagicMock()
        mock_http.delete = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        with pytest.raises(PermissionError):
            await client.delete("TBL_DDL_Context('123')")

    @pytest.mark.asyncio
    async def test_delete_404_raises_valueerror(self) -> None:
        client = FMODataClient()
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not Found",
            request=MagicMock(),
            response=mock_response,
        )

        mock_http = MagicMock()
        mock_http.delete = AsyncMock(return_value=mock_response)
        mock_http.is_closed = False
        client._client = mock_http

        with pytest.raises(ValueError, match="not found"):
            await client.delete("TBL_DDL_Context('bad')")


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
        update_tables(
            {"Location": {"_kp_LocationID": {"type": "number", "tier": "key", "pk": True}}}
        )
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
"_kp_LocationID" int,
"Company Name" varchar(255),
"Map" varbinary(4096),
"Timestamp_Create" datetime,
PRIMARY KEY (_kp_LocationID)
);"""
        result = parse_ddl(ddl)
        assert "Location" in result
        loc = result["Location"]
        assert loc["_kp_LocationID"]["type"] == "number"
        assert loc["_kp_LocationID"]["pk"] is True
        assert loc["_kp_LocationID"]["tier"] == "key"
        assert loc["Company Name"]["type"] == "text"
        assert loc["Map"]["type"] == "binary"
        assert loc["Timestamp_Create"]["type"] == "datetime"

    def test_parse_foreign_key(self) -> None:
        ddl = """CREATE TABLE "Orders" (
"PrimaryKey" varchar(255),
"_kf_LocationID" varchar(255),
PRIMARY KEY (PrimaryKey),
FOREIGN KEY (_kf_LocationID) REFERENCES Location(_kp_LocationID)
);"""
        result = parse_ddl(ddl)
        assert result["Orders"]["_kf_LocationID"]["fk"] is True
        assert result["Orders"]["_kf_LocationID"]["tier"] == "key"
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

    def test_annotation_overrides_name_heuristic(self) -> None:
        """Annotations take priority over name-based heuristics."""
        from filemaker_mcp.ddl import FieldAnnotations

        annotations: dict[str, FieldAnnotations] = {
            "cTotal": {"calculation": True},
            "sBalance": {"summary": True},
            "gFlag": {"global_": True},
        }

        ddl = """CREATE TABLE "Test" (
"cTotal" int,
"sBalance" int,
"gFlag" varchar(255),
"Name" varchar(255),
PRIMARY KEY (cTotal)
);"""
        result = parse_ddl(ddl, annotations={"Test": annotations})
        t = result["Test"]
        assert t["cTotal"]["tier"] == "internal"
        assert t["sBalance"]["tier"] == "internal"
        assert t["gFlag"]["tier"] == "internal"
        assert t["Name"]["tier"] == "standard"

    def test_no_annotations_preserves_heuristics(self) -> None:
        """Without annotations, behavior is identical to before."""
        ddl = """CREATE TABLE "Test" (
"_kp_ID" int,
"gGlobal" varchar(255),
"Name" varchar(255),
PRIMARY KEY (_kp_ID)
);"""
        result = parse_ddl(ddl)
        t = result["Test"]
        assert t["_kp_ID"]["tier"] == "key"
        assert t["gGlobal"]["tier"] == "internal"
        assert t["Name"]["tier"] == "standard"

    def test_annotation_does_not_override_key_tier(self) -> None:
        """PK/FK fields stay as 'key' even if annotated as calculation."""
        from filemaker_mcp.ddl import FieldAnnotations

        annotations: dict[str, FieldAnnotations] = {
            "_kp_ID": {"calculation": True},
        }
        ddl = """CREATE TABLE "Test" (
"_kp_ID" int,
PRIMARY KEY (_kp_ID)
);"""
        result = parse_ddl(ddl, annotations={"Test": annotations})
        assert result["Test"]["_kp_ID"]["tier"] == "key"

    def test_comment_annotation_populates_description(self) -> None:
        """FMComment annotation sets the description field in FieldDef."""
        from filemaker_mcp.ddl import FieldAnnotations

        annotations: dict[str, FieldAnnotations] = {
            "Name": {"comment": "Customer full name"},
        }
        ddl = """CREATE TABLE "Test" (
"Name" varchar(255),
PRIMARY KEY (Name)
);"""
        result = parse_ddl(ddl, annotations={"Test": annotations})
        assert result["Test"]["Name"].get("description") == "Customer full name"


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

        await client.get("Location", params={"$filter": "City eq 'Springfield'"})

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

        await client.get("Location", params={"$select": "Name,City,Region"})

        called_url = mock_http.get.call_args[0][0]
        assert "Name,City,Region" in called_url, f"Commas encoded: {called_url}"
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

        await client.get("Location", params={"$filter": "City eq 'Springfield'"})

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


@pytest.mark.usefixtures("populate_exposed_tables")
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
            await count_records("Invoices")

        params = mock_client.get.call_args[1].get("params", {})
        assert params.get("$select") == '"PrimaryKey"'

    @pytest.mark.asyncio
    async def test_count_uses_dynamic_pk_field(self) -> None:
        """Count query should use get_pk_field() instead of hardcoded PrimaryKey."""
        from filemaker_mcp.tools.query import count_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"@count": 10, "value": [{"_kp_LocationID": 42}]})

        with (
            patch("filemaker_mcp.tools.query.odata_client", mock_client),
            patch("filemaker_mcp.tools.query.get_pk_field", return_value="_kp_LocationID"),
        ):
            await count_records("Location")

        params = mock_client.get.call_args[1].get("params", {})
        assert params.get("$select") == '"_kp_LocationID"'

    @pytest.mark.asyncio
    async def test_count_with_filter(self) -> None:
        """Count with filter includes $filter in request."""
        from filemaker_mcp.tools.query import count_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"@count": 6, "value": [{"PrimaryKey": "x"}]})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            result = await count_records(
                "Invoices",
                filter="ServiceDate eq 2026-02-14",
            )

        params = mock_client.get.call_args[1].get("params", {})
        assert "ServiceDate" in params.get("$filter", "")
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


@pytest.mark.usefixtures("populate_exposed_tables")
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
                filter="City eq 'Springfield'",
                select="Name,City",
                top=10,
                skip=5,
                orderby="Name asc",
                count=True,
            )

        params = mock_client.get.call_args[1]["params"]
        assert params["$filter"] == "\"City\" eq 'Springfield'"
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
                    {"Name": "Smith", "City": "Springfield"},
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


@pytest.mark.usefixtures("populate_exposed_tables")
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
        mock_client.get = AsyncMock(return_value={"value": [{"PrimaryKey": 123, "Name": "Smith"}]})

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
        mock_client.get = AsyncMock(return_value={"value": [{"_kp_LocationID": 42}]})

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
            await get_record("Invoices", "ABC-123")

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
                        "_kp_LocationID": 100,
                        "Company Name": "Acme Corp",
                        "City": "Springfield",
                        "@odata.etag": "skip",
                        "@id": "http://example.com/Location(100)",
                        "@editLink": "Location(100)",
                    }
                ]
            }
        )

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            result = await get_record("Location", "100")

        assert "Acme Corp" in result
        assert "Springfield" in result
        assert "@odata.etag" not in result
        assert "@id" not in result
        assert "@editLink" not in result


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


@pytest.mark.usefixtures("populate_exposed_tables")
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
        f = "ServiceDate eq 2026-02-14"
        assert normalize_dates_in_filter(f) == f

    def test_bare_iso_date_range_unchanged(self) -> None:
        f = "ServiceDate ge 2026-01-01 and ServiceDate lt 2026-02-01"
        assert normalize_dates_in_filter(f) == f

    def test_non_date_string_unchanged(self) -> None:
        f = "City eq 'Springfield'"
        assert normalize_dates_in_filter(f) == f

    def test_empty_filter_unchanged(self) -> None:
        assert normalize_dates_in_filter("") == ""

    # --- Quoted ISO dates ---

    def test_single_quoted_iso_date(self) -> None:
        assert (
            normalize_dates_in_filter("ServiceDate eq '2026-02-14'") == "ServiceDate eq 2026-02-14"
        )

    def test_double_quoted_iso_date(self) -> None:
        assert (
            normalize_dates_in_filter('ServiceDate eq "2026-02-14"') == "ServiceDate eq 2026-02-14"
        )

    # --- ISO timestamps stripped to date ---

    def test_iso_timestamp_stripped(self) -> None:
        assert (
            normalize_dates_in_filter("ServiceDate eq 2026-02-14T00:00:00")
            == "ServiceDate eq 2026-02-14"
        )

    def test_iso_timestamp_with_utc_stripped(self) -> None:
        assert (
            normalize_dates_in_filter("ServiceDate ge 2026-02-14T00:00:00Z")
            == "ServiceDate ge 2026-02-14"
        )

    def test_iso_timestamp_with_offset_stripped(self) -> None:
        assert (
            normalize_dates_in_filter("ServiceDate eq 2026-02-14T14:30:00-05:00")
            == "ServiceDate eq 2026-02-14"
        )

    # --- US format dates ---

    def test_us_date_mm_dd_yyyy(self) -> None:
        assert normalize_dates_in_filter("ServiceDate eq 02/15/2026") == "ServiceDate eq 2026-02-15"

    def test_us_date_m_d_yyyy(self) -> None:
        assert normalize_dates_in_filter("ServiceDate eq 2/5/2026") == "ServiceDate eq 2026-02-05"

    def test_us_date_with_time(self) -> None:
        assert (
            normalize_dates_in_filter("ServiceDate eq 2/15/2026 3:45:00 PM")
            == "ServiceDate eq 2026-02-15"
        )

    def test_quoted_us_date(self) -> None:
        assert (
            normalize_dates_in_filter("ServiceDate eq '02/15/2026'") == "ServiceDate eq 2026-02-15"
        )

    # --- Combined filters ---

    def test_mixed_date_and_string(self) -> None:
        assert (
            normalize_dates_in_filter("ServiceDate ge '2026-02-01' and City eq 'Springfield'")
            == "ServiceDate ge 2026-02-01 and City eq 'Springfield'"
        )

    def test_two_dates_in_range(self) -> None:
        assert (
            normalize_dates_in_filter("ServiceDate ge '2026-01-01' and ServiceDate lt '2026-02-01'")
            == "ServiceDate ge 2026-01-01 and ServiceDate lt 2026-02-01"
        )


class TestSchemaDateHints:
    """Tests for date format hints in schema output."""

    def test_ddl_schema_datetime_field_has_hint(self) -> None:
        fields = {
            "ServiceDate": {"type": "datetime", "tier": "key"},
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
        field_types = {"ServiceDate": "datetime", "City": "text"}
        result = _format_inferred_schema("Test", field_types)
        assert "ServiceDate: datetime" in result
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
            normalize_dates_in_filter("ServiceDate eq '2026-02-14T14:30:00Z'")
            == "ServiceDate eq 2026-02-14"
        )

    def test_positive_timezone_offset(self) -> None:
        assert (
            normalize_dates_in_filter("ServiceDate eq 2026-02-14T14:30:00+05:30")
            == "ServiceDate eq 2026-02-14"
        )

    def test_fractional_seconds(self) -> None:
        """JavaScript toISOString() format."""
        assert (
            normalize_dates_in_filter("ServiceDate eq 2026-02-14T14:30:00.123Z")
            == "ServiceDate eq 2026-02-14"
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
    """Test 5-step DDL bootstrap: OData -> DDL script -> intersect -> annotations -> parse."""

    @pytest.mark.asyncio
    async def test_odata_discovery_failure_stops_early(self) -> None:
        """If OData discovery fails, bootstrap stops — no tables known."""
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=ConnectionError("down"))

        with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
            set_script_available(None)
            await bootstrap_ddl()

        # Script should not have been called
        mock_client.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_script_not_found_falls_back_to_odata_list(self) -> None:
        """If DDL script doesn't exist, uses OData list (includes TOs)."""
        service_doc = {
            "value": [
                {"name": "Orders", "url": "Orders"},
                {"name": "Orders Filtered", "url": "Orders Filtered"},
            ]
        }
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=service_doc)
        mock_client.post = AsyncMock(side_effect=ValueError("not found"))

        original_exposed = dict(EXPOSED_TABLES)
        try:
            with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
                set_script_available(None)
                await bootstrap_ddl()

            assert is_script_available() is False
            # Both tables exposed (no TO filtering without DDL script)
            assert "Orders" in EXPOSED_TABLES
            assert "Orders Filtered" in EXPOSED_TABLES
        finally:
            EXPOSED_TABLES.clear()
            EXPOSED_TABLES.update(original_exposed)
            set_script_available(None)

    @pytest.mark.asyncio
    async def test_intersect_filters_tos(self) -> None:
        """DDL base tables intersected with OData permissions filters out TOs."""
        # OData returns 3 EntitySets (1 base + 2 TOs)
        service_doc = {
            "value": [
                {"name": "Orders", "url": "Orders"},
                {"name": "Orders Filtered", "url": "Orders Filtered"},
                {"name": "Orders Global", "url": "Orders Global"},
            ]
        }
        # DDL script returns only the base table
        ddl_response = """CREATE TABLE "Orders" (
"_kp_OrderID" int,
"status" varchar(255),
PRIMARY KEY (_kp_OrderID)
);"""
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=[service_doc, {}])
        mock_client.post = AsyncMock(
            return_value={"scriptResult": {"code": 0, "resultParameter": ddl_response}}
        )

        original_exposed = dict(EXPOSED_TABLES)
        try:
            with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
                set_script_available(None)
                await bootstrap_ddl()

            # Only base table should be exposed
            assert "Orders" in EXPOSED_TABLES
            assert "Orders Filtered" not in EXPOSED_TABLES
            assert "Orders Global" not in EXPOSED_TABLES
            assert "Orders" in TABLES
        finally:
            EXPOSED_TABLES.clear()
            EXPOSED_TABLES.update(original_exposed)
            TABLES.pop("Orders", None)
            set_script_available(None)

    @pytest.mark.asyncio
    async def test_intersect_filters_no_access_tables(self) -> None:
        """Base tables not in OData permissions are filtered out."""
        # OData only permits Orders
        service_doc = {"value": [{"name": "Orders", "url": "Orders"}]}
        # DDL script returns Orders + Secret (not OData-permitted)
        ddl_response = """CREATE TABLE "Orders" (
"_kp_OrderID" int,
PRIMARY KEY (_kp_OrderID)
);
CREATE TABLE "Secret" (
"_kp_ID" int,
PRIMARY KEY (_kp_ID)
);"""
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=[service_doc, {}])
        mock_client.post = AsyncMock(
            return_value={"scriptResult": {"code": 0, "resultParameter": ddl_response}}
        )

        original_exposed = dict(EXPOSED_TABLES)
        try:
            with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
                set_script_available(None)
                await bootstrap_ddl()

            assert "Orders" in EXPOSED_TABLES
            assert "Secret" not in EXPOSED_TABLES
            # Secret should not be in TABLES cache either
            assert "Secret" not in TABLES
        finally:
            EXPOSED_TABLES.clear()
            EXPOSED_TABLES.update(original_exposed)
            TABLES.pop("Orders", None)
            set_script_available(None)

    @pytest.mark.asyncio
    async def test_script_unavailable_skips_ddl(self) -> None:
        """If script_available is already False, falls back to OData list."""
        service_doc = {"value": [{"name": "Orders", "url": "Orders"}]}
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=service_doc)

        original_exposed = dict(EXPOSED_TABLES)
        set_script_available(False)
        try:
            with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
                await bootstrap_ddl()

            mock_client.post.assert_not_called()
            assert "Orders" in EXPOSED_TABLES
        finally:
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
            await bootstrap_ddl()

    @pytest.mark.asyncio
    async def test_annotations_applied_to_base_tables(self) -> None:
        """Annotations from $metadata are applied when parsing DDL."""
        from filemaker_mcp.ddl import FIELD_ANNOTATIONS, clear_annotations

        service_doc = {"value": [{"name": "Orders", "url": "Orders"}]}
        metadata_xml = """<?xml version="1.0" encoding="utf-8"?>
        <edmx:Edmx Version="4.01" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
        <edmx:DataServices>
        <Schema Namespace="test" xmlns="http://docs.oasis-open.org/odata/ns/edm">
        <EntityType Name="Orders">
            <Key><PropertyRef Name="PK"/></Key>
            <Property Name="PK" Type="Edm.String" Nullable="false"/>
            <Property Name="cTotal" Type="Edm.Int32">
                <Annotation Term="com.filemaker.odata.Calculation" Bool="true"/>
            </Property>
        </EntityType>
        </Schema>
        </edmx:DataServices>
        </edmx:Edmx>"""

        ddl_response = """CREATE TABLE "Orders" (
"PK" varchar(255),
"cTotal" int,
PRIMARY KEY (PK)
);"""

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(
            return_value={"scriptResult": {"code": 0, "resultParameter": ddl_response}}
        )
        mock_client.get = AsyncMock(side_effect=[service_doc, {"metadata_xml": metadata_xml}])

        original_exposed = dict(EXPOSED_TABLES)
        clear_annotations()
        try:
            with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
                set_script_available(None)
                await bootstrap_ddl()

            assert "Orders" in FIELD_ANNOTATIONS
            assert FIELD_ANNOTATIONS["Orders"]["cTotal"]["calculation"] is True
            assert TABLES["Orders"]["cTotal"]["tier"] == "internal"
        finally:
            EXPOSED_TABLES.clear()
            EXPOSED_TABLES.update(original_exposed)
            clear_annotations()
            TABLES.pop("Orders", None)
            set_script_available(None)

    @pytest.mark.asyncio
    async def test_metadata_failure_degrades_gracefully(self) -> None:
        """If $metadata fetch fails, bootstrap continues with name heuristics only."""
        from filemaker_mcp.ddl import FIELD_ANNOTATIONS, clear_annotations

        service_doc = {"value": [{"name": "Orders", "url": "Orders"}]}
        ddl_response = """CREATE TABLE "Orders" (
"PK" varchar(255),
"cTotal" int,
PRIMARY KEY (PK)
);"""

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(
            return_value={"scriptResult": {"code": 0, "resultParameter": ddl_response}}
        )
        mock_client.get = AsyncMock(side_effect=[service_doc, ConnectionError("timeout")])

        original_exposed = dict(EXPOSED_TABLES)
        clear_annotations()
        try:
            with patch("filemaker_mcp.tools.schema.odata_client", mock_client):
                set_script_available(None)
                await bootstrap_ddl()

            assert FIELD_ANNOTATIONS == {}
            assert TABLES["Orders"]["cTotal"]["tier"] == "standard"
        finally:
            EXPOSED_TABLES.clear()
            EXPOSED_TABLES.update(original_exposed)
            clear_annotations()
            TABLES.pop("Orders", None)
            set_script_available(None)


class TestFieldNameQuoting:
    """Tests for FM OData field name quoting — wraps all field names in double quotes."""

    # --- $select quoting ---

    def test_select_single_field_no_spaces(self) -> None:
        assert quote_fields_in_select("City") == '"City"'

    def test_select_single_field_with_spaces(self) -> None:
        assert quote_fields_in_select("Company Name") == '"Company Name"'

    def test_select_multiple_fields(self) -> None:
        result = quote_fields_in_select("Company Name,City,Region")
        assert result == '"Company Name","City","Region"'

    def test_select_already_quoted(self) -> None:
        assert quote_fields_in_select('"Company Name"') == '"Company Name"'

    def test_select_mixed_quoted_unquoted(self) -> None:
        assert quote_fields_in_select('"Company Name",City') == '"Company Name","City"'

    def test_select_empty(self) -> None:
        assert quote_fields_in_select("") == ""

    def test_select_preserves_whitespace_trim(self) -> None:
        assert quote_fields_in_select("City , Region") == '"City","Region"'

    # --- $orderby quoting ---

    def test_orderby_single_field(self) -> None:
        assert quote_fields_in_orderby("City asc") == '"City" asc'

    def test_orderby_field_with_spaces(self) -> None:
        assert quote_fields_in_orderby("Company Name asc") == '"Company Name" asc'

    def test_orderby_no_direction(self) -> None:
        assert quote_fields_in_orderby("City") == '"City"'

    def test_orderby_desc(self) -> None:
        assert quote_fields_in_orderby("ServiceDate desc") == '"ServiceDate" desc'

    def test_orderby_multiple(self) -> None:
        assert (
            quote_fields_in_orderby("Company Name asc,City desc")
            == '"Company Name" asc,"City" desc'
        )

    def test_orderby_empty(self) -> None:
        assert quote_fields_in_orderby("") == ""

    def test_orderby_already_quoted(self) -> None:
        assert quote_fields_in_orderby('"Company Name" asc') == '"Company Name" asc'

    # --- $filter quoting ---

    def test_filter_simple_eq(self) -> None:
        assert quote_fields_in_filter("City eq 'Springfield'") == "\"City\" eq 'Springfield'"

    def test_filter_field_with_spaces(self) -> None:
        assert quote_fields_in_filter("Company Name eq 'Smith'") == "\"Company Name\" eq 'Smith'"

    def test_filter_date_comparison(self) -> None:
        assert quote_fields_in_filter("ServiceDate ge 2026-02-14") == '"ServiceDate" ge 2026-02-14'

    def test_filter_numeric_comparison(self) -> None:
        assert quote_fields_in_filter("Amount gt 500") == '"Amount" gt 500'

    def test_filter_and_compound(self) -> None:
        assert (
            quote_fields_in_filter("Region eq 'A' and Status eq 'Open'")
            == "\"Region\" eq 'A' and \"Status\" eq 'Open'"
        )

    def test_filter_or_compound(self) -> None:
        assert (
            quote_fields_in_filter("City eq 'Springfield' or City eq ''")
            == "\"City\" eq 'Springfield' or \"City\" eq ''"
        )

    def test_filter_range_two_dates(self) -> None:
        assert (
            quote_fields_in_filter("ServiceDate ge 2026-01-01 and ServiceDate lt 2026-02-01")
            == '"ServiceDate" ge 2026-01-01 and "ServiceDate" lt 2026-02-01'
        )

    def test_filter_empty(self) -> None:
        assert quote_fields_in_filter("") == ""

    def test_filter_already_quoted(self) -> None:
        assert quote_fields_in_filter("\"City\" eq 'Springfield'") == "\"City\" eq 'Springfield'"

    def test_filter_pk_field_with_underscore(self) -> None:
        assert quote_fields_in_filter("_kp_LocationID eq 12345") == '"_kp_LocationID" eq 12345'

    def test_filter_ne_operator(self) -> None:
        assert quote_fields_in_filter("Status ne 'Closed'") == "\"Status\" ne 'Closed'"

    def test_filter_le_ge_lt_gt_operators(self) -> None:
        assert quote_fields_in_filter("Amount le 1000") == '"Amount" le 1000'
        assert quote_fields_in_filter("Amount lt 1000") == '"Amount" lt 1000'

    def test_filter_contains_function(self) -> None:
        """OData contains() function — field name is first arg."""
        assert (
            quote_fields_in_filter("contains(Company Name,'Smith')")
            == "contains(\"Company Name\",'Smith')"
        )

    def test_filter_startswith_function(self) -> None:
        assert quote_fields_in_filter("startswith(City,'Cin')") == "startswith(\"City\",'Cin')"


@pytest.mark.usefixtures("populate_exposed_tables")
class TestFieldQuotingWiring:
    """Verify quoting is wired into query tools — params sent to odata_client have quoted fields."""

    @pytest.mark.asyncio
    async def test_query_records_quotes_filter(self) -> None:
        from filemaker_mcp.tools.query import query_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": [], "@count": 0})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await query_records("Location", filter="Company Name eq 'Smith'")

        params = mock_client.get.call_args[1]["params"]
        assert params["$filter"] == "\"Company Name\" eq 'Smith'"

    @pytest.mark.asyncio
    async def test_query_records_quotes_select(self) -> None:
        from filemaker_mcp.tools.query import query_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": [], "@count": 0})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await query_records("Location", select="Company Name,City,Region")

        params = mock_client.get.call_args[1]["params"]
        assert params["$select"] == '"Company Name","City","Region"'

    @pytest.mark.asyncio
    async def test_query_records_quotes_orderby(self) -> None:
        from filemaker_mcp.tools.query import query_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": [], "@count": 0})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await query_records("Location", orderby="Company Name asc")

        params = mock_client.get.call_args[1]["params"]
        assert params["$orderby"] == '"Company Name" asc'

    @pytest.mark.asyncio
    async def test_count_records_quotes_filter(self) -> None:
        from filemaker_mcp.tools.query import count_records

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"@count": 5, "value": [{"PrimaryKey": "x"}]})

        with patch("filemaker_mcp.tools.query.odata_client", mock_client):
            await count_records("Location", filter="Company Name eq 'Smith'")

        params = mock_client.get.call_args[1]["params"]
        assert params["$filter"] == "\"Company Name\" eq 'Smith'"

    @pytest.mark.asyncio
    async def test_get_record_quotes_pk_field(self) -> None:
        from filemaker_mcp.tools.query import get_record

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"value": [{"PrimaryKey": 123}]})

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
                "Invoices",
                filter="ServiceDate eq '2026-02-14'",
            )

        params = mock_client.get.call_args[1]["params"]
        # Date should be normalized (no quotes around date) AND field should be quoted
        assert params["$filter"] == '"ServiceDate" eq 2026-02-14'


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
        try:
            EXPOSED_TABLES["Location"] = "Customer locations."
            original_desc = EXPOSED_TABLES["Location"]
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


class TestTenantSwitching:
    """Test tenant switching logic."""

    def test_list_tenants_shows_configured(self) -> None:
        from filemaker_mcp.config import TenantConfig
        from filemaker_mcp.tools.tenant import _active_tenant, _tenants, list_tenants

        _tenants.clear()
        _tenants["acme"] = TenantConfig(
            name="acme", host="your-server.example.com", database="FileMaker"
        )
        _tenants["staging"] = TenantConfig(
            name="staging", host="staging.example.com", database="StagingDB"
        )
        _active_tenant["name"] = "acme"

        result = list_tenants()
        assert "acme" in result
        assert "staging" in result
        assert "active" in result.lower()

    @pytest.mark.asyncio
    async def test_use_tenant_switches(self) -> None:
        from filemaker_mcp.config import TenantConfig
        from filemaker_mcp.tools.tenant import _active_tenant, _tenants, use_tenant

        _tenants.clear()
        _tenants["acme"] = TenantConfig(
            name="acme",
            host="your-server.example.com",
            database="FileMaker",
            username="user1",
            password="pass1",
        )
        _tenants["staging"] = TenantConfig(
            name="staging",
            host="staging.example.com",
            database="StagingDB",
            username="user2",
            password="pass2",
        )
        _active_tenant["name"] = "acme"

        with (
            patch("filemaker_mcp.tools.tenant.reset_client", new_callable=AsyncMock) as mock_reset,
            patch("filemaker_mcp.tools.tenant.bootstrap_ddl", new_callable=AsyncMock) as mock_boot,
            patch("filemaker_mcp.tools.tenant.clear_tables") as mock_ct,
            patch("filemaker_mcp.tools.tenant.clear_exposed_tables") as mock_cet,
            patch("filemaker_mcp.tools.tenant.clear_schema_cache") as mock_csc,
        ):
            result = await use_tenant("staging")

        assert _active_tenant["name"] == "staging"
        mock_reset.assert_called_once()
        mock_boot.assert_called_once()
        mock_ct.assert_called_once()
        mock_cet.assert_called_once()
        mock_csc.assert_called_once()
        assert "staging" in result.lower()

    @pytest.mark.asyncio
    async def test_use_tenant_unknown_name(self) -> None:
        from filemaker_mcp.tools.tenant import _tenants, use_tenant

        _tenants.clear()
        result = await use_tenant("nonexistent")
        assert "not found" in result.lower() or "unknown" in result.lower()

    @pytest.mark.asyncio
    async def test_use_tenant_already_active(self) -> None:
        from filemaker_mcp.config import TenantConfig
        from filemaker_mcp.tools.tenant import _active_tenant, _tenants, use_tenant

        _tenants.clear()
        _tenants["acme"] = TenantConfig(
            name="acme",
            host="your-server.example.com",
            database="FileMaker",
        )
        _active_tenant["name"] = "acme"

        with patch("filemaker_mcp.tools.tenant.reset_client", new_callable=AsyncMock) as mock_reset:
            result = await use_tenant("acme")

        # Should not reset if already active
        mock_reset.assert_not_called()
        assert "already" in result.lower()

    def test_init_tenants_with_provider(self) -> None:
        """init_tenants accepts a CredentialProvider."""
        from filemaker_mcp.config import TenantConfig
        from filemaker_mcp.credential_provider import CredentialProvider
        from filemaker_mcp.tools.tenant import _active_tenant, _tenants, init_tenants

        class MockProvider:
            def get_tenant_names(self) -> list[str]:
                return ["mock_tenant"]

            def get_credentials(self, tenant: str) -> TenantConfig:
                return TenantConfig(name="mock_tenant", host="mock.example.com", database="MockDB")

            def get_default_tenant(self) -> str:
                return "mock_tenant"

        assert isinstance(MockProvider(), CredentialProvider)

        saved_tenants = dict(_tenants)
        saved_active = dict(_active_tenant)
        try:
            _tenants.clear()
            provider = MockProvider()
            result = init_tenants(provider)
            assert result == "mock_tenant"
            assert "mock_tenant" in _tenants
            assert _tenants["mock_tenant"].host == "mock.example.com"
        finally:
            _tenants.clear()
            _tenants.update(saved_tenants)
            _active_tenant.clear()
            _active_tenant.update(saved_active)


class TestTenantIntegration:
    """End-to-end tenant switching test."""

    @pytest.mark.asyncio
    async def test_full_tenant_switch_flow(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Load tenants, switch, verify state is clean."""
        from filemaker_mcp.ddl import TABLES
        from filemaker_mcp.tools.query import EXPOSED_TABLES
        from filemaker_mcp.tools.tenant import (
            _active_tenant,
            _tenants,
            get_active_tenant,
            init_tenants,
            use_tenant,
        )

        # Save original state
        saved_tables = dict(TABLES)
        saved_exposed = dict(EXPOSED_TABLES)
        saved_tenants = dict(_tenants)
        saved_active = dict(_active_tenant)

        # Clear stray prefixed env vars
        for key in list(os.environ):
            if key.endswith("_FM_HOST") and key != "FM_HOST":
                monkeypatch.delenv(key, raising=False)
        # Prevent load_dotenv() from reloading .env vars we just cleared
        monkeypatch.setattr("dotenv.load_dotenv", lambda: None)

        try:
            # Set up two tenants
            monkeypatch.setenv("ACME_FM_HOST", "your-server.example.com")
            monkeypatch.setenv("ACME_FM_DATABASE", "FileMaker")
            monkeypatch.setenv("ACME_FM_USERNAME", "user1")
            monkeypatch.setenv("ACME_FM_PASSWORD", "pass1")
            monkeypatch.setenv("STAGING_FM_HOST", "staging.example.com")
            monkeypatch.setenv("STAGING_FM_DATABASE", "StagingDB")
            monkeypatch.setenv("STAGING_FM_USERNAME", "user2")
            monkeypatch.setenv("STAGING_FM_PASSWORD", "pass2")
            monkeypatch.setenv("FM_DEFAULT_TENANT", "acme")
            monkeypatch.delenv("FM_HOST", raising=False)

            # Init tenants
            default = init_tenants()
            assert default == "acme"
            assert len(_tenants) == 2

            # Simulate some cached state from acme
            TABLES["Location"] = {"field": {"type": "text", "tier": "standard"}}
            EXPOSED_TABLES["Location"] = "A table."

            # Switch to staging (mock bootstrap to avoid real HTTP)
            with (
                patch(
                    "filemaker_mcp.tools.tenant.reset_client",
                    new_callable=AsyncMock,
                ),
                patch(
                    "filemaker_mcp.tools.tenant.bootstrap_ddl",
                    new_callable=AsyncMock,
                ),
            ):
                result = await use_tenant("staging")

            # State should be clean
            assert _active_tenant["name"] == "staging"
            assert len(TABLES) == 0  # cleared
            assert len(EXPOSED_TABLES) == 0  # cleared
            assert "staging" in result
            tenant = get_active_tenant()
            assert tenant is not None
            assert tenant.host == "staging.example.com"
        finally:
            # Restore original state
            TABLES.clear()
            TABLES.update(saved_tables)
            EXPOSED_TABLES.clear()
            EXPOSED_TABLES.update(saved_exposed)
            _tenants.clear()
            _tenants.update(saved_tenants)
            _active_tenant.clear()
            _active_tenant.update(saved_active)


class TestDDLContext:
    """Test DDL_CONTEXT cache management."""

    def test_update_context_stores_records(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT, update_context

        update_context(
            [
                {
                    "TableName": "Orders",
                    "FieldName": "Commercial",
                    "ContextType": "field_values",
                    "Context": "Boolean: 1=yes, empty/0=no",
                },
            ]
        )
        assert DDL_CONTEXT[("Orders", "Commercial", "field_values")] == {
            "context": "Boolean: 1=yes, empty/0=no",
        }

    def test_clear_context(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT, clear_context, update_context

        update_context(
            [
                {
                    "TableName": "Orders",
                    "FieldName": "",
                    "ContextType": "syntax_rule",
                    "Context": "ne not supported",
                },
            ]
        )
        assert len(DDL_CONTEXT) > 0
        clear_context()
        assert len(DDL_CONTEXT) == 0

    def test_update_context_deduplicates(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT, clear_context, update_context

        clear_context()
        update_context(
            [
                {
                    "TableName": "Orders",
                    "FieldName": "Status",
                    "ContextType": "field_values",
                    "Context": "old hint",
                },
            ]
        )
        update_context(
            [
                {
                    "TableName": "Orders",
                    "FieldName": "Status",
                    "ContextType": "field_values",
                    "Context": "new hint",
                },
            ]
        )
        assert DDL_CONTEXT[("Orders", "Status", "field_values")]["context"] == "new hint"
        assert len(DDL_CONTEXT) == 1

    def test_get_field_context(self) -> None:
        from filemaker_mcp.ddl import clear_context, get_field_context, update_context

        clear_context()
        update_context(
            [
                {
                    "TableName": "Orders",
                    "FieldName": "Commercial",
                    "ContextType": "field_values",
                    "Context": "Boolean: 1=yes",
                },
            ]
        )
        assert get_field_context("Orders", "Commercial") == "Boolean: 1=yes"
        assert get_field_context("Orders", "Nonexistent") is None

    def test_get_table_context(self) -> None:
        from filemaker_mcp.ddl import clear_context, get_table_context, update_context

        clear_context()
        update_context(
            [
                {
                    "TableName": "Orders",
                    "FieldName": "",
                    "ContextType": "syntax_rule",
                    "Context": "ne not supported",
                },
            ]
        )
        result = get_table_context("Orders")
        assert len(result) == 1
        assert result[0]["context"] == "ne not supported"

    def test_remove_context_existing(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT, clear_context, remove_context, update_context

        clear_context()
        update_context(
            [
                {
                    "TableName": "Orders",
                    "FieldName": "Commercial",
                    "ContextType": "field_values",
                    "Context": "Boolean: 1=yes",
                },
            ]
        )
        assert ("Orders", "Commercial", "field_values") in DDL_CONTEXT
        assert remove_context("Orders", "Commercial", "field_values") is True
        assert ("Orders", "Commercial", "field_values") not in DDL_CONTEXT

    def test_remove_context_missing(self) -> None:
        from filemaker_mcp.ddl import clear_context, remove_context

        clear_context()
        assert remove_context("Nonexistent", "field") is False


class TestGetContextValue:
    """Test generic DDL Context value lookup."""

    def test_returns_value_when_exists(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT, get_context_value

        saved = dict(DDL_CONTEXT)
        DDL_CONTEXT[("MyTable", "", "report_select")] = {"context": "Field1,Field2,Field3"}
        try:
            result = get_context_value("MyTable", "report_select")
            assert result == "Field1,Field2,Field3"
        finally:
            DDL_CONTEXT.clear()
            DDL_CONTEXT.update(saved)

    def test_returns_none_when_missing(self) -> None:
        from filemaker_mcp.ddl import get_context_value

        result = get_context_value("NonexistentTable", "report_select")
        assert result is None

    def test_returns_field_level_context(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT, get_context_value

        saved = dict(DDL_CONTEXT)
        DDL_CONTEXT[("MyTable", "MyField", "syntax_rule")] = {"context": "always uppercase"}
        try:
            result = get_context_value("MyTable", "syntax_rule", field="MyField")
            assert result == "always uppercase"
        finally:
            DDL_CONTEXT.clear()
            DDL_CONTEXT.update(saved)


class TestGetDateFields:
    """Test date field discovery from TABLES."""

    def test_returns_datetime_fields(self) -> None:
        from filemaker_mcp.ddl import TABLES, get_date_fields

        saved = dict(TABLES)
        TABLES["TestTable"] = {
            "Name": {"type": "text", "tier": "standard"},
            "ServiceDate": {"type": "datetime", "tier": "standard"},
            "Created": {"type": "datetime", "tier": "internal"},
            "Amount": {"type": "number", "tier": "standard"},
        }
        try:
            result = get_date_fields("TestTable")
            assert sorted(result) == ["Created", "ServiceDate"]
        finally:
            TABLES.clear()
            TABLES.update(saved)

    def test_returns_date_fields(self) -> None:
        from filemaker_mcp.ddl import TABLES, get_date_fields

        saved = dict(TABLES)
        TABLES["TestTable"] = {
            "OrderDate": {"type": "date", "tier": "standard"},
            "Name": {"type": "text", "tier": "standard"},
        }
        try:
            result = get_date_fields("TestTable")
            assert result == ["OrderDate"]
        finally:
            TABLES.clear()
            TABLES.update(saved)

    def test_returns_empty_for_unknown_table(self) -> None:
        from filemaker_mcp.ddl import get_date_fields

        assert get_date_fields("NonexistentTable") == []

    def test_returns_empty_when_no_date_fields(self) -> None:
        from filemaker_mcp.ddl import TABLES, get_date_fields

        saved = dict(TABLES)
        TABLES["TextOnly"] = {
            "Name": {"type": "text", "tier": "standard"},
            "Code": {"type": "text", "tier": "key"},
        }
        try:
            assert get_date_fields("TextOnly") == []
        finally:
            TABLES.clear()
            TABLES.update(saved)


class TestGetAllDateFields:
    """Test cross-table date field discovery."""

    def test_returns_tables_with_date_fields(self) -> None:
        from filemaker_mcp.ddl import TABLES, get_all_date_fields

        saved = dict(TABLES)
        TABLES.clear()
        TABLES["Invoices"] = {
            "ServiceDate": {"type": "datetime", "tier": "standard"},
            "Name": {"type": "text", "tier": "standard"},
        }
        TABLES["Drivers"] = {
            "DriverName": {"type": "text", "tier": "standard"},
        }
        TABLES["Orders"] = {
            "Order_Date": {"type": "date", "tier": "standard"},
            "Created": {"type": "datetime", "tier": "internal"},
        }
        try:
            result = get_all_date_fields()
            assert "Invoices" in result
            assert "Orders" in result
            assert "Drivers" not in result
            assert result["Invoices"] == ["ServiceDate"]
            assert sorted(result["Orders"]) == ["Created", "Order_Date"]
        finally:
            TABLES.clear()
            TABLES.update(saved)

    def test_returns_empty_when_no_tables(self) -> None:
        from filemaker_mcp.ddl import TABLES, get_all_date_fields

        saved = dict(TABLES)
        TABLES.clear()
        try:
            assert get_all_date_fields() == {}
        finally:
            TABLES.clear()
            TABLES.update(saved)


class TestLoadContext:
    """Test bootstrap step 6: load DDL context from FM."""

    @pytest.mark.asyncio
    async def test_load_context_populates_cache(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT, clear_context
        from filemaker_mcp.tools.schema import _load_context

        clear_context()
        with patch("filemaker_mcp.tools.schema.odata_client") as mock_client:
            mock_client.get = AsyncMock(
                return_value={
                    "value": [
                        {
                            "TableName": "Orders",
                            "FieldName": "Commercial",
                            "ContextType": "field_values",
                            "Context": "Boolean: 1=yes",
                        },
                        {
                            "TableName": "Orders",
                            "FieldName": "",
                            "ContextType": "syntax_rule",
                            "Context": "ne not supported",
                        },
                    ],
                }
            )
            await _load_context()

        assert DDL_CONTEXT[("Orders", "Commercial", "field_values")]["context"] == "Boolean: 1=yes"
        assert DDL_CONTEXT[("Orders", "", "syntax_rule")]["context"] == "ne not supported"

    @pytest.mark.asyncio
    async def test_load_context_table_not_found_is_silent(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT, clear_context
        from filemaker_mcp.tools.schema import _load_context

        clear_context()
        with patch("filemaker_mcp.tools.schema.odata_client") as mock_client:
            mock_client.get = AsyncMock(side_effect=ValueError("not found"))
            await _load_context()

        assert len(DDL_CONTEXT) == 0

    @pytest.mark.asyncio
    async def test_load_context_network_error_is_silent(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT, clear_context
        from filemaker_mcp.tools.schema import _load_context

        clear_context()
        with patch("filemaker_mcp.tools.schema.odata_client") as mock_client:
            mock_client.get = AsyncMock(side_effect=ConnectionError("timeout"))
            await _load_context()

        assert len(DDL_CONTEXT) == 0

    @pytest.mark.asyncio
    async def test_bootstrap_calls_load_context(self) -> None:
        """Verify bootstrap_ddl() calls _load_context as step 6."""
        with (
            patch("filemaker_mcp.tools.schema._retry_with_backoff") as mock_retry,
            patch("filemaker_mcp.tools.schema._fetch_base_table_ddl") as mock_ddl,
            patch("filemaker_mcp.tools.schema._load_context") as mock_load,
            patch("filemaker_mcp.tools.schema.odata_client") as mock_client,
            patch("filemaker_mcp.tools.schema.is_script_available", return_value=True),
        ):
            # Step 1: OData discover returns tables
            mock_retry.return_value = ["Orders", "Location"]
            # Step 2: DDL script returns CREATE TABLEs
            mock_ddl.return_value = 'CREATE TABLE "Orders" (\n);\nCREATE TABLE "Location" (\n);'
            # Step 4: $metadata
            mock_client.get = AsyncMock(return_value={"metadata_xml": ""})

            from filemaker_mcp.tools.schema import bootstrap_ddl

            await bootstrap_ddl()

            mock_load.assert_called_once()


class TestContextInSchema:
    """Test that DDL context hints appear in schema output."""

    def test_field_context_appears_as_comment(self) -> None:
        from filemaker_mcp.ddl import clear_context, update_context
        from filemaker_mcp.tools.schema import _format_ddl_schema

        clear_context()
        update_context(
            [
                {
                    "TableName": "Orders",
                    "FieldName": "Commercial",
                    "ContextType": "field_values",
                    "Context": "Boolean: 1=yes, empty/0=no",
                },
            ]
        )
        fields = {
            "Commercial": {"type": "text", "tier": "standard"},
            "Status": {"type": "text", "tier": "standard"},
        }
        result = _format_ddl_schema("Orders", fields)
        assert "-- Boolean: 1=yes, empty/0=no" in result
        assert "Status: text" in result

    def test_table_level_context_appears_in_header(self) -> None:
        from filemaker_mcp.ddl import clear_context, update_context
        from filemaker_mcp.tools.schema import _format_ddl_schema

        clear_context()
        update_context(
            [
                {
                    "TableName": "Orders",
                    "FieldName": "",
                    "ContextType": "syntax_rule",
                    "Context": "ne operator not supported",
                },
            ]
        )
        fields = {"Status": {"type": "text", "tier": "standard"}}
        result = _format_ddl_schema("Orders", fields)
        assert "ne operator not supported" in result

    def test_no_context_no_change(self) -> None:
        from filemaker_mcp.ddl import clear_context
        from filemaker_mcp.tools.schema import _format_ddl_schema

        clear_context()
        fields = {"Status": {"type": "text", "tier": "standard"}}
        result = _format_ddl_schema("Orders", fields)
        assert "  -- " not in result
        assert "Note:" not in result


class TestSaveContext:
    """Test save_context tool — writes operational learnings to FM."""

    @pytest.mark.asyncio
    async def test_save_new_context_posts_record(self) -> None:
        from filemaker_mcp.tools.context import save_context

        with patch("filemaker_mcp.tools.context.odata_client") as mock_client:
            mock_client.get = AsyncMock(return_value={"value": []})
            mock_client.post = AsyncMock(return_value={"value": [{"PrimaryKey": "42"}]})
            result = await save_context(
                table_name="Orders",
                context="Boolean: 1=yes, empty/0=no",
                field_name="Commercial",
                context_type="field_values",
            )
        assert "Created" in result
        mock_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_existing_context_patches_record(self) -> None:
        from filemaker_mcp.tools.context import save_context

        with patch("filemaker_mcp.tools.context.odata_client") as mock_client:
            # Existing record found
            existing_record = {
                "PrimaryKey": "99",
                "TableName": "Orders",
                "FieldName": "Commercial",
                "ContextType": "field_values",
                "Context": "old hint",
            }
            mock_client.get = AsyncMock(
                return_value={"value": [existing_record]},
            )
            mock_client.patch = AsyncMock(return_value={})
            result = await save_context(
                table_name="Orders",
                context="Boolean: 1=yes, empty/0=no",
                field_name="Commercial",
                context_type="field_values",
            )
        assert "Updated" in result
        mock_client.patch.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_context_updates_local_cache(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT, clear_context
        from filemaker_mcp.tools.context import save_context

        clear_context()
        with patch("filemaker_mcp.tools.context.odata_client") as mock_client:
            mock_client.get = AsyncMock(return_value={"value": []})
            mock_client.post = AsyncMock(return_value={"value": [{"PrimaryKey": "1"}]})
            await save_context(
                table_name="Orders",
                context="Boolean: 1=yes",
                field_name="Commercial",
                context_type="field_values",
            )
        assert DDL_CONTEXT[("Orders", "Commercial", "field_values")]["context"] == "Boolean: 1=yes"

    @pytest.mark.asyncio
    async def test_save_context_permission_error(self) -> None:
        from filemaker_mcp.tools.context import save_context

        with patch("filemaker_mcp.tools.context.odata_client") as mock_client:
            mock_client.get = AsyncMock(return_value={"value": []})
            mock_client.post = AsyncMock(side_effect=PermissionError("no write access"))
            result = await save_context(
                table_name="Orders",
                context="hint",
            )
        assert "Error" in result
        assert "write access" in result.lower() or "permission" in result.lower()


class TestDeleteContext:
    """Test delete_context tool — removes stale learnings from FM."""

    @pytest.mark.asyncio
    async def test_delete_existing_record(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT, clear_context, update_context
        from filemaker_mcp.tools.context import delete_context

        clear_context()
        update_context(
            [
                {
                    "TableName": "Orders",
                    "FieldName": "Commercial",
                    "ContextType": "field_values",
                    "Context": "Boolean: 1=yes",
                },
            ]
        )
        with patch("filemaker_mcp.tools.context.odata_client") as mock_client:
            mock_client.get = AsyncMock(
                return_value={
                    "value": [
                        {
                            "PrimaryKey": "42",
                            "TableName": "Orders",
                            "FieldName": "Commercial",
                            "ContextType": "field_values",
                        }
                    ]
                },
            )
            mock_client.delete = AsyncMock(return_value={})
            result = await delete_context(
                table_name="Orders",
                field_name="Commercial",
                context_type="field_values",
            )
        assert "Deleted" in result
        mock_client.delete.assert_called_once()
        assert ("Orders", "Commercial", "field_values") not in DDL_CONTEXT

    @pytest.mark.asyncio
    async def test_delete_nonexistent_record(self) -> None:
        from filemaker_mcp.tools.context import delete_context

        with patch("filemaker_mcp.tools.context.odata_client") as mock_client:
            mock_client.get = AsyncMock(return_value={"value": []})
            result = await delete_context(
                table_name="Nonexistent",
                field_name="field",
                context_type="field_values",
            )
        assert "nothing to delete" in result.lower()

    @pytest.mark.asyncio
    async def test_delete_permission_error(self) -> None:
        from filemaker_mcp.tools.context import delete_context

        with patch("filemaker_mcp.tools.context.odata_client") as mock_client:
            mock_client.get = AsyncMock(
                return_value={"value": [{"PrimaryKey": "42"}]},
            )
            mock_client.delete = AsyncMock(
                side_effect=PermissionError("no delete access"),
            )
            result = await delete_context(
                table_name="Orders",
                field_name="Commercial",
            )
        assert "Error" in result
        assert "delete access" in result.lower() or "permission" in result.lower()

    @pytest.mark.asyncio
    async def test_delete_removes_from_local_cache(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT, clear_context, update_context
        from filemaker_mcp.tools.context import delete_context

        clear_context()
        update_context(
            [
                {
                    "TableName": "TestTable",
                    "FieldName": "TestField",
                    "ContextType": "syntax_rule",
                    "Context": "some rule",
                },
            ]
        )
        assert ("TestTable", "TestField", "syntax_rule") in DDL_CONTEXT
        with patch("filemaker_mcp.tools.context.odata_client") as mock_client:
            mock_client.get = AsyncMock(
                return_value={"value": [{"PrimaryKey": "99"}]},
            )
            mock_client.delete = AsyncMock(return_value={})
            await delete_context(
                table_name="TestTable",
                field_name="TestField",
                context_type="syntax_rule",
            )
        assert ("TestTable", "TestField", "syntax_rule") not in DDL_CONTEXT


class TestCacheConfig:
    """Test get_cache_config() helper."""

    def setup_method(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT

        DDL_CONTEXT.clear()

    def test_date_key_config(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT, get_cache_config

        DDL_CONTEXT[("Invoices", "ServiceDate", "cache_config")] = {"context": "date_key"}
        config = get_cache_config("Invoices")
        assert config == {"mode": "date_range", "date_field": "ServiceDate"}

    def test_cache_all_config(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT, get_cache_config

        DDL_CONTEXT[("Drivers", "", "cache_config")] = {"context": "cache_all"}
        config = get_cache_config("Drivers")
        assert config == {"mode": "cache_all", "date_field": ""}

    def test_no_config(self) -> None:
        from filemaker_mcp.ddl import get_cache_config

        config = get_cache_config("UnknownTable")
        assert config is None

    def test_ignores_other_context_types(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT, get_cache_config

        DDL_CONTEXT[("Orders", "Order_Date", "field_values")] = {"context": "Date field for orders"}
        config = get_cache_config("Orders")
        assert config is None


class TestGetPKField:
    """Test get_pk_field() helper."""

    def setup_method(self) -> None:
        TABLES.clear()

    def test_finds_pk(self) -> None:
        from filemaker_mcp.ddl import get_pk_field

        TABLES["Orders"] = {
            "PrimaryKey": {"type": "text", "tier": "key", "pk": True},
            "Order_Date": {"type": "date", "tier": "standard"},
        }
        assert get_pk_field("Orders") == "PrimaryKey"

    def test_finds_alternate_pk(self) -> None:
        from filemaker_mcp.ddl import get_pk_field

        TABLES["Pickups"] = {
            "kp_pickup_id": {"type": "number", "tier": "key", "pk": True},
            "Status": {"type": "text", "tier": "standard"},
        }
        assert get_pk_field("Pickups") == "kp_pickup_id"

    def test_no_pk_returns_primarykey(self) -> None:
        from filemaker_mcp.ddl import get_pk_field

        TABLES["SomeTable"] = {
            "Name": {"type": "text", "tier": "standard"},
        }
        assert get_pk_field("SomeTable") == "PrimaryKey"

    def test_unknown_table(self) -> None:
        from filemaker_mcp.ddl import get_pk_field

        assert get_pk_field("Nonexistent") == "PrimaryKey"


class TestExtractDateRange:
    """Test date range extraction from OData $filter strings."""

    def test_ge_and_le(self) -> None:
        from filemaker_mcp.tools.query import extract_date_range

        result = extract_date_range(
            "ServiceDate ge 2025-01-01 and ServiceDate le 2025-12-31",
            "ServiceDate",
        )
        assert result == ("2025-01-01", "2025-12-31")

    def test_ge_only(self) -> None:
        from filemaker_mcp.tools.query import extract_date_range

        result = extract_date_range(
            "ServiceDate ge 2025-06-01",
            "ServiceDate",
        )
        assert result == ("2025-06-01", None)

    def test_le_only(self) -> None:
        from filemaker_mcp.tools.query import extract_date_range

        result = extract_date_range(
            "ServiceDate le 2025-12-31",
            "ServiceDate",
        )
        assert result == (None, "2025-12-31")

    def test_gt_and_lt(self) -> None:
        from filemaker_mcp.tools.query import extract_date_range

        result = extract_date_range(
            "ServiceDate gt 2025-01-01 and ServiceDate lt 2025-06-30",
            "ServiceDate",
        )
        assert result == ("2025-01-01", "2025-06-30")

    def test_no_date_field(self) -> None:
        from filemaker_mcp.tools.query import extract_date_range

        result = extract_date_range("Region eq 'A'", "ServiceDate")
        assert result == (None, None)

    def test_empty_filter(self) -> None:
        from filemaker_mcp.tools.query import extract_date_range

        result = extract_date_range("", "ServiceDate")
        assert result == (None, None)

    def test_mixed_filter(self) -> None:
        from filemaker_mcp.tools.query import extract_date_range

        result = extract_date_range(
            "ServiceDate ge 2025-01-01 and Region eq 'A' and ServiceDate le 2025-03-31",
            "ServiceDate",
        )
        assert result == ("2025-01-01", "2025-03-31")

    def test_quoted_field_name(self) -> None:
        from filemaker_mcp.tools.query import extract_date_range

        result = extract_date_range(
            '"ServiceDate" ge 2025-01-01',
            "ServiceDate",
        )
        assert result == ("2025-01-01", None)

    def test_eq_sets_both_bounds(self) -> None:
        """eq X should be treated as ge X and le X for caching."""
        from filemaker_mcp.tools.query import extract_date_range

        result = extract_date_range(
            "ServiceDate eq 2026-02-20",
            "ServiceDate",
        )
        assert result == ("2026-02-20", "2026-02-20")

    def test_eq_quoted_field(self) -> None:
        """eq with quoted field name should also work."""
        from filemaker_mcp.tools.query import extract_date_range

        result = extract_date_range(
            '"ServiceDate" eq 2026-02-20',
            "ServiceDate",
        )
        assert result == ("2026-02-20", "2026-02-20")

    def test_eq_with_other_filters(self) -> None:
        """eq date with non-date filters should extract the date."""
        from filemaker_mcp.tools.query import extract_date_range

        result = extract_date_range(
            "ServiceDate eq 2026-02-20 and Region eq 'A'",
            "ServiceDate",
        )
        assert result == ("2026-02-20", "2026-02-20")


class TestQueryRecordsCache:
    """Test that query_records uses table cache."""

    def setup_method(self) -> None:
        from filemaker_mcp.tools.analytics import _table_cache

        _table_cache.clear()

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("populate_exposed_tables")
    async def test_cache_miss_fetches_and_stores(self) -> None:
        """First query to a date-range table fetches from FM and caches."""
        from filemaker_mcp.tools.analytics import _table_cache
        from filemaker_mcp.tools.query import query_records

        mock_response = {
            "value": [
                {"PrimaryKey": "1", "ServiceDate": "2025-03-15", "Technician": "AR1"},
                {"PrimaryKey": "2", "ServiceDate": "2025-03-20", "Technician": "GR1"},
            ],
            "@count": 2,
        }
        mock_cache_config = {"mode": "date_range", "date_field": "ServiceDate"}
        mock_ddl = {
            "Invoices": {
                "PrimaryKey": {"type": "text", "pk": True},
                "ServiceDate": {"type": "date", "tier": "key"},
            }
        }

        with (
            patch("filemaker_mcp.tools.query.odata_client") as mock_client,
            patch("filemaker_mcp.tools.query.get_cache_config", return_value=mock_cache_config),
            patch("filemaker_mcp.tools.query.get_pk_field", return_value="PrimaryKey"),
            patch("filemaker_mcp.tools.query.TABLES", mock_ddl),
        ):
            mock_client.get = AsyncMock(return_value=mock_response)
            result = await query_records(
                table="Invoices",
                filter="ServiceDate ge 2025-03-01 and ServiceDate le 2025-03-31",
            )

        assert "Invoices" in _table_cache
        assert _table_cache["Invoices"].row_count == 2
        assert "AR1" in result

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("populate_exposed_tables")
    async def test_cache_hit_skips_fm(self) -> None:
        """Subsequent query within cached range doesn't call FM."""
        import pandas as pd_test

        from filemaker_mcp.tools.analytics import DatasetEntry, _table_cache
        from filemaker_mcp.tools.query import query_records

        _table_cache["Invoices"] = DatasetEntry(
            df=pd_test.DataFrame(
                {
                    "PrimaryKey": ["1", "2", "3"],
                    "ServiceDate": pd_test.to_datetime(["2025-03-15", "2025-03-20", "2025-03-25"]),
                    "Technician": ["AR1", "GR1", "AR1"],
                    "Region": ["A", "B", "A"],
                }
            ),
            table="Invoices",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 19),
            row_count=3,
            date_field="ServiceDate",
            date_min=date(2025, 3, 1),
            date_max=date(2025, 3, 31),
            pk_field="PrimaryKey",
        )
        mock_cache_config = {"mode": "date_range", "date_field": "ServiceDate"}

        with (
            patch("filemaker_mcp.tools.query.odata_client") as mock_client,
            patch("filemaker_mcp.tools.query.get_cache_config", return_value=mock_cache_config),
            patch("filemaker_mcp.tools.query.get_pk_field", return_value="PrimaryKey"),
        ):
            mock_client.get = AsyncMock()
            result = await query_records(
                table="Invoices",
                filter="ServiceDate ge 2025-03-10 and ServiceDate le 2025-03-28",
                top=10,
            )
            mock_client.get.assert_not_called()

        assert "AR1" in result

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("populate_exposed_tables")
    async def test_today_refresh_refetches_today(self) -> None:
        """When requested range includes today, always re-fetch today's data."""
        import pandas as pd_test

        from filemaker_mcp.tools.analytics import DatasetEntry, _table_cache
        from filemaker_mcp.tools.query import query_records

        today = date.today()
        yesterday = today - timedelta(days=1)
        week_ago = today - timedelta(days=7)

        # Pre-populate cache covering past week through today
        _table_cache["Invoices"] = DatasetEntry(
            df=pd_test.DataFrame(
                {
                    "PrimaryKey": ["1", "2"],
                    "ServiceDate": pd_test.to_datetime([yesterday.isoformat(), today.isoformat()]),
                    "Technician": ["AR1", "GR1"],
                }
            ),
            table="Invoices",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 19),
            row_count=2,
            date_field="ServiceDate",
            date_min=week_ago,
            date_max=today,
            pk_field="PrimaryKey",
        )
        mock_cache_config = {"mode": "date_range", "date_field": "ServiceDate"}

        # FM returns updated data for today (new record added)
        mock_response = {
            "value": [
                {"PrimaryKey": "2", "ServiceDate": today.isoformat(), "Technician": "GR1"},
                {"PrimaryKey": "3", "ServiceDate": today.isoformat(), "Technician": "NEW"},
            ],
            "@count": 2,
        }
        mock_ddl = {
            "Invoices": {
                "PrimaryKey": {"type": "text", "pk": True},
                "ServiceDate": {"type": "date", "tier": "key"},
            }
        }

        with (
            patch("filemaker_mcp.tools.query.odata_client") as mock_client,
            patch("filemaker_mcp.tools.query.get_cache_config", return_value=mock_cache_config),
            patch("filemaker_mcp.tools.query.get_pk_field", return_value="PrimaryKey"),
            patch("filemaker_mcp.tools.query.TABLES", mock_ddl),
        ):
            mock_client.get = AsyncMock(return_value=mock_response)
            result = await query_records(
                table="Invoices",
                filter=(
                    f"ServiceDate ge {week_ago.isoformat()} and ServiceDate le {today.isoformat()}"
                ),
                top=10,
            )
            # Should have called FM to re-fetch today despite full cache coverage
            mock_client.get.assert_called()

        # New record from today should be merged into cache
        assert _table_cache["Invoices"].row_count == 3
        assert "NEW" in result

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("populate_exposed_tables")
    async def test_historical_range_no_today_refresh(self) -> None:
        """When requested range is entirely in the past, no re-fetch."""
        import pandas as pd_test

        from filemaker_mcp.tools.analytics import DatasetEntry, _table_cache
        from filemaker_mcp.tools.query import query_records

        today = date.today()
        month_ago = today - timedelta(days=30)
        week_ago = today - timedelta(days=7)

        _table_cache["Invoices"] = DatasetEntry(
            df=pd_test.DataFrame(
                {
                    "PrimaryKey": ["1", "2"],
                    "ServiceDate": pd_test.to_datetime(
                        [month_ago.isoformat(), week_ago.isoformat()]
                    ),
                    "Technician": ["AR1", "GR1"],
                }
            ),
            table="Invoices",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 19),
            row_count=2,
            date_field="ServiceDate",
            date_min=month_ago,
            date_max=week_ago,
            pk_field="PrimaryKey",
        )
        mock_cache_config = {"mode": "date_range", "date_field": "ServiceDate"}

        with (
            patch("filemaker_mcp.tools.query.odata_client") as mock_client,
            patch("filemaker_mcp.tools.query.get_cache_config", return_value=mock_cache_config),
            patch("filemaker_mcp.tools.query.get_pk_field", return_value="PrimaryKey"),
        ):
            mock_client.get = AsyncMock()
            await query_records(
                table="Invoices",
                filter=(
                    f"ServiceDate ge {month_ago.isoformat()}"
                    f" and ServiceDate le {week_ago.isoformat()}"
                ),
                top=10,
            )
            # Fully cached historical range — no FM call
            mock_client.get.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("populate_exposed_tables")
    async def test_open_ended_range_refreshes_today(self) -> None:
        """Open-ended right bound (no max date) implies today — triggers refresh."""
        import pandas as pd_test

        from filemaker_mcp.tools.analytics import DatasetEntry, _table_cache
        from filemaker_mcp.tools.query import query_records

        today = date.today()
        week_ago = today - timedelta(days=7)

        _table_cache["Invoices"] = DatasetEntry(
            df=pd_test.DataFrame(
                {
                    "PrimaryKey": ["1"],
                    "ServiceDate": pd_test.to_datetime([week_ago.isoformat()]),
                    "Technician": ["AR1"],
                }
            ),
            table="Invoices",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 19),
            row_count=1,
            date_field="ServiceDate",
            date_min=week_ago,
            date_max=today,
            pk_field="PrimaryKey",
        )
        mock_cache_config = {"mode": "date_range", "date_field": "ServiceDate"}
        mock_response = {
            "value": [
                {"PrimaryKey": "2", "ServiceDate": today.isoformat(), "Technician": "NEW"},
            ],
            "@count": 1,
        }
        mock_ddl = {
            "Invoices": {
                "PrimaryKey": {"type": "text", "pk": True},
                "ServiceDate": {"type": "date", "tier": "key"},
            }
        }

        with (
            patch("filemaker_mcp.tools.query.odata_client") as mock_client,
            patch("filemaker_mcp.tools.query.get_cache_config", return_value=mock_cache_config),
            patch("filemaker_mcp.tools.query.get_pk_field", return_value="PrimaryKey"),
            patch("filemaker_mcp.tools.query.TABLES", mock_ddl),
        ):
            mock_client.get = AsyncMock(return_value=mock_response)
            # No upper bound — implicitly includes today
            await query_records(
                table="Invoices",
                filter=f"ServiceDate ge {week_ago.isoformat()}",
                top=10,
            )
            mock_client.get.assert_called()

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("populate_exposed_tables")
    async def test_no_cache_config_passes_through(self) -> None:
        """Tables without cache_config skip caching entirely."""
        from filemaker_mcp.tools.analytics import _table_cache
        from filemaker_mcp.tools.query import query_records

        mock_response = {"value": [{"Name": "Test"}], "@count": 1}

        with (
            patch("filemaker_mcp.tools.query.odata_client") as mock_client,
            patch("filemaker_mcp.tools.query.get_cache_config", return_value=None),
        ):
            mock_client.get = AsyncMock(return_value=mock_response)
            await query_records(table="Invoices", filter="Name eq 'Test'")

        assert "Invoices" not in _table_cache

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("populate_exposed_tables")
    async def test_cache_hit_applies_select(self) -> None:
        """Cache-hit path should only return columns listed in $select."""
        import pandas as pd_test

        from filemaker_mcp.tools.analytics import DatasetEntry, _table_cache
        from filemaker_mcp.tools.query import query_records

        _table_cache["Invoices"] = DatasetEntry(
            df=pd_test.DataFrame(
                {
                    "PrimaryKey": ["1", "2"],
                    "ServiceDate": pd_test.to_datetime(["2025-03-15", "2025-03-20"]),
                    "Technician": ["AR1", "GR1"],
                    "Region": ["A", "B"],
                    "Amount": [100.0, 200.0],
                    "City": ["Springfield", ""],
                }
            ),
            table="Invoices",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 19),
            row_count=2,
            date_field="ServiceDate",
            date_min=date(2025, 3, 1),
            date_max=date(2025, 3, 31),
            pk_field="PrimaryKey",
        )
        mock_cache_config = {"mode": "date_range", "date_field": "ServiceDate"}

        with (
            patch("filemaker_mcp.tools.query.odata_client") as mock_client,
            patch("filemaker_mcp.tools.query.get_cache_config", return_value=mock_cache_config),
            patch("filemaker_mcp.tools.query.get_pk_field", return_value="PrimaryKey"),
        ):
            mock_client.get = AsyncMock()
            result = await query_records(
                table="Invoices",
                filter="ServiceDate ge 2025-03-10 and ServiceDate le 2025-03-28",
                select="Technician,Region",
                top=10,
            )

        # Technician and Region should appear
        assert "AR1" in result
        assert "Region" in result
        # City and Amount should NOT appear (not in select)
        assert "Springfield" not in result
        assert "Amount" not in result

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("populate_exposed_tables")
    async def test_cache_all_hit_applies_select(self) -> None:
        """Cache-all hit path should also respect $select."""
        import pandas as pd_test

        from filemaker_mcp.tools.analytics import DatasetEntry, _table_cache
        from filemaker_mcp.tools.query import query_records

        _table_cache["Drivers"] = DatasetEntry(
            df=pd_test.DataFrame(
                {
                    "Driver_ID": [1, 2],
                    "Driver_Name": ["AR1", "GR1"],
                    "Region": ["A", "B"],
                }
            ),
            table="Drivers",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 19),
            row_count=2,
            date_field="",
            date_min=None,
            date_max=None,
            pk_field="Driver_ID",
        )
        mock_cache_config = {"mode": "cache_all", "date_field": ""}

        with (
            patch("filemaker_mcp.tools.query.odata_client") as mock_client,
            patch("filemaker_mcp.tools.query.get_cache_config", return_value=mock_cache_config),
            patch("filemaker_mcp.tools.query.get_pk_field", return_value="Driver_ID"),
        ):
            mock_client.get = AsyncMock()
            result = await query_records(
                table="Drivers",
                select="Driver_Name",
                top=10,
            )

        # Driver_Name should appear
        assert "AR1" in result
        # Region should NOT appear (not in select)
        assert "Region" not in result


class TestDateCacheBypass:
    """Test that non-date filters bypass the date-range cache path."""

    def setup_method(self) -> None:
        from filemaker_mcp.tools.analytics import _table_cache

        _table_cache.clear()

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("populate_exposed_tables")
    async def test_non_date_filter_bypasses_cache(self) -> None:
        """Filter on non-date field with no existing cache should NOT enter cache path."""
        from filemaker_mcp.tools.analytics import _table_cache
        from filemaker_mcp.tools.query import query_records

        mock_response = {
            "value": [
                {"PrimaryKey": "1", "ServiceDate": "2025-03-15", "Region": "A"},
            ],
            "@count": 1,
        }
        mock_cache_config = {"mode": "date_range", "date_field": "ServiceDate"}

        with (
            patch("filemaker_mcp.tools.query.odata_client") as mock_client,
            patch("filemaker_mcp.tools.query.get_cache_config", return_value=mock_cache_config),
            patch("filemaker_mcp.tools.query.get_pk_field", return_value="PrimaryKey"),
        ):
            mock_client.get = AsyncMock(return_value=mock_response)
            await query_records(
                table="Invoices",
                filter="Region eq 'A'",
            )
            # FM should be called directly with the Region filter (passthrough)
            mock_client.get.assert_called()
            call_args = mock_client.get.call_args
            call_params = call_args[1].get("params", {})
            assert "$filter" in call_params

        # Table should NOT be added to the date cache
        assert "Invoices" not in _table_cache

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("populate_exposed_tables")
    async def test_non_date_filter_uses_existing_cache(self) -> None:
        """Non-date filter with pre-warmed cache enters cache path and serves cached data."""
        import pandas as pd_test

        from filemaker_mcp.tools.analytics import DatasetEntry, _table_cache
        from filemaker_mcp.tools.query import query_records

        _table_cache["Invoices"] = DatasetEntry(
            df=pd_test.DataFrame(
                {
                    "PrimaryKey": ["1", "2", "3"],
                    "ServiceDate": pd_test.to_datetime(["2025-03-15", "2025-03-20", "2025-03-25"]),
                    "Technician": ["AR1", "GR1", "AR1"],
                    "Region": ["A", "B", "A"],
                }
            ),
            table="Invoices",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 19),
            row_count=3,
            date_field="ServiceDate",
            date_min=date(2025, 3, 1),
            date_max=date(2025, 3, 31),
            pk_field="PrimaryKey",
        )
        mock_cache_config = {"mode": "date_range", "date_field": "ServiceDate"}
        # Gap-fill calls return empty (no new records outside cached range)
        empty_response = {"value": [], "@count": 0}

        with (
            patch("filemaker_mcp.tools.query.odata_client") as mock_client,
            patch("filemaker_mcp.tools.query.get_cache_config", return_value=mock_cache_config),
            patch("filemaker_mcp.tools.query.get_pk_field", return_value="PrimaryKey"),
        ):
            mock_client.get = AsyncMock(return_value=empty_response)
            result = await query_records(
                table="Invoices",
                filter="Region eq 'A'",
                top=10,
            )

        # Cache path was used — result should contain cached Region A records
        assert "AR1" in result

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("populate_exposed_tables")
    async def test_mixed_date_and_non_date_filter(self) -> None:
        """Filter with both date range and non-date field should use cache + in-memory filter."""
        from filemaker_mcp.tools.analytics import _table_cache
        from filemaker_mcp.tools.query import query_records

        mock_response = {
            "value": [
                {
                    "PrimaryKey": "1",
                    "ServiceDate": "2025-03-15",
                    "Region": "A",
                    "Technician": "AR1",
                },
                {
                    "PrimaryKey": "2",
                    "ServiceDate": "2025-03-20",
                    "Region": "B",
                    "Technician": "GR1",
                },
            ],
            "@count": 2,
        }
        mock_cache_config = {"mode": "date_range", "date_field": "ServiceDate"}
        mock_ddl = {
            "Invoices": {
                "PrimaryKey": {"type": "text", "pk": True},
                "ServiceDate": {"type": "date", "tier": "key"},
            }
        }

        with (
            patch("filemaker_mcp.tools.query.odata_client") as mock_client,
            patch("filemaker_mcp.tools.query.get_cache_config", return_value=mock_cache_config),
            patch("filemaker_mcp.tools.query.get_pk_field", return_value="PrimaryKey"),
            patch("filemaker_mcp.tools.query.TABLES", mock_ddl),
        ):
            mock_client.get = AsyncMock(return_value=mock_response)
            await query_records(
                table="Invoices",
                filter="ServiceDate ge 2025-03-01 and Region eq 'A'",
            )

        # Cache path should have been used (date range present)
        assert "Invoices" in _table_cache

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("populate_exposed_tables")
    async def test_empty_filter_bypasses_date_cache(self) -> None:
        """Empty filter with no existing cache should NOT trigger unbounded cache fetch."""
        from filemaker_mcp.tools.analytics import _table_cache
        from filemaker_mcp.tools.query import query_records

        mock_response = {
            "value": [
                {"PrimaryKey": "1", "ServiceDate": "2025-03-15", "Technician": "AR1"},
            ],
            "@count": 1,
        }
        mock_cache_config = {"mode": "date_range", "date_field": "ServiceDate"}

        with (
            patch("filemaker_mcp.tools.query.odata_client") as mock_client,
            patch("filemaker_mcp.tools.query.get_cache_config", return_value=mock_cache_config),
            patch("filemaker_mcp.tools.query.get_pk_field", return_value="PrimaryKey"),
        ):
            mock_client.get = AsyncMock(return_value=mock_response)
            await query_records(
                table="Invoices",
                filter="",
            )
            # Should call FM directly (passthrough), not cache path
            mock_client.get.assert_called()

        # Table should NOT be in date cache
        assert "Invoices" not in _table_cache


class TestEnrichResults:
    """Test post-processor result enrichment."""

    def setup_method(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT

        DDL_CONTEXT.clear()

    def test_appends_field_hints(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT
        from filemaker_mcp.tools.query import _enrich_results

        DDL_CONTEXT[("Invoices", "Commercial", "field_values")] = {
            "context": "Boolean: 1=yes, empty/0=no"
        }
        DDL_CONTEXT[("Invoices", "Technician", "field_values")] = {
            "context": "AR1 handles ~80% of IH work"
        }
        formatted = "Showing 2 records:\n\n--- Record 1 ---\n  Commercial: 1\n  Technician: AR1\n"
        result = _enrich_results(formatted, "Invoices", ["Commercial", "Technician"])
        assert "Context" in result
        assert "Boolean" in result
        assert "AR1 handles" in result

    def test_no_context_no_section(self) -> None:
        from filemaker_mcp.tools.query import _enrich_results

        formatted = "Showing 1 records:\n\n--- Record 1 ---\n  Name: Test\n"
        result = _enrich_results(formatted, "SomeTable", ["Name"])
        assert result == formatted

    def test_only_matching_fields(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT
        from filemaker_mcp.tools.query import _enrich_results

        DDL_CONTEXT[("T", "A", "field_values")] = {"context": "hint for A"}
        DDL_CONTEXT[("T", "B", "field_values")] = {"context": "hint for B"}
        result = _enrich_results("text", "T", ["A"])
        assert "hint for A" in result
        assert "hint for B" not in result

    def test_cache_notification(self) -> None:
        from filemaker_mcp.tools.query import _enrich_results

        result = _enrich_results(
            "text", "T", ["A"], cache_info="10 rows cached for T (2025-01-01 → 2025-03-31)"
        )
        assert "Cache" in result
        assert "10 rows" in result


class TestTypicalReportDatePatterns:
    """Test date handling for typical human report requests.

    Maps common report types to the OData filters Claude would generate,
    then verifies both normalize_dates_in_filter (pre-processing) and
    extract_date_range (cache logic) handle them correctly.

    All dates use 2026-02-20 (Thursday) as "today".
    """

    # --- Daily Report: single day ---

    def test_daily_report_eq(self) -> None:
        """'Daily report for today' → eq single date."""
        f = "ServiceDate eq 2026-02-20"
        assert normalize_dates_in_filter(f) == f
        assert extract_date_range(f, "ServiceDate") == ("2026-02-20", "2026-02-20")

    def test_daily_report_eq_quoted(self) -> None:
        """Claude sometimes quotes date values."""
        f = "ServiceDate eq '2026-02-20'"
        normalized = normalize_dates_in_filter(f)
        assert normalized == "ServiceDate eq 2026-02-20"
        assert extract_date_range(normalized, "ServiceDate") == (
            "2026-02-20",
            "2026-02-20",
        )

    def test_daily_report_eq_timestamp(self) -> None:
        """Claude sometimes adds T00:00:00Z."""
        f = "ServiceDate eq 2026-02-20T00:00:00Z"
        normalized = normalize_dates_in_filter(f)
        assert normalized == "ServiceDate eq 2026-02-20"
        assert extract_date_range(normalized, "ServiceDate") == (
            "2026-02-20",
            "2026-02-20",
        )

    def test_daily_report_ge_le_range(self) -> None:
        """Some LLMs express single day as ge/le range."""
        f = "ServiceDate ge 2026-02-20 and ServiceDate le 2026-02-20"
        assert normalize_dates_in_filter(f) == f
        assert extract_date_range(f, "ServiceDate") == ("2026-02-20", "2026-02-20")

    # --- Daily Recap: yesterday ---

    def test_daily_recap_yesterday(self) -> None:
        """'Recap for yesterday' → eq previous day."""
        f = "ServiceDate eq 2026-02-19"
        assert normalize_dates_in_filter(f) == f
        assert extract_date_range(f, "ServiceDate") == ("2026-02-19", "2026-02-19")

    # --- Weekly: current week (Mon-Fri or Mon-Sun) ---

    def test_weekly_report(self) -> None:
        """'Weekly report' → Mon 2/16 to Fri 2/20 (today is Thu 2/20)."""
        f = "ServiceDate ge 2026-02-16 and ServiceDate le 2026-02-20"
        assert normalize_dates_in_filter(f) == f
        assert extract_date_range(f, "ServiceDate") == ("2026-02-16", "2026-02-20")

    def test_weekly_report_quoted_dates(self) -> None:
        """Weekly with quoted dates."""
        f = "ServiceDate ge '2026-02-16' and ServiceDate le '2026-02-20'"
        normalized = normalize_dates_in_filter(f)
        assert normalized == ("ServiceDate ge 2026-02-16 and ServiceDate le 2026-02-20")
        assert extract_date_range(normalized, "ServiceDate") == (
            "2026-02-16",
            "2026-02-20",
        )

    def test_weekly_report_with_additional_filter(self) -> None:
        """Weekly with a Region or Technician filter mixed in."""
        f = "ServiceDate ge 2026-02-16 and ServiceDate le 2026-02-20 and Region eq 'A'"
        assert extract_date_range(f, "ServiceDate") == ("2026-02-16", "2026-02-20")

    # --- Monthly: full month ---

    def test_monthly_report_current_month(self) -> None:
        """'February report' → ge Feb 1, le Feb 28."""
        f = "ServiceDate ge 2026-02-01 and ServiceDate le 2026-02-28"
        assert normalize_dates_in_filter(f) == f
        assert extract_date_range(f, "ServiceDate") == ("2026-02-01", "2026-02-28")

    def test_monthly_report_lt_next_month(self) -> None:
        """Alternative: ge Feb 1, lt Mar 1 (exclusive upper)."""
        f = "ServiceDate ge 2026-02-01 and ServiceDate lt 2026-03-01"
        assert normalize_dates_in_filter(f) == f
        assert extract_date_range(f, "ServiceDate") == ("2026-02-01", "2026-03-01")

    def test_monthly_report_previous_month(self) -> None:
        """'January report' → ge Jan 1, le Jan 31."""
        f = "ServiceDate ge 2026-01-01 and ServiceDate le 2026-01-31"
        assert normalize_dates_in_filter(f) == f
        assert extract_date_range(f, "ServiceDate") == ("2026-01-01", "2026-01-31")

    # --- YTD: year-to-date ---

    def test_ytd_open_ended(self) -> None:
        """'YTD report' → ge Jan 1 only (open upper bound)."""
        f = "ServiceDate ge 2026-01-01"
        assert normalize_dates_in_filter(f) == f
        assert extract_date_range(f, "ServiceDate") == ("2026-01-01", None)

    def test_ytd_with_upper_bound(self) -> None:
        """'YTD through today' → ge Jan 1, le today."""
        f = "ServiceDate ge 2026-01-01 and ServiceDate le 2026-02-20"
        assert normalize_dates_in_filter(f) == f
        assert extract_date_range(f, "ServiceDate") == ("2026-01-01", "2026-02-20")

    # --- Comp YTD: comparative year-to-date (current vs previous year) ---

    def test_comp_ytd_current_year(self) -> None:
        """Current YTD portion of comparative query."""
        f = "ServiceDate ge 2026-01-01 and ServiceDate le 2026-02-20"
        assert extract_date_range(f, "ServiceDate") == ("2026-01-01", "2026-02-20")

    def test_comp_ytd_previous_year(self) -> None:
        """Previous YTD portion — same month/day range, prior year."""
        f = "ServiceDate ge 2025-01-01 and ServiceDate le 2025-02-20"
        assert extract_date_range(f, "ServiceDate") == ("2025-01-01", "2025-02-20")

    # --- Previous MTD vs Current MTD ---

    def test_current_mtd(self) -> None:
        """Current month-to-date: Feb 1 through today."""
        f = "ServiceDate ge 2026-02-01 and ServiceDate le 2026-02-20"
        assert extract_date_range(f, "ServiceDate") == ("2026-02-01", "2026-02-20")

    def test_previous_mtd(self) -> None:
        """Previous MTD: Jan 1 through Jan 20 (same day-of-month)."""
        f = "ServiceDate ge 2026-01-01 and ServiceDate le 2026-01-20"
        assert extract_date_range(f, "ServiceDate") == ("2026-01-01", "2026-01-20")

    # --- MTD prev Year vs MTD Current ---

    def test_mtd_current_year(self) -> None:
        """Current year Feb MTD."""
        f = "ServiceDate ge 2026-02-01 and ServiceDate le 2026-02-20"
        assert extract_date_range(f, "ServiceDate") == ("2026-02-01", "2026-02-20")

    def test_mtd_previous_year(self) -> None:
        """Previous year Feb MTD — same month, prior year."""
        f = "ServiceDate ge 2025-02-01 and ServiceDate le 2025-02-20"
        assert extract_date_range(f, "ServiceDate") == ("2025-02-01", "2025-02-20")

    # --- US date format variants (from human-entered data) ---

    def test_us_date_daily(self) -> None:
        """Human enters 2/20/2026 instead of ISO."""
        f = "ServiceDate eq 2/20/2026"
        normalized = normalize_dates_in_filter(f)
        assert normalized == "ServiceDate eq 2026-02-20"
        assert extract_date_range(normalized, "ServiceDate") == (
            "2026-02-20",
            "2026-02-20",
        )

    def test_us_date_range(self) -> None:
        """Human enters US dates in a range."""
        f = "ServiceDate ge 1/1/2026 and ServiceDate le 2/20/2026"
        normalized = normalize_dates_in_filter(f)
        assert normalized == ("ServiceDate ge 2026-01-01 and ServiceDate le 2026-02-20")
        assert extract_date_range(normalized, "ServiceDate") == (
            "2026-01-01",
            "2026-02-20",
        )

    # --- Quoted timestamp variants (common Claude output) ---

    def test_quoted_timestamp_range(self) -> None:
        """Claude wraps timestamps in quotes for a range."""
        f = "ServiceDate ge '2026-01-01T00:00:00Z' and ServiceDate le '2026-02-20T23:59:59Z'"
        normalized = normalize_dates_in_filter(f)
        assert normalized == ("ServiceDate ge 2026-01-01 and ServiceDate le 2026-02-20")
        assert extract_date_range(normalized, "ServiceDate") == (
            "2026-01-01",
            "2026-02-20",
        )
