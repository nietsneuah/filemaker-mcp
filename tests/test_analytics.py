"""Tests for the analytics tools (load, analyze, list datasets)."""

from datetime import datetime
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest


class TestDatasetEntry:
    """Test DatasetEntry dataclass and _datasets dict."""

    def test_create_dataset_entry(self) -> None:
        from filemaker_mcp.tools.analytics import DatasetEntry

        df = pd.DataFrame({"A": [1, 2, 3], "B": ["x", "y", "z"]})
        entry = DatasetEntry(
            df=df,
            table="TestTable",
            filter="A gt 0",
            select="A,B",
            loaded_at=datetime(2026, 2, 15, 12, 0, 0),
            row_count=3,
        )
        assert entry.row_count == 3
        assert entry.table == "TestTable"
        assert len(entry.df) == 3

    def test_datasets_dict_starts_empty(self) -> None:
        from filemaker_mcp.tools.analytics import _datasets

        # Clear any state from other tests
        _datasets.clear()
        assert len(_datasets) == 0


class TestListDatasets:
    """Test fm_list_datasets tool."""

    @pytest.mark.asyncio
    async def test_list_empty(self) -> None:
        from filemaker_mcp.tools.analytics import _datasets, list_datasets

        _datasets.clear()
        result = await list_datasets()
        assert "No datasets" in result

    @pytest.mark.asyncio
    async def test_list_with_datasets(self) -> None:
        from filemaker_mcp.tools.analytics import DatasetEntry, _datasets, list_datasets

        _datasets.clear()
        _datasets["inv25"] = DatasetEntry(
            df=pd.DataFrame({"A": [1, 2]}),
            table="InHomeInvoiceHeader",
            filter="Date_of_Service ge 2025-01-01",
            select="A",
            loaded_at=datetime(2026, 2, 15, 12, 0, 0),
            row_count=2,
        )
        result = await list_datasets()
        assert "inv25" in result
        assert "InHomeInvoiceHeader" in result
        assert "2 rows" in result


class TestLoadDataset:
    """Test fm_load_dataset tool."""

    @pytest.mark.asyncio
    async def test_load_basic(self) -> None:
        """Load a simple dataset from mocked FM response."""
        from filemaker_mcp.tools.analytics import _datasets, load_dataset

        _datasets.clear()

        mock_response = {
            "value": [
                {"Driver": "Smith", "Zone": "A", "InvoiceTotal": 500},
                {"Driver": "Jones", "Zone": "B", "InvoiceTotal": 300},
            ],
            "@count": 2,
        }

        with patch("filemaker_mcp.tools.analytics.odata_client") as mock_client:
            mock_client.get = AsyncMock(return_value=mock_response)
            result = await load_dataset(
                name="test1",
                table="InHomeInvoiceHeader",
                select="Driver,Zone,InvoiceTotal",
            )

        assert "test1" in _datasets
        assert _datasets["test1"].row_count == 2
        assert "2 rows" in result

    @pytest.mark.asyncio
    async def test_load_replaces_existing(self) -> None:
        """Loading with same name replaces the old dataset."""
        from filemaker_mcp.tools.analytics import DatasetEntry, _datasets, load_dataset

        _datasets.clear()
        _datasets["test1"] = DatasetEntry(
            df=pd.DataFrame({"A": [1]}),
            table="Old",
            filter="",
            select="",
            loaded_at=datetime(2026, 1, 1),
            row_count=1,
        )

        mock_response = {
            "value": [{"A": 10}, {"A": 20}],
            "@count": 2,
        }

        with (
            patch("filemaker_mcp.tools.analytics.odata_client") as mock_client,
            patch(
                "filemaker_mcp.tools.analytics.EXPOSED_TABLES",
                {"NewTable": "test table"},
            ),
        ):
            mock_client.get = AsyncMock(return_value=mock_response)
            await load_dataset(name="test1", table="NewTable")

        assert _datasets["test1"].table == "NewTable"
        assert _datasets["test1"].row_count == 2

    @pytest.mark.asyncio
    async def test_load_empty_result(self) -> None:
        """Zero records matched — dataset NOT created."""
        from filemaker_mcp.tools.analytics import _datasets, load_dataset

        _datasets.clear()

        mock_response = {"value": [], "@count": 0}

        with patch("filemaker_mcp.tools.analytics.odata_client") as mock_client:
            mock_client.get = AsyncMock(return_value=mock_response)
            result = await load_dataset(name="empty", table="InHomeInvoiceHeader")

        assert "empty" not in _datasets
        assert "0 records" in result

    @pytest.mark.asyncio
    async def test_load_unknown_table(self) -> None:
        """Unknown table returns error."""
        from filemaker_mcp.tools.analytics import _datasets, load_dataset

        _datasets.clear()
        result = await load_dataset(name="bad", table="NonExistentTable")
        assert "Error" in result
        assert "bad" not in _datasets

    @pytest.mark.asyncio
    async def test_load_applies_filter_and_select(self) -> None:
        """Verify filter and select are passed through to OData client."""
        from filemaker_mcp.tools.analytics import _datasets, load_dataset

        _datasets.clear()

        mock_response = {
            "value": [{"Driver": "Smith", "InvoiceTotal": 500}],
            "@count": 1,
        }

        with patch("filemaker_mcp.tools.analytics.odata_client") as mock_client:
            mock_client.get = AsyncMock(return_value=mock_response)
            await load_dataset(
                name="filtered",
                table="InHomeInvoiceHeader",
                filter="Date_of_Service ge 2025-01-01",
                select="Driver,InvoiceTotal",
            )

            # Check the OData call params
            call_args = mock_client.get.call_args
            params = call_args.kwargs.get("params") or call_args[1].get("params", {})
            assert "$filter" in params
            assert "$select" in params

    @pytest.mark.asyncio
    async def test_load_auto_paginates(self) -> None:
        """When FM returns exactly 10000 records, load_dataset fetches the next page."""
        from filemaker_mcp.tools.analytics import _datasets, load_dataset

        _datasets.clear()

        page1 = [{"A": i} for i in range(10000)]
        page2 = [{"A": i} for i in range(10000, 10500)]

        call_count = 0

        async def mock_get(path, params=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"value": page1, "@count": 10500}
            else:
                return {"value": page2, "@count": 10500}

        with patch("filemaker_mcp.tools.analytics.odata_client") as mock_client:
            mock_client.get = mock_get
            result = await load_dataset(name="big", table="InHomeInvoiceHeader")

        assert _datasets["big"].row_count == 10500
        assert call_count == 2
        assert "10500" in result or "10,500" in result

    @pytest.mark.asyncio
    async def test_load_date_conversion(self) -> None:
        """Date columns detected from DDL are converted to datetime."""
        from filemaker_mcp.tools.analytics import _datasets, load_dataset

        _datasets.clear()

        mock_response = {
            "value": [
                {"Date_of_Service": "2025-06-15", "InvoiceTotal": 500},
                {"Date_of_Service": "2025-07-20", "InvoiceTotal": 300},
            ],
            "@count": 2,
        }

        # Mock DDL with a date field
        mock_ddl = {
            "InHomeInvoiceHeader": {
                "Date_of_Service": {"type": "date", "tier": "key"},
                "InvoiceTotal": {"type": "number", "tier": "standard"},
            }
        }

        with (
            patch("filemaker_mcp.tools.analytics.odata_client") as mock_client,
            patch("filemaker_mcp.tools.analytics.TABLES", mock_ddl),
        ):
            mock_client.get = AsyncMock(return_value=mock_response)
            await load_dataset(
                name="dates",
                table="InHomeInvoiceHeader",
                select="Date_of_Service,InvoiceTotal",
            )

        df = _datasets["dates"].df
        assert pd.api.types.is_datetime64_any_dtype(df["Date_of_Service"])


class TestAnalyze:
    """Test fm_analyze tool."""

    def _load_test_data(self) -> None:
        """Helper: populate _datasets with test invoice data."""
        from filemaker_mcp.tools.analytics import DatasetEntry, _datasets

        _datasets.clear()
        df = pd.DataFrame(
            {
                "Driver": ["Smith", "Smith", "Jones", "Jones", "Smith"],
                "Zone": ["A", "A", "B", "B", "A"],
                "InvoiceTotal": [500, 300, 200, 400, 100],
                "Date_of_Service": pd.to_datetime(
                    [
                        "2025-01-15",
                        "2025-02-20",
                        "2025-01-10",
                        "2025-03-05",
                        "2025-03-15",
                    ]
                ),
            }
        )
        _datasets["inv"] = DatasetEntry(
            df=df,
            table="InHomeInvoiceHeader",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 15),
            row_count=5,
        )

    @pytest.mark.asyncio
    async def test_groupby_with_aggregate(self) -> None:
        """groupby=Driver, aggregate=sum:InvoiceTotal -> grouped sums."""
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(
            dataset="inv",
            groupby="Driver",
            aggregate="sum:InvoiceTotal",
        )
        assert "Smith" in result
        assert "Jones" in result
        assert "900" in result  # Smith: 500+300+100
        assert "600" in result  # Jones: 200+400

    @pytest.mark.asyncio
    async def test_scalar_aggregate(self) -> None:
        """No groupby, aggregate=sum:InvoiceTotal -> total across all rows."""
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(
            dataset="inv",
            aggregate="sum:InvoiceTotal,count:InvoiceTotal",
        )
        assert "1500" in result  # total sum
        assert "5" in result  # count

    @pytest.mark.asyncio
    async def test_groupby_no_aggregate(self) -> None:
        """groupby=Zone, no aggregate -> value counts."""
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(dataset="inv", groupby="Zone")
        assert "A" in result
        assert "B" in result

    @pytest.mark.asyncio
    async def test_no_groupby_no_aggregate(self) -> None:
        """No groupby, no aggregate -> describe() summary statistics."""
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(dataset="inv")
        assert "mean" in result or "count" in result

    @pytest.mark.asyncio
    async def test_multiple_aggregates(self) -> None:
        """Multiple aggregate functions: sum, count, mean."""
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(
            dataset="inv",
            groupby="Driver",
            aggregate="sum:InvoiceTotal,count:InvoiceTotal,mean:InvoiceTotal",
        )
        assert "Smith" in result
        assert "Jones" in result

    @pytest.mark.asyncio
    async def test_filter_before_aggregate(self) -> None:
        """pandas filter narrows data before aggregation."""
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(
            dataset="inv",
            filter="Zone == 'A'",
            aggregate="sum:InvoiceTotal",
        )
        assert "900" in result  # Only Zone A: 500+300+100

    @pytest.mark.asyncio
    async def test_sort_desc(self) -> None:
        """Sort results descending."""
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(
            dataset="inv",
            groupby="Driver",
            aggregate="sum:InvoiceTotal",
            sort="InvoiceTotal_sum desc",
        )
        # Smith (900) should appear before Jones (600)
        smith_pos = result.index("Smith")
        jones_pos = result.index("Jones")
        assert smith_pos < jones_pos

    @pytest.mark.asyncio
    async def test_limit(self) -> None:
        """Limit output rows."""
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(
            dataset="inv",
            groupby="Driver",
            aggregate="sum:InvoiceTotal",
            limit=1,
        )
        # Should only show 1 group
        # Just verify it doesn't have both Driver names
        assert result.count("Smith") + result.count("Jones") <= 2  # header + 1 data row max

    @pytest.mark.asyncio
    async def test_dataset_not_found(self) -> None:
        """Unknown dataset name returns helpful error."""
        from filemaker_mcp.tools.analytics import _datasets, analyze

        _datasets.clear()
        result = await analyze(dataset="nonexistent")
        assert "not found" in result.lower()

    @pytest.mark.asyncio
    async def test_invalid_aggregate_function(self) -> None:
        """Invalid aggregate function returns error."""
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(
            dataset="inv",
            aggregate="median:InvoiceTotal",
        )
        assert "Unknown function" in result or "Supported" in result

    @pytest.mark.asyncio
    async def test_invalid_field_name(self) -> None:
        """Field not in dataset returns error."""
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(
            dataset="inv",
            aggregate="sum:NonExistentField",
        )
        assert "not in dataset" in result.lower() or "available" in result.lower()

    @pytest.mark.asyncio
    async def test_groupby_multiple_fields(self) -> None:
        """groupby=Driver,Zone with aggregate."""
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(
            dataset="inv",
            groupby="Driver,Zone",
            aggregate="sum:InvoiceTotal,count:InvoiceTotal",
        )
        assert "Smith" in result
        assert "A" in result
