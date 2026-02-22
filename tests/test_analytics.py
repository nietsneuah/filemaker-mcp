"""Tests for the analytics tools (load, analyze, list datasets)."""

from collections.abc import Generator
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from filemaker_mcp.tools.query import EXPOSED_TABLES


@pytest.fixture()
def populate_exposed_tables():
    """Temporarily populate EXPOSED_TABLES for tests that need table validation to pass."""
    saved = dict(EXPOSED_TABLES)
    EXPOSED_TABLES.update(
        {
            "Invoices": "Service invoices.",
        }
    )
    yield
    EXPOSED_TABLES.clear()
    EXPOSED_TABLES.update(saved)


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
            table="Invoices",
            filter="ServiceDate ge 2025-01-01",
            select="A",
            loaded_at=datetime(2026, 2, 15, 12, 0, 0),
            row_count=2,
        )
        result = await list_datasets()
        assert "inv25" in result
        assert "Invoices" in result
        assert "2 rows" in result


@pytest.mark.usefixtures("populate_exposed_tables")
class TestLoadDataset:
    """Test fm_load_dataset tool."""

    @pytest.mark.asyncio
    async def test_load_basic(self) -> None:
        """Load a simple dataset from mocked FM response."""
        from filemaker_mcp.tools.analytics import _datasets, load_dataset

        _datasets.clear()

        mock_response = {
            "value": [
                {"Technician": "Smith", "Region": "A", "Amount": 500},
                {"Technician": "Jones", "Region": "B", "Amount": 300},
            ],
            "@count": 2,
        }

        with patch("filemaker_mcp.tools.analytics.odata_client") as mock_client:
            mock_client.get = AsyncMock(return_value=mock_response)
            result = await load_dataset(
                name="test1",
                table="Invoices",
                select="Technician,Region,Amount",
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
            result = await load_dataset(name="empty", table="Invoices")

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
            "value": [{"Technician": "Smith", "Amount": 500}],
            "@count": 1,
        }

        with patch("filemaker_mcp.tools.analytics.odata_client") as mock_client:
            mock_client.get = AsyncMock(return_value=mock_response)
            await load_dataset(
                name="filtered",
                table="Invoices",
                filter="ServiceDate ge 2025-01-01",
                select="Technician,Amount",
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
            result = await load_dataset(name="big", table="Invoices")

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
                {"ServiceDate": "2025-06-15", "Amount": 500},
                {"ServiceDate": "2025-07-20", "Amount": 300},
            ],
            "@count": 2,
        }

        # Mock DDL with a date field
        mock_ddl = {
            "Invoices": {
                "ServiceDate": {"type": "date", "tier": "key"},
                "Amount": {"type": "number", "tier": "standard"},
            }
        }

        with (
            patch("filemaker_mcp.tools.analytics.odata_client") as mock_client,
            patch("filemaker_mcp.tools.analytics.TABLES", mock_ddl),
        ):
            mock_client.get = AsyncMock(return_value=mock_response)
            await load_dataset(
                name="dates",
                table="Invoices",
                select="ServiceDate,Amount",
            )

        df = _datasets["dates"].df
        assert pd.api.types.is_datetime64_any_dtype(df["ServiceDate"])


class TestAnalyze:
    """Test fm_analyze tool."""

    def _load_test_data(self) -> None:
        """Helper: populate _datasets with test invoice data."""
        from filemaker_mcp.tools.analytics import DatasetEntry, _datasets

        _datasets.clear()
        df = pd.DataFrame(
            {
                "Technician": ["Smith", "Smith", "Jones", "Jones", "Smith"],
                "Region": ["A", "A", "B", "B", "A"],
                "Amount": [500, 300, 200, 400, 100],
                "ServiceDate": pd.to_datetime(
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
            table="Invoices",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 15),
            row_count=5,
        )

    @pytest.mark.asyncio
    async def test_groupby_with_aggregate(self) -> None:
        """groupby=Technician, aggregate=sum:Amount -> grouped sums."""
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(
            dataset="inv",
            groupby="Technician",
            aggregate="sum:Amount",
        )
        assert "Smith" in result
        assert "Jones" in result
        assert "900" in result  # Smith: 500+300+100
        assert "600" in result  # Jones: 200+400

    @pytest.mark.asyncio
    async def test_scalar_aggregate(self) -> None:
        """No groupby, aggregate=sum:Amount -> total across all rows."""
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(
            dataset="inv",
            aggregate="sum:Amount,count:Amount",
        )
        assert "1500" in result  # total sum
        assert "5" in result  # count

    @pytest.mark.asyncio
    async def test_groupby_no_aggregate(self) -> None:
        """groupby=Region, no aggregate -> value counts."""
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(dataset="inv", groupby="Region")
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
            groupby="Technician",
            aggregate="sum:Amount,count:Amount,mean:Amount",
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
            filter="Region == 'A'",
            aggregate="sum:Amount",
        )
        assert "900" in result  # Only Region A: 500+300+100

    @pytest.mark.asyncio
    async def test_sort_desc(self) -> None:
        """Sort results descending."""
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(
            dataset="inv",
            groupby="Technician",
            aggregate="sum:Amount",
            sort="Amount_sum desc",
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
            groupby="Technician",
            aggregate="sum:Amount",
            limit=1,
        )
        # Should only show 1 group
        # Just verify it doesn't have both Technician names
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
            aggregate="variance:Amount",
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
        """groupby=Technician,Region with aggregate."""
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(
            dataset="inv",
            groupby="Technician,Region",
            aggregate="sum:Amount,count:Amount",
        )
        assert "Smith" in result
        assert "A" in result


class TestTableCache:
    """Test table-level DataFrame cache."""

    def setup_method(self) -> None:
        from filemaker_mcp.tools.analytics import _table_cache

        _table_cache.clear()

    def test_table_cache_starts_empty(self) -> None:
        from filemaker_mcp.tools.analytics import _table_cache

        assert len(_table_cache) == 0

    def test_store_entry(self) -> None:
        from filemaker_mcp.tools.analytics import DatasetEntry, _table_cache

        _table_cache["Invoices"] = DatasetEntry(
            df=pd.DataFrame({"A": [1, 2]}),
            table="Invoices",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 19),
            row_count=2,
            date_field="ServiceDate",
            date_min=date(2025, 1, 1),
            date_max=date(2025, 6, 30),
            pk_field="PrimaryKey",
        )
        assert "Invoices" in _table_cache
        assert _table_cache["Invoices"].date_min == date(2025, 1, 1)

    @pytest.mark.asyncio
    async def test_flush_all(self) -> None:
        from filemaker_mcp.tools.analytics import DatasetEntry, _table_cache, flush_datasets

        _table_cache["T1"] = DatasetEntry(
            df=pd.DataFrame({"A": [1]}),
            table="T1",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 19),
            row_count=1,
            date_field="",
            date_min=None,
            date_max=None,
            pk_field="PrimaryKey",
        )
        _table_cache["T2"] = DatasetEntry(
            df=pd.DataFrame({"A": [2]}),
            table="T2",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 19),
            row_count=1,
            date_field="",
            date_min=None,
            date_max=None,
            pk_field="PrimaryKey",
        )
        result = await flush_datasets()
        assert len(_table_cache) == 0
        assert "2" in result

    @pytest.mark.asyncio
    async def test_flush_single_table(self) -> None:
        from filemaker_mcp.tools.analytics import DatasetEntry, _table_cache, flush_datasets

        _table_cache["T1"] = DatasetEntry(
            df=pd.DataFrame({"A": [1]}),
            table="T1",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 19),
            row_count=1,
            date_field="",
            date_min=None,
            date_max=None,
            pk_field="PrimaryKey",
        )
        _table_cache["T2"] = DatasetEntry(
            df=pd.DataFrame({"A": [2]}),
            table="T2",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 19),
            row_count=1,
            date_field="",
            date_min=None,
            date_max=None,
            pk_field="PrimaryKey",
        )
        await flush_datasets(table="T1")
        assert "T1" not in _table_cache
        assert "T2" in _table_cache

    @pytest.mark.asyncio
    async def test_flush_nonexistent_table(self) -> None:
        from filemaker_mcp.tools.analytics import flush_datasets

        result = await flush_datasets(table="Nonexistent")
        assert "no" in result.lower()


class TestDateRangeMerge:
    """Test date range gap computation and DataFrame merge."""

    def test_no_existing_cache(self) -> None:
        from filemaker_mcp.tools.analytics import compute_date_gaps

        gaps = compute_date_gaps(
            existing_min=None,
            existing_max=None,
            requested_min="2025-01-01",
            requested_max="2025-06-30",
        )
        assert gaps == [("2025-01-01", "2025-06-30")]

    def test_fully_covered(self) -> None:
        from filemaker_mcp.tools.analytics import compute_date_gaps

        gaps = compute_date_gaps(
            existing_min="2025-01-01",
            existing_max="2025-12-31",
            requested_min="2025-03-01",
            requested_max="2025-06-30",
        )
        assert gaps == []

    def test_extend_right(self) -> None:
        from filemaker_mcp.tools.analytics import compute_date_gaps

        gaps = compute_date_gaps(
            existing_min="2025-01-01",
            existing_max="2025-06-30",
            requested_min="2025-04-01",
            requested_max="2025-12-31",
        )
        assert len(gaps) == 1
        assert gaps[0][0] == "2025-07-01"
        assert gaps[0][1] == "2025-12-31"

    def test_extend_left(self) -> None:
        from filemaker_mcp.tools.analytics import compute_date_gaps

        gaps = compute_date_gaps(
            existing_min="2025-06-01",
            existing_max="2025-12-31",
            requested_min="2025-01-01",
            requested_max="2025-09-30",
        )
        assert len(gaps) == 1
        assert gaps[0][0] == "2025-01-01"
        assert gaps[0][1] == "2025-05-31"

    def test_extend_both_sides(self) -> None:
        from filemaker_mcp.tools.analytics import compute_date_gaps

        gaps = compute_date_gaps(
            existing_min="2025-04-01",
            existing_max="2025-06-30",
            requested_min="2025-01-01",
            requested_max="2025-12-31",
        )
        assert len(gaps) == 2

    def test_no_requested_max(self) -> None:
        from filemaker_mcp.tools.analytics import compute_date_gaps

        gaps = compute_date_gaps(
            existing_min="2025-01-01",
            existing_max="2025-06-30",
            requested_min="2025-03-01",
            requested_max=None,
        )
        assert len(gaps) == 1
        assert gaps[0][0] == "2025-07-01"
        assert gaps[0][1] is None

    def test_no_requested_bounds(self) -> None:
        from filemaker_mcp.tools.analytics import compute_date_gaps

        gaps = compute_date_gaps(
            existing_min="2025-03-01",
            existing_max="2025-06-30",
            requested_min=None,
            requested_max=None,
        )
        assert len(gaps) == 2

    def setup_method(self) -> None:
        from filemaker_mcp.tools.analytics import _table_cache

        _table_cache.clear()

    def test_merge_new_table(self) -> None:
        from filemaker_mcp.tools.analytics import _table_cache, merge_into_table_cache

        new_df = pd.DataFrame(
            {
                "PrimaryKey": [1, 2, 3],
                "ServiceDate": pd.to_datetime(["2025-01-15", "2025-02-20", "2025-03-10"]),
                "Amount": [100, 200, 300],
            }
        )
        merge_into_table_cache(
            table="Invoices",
            new_df=new_df,
            date_field="ServiceDate",
            pk_field="PrimaryKey",
            date_min="2025-01-01",
            date_max="2025-03-31",
        )
        assert "Invoices" in _table_cache
        assert _table_cache["Invoices"].row_count == 3

    def test_merge_extends_existing(self) -> None:
        from filemaker_mcp.tools.analytics import DatasetEntry, _table_cache, merge_into_table_cache

        existing_df = pd.DataFrame(
            {
                "PrimaryKey": [1, 2],
                "ServiceDate": pd.to_datetime(["2025-01-15", "2025-02-20"]),
                "Amount": [100, 200],
            }
        )
        _table_cache["T"] = DatasetEntry(
            df=existing_df,
            table="T",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 19),
            row_count=2,
            date_field="ServiceDate",
            date_min=date(2025, 1, 1),
            date_max=date(2025, 2, 28),
            pk_field="PrimaryKey",
        )
        new_df = pd.DataFrame(
            {
                "PrimaryKey": [2, 3],
                "ServiceDate": pd.to_datetime(["2025-02-20", "2025-03-15"]),
                "Amount": [200, 300],
            }
        )
        merge_into_table_cache(
            table="T",
            new_df=new_df,
            date_field="ServiceDate",
            pk_field="PrimaryKey",
            date_min="2025-01-01",
            date_max="2025-03-31",
        )
        assert _table_cache["T"].row_count == 3
        assert _table_cache["T"].date_max == date(2025, 3, 31)

    def test_merge_enforces_row_limit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from filemaker_mcp.tools import analytics
        from filemaker_mcp.tools.analytics import _table_cache, merge_into_table_cache

        monkeypatch.setattr(analytics, "MAX_ROWS_PER_TABLE", 5)

        dates = pd.date_range("2025-01-01", periods=10, freq="D")
        big_df = pd.DataFrame(
            {
                "PrimaryKey": list(range(10)),
                "ServiceDate": dates,
                "Val": list(range(10)),
            }
        )
        merge_into_table_cache(
            table="BigTable",
            new_df=big_df,
            date_field="ServiceDate",
            pk_field="PrimaryKey",
            date_min="2025-01-01",
            date_max="2025-01-10",
        )
        entry = _table_cache["BigTable"]
        assert entry.row_count == 5
        # Should keep the 5 most recent rows (PK 5-9)
        assert set(entry.df["PrimaryKey"].tolist()) == {5, 6, 7, 8, 9}


class TestNewAggFunctions:
    """Test median, nunique, std aggregation functions."""

    def _load_test_data(self) -> None:
        from filemaker_mcp.tools.analytics import DatasetEntry, _datasets

        _datasets.clear()
        df = pd.DataFrame(
            {
                "Technician": ["Smith", "Smith", "Jones", "Jones", "Smith"],
                "Region": ["A", "A", "B", "B", "A"],
                "Amount": [500, 300, 200, 400, 100],
                "ServiceDate": pd.to_datetime(
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
            table="Invoices",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 15),
            row_count=5,
        )

    @pytest.mark.asyncio
    async def test_median_aggregate(self) -> None:
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(dataset="inv", aggregate="median:Amount")
        assert "300" in result

    @pytest.mark.asyncio
    async def test_nunique_aggregate(self) -> None:
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(dataset="inv", groupby="Region", aggregate="nunique:Technician")
        assert "1" in result

    @pytest.mark.asyncio
    async def test_std_aggregate(self) -> None:
        from filemaker_mcp.tools.analytics import analyze

        self._load_test_data()
        result = await analyze(dataset="inv", aggregate="std:Amount")
        assert "158" in result


class TestTimeSeries:
    """Test time-series period aggregation."""

    def _load_monthly_data(self) -> None:
        from filemaker_mcp.tools.analytics import DatasetEntry, _datasets

        _datasets.clear()
        df = pd.DataFrame(
            {
                "ServiceDate": pd.to_datetime(
                    [
                        "2025-01-15",
                        "2025-01-20",
                        "2025-02-10",
                        "2025-02-25",
                        "2025-03-05",
                    ]
                ),
                "Amount": [100, 200, 300, 400, 500],
            }
        )
        _datasets["ts"] = DatasetEntry(
            df=df,
            table="T",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 19),
            row_count=5,
        )

    @pytest.mark.asyncio
    async def test_monthly_aggregation(self) -> None:
        from filemaker_mcp.tools.analytics import analyze

        self._load_monthly_data()
        result = await analyze(
            dataset="ts",
            groupby="ServiceDate",
            aggregate="sum:Amount",
            period="month",
        )
        assert "300" in result  # Jan: 100+200
        assert "700" in result  # Feb: 300+400
        assert "500" in result  # Mar: 500

    @pytest.mark.asyncio
    async def test_weekly_aggregation(self) -> None:
        from filemaker_mcp.tools.analytics import analyze

        self._load_monthly_data()
        result = await analyze(
            dataset="ts",
            groupby="ServiceDate",
            aggregate="count:Amount",
            period="week",
        )
        assert "1" in result or "2" in result

    @pytest.mark.asyncio
    async def test_invalid_period(self) -> None:
        from filemaker_mcp.tools.analytics import analyze

        self._load_monthly_data()
        result = await analyze(
            dataset="ts",
            groupby="ServiceDate",
            aggregate="sum:Amount",
            period="hourly",
        )
        assert "invalid" in result.lower() or "supported" in result.lower()


class TestPivot:
    """Test pivot cross-tabulation."""

    def _load_pivot_data(self) -> None:
        from filemaker_mcp.tools.analytics import DatasetEntry, _datasets

        _datasets.clear()
        df = pd.DataFrame(
            {
                "Technician": ["AR1", "AR1", "AR1", "GR1", "GR1"],
                "Region": ["A", "A", "B", "A", "B"],
                "Amount": [100, 200, 300, 400, 500],
            }
        )
        _datasets["pv"] = DatasetEntry(
            df=df,
            table="T",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 19),
            row_count=5,
        )

    @pytest.mark.asyncio
    async def test_pivot_count(self) -> None:
        from filemaker_mcp.tools.analytics import analyze

        self._load_pivot_data()
        result = await analyze(
            dataset="pv",
            groupby="Technician",
            pivot_column="Region",
            aggregate="count:Amount",
        )
        assert "AR1" in result
        assert "GR1" in result

    @pytest.mark.asyncio
    async def test_pivot_sum(self) -> None:
        from filemaker_mcp.tools.analytics import analyze

        self._load_pivot_data()
        result = await analyze(
            dataset="pv",
            groupby="Technician",
            pivot_column="Region",
            aggregate="sum:Amount",
        )
        assert "300" in result  # AR1 Region A: 100+200
        assert "AR1" in result

    @pytest.mark.asyncio
    async def test_pivot_invalid_column(self) -> None:
        from filemaker_mcp.tools.analytics import analyze

        self._load_pivot_data()
        result = await analyze(
            dataset="pv",
            groupby="Technician",
            pivot_column="Nonexistent",
            aggregate="count:Amount",
        )
        assert "not in dataset" in result.lower()


class TestAnalyzeTableCacheFallback:
    """Test that analyze() falls back to _table_cache when named dataset not found."""

    @pytest.mark.asyncio
    async def test_resolves_from_table_cache(self) -> None:
        from filemaker_mcp.tools.analytics import DatasetEntry, _datasets, _table_cache, analyze

        _datasets.clear()
        _table_cache.clear()
        _table_cache["Invoices"] = DatasetEntry(
            df=pd.DataFrame(
                {
                    "Technician": ["AR1", "GR1"],
                    "Amount": [500, 300],
                }
            ),
            table="Invoices",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 19),
            row_count=2,
            date_field="ServiceDate",
            date_min=date(2025, 1, 1),
            date_max=date(2025, 12, 31),
            pk_field="PrimaryKey",
        )
        result = await analyze(
            dataset="Invoices",
            aggregate="sum:Amount",
        )
        assert "800" in result

    @pytest.mark.asyncio
    async def test_named_dataset_takes_precedence(self) -> None:
        from filemaker_mcp.tools.analytics import DatasetEntry, _datasets, _table_cache, analyze

        _datasets.clear()
        _table_cache.clear()
        _datasets["inv"] = DatasetEntry(
            df=pd.DataFrame({"Amount": [100]}),
            table="T",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 19),
            row_count=1,
        )
        _table_cache["inv"] = DatasetEntry(
            df=pd.DataFrame({"Amount": [999]}),
            table="inv",
            filter="",
            select="",
            loaded_at=datetime(2026, 2, 19),
            row_count=1,
            date_field="",
            date_min=None,
            date_max=None,
            pk_field="PK",
        )
        result = await analyze(dataset="inv", aggregate="sum:Amount")
        assert "100" in result


class TestValueMapParsing:
    """Tests for _parse_value_maps helper."""

    def test_valid_json_dict(self) -> None:
        from filemaker_mcp.tools.analytics import _parse_value_maps

        result = _parse_value_maps('{"Jake": "Jacob Owens", "Bob": "Robert Smith"}')
        assert result == {"Jake": "Jacob Owens", "Bob": "Robert Smith"}

    def test_empty_string(self) -> None:
        from filemaker_mcp.tools.analytics import _parse_value_maps

        assert _parse_value_maps("") == {}

    def test_none_input(self) -> None:
        from filemaker_mcp.tools.analytics import _parse_value_maps

        assert _parse_value_maps(None) == {}

    def test_malformed_json(self) -> None:
        from filemaker_mcp.tools.analytics import _parse_value_maps

        assert _parse_value_maps("not json") == {}

    def test_json_array_rejected(self) -> None:
        from filemaker_mcp.tools.analytics import _parse_value_maps

        assert _parse_value_maps("[1, 2, 3]") == {}

    def test_json_non_string_values_coerced(self) -> None:
        from filemaker_mcp.tools.analytics import _parse_value_maps

        result = _parse_value_maps('{"1": "Active", "2": "Inactive"}')
        assert result == {"1": "Active", "2": "Inactive"}


class TestApplyNormalization:
    """Tests for _apply_normalization helper."""

    def test_single_field_mapping(self) -> None:
        from filemaker_mcp.tools.analytics import _apply_normalization

        df = pd.DataFrame({"Driver": ["Jake", "Jacob Owens", "Jake", "Mike"]})
        mapping = {"Driver": {"Jake": "Jacob Owens"}}
        result_df, notes = _apply_normalization(df, mapping)
        assert list(result_df["Driver"]) == ["Jacob Owens", "Jacob Owens", "Jacob Owens", "Mike"]
        assert "Jake" in notes[0]
        assert "Jacob Owens" in notes[0]
        assert "2" in notes[0]  # 2 rows changed

    def test_multiple_field_mappings(self) -> None:
        from filemaker_mcp.tools.analytics import _apply_normalization

        df = pd.DataFrame(
            {
                "Driver": ["Jake", "Jacob Owens"],
                "Zone": ["CIN", "DAY"],
            }
        )
        mapping = {
            "Driver": {"Jake": "Jacob Owens"},
            "Zone": {"CIN": "", "DAY": ""},
        }
        result_df, notes = _apply_normalization(df, mapping)
        assert list(result_df["Driver"]) == ["Jacob Owens", "Jacob Owens"]
        assert list(result_df["Zone"]) == ["", ""]
        assert len(notes) == 2

    def test_no_mappings(self) -> None:
        from filemaker_mcp.tools.analytics import _apply_normalization

        df = pd.DataFrame({"Driver": ["Jake", "Mike"]})
        result_df, notes = _apply_normalization(df, {})
        assert list(result_df["Driver"]) == ["Jake", "Mike"]
        assert notes == []

    def test_mapping_source_not_in_data(self) -> None:
        from filemaker_mcp.tools.analytics import _apply_normalization

        df = pd.DataFrame({"Driver": ["Mike", "Sam"]})
        mapping = {"Driver": {"Jake": "Jacob Owens"}}
        result_df, notes = _apply_normalization(df, mapping)
        assert list(result_df["Driver"]) == ["Mike", "Sam"]
        assert notes == []  # No changes, no note

    def test_original_df_unchanged(self) -> None:
        from filemaker_mcp.tools.analytics import _apply_normalization

        df = pd.DataFrame({"Driver": ["Jake", "Mike"]})
        mapping = {"Driver": {"Jake": "Jacob Owens"}}
        _apply_normalization(df, mapping)
        assert list(df["Driver"]) == ["Jake", "Mike"]  # Original untouched


class TestCollectValueMaps:
    """Tests for _collect_value_maps — reads DDL Context for value_map entries."""

    def test_finds_mapping_for_groupby_field(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT
        from filemaker_mcp.tools.analytics import _collect_value_maps

        DDL_CONTEXT.clear()
        DDL_CONTEXT[("Invoices", "Technician", "value_map")] = {
            "context": '{"Jake": "Jacob Owens"}'
        }
        result = _collect_value_maps("Invoices", ["Technician"])
        assert result == {"Technician": {"Jake": "Jacob Owens"}}
        DDL_CONTEXT.clear()

    def test_no_mapping_for_field(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT
        from filemaker_mcp.tools.analytics import _collect_value_maps

        DDL_CONTEXT.clear()
        result = _collect_value_maps("Invoices", ["Technician"])
        assert result == {}
        DDL_CONTEXT.clear()

    def test_malformed_json_skipped_with_warning(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT
        from filemaker_mcp.tools.analytics import _collect_value_maps

        DDL_CONTEXT.clear()
        DDL_CONTEXT[("Invoices", "Technician", "value_map")] = {"context": "not json"}
        result = _collect_value_maps("Invoices", ["Technician"])
        assert result == {}
        DDL_CONTEXT.clear()

    def test_multiple_fields(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT
        from filemaker_mcp.tools.analytics import _collect_value_maps

        DDL_CONTEXT.clear()
        DDL_CONTEXT[("Invoices", "Technician", "value_map")] = {
            "context": '{"Jake": "Jacob Owens"}'
        }
        DDL_CONTEXT[("Invoices", "Region", "value_map")] = {"context": '{"CIN": ""}'}
        result = _collect_value_maps("Invoices", ["Technician", "Region"])
        assert result == {
            "Technician": {"Jake": "Jacob Owens"},
            "Region": {"CIN": ""},
        }
        DDL_CONTEXT.clear()

    def test_empty_fields_list(self) -> None:
        from filemaker_mcp.tools.analytics import _collect_value_maps

        assert _collect_value_maps("Invoices", []) == {}


class TestAnalyzeNormalization:
    """Tests for normalization integration in analyze()."""

    @pytest.fixture(autouse=True)
    def _setup_dataset(self) -> Generator[None, None, None]:
        """Load a test dataset with values that need normalization."""
        from filemaker_mcp.ddl import DDL_CONTEXT
        from filemaker_mcp.tools.analytics import DatasetEntry, _datasets

        df = pd.DataFrame(
            {
                "Technician": ["Jake", "Jake", "Jacob Owens", "Mike"],
                "Region": ["CIN", "CIN", "DAY", "DAY"],
                "Amount": [100.0, 200.0, 150.0, 300.0],
            }
        )
        _datasets["test_norm"] = DatasetEntry(
            df=df,
            table="Invoices",
            filter="",
            select="",
            loaded_at=datetime.now(),
            row_count=4,
        )
        DDL_CONTEXT[("Invoices", "Technician", "value_map")] = {
            "context": '{"Jake": "Jacob Owens"}'
        }
        DDL_CONTEXT[("Invoices", "Region", "value_map")] = {
            "context": '{"CIN": "", "DAY": ""}'
        }
        yield
        _datasets.pop("test_norm", None)
        DDL_CONTEXT.clear()

    @pytest.mark.asyncio
    async def test_groupby_normalizes(self) -> None:
        from filemaker_mcp.tools.analytics import analyze

        result = await analyze("test_norm", groupby="Technician", aggregate="sum:Amount")
        # Jake (100+200) merged with Jacob Owens (150) = 450
        assert "Jacob Owens" in result
        assert "Jake" not in result.split("Normalized")[0]  # Jake gone from data
        assert "450" in result

    @pytest.mark.asyncio
    async def test_normalization_note_appended(self) -> None:
        from filemaker_mcp.tools.analytics import analyze

        result = await analyze("test_norm", groupby="Technician", aggregate="sum:Amount")
        assert "Normalized:" in result
        assert "Jake" in result  # In the note
        assert "Jacob Owens" in result

    @pytest.mark.asyncio
    async def test_groupby_value_counts_normalizes(self) -> None:
        from filemaker_mcp.tools.analytics import analyze

        result = await analyze("test_norm", groupby="Technician")
        assert "Jacob Owens" in result
        assert "3" in result  # Jake(2) + Jacob Owens(1) = 3

    @pytest.mark.asyncio
    async def test_pivot_normalizes(self) -> None:
        from filemaker_mcp.tools.analytics import analyze

        result = await analyze(
            "test_norm",
            groupby="Region",
            pivot_column="Technician",
            aggregate="sum:Amount",
        )
        assert "Jacob Owens" in result
        assert "" in result
        assert "" in result

    @pytest.mark.asyncio
    async def test_no_mapping_unchanged(self) -> None:
        from filemaker_mcp.ddl import DDL_CONTEXT
        from filemaker_mcp.tools.analytics import analyze

        DDL_CONTEXT.clear()  # Remove all mappings
        result = await analyze("test_norm", groupby="Technician", aggregate="sum:Amount")
        assert "Jake" in result  # Raw value preserved
        assert "Normalized:" not in result

    @pytest.mark.asyncio
    async def test_original_dataset_untouched(self) -> None:
        from filemaker_mcp.tools.analytics import _datasets, analyze

        await analyze("test_norm", groupby="Technician", aggregate="sum:Amount")
        original = _datasets["test_norm"].df
        assert "Jake" in original["Technician"].values  # Still has raw value

    @pytest.mark.asyncio
    async def test_scalar_aggregate_no_normalization(self) -> None:
        from filemaker_mcp.tools.analytics import analyze

        result = await analyze("test_norm", aggregate="sum:Amount")
        assert "Normalized:" not in result  # No groupby, no normalization
