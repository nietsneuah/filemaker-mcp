"""Tests for ReportDates — date arithmetic for report patterns."""

from datetime import date


class TestReportDatesSinglePeriod:
    """Single-period queries: no comparison, just one date range."""

    def test_daily(self) -> None:
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 2, 20))
        assert rd.daily() == ("2026-02-20", "2026-02-20")

    def test_yesterday(self) -> None:
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 2, 20))
        assert rd.yesterday() == ("2026-02-19", "2026-02-19")

    def test_wtd(self) -> None:
        """Week to date: Monday through today (Fri 2/20)."""
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 2, 20))
        assert rd.wtd() == ("2026-02-16", "2026-02-20")

    def test_wtd_on_monday(self) -> None:
        """WTD on Monday = just that day."""
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 2, 16))
        assert rd.wtd() == ("2026-02-16", "2026-02-16")

    def test_mtd(self) -> None:
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 2, 20))
        assert rd.mtd() == ("2026-02-01", "2026-02-20")

    def test_full_month(self) -> None:
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 2, 20))
        assert rd.full_month() == ("2026-02-01", "2026-02-28")

    def test_full_month_leap_year(self) -> None:
        """2024 is a leap year — Feb has 29 days."""
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2024, 2, 15))
        assert rd.full_month() == ("2024-02-01", "2024-02-29")

    def test_qtd(self) -> None:
        """Q1 to date: Jan 1 → today."""
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 2, 20))
        assert rd.qtd() == ("2026-01-01", "2026-02-20")

    def test_qtd_q2(self) -> None:
        """Q2: Apr 1 → May 15."""
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 5, 15))
        assert rd.qtd() == ("2026-04-01", "2026-05-15")

    def test_qtd_q3(self) -> None:
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 8, 10))
        assert rd.qtd() == ("2026-07-01", "2026-08-10")

    def test_qtd_q4(self) -> None:
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 11, 5))
        assert rd.qtd() == ("2026-10-01", "2026-11-05")

    def test_ytd(self) -> None:
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 2, 20))
        assert rd.ytd() == ("2026-01-01", "2026-02-20")


class TestReportDatesComparative:
    """Comparative queries: current vs previous period with matching offset."""

    def test_dod(self) -> None:
        """Day over day: today vs yesterday."""
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 2, 20))
        current, previous = rd.dod()
        assert current == ("2026-02-20", "2026-02-20")
        assert previous == ("2026-02-19", "2026-02-19")

    def test_wow(self) -> None:
        """Week over week: this WTD vs same days last week."""
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 2, 20))
        current, previous = rd.wow()
        assert current == ("2026-02-16", "2026-02-20")
        assert previous == ("2026-02-09", "2026-02-13")

    def test_mom(self) -> None:
        """Month over month: full Feb vs full Jan."""
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 2, 20))
        current, previous = rd.mom()
        assert current == ("2026-02-01", "2026-02-28")
        assert previous == ("2026-01-01", "2026-01-31")

    def test_mom_january(self) -> None:
        """MOM in January wraps to previous year December."""
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 1, 15))
        current, previous = rd.mom()
        assert current == ("2026-01-01", "2026-01-31")
        assert previous == ("2025-12-01", "2025-12-31")

    def test_cmtd_vs_pmtd(self) -> None:
        """Current MTD vs previous MTD: same day offset."""
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 2, 20))
        current, previous = rd.cmtd_vs_pmtd()
        assert current == ("2026-02-01", "2026-02-20")
        assert previous == ("2026-01-01", "2026-01-20")

    def test_cmtd_vs_pmtd_day31(self) -> None:
        """Day 31 in March — prev month (Feb) only has 28 days. Cap at month end."""
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 3, 31))
        current, previous = rd.cmtd_vs_pmtd()
        assert current == ("2026-03-01", "2026-03-31")
        assert previous == ("2026-02-01", "2026-02-28")

    def test_cmtd_vs_pmtd_january(self) -> None:
        """CMTD in January: previous is December of prior year."""
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 1, 15))
        current, previous = rd.cmtd_vs_pmtd()
        assert current == ("2026-01-01", "2026-01-15")
        assert previous == ("2025-12-01", "2025-12-15")

    def test_mtd_cy_vs_py(self) -> None:
        """MTD current year vs prior year: same month, same day offset."""
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 2, 20))
        current, previous = rd.mtd_cy_vs_py()
        assert current == ("2026-02-01", "2026-02-20")
        assert previous == ("2025-02-01", "2025-02-20")

    def test_ytd_cy_vs_py(self) -> None:
        """YTD current year vs prior year."""
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 2, 20))
        current, previous = rd.ytd_cy_vs_py()
        assert current == ("2026-01-01", "2026-02-20")
        assert previous == ("2025-01-01", "2025-02-20")

    def test_qtd_cq_vs_pq(self) -> None:
        """QTD current quarter vs previous quarter: same offset into quarter."""
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 2, 20))
        current, previous = rd.qtd_cq_vs_pq()
        # Q1: Jan 1 → Feb 20 = 51 days offset
        assert current == ("2026-01-01", "2026-02-20")
        # Q4 2025: Oct 1 + 51 days = Nov 20
        assert previous == ("2025-10-01", "2025-11-20")

    def test_qtd_cq_vs_pq_q2(self) -> None:
        """Q2 vs Q1: May 15 is day 45 of Q2, so Q1 prev = Jan 1 + 44 = Feb 14."""
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 5, 15))
        current, previous = rd.qtd_cq_vs_pq()
        assert current == ("2026-04-01", "2026-05-15")
        assert previous == ("2026-01-01", "2026-02-14")

    def test_qtd_cq_vs_pq_py(self) -> None:
        """QTD current quarter vs same quarter prior year."""
        from filemaker_mcp.dates import ReportDates

        rd = ReportDates(date(2026, 2, 20))
        current, previous = rd.qtd_cq_vs_pq_py()
        assert current == ("2026-01-01", "2026-02-20")
        assert previous == ("2025-01-01", "2025-02-20")


class TestBuildPeriodFilter:
    """Test OData filter string construction."""

    def test_single_day(self) -> None:
        from filemaker_mcp.dates import build_period_filter

        result = build_period_filter("ServiceDate", "2026-02-20", "2026-02-20")
        assert result == "ServiceDate eq 2026-02-20"

    def test_range(self) -> None:
        from filemaker_mcp.dates import build_period_filter

        result = build_period_filter("ServiceDate", "2026-02-01", "2026-02-20")
        assert result == "ServiceDate ge 2026-02-01 and ServiceDate le 2026-02-20"

    def test_custom_field_name(self) -> None:
        from filemaker_mcp.dates import build_period_filter

        result = build_period_filter("Order_Date", "2026-01-01", "2026-03-31")
        assert result == "Order_Date ge 2026-01-01 and Order_Date le 2026-03-31"
