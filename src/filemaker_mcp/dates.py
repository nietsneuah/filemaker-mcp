"""Generic date period arithmetic for report patterns.

Schema-agnostic — no imports from filemaker_mcp, no pandas, no httpx.
Callers pass in field names dynamically (from DDL Context or elsewhere).
"""

from __future__ import annotations

import calendar
from datetime import date, timedelta


class ReportDates:
    """Compute date ranges for all report patterns from a single current_date.

    Every method returns a tuple of (start_iso, end_iso) strings.
    Comparative methods return ((current_start, current_end), (prev_start, prev_end)).
    """

    def __init__(self, current_date: date) -> None:
        self.today = current_date

    def _iso(self, d: date) -> str:
        return d.isoformat()

    def _month_end(self, year: int, month: int) -> date:
        """Last day of given month."""
        _, last_day = calendar.monthrange(year, month)
        return date(year, month, last_day)

    def _quarter_start(self, d: date) -> date:
        """First day of the quarter containing d."""
        q_month = ((d.month - 1) // 3) * 3 + 1
        return date(d.year, q_month, 1)

    def _prev_month_start(self, d: date) -> date:
        """First day of the month before d's month."""
        if d.month == 1:
            return date(d.year - 1, 12, 1)
        return date(d.year, d.month - 1, 1)

    # --- Single-period ---

    def daily(self) -> tuple[str, str]:
        iso = self._iso(self.today)
        return (iso, iso)

    def yesterday(self) -> tuple[str, str]:
        y = self._iso(self.today - timedelta(days=1))
        return (y, y)

    def wtd(self) -> tuple[str, str]:
        """Week to date: Monday of current week through today."""
        monday = self.today - timedelta(days=self.today.weekday())
        return (self._iso(monday), self._iso(self.today))

    def mtd(self) -> tuple[str, str]:
        start = date(self.today.year, self.today.month, 1)
        return (self._iso(start), self._iso(self.today))

    def full_month(self) -> tuple[str, str]:
        start = date(self.today.year, self.today.month, 1)
        end = self._month_end(self.today.year, self.today.month)
        return (self._iso(start), self._iso(end))

    def qtd(self) -> tuple[str, str]:
        start = self._quarter_start(self.today)
        return (self._iso(start), self._iso(self.today))

    def ytd(self) -> tuple[str, str]:
        start = date(self.today.year, 1, 1)
        return (self._iso(start), self._iso(self.today))

    # --- Comparative ---

    def dod(self) -> tuple[tuple[str, str], tuple[str, str]]:
        """Day over day."""
        return (self.daily(), self.yesterday())

    def wow(self) -> tuple[tuple[str, str], tuple[str, str]]:
        """Week over week: this WTD vs same days previous week."""
        current = self.wtd()
        monday = self.today - timedelta(days=self.today.weekday())
        prev_monday = monday - timedelta(days=7)
        prev_end = prev_monday + (self.today - monday)
        return (current, (self._iso(prev_monday), self._iso(prev_end)))

    def mom(self) -> tuple[tuple[str, str], tuple[str, str]]:
        """Month over month: full current month vs full previous month."""
        current = self.full_month()
        prev_start = self._prev_month_start(self.today)
        prev_end = self._month_end(prev_start.year, prev_start.month)
        return (current, (self._iso(prev_start), self._iso(prev_end)))

    def cmtd_vs_pmtd(self) -> tuple[tuple[str, str], tuple[str, str]]:
        """Current MTD vs previous MTD: same day-of-month offset."""
        current = self.mtd()
        prev_start = self._prev_month_start(self.today)
        prev_month_end = self._month_end(prev_start.year, prev_start.month)
        prev_day = min(self.today.day, prev_month_end.day)
        prev_end = date(prev_start.year, prev_start.month, prev_day)
        return (current, (self._iso(prev_start), self._iso(prev_end)))

    def mtd_cy_vs_py(self) -> tuple[tuple[str, str], tuple[str, str]]:
        """MTD current year vs same month prior year."""
        current = self.mtd()
        prev_start = date(self.today.year - 1, self.today.month, 1)
        prev_day = min(
            self.today.day,
            self._month_end(prev_start.year, prev_start.month).day,
        )
        prev_end = date(prev_start.year, self.today.month, prev_day)
        return (current, (self._iso(prev_start), self._iso(prev_end)))

    def ytd_cy_vs_py(self) -> tuple[tuple[str, str], tuple[str, str]]:
        """YTD current year vs prior year through same month/day."""
        current = self.ytd()
        prev_start = date(self.today.year - 1, 1, 1)
        try:
            prev_end = date(self.today.year - 1, self.today.month, self.today.day)
        except ValueError:
            prev_end = self._month_end(self.today.year - 1, self.today.month)
        return (current, (self._iso(prev_start), self._iso(prev_end)))

    def qtd_cq_vs_pq(self) -> tuple[tuple[str, str], tuple[str, str]]:
        """QTD current quarter vs previous quarter: same offset into quarter."""
        current_q_start = self._quarter_start(self.today)
        current = self.qtd()
        offset_days = (self.today - current_q_start).days
        if current_q_start.month == 1:
            prev_q_start = date(self.today.year - 1, 10, 1)
        else:
            prev_q_start = date(self.today.year, current_q_start.month - 3, 1)
        prev_end = prev_q_start + timedelta(days=offset_days)
        return (current, (self._iso(prev_q_start), self._iso(prev_end)))

    def qtd_cq_vs_pq_py(self) -> tuple[tuple[str, str], tuple[str, str]]:
        """QTD current quarter vs same quarter prior year."""
        current = self.qtd()
        current_q_start = self._quarter_start(self.today)
        prev_q_start = date(self.today.year - 1, current_q_start.month, 1)
        try:
            prev_end = date(self.today.year - 1, self.today.month, self.today.day)
        except ValueError:
            prev_end = self._month_end(self.today.year - 1, self.today.month)
        return (current, (self._iso(prev_q_start), self._iso(prev_end)))


def build_period_filter(date_field: str, start: str, end: str) -> str:
    """Build an OData filter expression for a date range.

    Args:
        date_field: Field name (dynamic — passed in by caller).
        start: ISO date string (YYYY-MM-DD).
        end: ISO date string (YYYY-MM-DD).

    Returns:
        "field eq start" if start==end, else "field ge start and field le end".
    """
    if start == end:
        return f"{date_field} eq {start}"
    return f"{date_field} ge {start} and {date_field} le {end}"
