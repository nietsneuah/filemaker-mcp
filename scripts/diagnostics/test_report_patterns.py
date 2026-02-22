"""Report Pattern Integration Test Runner.

Runs realistic report queries against a live FM server, captures structured
metrics (response size, timing, cache behavior), and produces a JSON baseline
+ markdown summary for system improvement analysis.

Developer tool — discovers date fields from DDL (bootstrap metadata), probes
each candidate to validate it works in OData, then runs full report patterns
on validated fields. Optionally feeds back validated configs to DDL_Context.

Usage:
    cd /path/to/filemaker-mcp
    uv run python scripts/test_report_patterns.py
    uv run python scripts/test_report_patterns.py --seed  # write cache_config to DDL Context
    uv run python scripts/test_report_patterns.py --tenant acme_uat
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # scripts/ root
from _common import add_tenant_arg, bootstrap_tenant

from filemaker_mcp.dates import ReportDates, build_period_filter

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DDL-driven schema discovery
# ---------------------------------------------------------------------------


def discover_date_fields() -> list[dict[str, str]]:
    """Find all date/datetime fields across OData-exposed tables.

    Scans TABLES (from bootstrap DDL) for fields with type 'datetime' or 'date',
    intersected with EXPOSED_TABLES (OData-accessible). No DDL_Context dependency.

    Returns list of {"table": name, "date_field": field_name} dicts.
    """
    from filemaker_mcp.ddl import get_all_date_fields
    from filemaker_mcp.tools.query import EXPOSED_TABLES

    candidates = []
    for table, fields in get_all_date_fields().items():
        if table in EXPOSED_TABLES:
            for field in fields:
                candidates.append({"table": table, "date_field": field})
    return candidates


PROBE_TIMEOUT_S = 30  # Probe timeout — shorter than full query


async def probe_date_field(table: str, date_field: str) -> str | None:
    """Probe a date field with a simple daily query to verify it works in OData.

    Returns None on success, or an error message string on failure.
    Uses a tight timeout to avoid hanging on tables with data issues.
    """
    from filemaker_mcp.tools.query import query_records

    today = date.today().isoformat()
    filter_str = build_period_filter(date_field, today, today)
    try:
        result = await asyncio.wait_for(
            query_records(
                table=table,
                filter=filter_str,
                select=date_field,
                top=1,
            ),
            timeout=PROBE_TIMEOUT_S,
        )
        if result.startswith("Error:"):
            return result
        return None
    except TimeoutError:
        return f"Probe timeout after {PROBE_TIMEOUT_S}s"
    except Exception as e:
        return str(e)


def get_report_select(table: str) -> str:
    """Read report_select from DDL Context for a table.

    Returns comma-separated field list, or empty string (= all columns).
    """
    from filemaker_mcp.ddl import get_context_value

    return get_context_value(table, "report_select") or ""


# ---------------------------------------------------------------------------
# Query execution + metric capture
# ---------------------------------------------------------------------------


@dataclass
class QueryResult:
    """Structured metrics from a single query execution."""

    pattern: str
    label: str  # "single", "current", or "previous"
    table: str
    filter_raw: str
    filter_normalized: str
    date_range: list[str | None]
    select: str
    rows: int = 0
    columns: int = 0
    response_chars: int = 0
    elapsed_s: float = 0.0
    cache_status: str = ""
    error: str | None = None


QUERY_TIMEOUT_S = 60  # Per-query timeout in seconds


async def run_query(
    pattern: str,
    label: str,
    date_range: tuple[str, str],
    select: str,
    table: str,
    date_field: str,
) -> QueryResult:
    """Execute a single query and capture metrics."""
    from filemaker_mcp.tools.query import (
        extract_date_range,
        normalize_dates_in_filter,
        query_records,
    )

    filter_raw = build_period_filter(date_field, date_range[0], date_range[1])
    filter_normalized = normalize_dates_in_filter(filter_raw)
    extracted = extract_date_range(filter_normalized, date_field)

    t0 = time.perf_counter()
    try:
        result_text = await asyncio.wait_for(
            query_records(
                table=table,
                filter=filter_raw,
                select=select,
                top=10000,
                count=True,
            ),
            timeout=QUERY_TIMEOUT_S,
        )
        elapsed = time.perf_counter() - t0

        # Parse row count from response text: "Found N total records"
        rows = 0
        for line in result_text.split("\n"):
            if "total records" in line.lower():
                m = re.search(r"(\d+)\s+total\s+records", line, re.IGNORECASE)
                if m:
                    rows = int(m.group(1))
                break

        # Count columns from first record block
        columns = 0
        in_record = False
        for line in result_text.split("\n"):
            if line.strip().startswith("--- Record 1 ---"):
                in_record = True
                continue
            if in_record:
                if line.strip().startswith("---"):
                    break
                if ":" in line and line.strip():
                    columns += 1

        # Detect cache status from response text
        if "Cache" in result_text and "rows cached" in result_text.lower():
            cache_status = "HIT"
        else:
            cache_status = "MISS"

        return QueryResult(
            pattern=pattern,
            label=label,
            table=table,
            filter_raw=filter_raw,
            filter_normalized=filter_normalized,
            date_range=[extracted[0], extracted[1]],
            select=select or "(all)",
            rows=rows,
            columns=columns,
            response_chars=len(result_text),
            elapsed_s=round(elapsed, 3),
            cache_status=cache_status,
        )

    except TimeoutError:
        elapsed = time.perf_counter() - t0
        return QueryResult(
            pattern=pattern,
            label=label,
            table=table,
            filter_raw=filter_raw,
            filter_normalized=filter_normalized,
            date_range=[extracted[0], extracted[1]],
            select=select or "(all)",
            elapsed_s=round(elapsed, 3),
            error=f"Timeout after {QUERY_TIMEOUT_S}s",
        )

    except Exception as e:
        elapsed = time.perf_counter() - t0
        return QueryResult(
            pattern=pattern,
            label=label,
            table=table,
            filter_raw=filter_raw,
            filter_normalized=filter_normalized,
            date_range=[extracted[0], extracted[1]],
            select=select or "(all)",
            elapsed_s=round(elapsed, 3),
            error=str(e),
        )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def get_all_patterns(rd: ReportDates) -> list[tuple[str, str, tuple[str, str]]]:
    """Build all query patterns as (pattern_name, label, date_range) triples."""
    patterns: list[tuple[str, str, tuple[str, str]]] = []

    # Single-period
    patterns.append(("Daily", "single", rd.daily()))
    patterns.append(("Yesterday", "single", rd.yesterday()))
    patterns.append(("WTD", "single", rd.wtd()))
    patterns.append(("MTD", "single", rd.mtd()))
    patterns.append(("Full Month", "single", rd.full_month()))
    patterns.append(("QTD", "single", rd.qtd()))
    patterns.append(("YTD", "single", rd.ytd()))

    # Comparative
    for name, method in [
        ("DOD", rd.dod),
        ("WOW", rd.wow),
        ("MOM", rd.mom),
        ("CMTD vs PMTD", rd.cmtd_vs_pmtd),
        ("MTD CY vs PY", rd.mtd_cy_vs_py),
        ("YTD CY vs PY", rd.ytd_cy_vs_py),
        ("QTD CQ vs PQ", rd.qtd_cq_vs_pq),
        ("QTD CQ vs PQ PY", rd.qtd_cq_vs_pq_py),
    ]:
        current, previous = method()
        patterns.append((name, "current", current))
        patterns.append((name, "previous", previous))

    return patterns


async def run_pass(
    patterns: list[tuple[str, str, tuple[str, str]]],
    select: str,
    pass_name: str,
    table: str,
    date_field: str,
) -> list[QueryResult]:
    """Run all patterns sequentially, return results.

    Fail-fast: if any of the first 3 queries error, abort this pass.
    This prevents wasting minutes on tables with systemic OData issues.
    """
    results: list[QueryResult] = []
    early_errors = 0
    for i, (pattern_name, label, date_range) in enumerate(patterns):
        print(f"  {pass_name}: {pattern_name} ({label})...", end=" ", flush=True)
        result = await run_query(pattern_name, label, date_range, select, table, date_field)
        if result.error:
            print(f"ERROR: {result.error}")
            if i < 3:
                early_errors += 1
                if early_errors >= 2:
                    print(
                        f"  *** FAIL-FAST: {early_errors} early errors"
                        f" — skipping rest of {pass_name}"
                    )
                    results.append(result)
                    break
        else:
            print(
                f"{result.rows} rows, {result.response_chars:,} chars, "
                f"{result.elapsed_s}s, {result.cache_status}"
            )
        results.append(result)
    return results


def format_table(results: list[QueryResult], title: str) -> str:
    """Format results as a markdown table."""
    lines = [
        f"\n{title}",
        f"{'Pattern':<20} | {'Label':<8} | {'Filter':<45} | {'Rows':>5} | "
        f"{'Chars':>8} | {'Time':>6} | Cache",
        "-" * 115,
    ]
    for r in results:
        if r.error:
            lines.append(f"{r.pattern:<20} | {r.label:<8} | ERROR: {r.error}")
        else:
            # Strip date field name prefix for compact display
            filt = r.filter_raw
            for prefix in [r.table + ".", ""]:
                filt = filt.replace(prefix, "")
            # Remove repeated field name from multi-clause filters
            parts = filt.split(" and ")
            if len(parts) == 2:
                filt = " and ".join(
                    p.split(" ", 1)[-1] if i > 0 else p for i, p in enumerate(parts)
                )
            if len(filt) > 45:
                filt = filt[:42] + "..."
            lines.append(
                f"{r.pattern:<20} | {r.label:<8} | {filt:<45} | "
                f"{r.rows:>5} | {r.response_chars:>8,} | {r.elapsed_s:>5.1f}s | "
                f"{r.cache_status}"
            )
    return "\n".join(lines)


def format_select_impact(with_select: list[QueryResult], no_select: list[QueryResult]) -> str:
    """Format comparison between with-select and no-select."""
    lines = [
        "\nSELECT IMPACT (chars with vs without $select):",
        f"{'Pattern':<20} | {'Label':<8} | {'With':>10} | {'Without':>10} | {'Reduction':>9}",
        "-" * 70,
    ]
    for ws, ns in zip(with_select, no_select, strict=False):
        if ws.error or ns.error:
            continue
        reduction = (
            f"{(1 - ws.response_chars / ns.response_chars) * 100:.0f}%"
            if ns.response_chars > 0
            else "N/A"
        )
        lines.append(
            f"{ws.pattern:<20} | {ws.label:<8} | {ws.response_chars:>10,} | "
            f"{ns.response_chars:>10,} | {reduction:>9}"
        )
    return "\n".join(lines)


async def main() -> None:
    """Run all report pattern tests."""
    from filemaker_mcp.auth import odata_client
    from filemaker_mcp.tools.analytics import _table_cache

    parser = argparse.ArgumentParser(description="Report pattern integration tests")
    parser.add_argument("--seed", action="store_true", help="Write cache_config to DDL Context")
    add_tenant_arg(parser)
    args = parser.parse_args()

    seed_mode = args.seed

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    # Bootstrap FM connection
    print("Bootstrapping FM connection...")
    tenant = await bootstrap_tenant(args.tenant)
    print(f"Connected to {tenant.host}/{tenant.database} as {tenant.username}")

    # Discover date fields from DDL (bootstrap already populated TABLES)
    candidates = discover_date_fields()
    if not candidates:
        print("ERROR: No date/datetime fields found in any OData-exposed table.")
        print("Check that bootstrap_ddl() completed successfully.")
        sys.exit(1)

    print(f"Found {len(candidates)} date field candidates. Probing...")

    # Probe each candidate — skip fields that don't work in OData
    report_tables: list[dict[str, str]] = []
    failed_probes: list[dict[str, str]] = []
    for candidate in candidates:
        table = candidate["table"]
        date_field = candidate["date_field"]
        error = await probe_date_field(table, date_field)
        if error:
            print(f"  SKIP {table}.{date_field}: {error}")
            failed_probes.append({**candidate, "error": error})
        else:
            print(f"  OK   {table}.{date_field}")
            report_tables.append(candidate)

    if not report_tables:
        print("\nERROR: All date field probes failed. No tables to test.")
        sys.exit(1)

    print(f"\n{len(report_tables)} validated, {len(failed_probes)} skipped")

    # Date arithmetic
    current_date = date.today()
    rd = ReportDates(current_date)
    patterns = get_all_patterns(rd)
    print(f"Current date: {current_date} — {len(patterns)} queries per pass")

    all_reports: list[dict] = []

    for rt in report_tables:
        table = rt["table"]
        date_field = rt["date_field"]
        select_fields = get_report_select(table)
        select_label = f"{len(select_fields.split(','))} cols" if select_fields else "all cols"

        print(f"\n{'=' * 60}")
        print(f"TABLE: {table}.{date_field} (select={select_label})")
        print(f"{'=' * 60}")

        # Pass 1: Cold cache, with $select
        _table_cache.clear()
        print("\nPASS 1: Cold cache, with $select")
        cold_select = await run_pass(patterns, select_fields, "cold+select", table, date_field)

        # Pass 2: Cold cache, no $select (flush cache first)
        _table_cache.clear()
        print("\nPASS 2: Cold cache, no $select (all columns)")
        cold_no_select = await run_pass(patterns, "", "cold+no_select", table, date_field)

        # Pass 3: Warm cache, with $select (cache populated from pass 2)
        print("\nPASS 3: Warm cache, with $select")
        warm_select = await run_pass(patterns, select_fields, "warm+select", table, date_field)

        # Pass 4: Warm cache, no $select
        print("\nPASS 4: Warm cache, no $select")
        warm_no_select = await run_pass(patterns, "", "warm+no_select", table, date_field)

        # --- Summary output ---
        print(f"\n{'=' * 80}")
        print(f"RESULTS — {table}.{date_field} — {current_date} ({tenant.host}/{tenant.database})")
        print("=" * 80)
        print(format_table(cold_select, "COLD CACHE — With $select:"))
        print(format_table(cold_no_select, "COLD CACHE — No $select:"))
        print(format_table(warm_select, "WARM CACHE — With $select:"))
        print(format_table(warm_no_select, "WARM CACHE — No $select:"))
        if select_fields:
            print(format_select_impact(cold_select, cold_no_select))

        all_reports.append(
            {
                "table": table,
                "date_field": date_field,
                "select_fields": select_fields,
                "passes": {
                    "cold_with_select": [asdict(r) for r in cold_select],
                    "cold_no_select": [asdict(r) for r in cold_no_select],
                    "warm_with_select": [asdict(r) for r in warm_select],
                    "warm_no_select": [asdict(r) for r in warm_no_select],
                },
                "summary": {
                    "total_queries": sum(
                        len(p)
                        for p in [
                            cold_select,
                            cold_no_select,
                            warm_select,
                            warm_no_select,
                        ]
                    ),
                    "errors": sum(
                        1
                        for p in [
                            cold_select,
                            cold_no_select,
                            warm_select,
                            warm_no_select,
                        ]
                        for r in p
                        if r.error
                    ),
                },
            }
        )

    # Seed DDL_Context if requested
    if seed_mode:
        from filemaker_mcp.tools.context import save_context

        print("\nSeeding DDL_Context for validated fields...")

        # Write cache_config for validated date fields
        for rt in report_tables:
            table = rt["table"]
            field = rt["date_field"]
            result = await save_context(
                table_name=table,
                context="date_key",
                field_name=field,
                context_type="cache_config",
                source="test_report_patterns:validated",
            )
            print(f"  cache_config {table}.{field}: {result}")

        # Note report_select status for validated tables
        seeded_tables = {rt["table"] for rt in report_tables}
        for table in seeded_tables:
            existing = get_report_select(table)
            if not existing:
                print(f"  report_select {table}: not set (set manually if needed)")

    # JSON report
    report = {
        "run_date": current_date.isoformat(),
        "current_date": current_date.isoformat(),
        "server": tenant.host,
        "database": tenant.database,
        "discovery": {
            "candidates": len(candidates),
            "validated": len(report_tables),
            "failed": [
                {"table": f["table"], "field": f["date_field"], "error": f["error"]}
                for f in failed_probes
            ],
        },
        "tables": all_reports,
    }

    report_dir = Path(__file__).parent.parent.parent / "docs" / "internal" / "reports"
    report_dir.mkdir(exist_ok=True)
    report_path = report_dir / f"report-patterns-{current_date}.json"
    report_path.write_text(json.dumps(report, indent=2))
    print(f"\nJSON report saved to: {report_path}")

    await odata_client.close()


if __name__ == "__main__":
    asyncio.run(main())
