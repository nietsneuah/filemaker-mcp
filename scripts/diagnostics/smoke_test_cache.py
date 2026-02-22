"""Live smoke test for the hybrid cache + enrichment + analytics pipeline.

Tests the full flow against the live FM UAT server:
1. Date-range caching (Invoices)
2. Cache-all caching (Drivers)
3. Result enrichment (context hints)
4. Table cache fallback in analyze()
5. Time-series aggregation
6. Pivot cross-tabulation
7. Flush + re-fetch

Run: cd ~/Dev/filemaker-mcp && uv run python scripts/smoke_test_cache.py
     cd ~/Dev/filemaker-mcp && uv run python scripts/smoke_test_cache.py --tenant acme_uat
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # scripts/ root
from _common import add_tenant_arg, bootstrap_tenant

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke test: cache + enrichment")
    add_tenant_arg(parser)
    args = parser.parse_args()

    from filemaker_mcp.ddl import get_cache_config
    from filemaker_mcp.tools.analytics import _table_cache, analyze, flush_datasets
    from filemaker_mcp.tools.query import query_records

    passed = 0
    failed = 0

    def check(name: str, condition: bool, detail: str = "") -> None:
        nonlocal passed, failed
        status = "PASS" if condition else "FAIL"
        if not condition:
            failed += 1
        else:
            passed += 1
        suffix = f" — {detail}" if detail else ""
        print(f"  [{status}] {name}{suffix}")

    # --- Setup ---
    print("=== Setup ===")
    tenant = await bootstrap_tenant(args.tenant)
    print(f"Connected to {tenant.host}/{tenant.database}")

    # Verify cache configs loaded
    ihdr_config = get_cache_config("Invoices")
    check(
        "cache_config loaded for Invoices",
        ihdr_config is not None and ihdr_config["mode"] == "date_range",
        str(ihdr_config),
    )
    drivers_config = get_cache_config("Drivers")
    check(
        "cache_config loaded for Drivers",
        drivers_config is not None and drivers_config["mode"] == "cache_all",
        str(drivers_config),
    )

    # --- Test 1: Date-range caching ---
    print("\n=== Test 1: Date-Range Cache (Invoices) ===")
    _table_cache.clear()  # Start fresh

    result1 = await query_records(
        table="Invoices",
        filter="ServiceDate ge 2026-01-01 and ServiceDate le 2026-01-31",
        top=5,
    )
    check("First query returns records", "Record 1" in result1 or "Found" in result1)
    check("Invoices is now cached", "Invoices" in _table_cache)
    if "Invoices" in _table_cache:
        entry = _table_cache["Invoices"]
        check("Cache has rows", entry.row_count > 0, f"{entry.row_count} rows")
        check(
            "Date bounds set",
            entry.date_min is not None and entry.date_max is not None,
            f"{entry.date_min} → {entry.date_max}",
        )
        initial_rows = entry.row_count

    check("Result has Cache section", "--- Cache ---" in result1)

    # --- Test 2: Cache hit (narrower range) ---
    print("\n=== Test 2: Cache Hit (Subset Query) ===")
    result2 = await query_records(
        table="Invoices",
        filter="ServiceDate ge 2026-01-10 and ServiceDate le 2026-01-20",
        top=5,
    )
    check(
        "Subset query returns records",
        "Record 1" in result2 or "Found" in result2 or "No records" in result2,
    )
    if "Invoices" in _table_cache:
        entry = _table_cache["Invoices"]
        check(
            "Cache row count unchanged (no new fetch)",
            entry.row_count == initial_rows,
            f"still {entry.row_count} rows",
        )

    # --- Test 3: Cache extension (wider range) ---
    print("\n=== Test 3: Cache Extension (Wider Range) ===")
    result3 = await query_records(
        table="Invoices",
        filter="ServiceDate ge 2025-12-01 and ServiceDate le 2026-02-28",
        top=5,
    )
    check("Extended query returns records", "Record 1" in result3 or "Found" in result3)
    if "Invoices" in _table_cache:
        entry = _table_cache["Invoices"]
        check(
            "Cache grew after extension",
            entry.row_count >= initial_rows,
            f"now {entry.row_count} rows (was {initial_rows})",
        )

    # --- Test 4: Cache-all (Drivers) ---
    print("\n=== Test 4: Cache-All (Drivers) ===")
    _table_cache.pop("Drivers", None)  # Ensure clean

    result4 = await query_records(table="Drivers", top=5)
    check("Drivers query returns records", "Record 1" in result4 or "Found" in result4)
    check("Drivers is now cached", "Drivers" in _table_cache)
    if "Drivers" in _table_cache:
        check(
            "Drivers cache has rows",
            _table_cache["Drivers"].row_count > 0,
            f"{_table_cache['Drivers'].row_count} rows",
        )

    # --- Test 5: Analyze from table cache ---
    print("\n=== Test 5: Analyze from Table Cache ===")
    result5 = await analyze(dataset="Invoices")
    check(
        "analyze() resolves table cache",
        "Summary statistics" in result5,
        result5[:80] + "..." if len(result5) > 80 else result5,
    )

    # --- Test 6: Time-series ---
    print("\n=== Test 6: Time-Series Aggregation ===")
    result6 = await analyze(
        dataset="Invoices",
        groupby="ServiceDate",
        aggregate="count:ServiceDate",
        period="month",
    )
    check(
        "Time-series returns results",
        "Time-series" in result6 or "error" not in result6.lower(),
        result6[:100] + "..." if len(result6) > 100 else result6,
    )

    # --- Test 7: New agg functions ---
    print("\n=== Test 7: New Aggregation Functions ===")
    # Find a numeric column to aggregate
    if "Invoices" in _table_cache:
        cols = _table_cache["Invoices"].df.columns.tolist()
        # Try Amount or any numeric-looking column
        numeric_col = None
        for c in cols:
            if "total" in c.lower() or "amount" in c.lower() or "price" in c.lower():
                numeric_col = c
                break
        if numeric_col:
            result7 = await analyze(
                dataset="Invoices",
                aggregate=f"median:{numeric_col},std:{numeric_col}",
            )
            check(
                f"Median + std work on {numeric_col}",
                "Analysis" in result7 and "error" not in result7.lower(),
                result7[:100] + "..." if len(result7) > 100 else result7,
            )
        else:
            check("Found numeric column for agg test", False, f"columns: {cols[:10]}")

    # --- Test 8: Flush + re-fetch ---
    print("\n=== Test 8: Flush + Re-fetch ===")
    flush_result = await flush_datasets(table="Invoices")
    check("Flush confirms removal", "Flushed" in flush_result, flush_result)
    check("Table removed from cache", "Invoices" not in _table_cache)

    result8 = await query_records(
        table="Invoices",
        filter="ServiceDate ge 2026-01-01 and ServiceDate le 2026-01-31",
        top=3,
    )
    check("Re-fetch works after flush", "Record 1" in result8 or "Found" in result8)
    check("Re-cached after flush", "Invoices" in _table_cache)

    # --- Test 9: Result enrichment ---
    print("\n=== Test 9: Result Enrichment ===")
    # Check if any context hints are present in any of the results
    has_context = any("--- Context ---" in r for r in [result1, result2, result3, result4, result8])
    check(
        "At least one result has context hints",
        has_context,
        "Context hints depend on DDL Context entries for the queried fields",
    )

    # --- Summary ---
    print(f"\n{'=' * 40}")
    print(f"Results: {passed} passed, {failed} failed, {passed + failed} total")
    if failed > 0:
        print("SOME TESTS FAILED — review output above")
        sys.exit(1)
    else:
        print("ALL TESTS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
