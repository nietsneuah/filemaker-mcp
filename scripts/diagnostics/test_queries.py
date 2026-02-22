"""Realistic test queries demonstrating the hybrid cache + enrichment + analytics pipeline.

Simulates the kind of queries Claude Desktop would make in a real session.

Run: cd ~/Dev/filemaker-mcp && uv run python scripts/test_queries.py
     cd ~/Dev/filemaker-mcp && uv run python scripts/test_queries.py --tenant acme_uat
"""

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # scripts/ root
from _common import add_tenant_arg, bootstrap_tenant

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Realistic cache + enrichment + analytics queries")
    add_tenant_arg(parser)
    args = parser.parse_args()

    from filemaker_mcp.tools.analytics import _table_cache, analyze, flush_datasets
    from filemaker_mcp.tools.query import query_records

    # --- Setup ---
    tenant = await bootstrap_tenant(args.tenant)
    print(f"Connected to {tenant.host}/{tenant.database}\n")

    # Flush to start clean
    await flush_datasets()

    # =========================================================================
    # QUERY 1: "Show me January 2026 invoices"
    # Expected: Fetches from FM, caches, returns enriched results
    # =========================================================================
    print("=" * 70)
    print("QUERY 1: Show me January 2026 invoices (first query — FM fetch)")
    print("=" * 70)
    t0 = time.perf_counter()
    result = await query_records(
        table="Invoices",
        filter="ServiceDate ge 2026-01-01 and ServiceDate le 2026-01-31",
        select="PrimaryKey,Customer Name,ServiceDate,InvoiceSubTotal,Zone,Driver",
        top=5,
        orderby="ServiceDate desc",
    )
    t1 = time.perf_counter()
    print(result)
    print(f"\n[Timing: {t1 - t0:.2f}s]")
    if "Invoices" in _table_cache:
        entry = _table_cache["Invoices"]
        print(f"[Cache: {entry.row_count} rows, {entry.date_min} → {entry.date_max}]")

    # =========================================================================
    # QUERY 2: Same table, narrower range — should be a cache hit
    # =========================================================================
    print("\n" + "=" * 70)
    print("QUERY 2: Show me Jan 10-20 invoices (cache hit — no FM call)")
    print("=" * 70)
    t0 = time.perf_counter()
    result = await query_records(
        table="Invoices",
        filter="ServiceDate ge 2026-01-10 and ServiceDate le 2026-01-20",
        select="PrimaryKey,Customer Name,ServiceDate,InvoiceSubTotal,Zone,Driver",
        top=5,
        orderby="InvoiceSubTotal desc",
    )
    t1 = time.perf_counter()
    print(result)
    print(f"\n[Timing: {t1 - t0:.3f}s — should be <0.01s for cache hit]")

    # =========================================================================
    # QUERY 3: Same table with non-date filter on cached data
    # =========================================================================
    print("\n" + "=" * 70)
    print("QUERY 3: January invoices for Zone A only (cache + in-memory filter)")
    print("=" * 70)
    t0 = time.perf_counter()
    result = await query_records(
        table="Invoices",
        filter="ServiceDate ge 2026-01-01 and ServiceDate le 2026-01-31 and Zone eq 'A'",
        select="PrimaryKey,Customer Name,ServiceDate,InvoiceSubTotal,Zone,Driver",
        top=5,
    )
    t1 = time.perf_counter()
    print(result)
    print(f"\n[Timing: {t1 - t0:.3f}s]")

    # =========================================================================
    # QUERY 4: Extend the cache range — fetch Feb 2026
    # =========================================================================
    print("\n" + "=" * 70)
    print("QUERY 4: February 2026 invoices (gap fetch — extends cache)")
    print("=" * 70)
    rows_before = _table_cache.get("Invoices", None)
    rows_before_count = rows_before.row_count if rows_before else 0
    t0 = time.perf_counter()
    result = await query_records(
        table="Invoices",
        filter="ServiceDate ge 2026-02-01 and ServiceDate le 2026-02-28",
        select="PrimaryKey,Customer Name,ServiceDate,InvoiceSubTotal,Zone",
        top=5,
    )
    t1 = time.perf_counter()
    print(result)
    entry = _table_cache.get("Invoices")
    if entry:
        print(f"\n[Timing: {t1 - t0:.2f}s]")
        print(
            f"[Cache grew: {rows_before_count} → {entry.row_count} rows, "
            f"{entry.date_min} → {entry.date_max}]"
        )

    # =========================================================================
    # QUERY 5: Cache-all table — Drivers
    # =========================================================================
    print("\n" + "=" * 70)
    print("QUERY 5: List all drivers (cache_all mode)")
    print("=" * 70)
    t0 = time.perf_counter()
    result = await query_records(table="Drivers", top=20)
    t1 = time.perf_counter()
    print(result)
    print(f"\n[Timing: {t1 - t0:.2f}s]")

    # Second call should be instant
    t0 = time.perf_counter()
    await query_records(table="Drivers", top=20)
    t1 = time.perf_counter()
    print(f"[Second call timing: {t1 - t0:.4f}s — cache hit]")

    # =========================================================================
    # ANALYTICS 1: Revenue by zone (from table cache)
    # =========================================================================
    print("\n" + "=" * 70)
    print("ANALYTICS 1: Revenue by zone (Jan+Feb 2026, from table cache)")
    print("=" * 70)
    result = await analyze(
        dataset="Invoices",
        groupby="Zone",
        aggregate="sum:InvoiceSubTotal,count:InvoiceSubTotal,mean:InvoiceSubTotal",
        sort="InvoiceSubTotal_sum desc",
    )
    print(result)

    # =========================================================================
    # ANALYTICS 2: Revenue by driver
    # =========================================================================
    print("\n" + "=" * 70)
    print("ANALYTICS 2: Revenue by driver (top 10)")
    print("=" * 70)
    result = await analyze(
        dataset="Invoices",
        groupby="Driver",
        aggregate="sum:InvoiceSubTotal,count:InvoiceSubTotal",
        sort="InvoiceSubTotal_sum desc",
        limit=10,
    )
    print(result)

    # =========================================================================
    # ANALYTICS 3: Monthly time-series
    # =========================================================================
    print("\n" + "=" * 70)
    print("ANALYTICS 3: Monthly revenue trend (time-series)")
    print("=" * 70)
    result = await analyze(
        dataset="Invoices",
        groupby="ServiceDate",
        aggregate="sum:InvoiceSubTotal,count:InvoiceSubTotal",
        period="month",
    )
    print(result)

    # =========================================================================
    # ANALYTICS 4: Pivot — Zone x Driver revenue
    # =========================================================================
    print("\n" + "=" * 70)
    print("ANALYTICS 4: Pivot table — Zone (rows) x Driver (columns) revenue")
    print("=" * 70)
    result = await analyze(
        dataset="Invoices",
        groupby="Zone",
        aggregate="sum:InvoiceSubTotal",
        pivot_column="Driver",
    )
    print(result)

    # =========================================================================
    # ANALYTICS 5: Statistical summary
    # =========================================================================
    print("\n" + "=" * 70)
    print("ANALYTICS 5: Invoice amount statistics (median, std, nunique)")
    print("=" * 70)
    result = await analyze(
        dataset="Invoices",
        aggregate="median:InvoiceSubTotal,std:InvoiceSubTotal,nunique:Zone,nunique:Driver",
    )
    print(result)

    # =========================================================================
    # FLUSH + VERIFY
    # =========================================================================
    print("\n" + "=" * 70)
    print("FLUSH: Clear all caches")
    print("=" * 70)
    result = await flush_datasets()
    print(result)
    print(f"Table cache empty: {len(_table_cache) == 0}")


if __name__ == "__main__":
    asyncio.run(main())
