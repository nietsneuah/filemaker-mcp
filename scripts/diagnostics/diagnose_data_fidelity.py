"""Data fidelity diagnostic — discovers gaps between FM storage and OData responses.

Tests PK integrity, FK round-trips, type consistency, field completeness,
and metadata key patterns across all exposed tables.

Run: cd ~/Dev/filemaker-mcp && uv run python scripts/diagnose_data_fidelity.py
     cd ~/Dev/filemaker-mcp && uv run python scripts/diagnose_data_fidelity.py --tenant acme_uat
"""

import argparse
import asyncio
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # scripts/ root
from _common import add_tenant_arg, bootstrap_tenant

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

TIMEOUT = 15.0  # seconds per OData call
SAMPLE_SIZE = 5  # records to fetch per table


async def main() -> None:
    parser = argparse.ArgumentParser(description="Data fidelity diagnostic")
    add_tenant_arg(parser)
    args = parser.parse_args()

    from filemaker_mcp.auth import odata_client
    from filemaker_mcp.ddl import TABLES, get_pk_field
    from filemaker_mcp.tools.query import EXPOSED_TABLES

    passed = 0
    failed = 0
    warnings = 0

    def check(name: str, condition: bool, detail: str = "") -> bool:
        nonlocal passed, failed
        status = "PASS" if condition else "FAIL"
        if condition:
            passed += 1
        else:
            failed += 1
        suffix = f" — {detail}" if detail else ""
        print(f"  [{status}] {name}{suffix}")
        return condition

    def warn(name: str, detail: str = "") -> None:
        nonlocal warnings
        warnings += 1
        suffix = f" — {detail}" if detail else ""
        print(f"  [WARN] {name}{suffix}")

    # --- Report accumulator ---
    report: dict = {
        "run_date": date.today().isoformat(),
        "run_time": datetime.now().isoformat(),
        "tables_tested": 0,
        "pk_integrity": {},
        "fk_round_trips": {},
        "type_consistency": {},
        "field_completeness": {},
        "metadata_keys": {},
        "summary": {},
    }

    # --- Setup ---
    print("=== Setup ===")
    tenant = await bootstrap_tenant(args.tenant)
    print(f"Connected to {tenant.host}/{tenant.database}")
    report["server"] = f"{tenant.host}/{tenant.database}"

    tables = list(EXPOSED_TABLES.keys())
    report["tables_tested"] = len(tables)
    print(f"Tables to test: {', '.join(tables)}\n")

    # =========================================================================
    # A. PK INTEGRITY
    # =========================================================================
    print("=== A. PK Integrity ===")
    for table in tables:
        pk_field = get_pk_field(table)
        table_report: dict = {
            "pk_field": pk_field,
            "sample_values": [],
            "empty_pks": 0,
            "round_trip_pass": 0,
            "round_trip_fail": 0,
            "truncation_detected": False,
            "errors": [],
        }
        print(f"\n  [{table}] PK field: {pk_field}")

        try:
            data = await asyncio.wait_for(
                odata_client.get(
                    table,
                    params={
                        "$top": str(SAMPLE_SIZE),
                        "$select": f'"{pk_field}"',
                    },
                ),
                timeout=TIMEOUT,
            )
            records = data.get("value", [])
            if not records:
                warn(f"{table}: no records returned")
                table_report["errors"].append("no records")
                report["pk_integrity"][table] = table_report
                continue

            for rec in records:
                pk_val = rec.get(pk_field)
                table_report["sample_values"].append(str(pk_val) if pk_val is not None else None)

                # Check: PK non-empty
                if pk_val is None or pk_val == "":
                    table_report["empty_pks"] += 1
                    check(f"{table} PK non-empty", False, f"got: {pk_val!r}")
                    continue

                pk_str = str(pk_val)

                # Check: prefix truncation patterns
                # FM serial PKs often have letter prefixes (R-, IH-, etc.)
                # OData may strip them, leaving just the numeric part or "-NNNN"
                if pk_str.startswith("-") and pk_str[1:].isdigit():
                    table_report["truncation_detected"] = True
                    check(
                        f"{table} PK format",
                        False,
                        f"PK '{pk_str}' starts with '-' — likely prefix truncated",
                    )
                else:
                    check(f"{table} PK format", True, f"'{pk_str}'")

                # Round-trip: look up this PK via get_record filter
                try:
                    rt_data = await asyncio.wait_for(
                        odata_client.get(
                            table,
                            params={
                                "$filter": f'"{pk_field}" eq '
                                + (pk_str if pk_str.isdigit() else f"'{pk_str}'"),
                                "$top": "1",
                                "$select": f'"{pk_field}"',
                            },
                        ),
                        timeout=TIMEOUT,
                    )
                    rt_records = rt_data.get("value", [])
                    if rt_records:
                        rt_pk = rt_records[0].get(pk_field)
                        if str(rt_pk) == pk_str:
                            table_report["round_trip_pass"] += 1
                        else:
                            table_report["round_trip_fail"] += 1
                            check(
                                f"{table} PK round-trip",
                                False,
                                f"queried '{pk_str}', got back '{rt_pk}'",
                            )
                    else:
                        table_report["round_trip_fail"] += 1
                        check(
                            f"{table} PK round-trip",
                            False,
                            f"queried '{pk_str}', got 0 records",
                        )
                except Exception as e:
                    table_report["round_trip_fail"] += 1
                    table_report["errors"].append(f"round-trip: {e}")
                    check(f"{table} PK round-trip", False, f"error: {e}")

        except TimeoutError:
            table_report["errors"].append("timeout")
            check(f"{table} PK fetch", False, "timeout")
        except Exception as e:
            table_report["errors"].append(str(e))
            check(f"{table} PK fetch", False, str(e))

        report["pk_integrity"][table] = table_report

    # =========================================================================
    # B. FK ROUND-TRIPS
    # =========================================================================
    print("\n=== B. FK Round-Trips ===")
    for table in tables:
        table_ddl = TABLES.get(table, {})
        fk_fields = {
            name: fdef
            for name, fdef in table_ddl.items()
            if fdef.get("fk") or name.startswith("_kf_")
        }
        if not fk_fields:
            continue

        fk_report: dict = {"fields": {}}
        print(f"\n  [{table}] FK fields: {', '.join(fk_fields.keys())}")

        for fk_name in fk_fields:
            field_report: dict = {
                "target_table": None,
                "target_pk": None,
                "sample_values": [],
                "matches": 0,
                "misses": 0,
                "errors": [],
            }

            # Infer target table from _kf_ naming convention
            # _kf_LocationID → look for a table with _kp_LocationID
            suffix = fk_name.replace("_kf_", "").replace("_kF_", "")
            target_table = None
            target_pk = None
            for candidate in tables:
                candidate_ddl = TABLES.get(candidate, {})
                for fname, fdef in candidate_ddl.items():
                    if fdef.get("pk") and suffix.lower() in fname.lower():
                        target_table = candidate
                        target_pk = fname
                        break
                if target_table:
                    break

            field_report["target_table"] = target_table
            field_report["target_pk"] = target_pk

            if not target_table:
                warn(f"{table}.{fk_name}: cannot infer target table for suffix '{suffix}'")
                fk_report["fields"][fk_name] = field_report
                continue

            print(f"    {fk_name} → {target_table}.{target_pk}")

            try:
                data = await asyncio.wait_for(
                    odata_client.get(
                        table,
                        params={
                            "$top": "3",
                            "$select": f'"{fk_name}"',
                        },
                    ),
                    timeout=TIMEOUT,
                )
                records = data.get("value", [])

                for rec in records:
                    fk_val = rec.get(fk_name)
                    if fk_val is None or fk_val == "":
                        continue
                    fk_str = str(fk_val)
                    field_report["sample_values"].append(fk_str)

                    # Look up FK value in target table
                    try:
                        target_data = await asyncio.wait_for(
                            odata_client.get(
                                target_table,
                                params={
                                    "$filter": f'"{target_pk}" eq '
                                    + (fk_str if fk_str.isdigit() else f"'{fk_str}'"),
                                    "$top": "1",
                                    "$select": f'"{target_pk}"',
                                },
                            ),
                            timeout=TIMEOUT,
                        )
                        target_records = target_data.get("value", [])
                        if target_records:
                            field_report["matches"] += 1
                            check(
                                f"{table}.{fk_name} → {target_table}",
                                True,
                                f"FK '{fk_str}' found",
                            )
                        else:
                            field_report["misses"] += 1
                            check(
                                f"{table}.{fk_name} → {target_table}",
                                False,
                                f"FK '{fk_str}' NOT found in {target_table}",
                            )
                    except Exception as e:
                        field_report["errors"].append(str(e))
                        check(f"{table}.{fk_name} round-trip", False, str(e))

            except TimeoutError:
                field_report["errors"].append("timeout")
                check(f"{table}.{fk_name} fetch", False, "timeout")
            except Exception as e:
                field_report["errors"].append(str(e))
                check(f"{table}.{fk_name} fetch", False, str(e))

            fk_report["fields"][fk_name] = field_report

        report["fk_round_trips"][table] = fk_report

    # =========================================================================
    # C. TYPE CONSISTENCY
    # =========================================================================
    print("\n=== C. Type Consistency ===")
    for table in tables:
        table_ddl = TABLES.get(table, {})
        if not table_ddl:
            warn(f"{table}: no DDL available, skipping type check")
            continue

        type_report: dict = {"mismatches": [], "checked": 0}

        try:
            data = await asyncio.wait_for(
                odata_client.get(table, params={"$top": str(SAMPLE_SIZE)}),
                timeout=TIMEOUT,
            )
            records = data.get("value", [])
            if not records:
                warn(f"{table}: no records for type check")
                report["type_consistency"][table] = type_report
                continue

            # Check first record against DDL types
            rec = records[0]
            for field_name, field_def in table_ddl.items():
                expected_type = field_def.get("type", "")
                val = rec.get(field_name)
                if val is None or val == "":
                    continue  # Can't check type of null/empty

                type_report["checked"] += 1
                actual_type = type(val).__name__
                mismatch = False

                if (
                    (expected_type == "number" and not isinstance(val, (int, float)))
                    or (expected_type in ("datetime", "date") and not isinstance(val, str))
                    or (expected_type == "text" and not isinstance(val, str))
                ):
                    mismatch = True

                if mismatch:
                    detail = (
                        f"{field_name}: DDL says '{expected_type}', got {actual_type} ({val!r})"
                    )
                    type_report["mismatches"].append(detail)
                    check(f"{table} type: {field_name}", False, detail)

            if not type_report["mismatches"]:
                check(
                    f"{table} type consistency",
                    True,
                    f"{type_report['checked']} fields checked",
                )

        except TimeoutError:
            check(f"{table} type check", False, "timeout")
        except Exception as e:
            check(f"{table} type check", False, str(e))

        report["type_consistency"][table] = type_report

    # =========================================================================
    # D. FIELD COMPLETENESS
    # =========================================================================
    print("\n=== D. Field Completeness ===")
    for table in tables:
        table_ddl = TABLES.get(table, {})
        comp_report: dict = {
            "ddl_fields": sorted(table_ddl.keys()) if table_ddl else [],
            "odata_fields": [],
            "in_ddl_not_odata": [],
            "in_odata_not_ddl": [],
        }

        try:
            data = await asyncio.wait_for(
                odata_client.get(table, params={"$top": "1"}),
                timeout=TIMEOUT,
            )
            records = data.get("value", [])
            if not records:
                warn(f"{table}: no records for completeness check")
                report["field_completeness"][table] = comp_report
                continue

            odata_fields = [k for k in records[0] if not k.startswith("@")]
            comp_report["odata_fields"] = sorted(odata_fields)

            if table_ddl:
                ddl_set = set(table_ddl.keys())
                odata_set = set(odata_fields)

                missing_from_odata = sorted(ddl_set - odata_set)
                extra_in_odata = sorted(odata_set - ddl_set)

                comp_report["in_ddl_not_odata"] = missing_from_odata
                comp_report["in_odata_not_ddl"] = extra_in_odata

                if missing_from_odata:
                    warn(
                        f"{table}: {len(missing_from_odata)} DDL fields missing from OData",
                        ", ".join(missing_from_odata[:5])
                        + ("..." if len(missing_from_odata) > 5 else ""),
                    )
                if extra_in_odata:
                    warn(
                        f"{table}: {len(extra_in_odata)} OData fields not in DDL",
                        ", ".join(extra_in_odata[:5]) + ("..." if len(extra_in_odata) > 5 else ""),
                    )
                if not missing_from_odata and not extra_in_odata:
                    check(
                        f"{table} field completeness",
                        True,
                        f"{len(ddl_set)} fields match",
                    )
                else:
                    check(
                        f"{table} field completeness",
                        False,
                        f"DDL={len(ddl_set)}, OData={len(odata_set)}, "
                        f"missing={len(missing_from_odata)}, extra={len(extra_in_odata)}",
                    )
            else:
                warn(f"{table}: no DDL — {len(odata_fields)} OData fields found")

        except TimeoutError:
            check(f"{table} completeness", False, "timeout")
        except Exception as e:
            check(f"{table} completeness", False, str(e))

        report["field_completeness"][table] = comp_report

    # =========================================================================
    # E. METADATA KEYS
    # =========================================================================
    print("\n=== E. Metadata Keys ===")
    for table in tables:
        meta_report: dict = {"at_keys": [], "at_odata_keys": [], "other_at_keys": []}

        try:
            data = await asyncio.wait_for(
                odata_client.get(table, params={"$top": "1"}),
                timeout=TIMEOUT,
            )
            records = data.get("value", [])
            if not records:
                report["metadata_keys"][table] = meta_report
                continue

            at_keys = [k for k in records[0] if k.startswith("@")]
            meta_report["at_keys"] = at_keys
            meta_report["at_odata_keys"] = [k for k in at_keys if k.startswith("@odata")]
            meta_report["other_at_keys"] = [k for k in at_keys if not k.startswith("@odata")]

            if meta_report["other_at_keys"]:
                warn(
                    f"{table}: non-@odata metadata keys",
                    ", ".join(meta_report["other_at_keys"]),
                )
            if at_keys:
                check(
                    f"{table} metadata keys",
                    True,
                    f"{len(at_keys)} keys: {', '.join(at_keys)}",
                )

        except TimeoutError:
            check(f"{table} metadata", False, "timeout")
        except Exception as e:
            check(f"{table} metadata", False, str(e))

        report["metadata_keys"][table] = meta_report

    # =========================================================================
    # SUMMARY
    # =========================================================================
    report["summary"] = {
        "passed": passed,
        "failed": failed,
        "warnings": warnings,
    }

    print(f"\n{'=' * 60}")
    print(f"RESULTS: {passed} passed, {failed} failed, {warnings} warnings")
    print(f"{'=' * 60}")

    # Write JSON report
    report_dir = Path(__file__).parent.parent.parent / "docs" / "internal" / "reports"
    report_dir.mkdir(exist_ok=True)
    report_path = report_dir / f"data-fidelity-{date.today()}.json"
    report_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"\nReport saved to {report_path}")

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    asyncio.run(main())
