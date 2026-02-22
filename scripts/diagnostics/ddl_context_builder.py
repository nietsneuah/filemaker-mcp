"""DDL Context Builder — training tool for tenant schema profiling.

Analyzes a tenant's FM schema, classifies fields using universal naming
rules + DDL_Context overrides, and writes field_class entries back to FM.

Run once per tenant onboarding. Re-run when schema changes — idempotent,
preserves human overrides.

Usage:
    uv run python scripts/ddl_context_builder.py --help
    uv run python scripts/ddl_context_builder.py --rules
    uv run python scripts/ddl_context_builder.py --classes
    uv run python scripts/ddl_context_builder.py --dry-run
    uv run python scripts/ddl_context_builder.py
    uv run python scripts/ddl_context_builder.py --metadata
    uv run python scripts/ddl_context_builder.py --tenant acme_uat --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # scripts/ root
from _common import add_tenant_arg, bootstrap_tenant
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from filemaker_mcp.field_classifier import (
    RULES,
    ClassificationResult,
    classify_table,
    compute_diff,
    enrich_from_annotations,
    read_overrides,
)

console = Console()
logger = logging.getLogger(__name__)

# Classification value definitions for --classes
CLASS_DEFS = [
    ("key", "Primary/foreign key", "No", "Yes (quoting handled)", "Yes — essential"),
    ("stored", "Normal stored field", "Yes", "Yes", "Maybe"),
    ("internal", "Utility/speed field", "No", "Avoid", "No"),
    ("calculated", "Unstored calculation", "Avoid", "No — triggers eval", "No"),
    ("summary", "Summary field", "Avoid", "No — slow", "No"),
    ("global", "Global storage field", "No", "No", "No"),
]


def build_parser() -> argparse.ArgumentParser:
    """Build CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="ddl_context_builder",
        description="DDL Context Builder — training tool for tenant schema profiling.",
        epilog=(
            "Examples:\n"
            "  uv run python scripts/ddl_context_builder.py --rules\n"
            "  uv run python scripts/ddl_context_builder.py --dry-run\n"
            "  uv run python scripts/ddl_context_builder.py --metadata\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Classify fields but don't write to FM",
    )
    parser.add_argument(
        "--metadata",
        action="store_true",
        help="Enrich uncertain fields using $metadata annotations",
    )
    parser.add_argument(
        "--rules",
        action="store_true",
        help="Print classification rules table and exit",
    )
    parser.add_argument(
        "--classes",
        action="store_true",
        help="Print field_class definitions and exit",
    )
    add_tenant_arg(parser)
    return parser


def show_rules() -> None:
    """Print the classification rules table."""
    table = Table(title="Classification Rules", show_lines=True)
    table.add_column("Priority", justify="center", style="cyan")
    table.add_column("Rule Name", style="bold")
    table.add_column("Pattern", style="green")
    table.add_column("Class", style="yellow")
    table.add_column("Confidence", style="magenta")

    for rule in RULES:
        pattern = rule.pattern if rule.pattern != "default" else "[dim]everything else[/dim]"
        table.add_row(
            str(rule.priority),
            rule.name,
            pattern,
            rule.field_class,
            rule.confidence,
        )

    console.print()
    console.print(table)
    console.print("\n[dim]Rules applied in priority order. First match wins.[/dim]")
    console.print("[dim]Override via DDL_Context: per-field, per-table, or tenant-wide.[/dim]")


def show_classes() -> None:
    """Print the field_class definitions table."""
    table = Table(title="Field Classification Values", show_lines=True)
    table.add_column("Class", style="bold yellow")
    table.add_column("Meaning")
    table.add_column("In $select?", justify="center")
    table.add_column("In $filter?", justify="center")
    table.add_column("In joins?", justify="center")

    for cls, meaning, select, filt, joins in CLASS_DEFS:
        table.add_row(cls, meaning, select, filt, joins)

    console.print()
    console.print(table)


def collect_existing_field_classes(
    context: dict[tuple[str, str, str], dict[str, str]],
) -> dict[tuple[str, str], str]:
    """Extract existing field_class entries from DDL_CONTEXT."""
    return {
        (table, field): value["context"]
        for (table, field, ctx_type), value in context.items()
        if ctx_type == "field_class" and field and field != "*"
    }


async def run(args: argparse.Namespace) -> None:
    """Run the DDL Context Builder."""
    from filemaker_mcp.auth import odata_client
    from filemaker_mcp.ddl import DDL_CONTEXT, FIELD_ANNOTATIONS, TABLES
    from filemaker_mcp.tools.context import save_context

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    t0 = time.perf_counter()

    # Bootstrap FM connection
    with console.status("[bold cyan]Bootstrapping FM connection...", spinner="dots"):
        tenant = await bootstrap_tenant(args.tenant)

    elapsed_bootstrap = time.perf_counter() - t0
    total_fields = sum(len(f) for f in TABLES.values())
    console.print(
        f"[bold green]Connected[/] to {tenant.host}/{tenant.database} "
        f"[dim]({elapsed_bootstrap:.1f}s)[/dim]"
    )
    console.print(f"  {len(TABLES)} tables, {total_fields} fields")

    # Read existing overrides
    overrides = read_overrides(DDL_CONTEXT)
    if overrides.field_overrides:
        console.print(f"  {len(overrides.field_overrides)} field overrides")
    if overrides.disabled_rules_global:
        console.print(f"  {len(overrides.disabled_rules_global)} tenant-wide rule disables")
    tbl_disables = sum(len(v) for v in overrides.disabled_rules_by_table.values())
    if tbl_disables:
        console.print(f"  {tbl_disables} table-level rule disables")

    # Classify all fields
    all_results: dict[tuple[str, str], ClassificationResult] = {}
    with console.status("[bold cyan]Classifying fields...", spinner="dots"):
        for table_name, schema in TABLES.items():
            table_results = classify_table(table_name, schema, overrides=overrides)
            for field_name, result in table_results.items():
                all_results[(table_name, field_name)] = result

    # Enrich uncertain fields with $metadata if requested
    if args.metadata and FIELD_ANNOTATIONS:
        uncertain = {k: v for k, v in all_results.items() if v.confidence == "low"}
        if uncertain:
            enriched = enrich_from_annotations(uncertain, FIELD_ANNOTATIONS)
            all_results.update(enriched)
            enriched_count = sum(1 for k in uncertain if enriched[k].rule_name == "metadata")
            console.print(f"  {enriched_count} fields enriched from $metadata")

    # Diff against existing DDL_Context
    existing = collect_existing_field_classes(DDL_CONTEXT)
    diff = compute_diff(all_results, existing)

    # Classification summary table
    class_counts: dict[str, int] = {}
    for result in all_results.values():
        class_counts[result.field_class] = class_counts.get(result.field_class, 0) + 1

    cls_table = Table(title="Classification Summary")
    cls_table.add_column("Class", style="bold yellow")
    cls_table.add_column("Fields", justify="right", style="cyan")
    cls_table.add_column("Percent", justify="right")

    for cls in ["key", "stored", "internal", "calculated", "summary", "global"]:
        count = class_counts.get(cls, 0)
        if count > 0:
            pct = count / len(all_results) * 100 if all_results else 0
            cls_table.add_row(cls, str(count), f"{pct:.1f}%")

    console.print()
    console.print(cls_table)

    # Confidence breakdown
    conf_counts: dict[str, int] = {}
    for result in all_results.values():
        conf_counts[result.confidence] = conf_counts.get(result.confidence, 0) + 1

    conf_table = Table(title="Confidence")
    conf_table.add_column("Level", style="bold")
    conf_table.add_column("Fields", justify="right", style="cyan")
    for conf in ["high", "medium", "low"]:
        count = conf_counts.get(conf, 0)
        if count > 0:
            conf_table.add_row(conf, str(count))
    console.print(conf_table)

    # Diff summary
    diff_table = Table(title="Changes from Prior Run")
    diff_table.add_column("Status", style="bold")
    diff_table.add_column("Fields", justify="right", style="cyan")
    diff_table.add_row("[green]new[/green]", str(len(diff.new)))
    diff_table.add_row("[yellow]changed[/yellow]", str(len(diff.changed)))
    diff_table.add_row("unchanged", str(len(diff.unchanged)))
    diff_table.add_row("[red]removed[/red]", str(len(diff.removed)))
    console.print(diff_table)

    # Write to DDL_Context (unless dry-run)
    writes_to_do = {**diff.new, **diff.changed}
    if writes_to_do and not args.dry_run:
        console.print()
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Writing field_class entries...", total=len(writes_to_do) * 2)
            for (table, field), result in writes_to_do.items():
                await save_context(
                    table_name=table,
                    field_name=field,
                    context=result.field_class,
                    context_type="field_class",
                    source=f"ddl_context_builder:{result.rule_name}",
                )
                progress.advance(task)

            for (table, field), result in writes_to_do.items():
                source_val = (
                    f"rule:{result.rule_name}" if result.rule_name != "override" else "override"
                )
                await save_context(
                    table_name=table,
                    field_name=field,
                    context=source_val,
                    context_type="classification_source",
                    source="ddl_context_builder",
                )
                progress.advance(task)

        console.print(f"[bold green]Done.[/] {len(writes_to_do)} entries written.")
    elif writes_to_do and args.dry_run:
        console.print(
            f"\n[bold yellow][DRY RUN][/] Would write {len(writes_to_do)} field_class entries."
        )
    else:
        console.print("\n[dim]No changes to write.[/dim]")

    # JSON report
    report = {
        "run_date": date.today().isoformat(),
        "server": tenant.host,
        "database": tenant.database,
        "bootstrap_time_s": round(elapsed_bootstrap, 1),
        "tables": len(TABLES),
        "fields": len(all_results),
        "classification": class_counts,
        "confidence": conf_counts,
        "overrides": {
            "field_overrides": len(overrides.field_overrides),
            "global_rule_disables": len(overrides.disabled_rules_global),
            "table_rule_disables": tbl_disables,
        },
        "diff": {
            "new": len(diff.new),
            "changed": len(diff.changed),
            "unchanged": len(diff.unchanged),
            "removed": len(diff.removed),
        },
        "dry_run": args.dry_run,
        "metadata_used": args.metadata and bool(FIELD_ANNOTATIONS),
    }

    report_dir = Path(__file__).parent.parent.parent / "docs" / "internal" / "reports"
    report_dir.mkdir(exist_ok=True)
    report_path = report_dir / f"context-builder-{date.today()}.json"
    report_path.write_text(json.dumps(report, indent=2))
    console.print(f"\n[dim]JSON report saved to: {report_path}[/dim]")

    elapsed_total = time.perf_counter() - t0
    console.print(f"[dim]Total time: {elapsed_total:.1f}s[/dim]")

    await odata_client.close()


def main() -> None:
    """Entry point — parse args, dispatch."""
    parser = build_parser()
    args = parser.parse_args()

    if args.rules:
        show_rules()
        return

    if args.classes:
        show_classes()
        return

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
