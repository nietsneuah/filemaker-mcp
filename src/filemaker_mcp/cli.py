"""FileMaker CLI — unified entry point for dev scripts and server.

Usage:
    mcp-tools --help          # list all commands
    mcp-tools serve           # start MCP server
    mcp-tools diagnose        # run data fidelity diagnostics
    mcp-tools cmtd --tenant staging
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

# Commands: name → (script_filename, description)
# Scripts live in <repo>/scripts/ — only available when running from source.
COMMANDS: dict[str, tuple[str, str]] = {
    "cmtd": ("cmtd_vs_py.py", "CMTD comparison vs same period last year"),
    "context-build": ("ddl_context_builder.py", "Build DDL Context for a tenant"),
    "context-dump": ("dump_context.py", "Dump DDL_CONTEXT entries from FM"),
    "diagnose": ("diagnose_data_fidelity.py", "Run data fidelity diagnostics"),
    "generate-ddl": ("generate_ddl.py", "Generate DDL from live FM OData"),
    "orders": ("query_today_orders.py", "Query today's orders"),
    "public-fork": ("create_public_release.py", "Create public release fork"),
    "smoke-test": ("smoke_test_cache.py", "Smoke test cache + enrichment pipeline"),
    "test-efficiency": ("test_efficiency.py", "Measure response sizes and token usage"),
    "test-queries": ("test_queries.py", "Run realistic cache + analytics queries"),
    "test-reports": ("test_report_patterns.py", "Test report patterns against live FM"),
}

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"


def _print_help() -> None:
    print("FileMaker CLI — dev tools and server\n")
    print("Usage: mcp-tools <command> [options]\n")
    print("Commands:")

    # Built-in commands
    print(f"  {'serve':<20} Start the MCP server (stdio)")
    print(f"  {'tenants':<20} List configured tenants")
    print()

    # Script commands
    max_name = max(len(name) for name in COMMANDS)
    col = max(max_name + 2, 20)
    for name, (_, desc) in sorted(COMMANDS.items()):
        print(f"  {name:<{col}} {desc}")

    print()
    print("Options:")
    print("  --tenant NAME      Tenant to connect to (most commands)")
    print("  --help, -h         Show this help or command-specific help")
    print()
    print("Examples:")
    print("  mcp-tools serve")
    print("  mcp-tools diagnose --tenant staging")
    print("  mcp-tools cmtd --tenant staging")
    print("  mcp-tools context-dump --help")


def main() -> None:
    """Entry point for the mcp-tools CLI."""
    args = sys.argv[1:]

    if not args or args[0] in ("--help", "-h"):
        _print_help()
        sys.exit(0)

    command = args[0]
    rest = args[1:]

    # Built-in: serve
    if command == "serve":
        from filemaker_mcp.server import main as server_main

        server_main()
        return

    # Built-in: tenants
    if command == "tenants":
        from filemaker_mcp.tools.tenant import init_tenants, list_tenants

        init_tenants()
        print(list_tenants())
        return

    # Script dispatch
    if command not in COMMANDS:
        print(f"Unknown command: {command}\n", file=sys.stderr)
        _print_help()
        sys.exit(1)

    script_file, _ = COMMANDS[command]

    if not SCRIPTS_DIR.is_dir():
        print(
            "ERROR: scripts/ directory not found. "
            "CLI scripts are only available when running from the source repo.",
            file=sys.stderr,
        )
        sys.exit(1)

    script_path = SCRIPTS_DIR / script_file
    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}", file=sys.stderr)
        sys.exit(1)

    # Dispatch — run script with remaining args, cwd=scripts/ so _common imports work
    result = subprocess.run(
        [sys.executable, str(script_path), *rest],
        cwd=str(SCRIPTS_DIR),
    )
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
