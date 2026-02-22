"""Shared helpers for CLI scripts.

Provides consistent --tenant flag handling and FM bootstrap
so each script doesn't duplicate the init_tenants → reset_client
→ bootstrap_ddl wiring.

If tenants are already initialised (e.g. called from within a running
MCP server process), the current active tenant is reused unless
--tenant explicitly overrides it.

On cold start with multiple tenants, --tenant is required — there is
no silent default to avoid accidentally running against Production.
"""

from __future__ import annotations

import argparse
import sys

from filemaker_mcp.config import TenantConfig


def add_tenant_arg(parser: argparse.ArgumentParser) -> None:
    """Add a --tenant flag to an argparse parser."""
    parser.add_argument(
        "--tenant",
        default="",
        help="Tenant name to connect to (required when multiple tenants configured)",
    )


def _available_tenants_msg() -> str:
    """Format the list of available tenants for error messages."""
    from filemaker_mcp.tools.tenant import list_tenants

    return list_tenants()


async def bootstrap_tenant(tenant_name: str = "") -> TenantConfig:
    """Initialise tenants, optionally switch, bootstrap DDL, return config.

    If a tenant is already active in memory (e.g. server process), reuses
    it unless *tenant_name* explicitly requests a different one.

    On cold start:
    - 1 tenant configured → use it automatically
    - Multiple tenants, no --tenant → error with available list
    - --tenant specified → use it

    Args:
        tenant_name: Override tenant. Empty string uses whoever is already
                     active, or the sole tenant on cold start.

    Returns:
        The active TenantConfig after bootstrap.

    Raises:
        SystemExit: If no tenant is configured, tenant name is unknown,
                    or multiple tenants exist without --tenant specified.
    """
    from filemaker_mcp.auth import reset_client
    from filemaker_mcp.tools.schema import bootstrap_ddl
    from filemaker_mcp.tools.tenant import get_active_tenant, init_tenants, use_tenant

    # If tenants aren't loaded yet, initialise from .env
    existing = get_active_tenant()
    cold_start = existing is None
    if cold_start:
        init_tenants()

    if tenant_name:
        result = await use_tenant(tenant_name)
        if result.startswith("Unknown tenant"):
            print(f"ERROR: {result}", file=sys.stderr)
            sys.exit(1)
        if result.startswith("Already connected"):
            # use_tenant skips bootstrap when tenant is already active —
            # but on cold start we still need it.
            tenant = get_active_tenant()
            if tenant and cold_start:
                await reset_client(tenant)
                await bootstrap_ddl()
        tenant = get_active_tenant()
    elif cold_start:
        # Cold start without explicit --tenant: require it if ambiguous
        from filemaker_mcp.tools.tenant import _tenants

        if len(_tenants) > 1:
            print(
                "ERROR: Multiple tenants configured — specify --tenant\n",
                file=sys.stderr,
            )
            print(_available_tenants_msg(), file=sys.stderr)
            sys.exit(1)

        tenant = get_active_tenant()
        if not tenant:
            print("ERROR: No tenant configured. Check .env", file=sys.stderr)
            sys.exit(1)
        await reset_client(tenant)
        await bootstrap_ddl()
    else:
        # Warm start — reuse active tenant
        tenant = existing
        await reset_client(tenant)
        await bootstrap_ddl()

    assert tenant is not None  # for type checker
    return tenant
