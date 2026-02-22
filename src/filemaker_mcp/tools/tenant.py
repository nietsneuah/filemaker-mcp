"""Tenant switching for multi-tenant developer tool.

Manages named tenant configurations and switches the active
FM connection between them. All existing tools automatically
use the active tenant.
"""

import logging
from typing import TYPE_CHECKING

from filemaker_mcp.auth import reset_client
from filemaker_mcp.config import TenantConfig
from filemaker_mcp.ddl import clear_tables
from filemaker_mcp.tools.query import clear_exposed_tables
from filemaker_mcp.tools.schema import bootstrap_ddl, clear_schema_cache

if TYPE_CHECKING:
    from filemaker_mcp.credential_provider import CredentialProvider

logger = logging.getLogger(__name__)

# Module-level tenant state
_tenants: dict[str, TenantConfig] = {}
_active_tenant: dict[str, str] = {"name": ""}
_provider: "CredentialProvider | None" = None


def init_tenants(provider: "CredentialProvider | None" = None) -> str:
    """Load tenant configs and set the default.

    Called once at server startup.

    Args:
        provider: Credential source. If None, creates EnvCredentialProvider
                  (reads .env — zero config for local dev).

    Returns:
        The default tenant name.
    """
    global _provider

    if provider is None:
        from filemaker_mcp.credential_provider import EnvCredentialProvider

        provider = EnvCredentialProvider()

    _provider = provider
    _tenants.clear()

    for name in provider.get_tenant_names():
        _tenants[name] = provider.get_credentials(name)

    default_name = provider.get_default_tenant()
    _active_tenant["name"] = default_name
    logger.info(
        "Loaded %d tenant(s): %s (default: %s)",
        len(_tenants),
        ", ".join(sorted(_tenants.keys())),
        default_name,
    )
    return default_name


def get_active_tenant() -> TenantConfig | None:
    """Return the currently active tenant config."""
    name = _active_tenant["name"]
    return _tenants.get(name)


async def use_tenant(name: str) -> str:
    """Switch to a different FileMaker tenant.

    Closes the current connection, clears all cached schema data,
    reconnects with new credentials, and bootstraps the new tenant.

    Args:
        name: Tenant name (case-insensitive).

    Returns:
        Status message with connection details.
    """
    name = name.lower()

    if name not in _tenants:
        available = ", ".join(sorted(_tenants.keys()))
        return f"Unknown tenant '{name}'. Available: {available}"

    if name == _active_tenant["name"]:
        tenant = _tenants[name]
        return f"Already connected to '{name}' ({tenant.host}/{tenant.database})."

    tenant = _tenants[name]

    # 1. Clear all cached state
    clear_tables()
    clear_exposed_tables()
    clear_schema_cache()

    # 2. Reset HTTP client with new credentials
    await reset_client(tenant)

    # 3. Update active tenant
    _active_tenant["name"] = name
    logger.info("Switched to tenant '%s' (%s/%s)", name, tenant.host, tenant.database)

    # 4. Bootstrap — discover tables and fetch DDL
    await bootstrap_ddl()

    # 5. Report
    from filemaker_mcp.ddl import TABLES
    from filemaker_mcp.tools.query import EXPOSED_TABLES

    return (
        f"Switched to '{name}'.\n"
        f"  Host: {tenant.host}\n"
        f"  Database: {tenant.database}\n"
        f"  Tables discovered: {len(EXPOSED_TABLES)}\n"
        f"  DDL cached: {len(TABLES)} table(s)"
    )


def list_tenants() -> str:
    """List all configured tenants and show which is active.

    Returns:
        Formatted list of tenants with connection details.
    """
    if not _tenants:
        return "No tenants configured. Set *_FM_HOST env vars or FM_HOST for single tenant."

    lines = ["Configured tenants:\n"]
    active = _active_tenant["name"]
    for name in sorted(_tenants.keys()):
        t = _tenants[name]
        marker = " (active)" if name == active else ""
        lines.append(f"  {name}{marker} — {t.host}/{t.database}")

    return "\n".join(lines)
