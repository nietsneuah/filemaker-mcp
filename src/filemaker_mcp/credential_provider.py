"""Credential providers for FM Core.

Decouples credential sourcing from Core logic. The built-in
EnvCredentialProvider reads .env files (zero config for local dev).
When no .env exists, consumers provide their own CredentialProvider.
"""

import os
from typing import Protocol, runtime_checkable

from filemaker_mcp.config import TenantConfig


@runtime_checkable
class CredentialProvider(Protocol):
    """Interface for providing FM credentials to Core.

    Implementations source credentials from wherever the consumer
    stores them: .env files, Fly secrets, Supabase, etc.
    """

    def get_tenant_names(self) -> list[str]:
        """Return all available tenant names."""
        ...

    def get_credentials(self, tenant: str) -> TenantConfig:
        """Return credentials for a specific tenant.

        Raises:
            KeyError: If tenant name is not found.
        """
        ...

    def get_default_tenant(self) -> str:
        """Return the default tenant name, or empty string if none."""
        ...


class EnvCredentialProvider:
    """Credential provider that reads from environment variables / .env files.

    Zero config: if a .env file exists, credentials are loaded automatically.
    Scans for {PREFIX}_FM_HOST patterns for multi-tenant, falls back to
    FM_HOST for single-tenant.
    """

    def __init__(self) -> None:
        from dotenv import load_dotenv

        load_dotenv()
        self._tenants = self._discover_tenants()

    def _discover_tenants(self) -> dict[str, TenantConfig]:
        """Scan environment for tenant configurations."""
        tenants: dict[str, TenantConfig] = {}

        for key, value in os.environ.items():
            if key.endswith("_FM_HOST") and key != "FM_HOST":
                prefix = key[: -len("_FM_HOST")]
                name = prefix.lower()
                tenants[name] = TenantConfig(
                    name=name,
                    host=value,
                    database=os.environ.get(f"{prefix}_FM_DATABASE", ""),
                    username=os.environ.get(f"{prefix}_FM_USERNAME", "mcp_agent"),
                    password=os.environ.get(f"{prefix}_FM_PASSWORD", ""),
                    verify_ssl=os.environ.get(f"{prefix}_FM_VERIFY_SSL", "true").lower() == "true",
                    timeout=int(os.environ.get(f"{prefix}_FM_TIMEOUT", "60")),
                )

        if not tenants:
            host = os.environ.get("FM_HOST", "")
            if host:
                tenants["default"] = TenantConfig(
                    name="default",
                    host=host,
                    database=os.environ.get("FM_DATABASE", ""),
                    username=os.environ.get("FM_USERNAME", "mcp_agent"),
                    password=os.environ.get("FM_PASSWORD", ""),
                    verify_ssl=os.environ.get("FM_VERIFY_SSL", "true").lower() == "true",
                    timeout=int(os.environ.get("FM_TIMEOUT", "60")),
                )

        return tenants

    def get_tenant_names(self) -> list[str]:
        """Return all discovered tenant names."""
        return sorted(self._tenants.keys())

    def get_credentials(self, tenant: str) -> TenantConfig:
        """Return credentials for a specific tenant.

        Raises:
            KeyError: If tenant name is not found.
        """
        if tenant not in self._tenants:
            raise KeyError(
                f"Tenant '{tenant}' not found. Available: {', '.join(self.get_tenant_names())}"
            )
        return self._tenants[tenant]

    def get_default_tenant(self) -> str:
        """Return the default tenant name from FM_DEFAULT_TENANT or first available."""
        default = os.environ.get("FM_DEFAULT_TENANT", "").lower()
        if default and default in self._tenants:
            return default
        if "default" in self._tenants:
            return "default"
        names = self.get_tenant_names()
        return names[0] if names else ""
