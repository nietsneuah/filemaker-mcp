"""Configuration management for FileMaker MCP Server.

Loads settings from environment variables or .env file.
All sensitive values come from env vars — never hardcoded.
"""

import os
from dataclasses import dataclass

from pydantic_settings import BaseSettings


@dataclass
class TenantConfig:
    """Connection config for one FileMaker tenant."""

    name: str
    host: str
    database: str
    username: str = "mcp_agent"
    password: str = ""
    verify_ssl: bool = True
    timeout: int = 60


def load_tenants() -> dict[str, TenantConfig]:
    """Discover tenant configs from environment variables.

    .. deprecated::
        Use ``EnvCredentialProvider`` from ``credential_provider.py`` instead.
        This function is preserved for backward compatibility.

    Scans for *_FM_HOST patterns (e.g. ACME_FM_HOST) and builds
    a TenantConfig for each. Falls back to FM_HOST/FM_DATABASE as
    a single "default" tenant if no prefixed vars found.
    """
    from dotenv import load_dotenv

    load_dotenv()  # Ensure .env vars are in os.environ

    tenants: dict[str, TenantConfig] = {}

    # Scan for prefixed tenant vars: {PREFIX}_FM_HOST
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

    # Fallback: single-tenant from FM_HOST
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


def get_default_tenant_name(tenants: dict[str, TenantConfig]) -> str:
    """Return the default tenant name from FM_DEFAULT_TENANT or first available.

    .. deprecated::
        Use ``CredentialProvider.get_default_tenant()`` instead.
    """
    default = os.environ.get("FM_DEFAULT_TENANT", "").lower()
    if default and default in tenants:
        return default
    if "default" in tenants:
        return "default"
    return sorted(tenants.keys())[0] if tenants else ""


class Settings(BaseSettings):
    """FileMaker MCP Server settings.

    Values are loaded from environment variables.
    When running via Claude Desktop/Code, env vars are set in the MCP config JSON.
    For local development, use a .env file.
    """

    # FileMaker Server connection
    fm_host: str = "your-server.example.com"
    fm_database: str = ""
    fm_username: str = "mcp_agent"
    fm_password: str = ""

    # API configuration
    fm_verify_ssl: bool = True
    fm_timeout: int = 60

    # Logging
    log_level: str = "INFO"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}

    @property
    def odata_base_url(self) -> str:
        """Base URL for OData v4 API."""
        return f"https://{self.fm_host}/fmi/odata/v4/{self.fm_database}"

    @property
    def data_api_base_url(self) -> str:
        """Base URL for FileMaker Data API."""
        return f"https://{self.fm_host}/fmi/data/vLatest/databases/{self.fm_database}"

    @property
    def basic_auth(self) -> tuple[str, str]:
        """Basic auth tuple for OData API."""
        return (self.fm_username, self.fm_password)


# Singleton settings instance
settings = Settings()
