"""Configuration management for FileMaker MCP Server.

Loads settings from environment variables or .env file.
All sensitive values come from env vars — never hardcoded.
"""

from pydantic_settings import BaseSettings


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

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

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
