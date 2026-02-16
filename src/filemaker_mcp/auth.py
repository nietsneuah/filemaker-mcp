"""FileMaker API authentication management.

Supports two auth patterns:
- OData v4: Basic auth (stateless, used for read queries)
- Data API: Session tokens (stateful, needed for CRUD and scripts)

Phase 1 uses OData (Basic auth) exclusively.
Data API session management is stubbed for Phase 2.
"""

import logging
import urllib.parse
from typing import Any

import httpx

from filemaker_mcp.config import settings

logger = logging.getLogger(__name__)


class FMODataClient:
    """Async HTTP client for FileMaker OData v4 API.

    Uses Basic auth. Stateless — no session management needed.
    Handles common error patterns from FM Server.
    """

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the async HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=settings.odata_base_url,
                auth=settings.basic_auth,
                verify=settings.fm_verify_ssl,
                timeout=settings.fm_timeout,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
            )
        return self._client

    async def get(self, path: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        """Make a GET request to the OData API.

        Args:
            path: Relative path (e.g., "Location" or "$metadata")
            params: OData query parameters ($filter, $select, $top, etc.)

        Returns:
            Parsed JSON response

        Raises:
            httpx.HTTPStatusError: On 4xx/5xx responses
            ConnectionError: When FM Server is unreachable
        """
        client = await self._get_client()
        try:
            request_timeout = 120.0 if path == "$metadata" else None
            # FM OData rejects '+' for spaces — must use %20. Also needs
            # literal $ (param keys) and , ($select lists) preserved.
            url = f"/{path}"
            if params:
                qs = urllib.parse.urlencode(
                    params,
                    quote_via=lambda s, safe="", encoding=None, errors=None: urllib.parse.quote(
                        s, safe="$,/'"
                    ),
                )
                url = f"{url}?{qs}"
            response = await client.get(url, timeout=request_timeout)
            response.raise_for_status()

            # $metadata returns XML, everything else returns JSON
            if path == "$metadata":
                return {"metadata_xml": response.text}

            return response.json()  # type: ignore[no-any-return]

        except httpx.ConnectError as e:
            logger.error("Cannot connect to FM Server at %s: %s", settings.fm_host, e)
            raise ConnectionError(
                f"Cannot connect to FileMaker Server at {settings.fm_host}. "
                "Verify the server is running and accessible."
            ) from e

        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status == 401:
                logger.error("Authentication failed for user %s", settings.fm_username)
                raise PermissionError(
                    f"Authentication failed for FM user '{settings.fm_username}'. "
                    "Check credentials and extended privileges (fmodata)."
                ) from e
            elif status == 404:
                logger.error("Resource not found: %s", path)
                raise ValueError(
                    f"Resource not found: '{path}'. "
                    "Verify the table name and that it's exposed via OData."
                ) from e
            else:
                # Try to extract FM's error message from JSON response
                fm_error_msg = ""
                try:
                    error_data = e.response.json()
                    if isinstance(error_data, dict) and "error" in error_data:
                        error_obj = error_data["error"]
                        if isinstance(error_obj, dict) and "message" in error_obj:
                            fm_error_msg = str(error_obj["message"])
                except Exception:
                    fm_error_msg = e.response.text[:500]

                logger.error("FM OData error %d: %s", status, fm_error_msg or e.response.text)
                raise ValueError(
                    f"FileMaker OData error ({status}): {fm_error_msg or f'Status {status}'}"
                ) from e

    async def post(self, path: str, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
        """Make a POST request to the OData API.

        Used for OData action invocation (e.g., Script.{name}).

        Args:
            path: Relative path (e.g., "Script.RefreshDDL")
            json_body: JSON body to send with the request

        Returns:
            Parsed JSON response

        Raises:
            PermissionError: On 401 authentication failure
            ValueError: On 404 or other HTTP errors
            ConnectionError: When FM Server is unreachable
        """
        client = await self._get_client()
        try:
            response = await client.post(f"/{path}", json=json_body)
            response.raise_for_status()
            return response.json()  # type: ignore[no-any-return]

        except httpx.ConnectError as e:
            logger.error("Cannot connect to FM Server at %s: %s", settings.fm_host, e)
            raise ConnectionError(
                f"Cannot connect to FileMaker Server at {settings.fm_host}. "
                "Verify the server is running and accessible."
            ) from e

        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status == 401:
                logger.error("Authentication failed for user %s", settings.fm_username)
                raise PermissionError(
                    f"Authentication failed for FM user '{settings.fm_username}'. "
                    "Check credentials and extended privileges (fmodata)."
                ) from e
            elif status == 404:
                logger.error("Resource not found: %s", path)
                raise ValueError(
                    f"Resource not found: '{path}'. "
                    "Verify the table name and that it's exposed via OData."
                ) from e
            else:
                # Try to extract FM's error message from JSON response
                fm_error_msg = ""
                try:
                    error_data = e.response.json()
                    if isinstance(error_data, dict) and "error" in error_data:
                        error_obj = error_data["error"]
                        if isinstance(error_obj, dict) and "message" in error_obj:
                            fm_error_msg = str(error_obj["message"])
                except Exception:
                    fm_error_msg = e.response.text[:500]

                logger.error("FM OData error %d: %s", status, fm_error_msg or e.response.text)
                raise ValueError(
                    f"FileMaker OData error ({status}): {fm_error_msg or f'Status {status}'}"
                ) from e

    async def close(self) -> None:
        """Close the HTTP client connection."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            logger.debug("OData client connection closed")


class FMDataAPIClient:
    """Async HTTP client for FileMaker Data API.

    Uses session-based auth (login → token → use → logout).
    Needed for CRUD operations and script execution (Phase 2).

    NOT YET IMPLEMENTED — stub for Phase 2.
    """

    def __init__(self) -> None:
        self._token: str | None = None
        self._client: httpx.AsyncClient | None = None

    async def login(self) -> str:
        """Authenticate and get a session token."""
        raise NotImplementedError("Data API client is Phase 2")

    async def logout(self) -> None:
        """Release the session token."""
        raise NotImplementedError("Data API client is Phase 2")


# Singleton OData client
odata_client = FMODataClient()
