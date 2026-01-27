"""Write-capable client for AAS Repository REST API.

This module provides a client for writing property values back to an
AAS repository (BaSyx/FA³ST compatible), enabling bidirectional sync
between MQTT and AAS.
"""

from __future__ import annotations

import base64
import logging
import time
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

import httpx

from aas_uns_bridge.observability.metrics import METRICS

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])

# HTTP status codes that indicate transient errors worth retrying
# 429: Too Many Requests, 5xx: Server errors
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
) -> Callable[[F], F]:
    """Decorator for retrying failed HTTP requests with exponential backoff.

    Only retries on transient errors (5xx, 429, network errors).
    Does not retry on 4xx client errors (400, 404, 422) that will always fail.

    Args:
        max_retries: Maximum number of retry attempts.
        base_delay: Initial delay between retries in seconds.
        max_delay: Maximum delay between retries in seconds.

    Returns:
        Decorated function that retries on transient AasWriteError.
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception: Exception | None = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except AasWriteError as e:
                    last_exception = e

                    # Don't retry client errors (4xx) - they will always fail
                    if e.status_code and e.status_code not in RETRYABLE_STATUS_CODES:
                        logger.warning(
                            "AAS write failed with non-retryable status %d: %s",
                            e.status_code,
                            e,
                        )
                        raise

                    METRICS.aas_write_retries_total.inc()
                    if attempt < max_retries - 1:
                        delay = min(base_delay * (2**attempt), max_delay)
                        logger.warning(
                            "AAS write failed (attempt %d/%d), retrying in %.1fs: %s",
                            attempt + 1,
                            max_retries,
                            delay,
                            e,
                        )
                        time.sleep(delay)
            raise last_exception  # type: ignore[misc]

        return wrapper  # type: ignore[return-value]

    return decorator


class AasWriteError(Exception):
    """Raised when AAS write operations fail."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class AasRepositoryClient:
    """Client for AAS repository REST API writes (BaSyx/FA³ST compatible).

    Provides methods to update property values in an AAS repository,
    supporting the DotAAS Part 2 REST API specification.
    """

    def __init__(
        self,
        base_url: str,
        auth_token: str | None = None,
        timeout: float = 30.0,
    ):
        """Initialize the repository client.

        Args:
            base_url: Base URL of the AAS repository.
            auth_token: Optional bearer token for authentication.
            timeout: Request timeout in seconds.
        """
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

        headers: dict[str, str] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

        self._client = httpx.Client(
            base_url=self._base_url,
            headers=headers,
            timeout=timeout,
        )

    def close(self) -> None:
        """Close the HTTP client."""
        self._client.close()

    def __enter__(self) -> AasRepositoryClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def _encode_id(self, identifier: str) -> str:
        """Base64URL encode an identifier for API paths."""
        return base64.urlsafe_b64encode(identifier.encode()).decode().rstrip("=")

    @retry_with_backoff(max_retries=3, base_delay=1.0, max_delay=30.0)
    def update_property(
        self,
        submodel_id: str,
        property_path: str,
        value: Any,
    ) -> None:
        """Update a property value in the AAS repository.

        Uses the DotAAS Part 2 API: PATCH /submodels/{id}/submodel-elements/{path}/$value

        Args:
            submodel_id: The submodel identifier.
            property_path: IdShort path to the property (dot-separated).
            value: The new value to set.

        Raises:
            AasWriteError: If the update fails.
        """
        encoded_id = self._encode_id(submodel_id)
        url = f"/submodels/{encoded_id}/submodel-elements/{property_path}/$value"

        try:
            response = self._client.patch(url, json=value)
            response.raise_for_status()
            logger.debug("Updated %s/%s to %s", submodel_id, property_path, value)
        except httpx.HTTPStatusError as e:
            raise AasWriteError(
                f"Failed to update {property_path}: {e.response.text}",
                status_code=e.response.status_code,
            ) from e
        except httpx.HTTPError as e:
            raise AasWriteError(f"Failed to update {property_path}: {e}") from e

    def get_property(
        self,
        submodel_id: str,
        property_path: str,
    ) -> Any:
        """Read current property value from the repository.

        Args:
            submodel_id: The submodel identifier.
            property_path: IdShort path to the property.

        Returns:
            The current property value.

        Raises:
            AasWriteError: If the read fails.
        """
        encoded_id = self._encode_id(submodel_id)
        url = f"/submodels/{encoded_id}/submodel-elements/{property_path}/$value"

        try:
            response = self._client.get(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            raise AasWriteError(
                f"Failed to read {property_path}: {e.response.text}",
                status_code=e.response.status_code,
            ) from e
        except httpx.HTTPError as e:
            raise AasWriteError(f"Failed to read {property_path}: {e}") from e

    def get_submodel_element(
        self,
        submodel_id: str,
        element_path: str,
    ) -> dict[str, Any]:
        """Get full submodel element (including metadata).

        Args:
            submodel_id: The submodel identifier.
            element_path: IdShort path to the element.

        Returns:
            Full element descriptor as dict.

        Raises:
            AasWriteError: If the read fails.
        """
        encoded_id = self._encode_id(submodel_id)
        url = f"/submodels/{encoded_id}/submodel-elements/{element_path}"

        try:
            response = self._client.get(url)
            response.raise_for_status()
            result: dict[str, Any] = response.json()
            return result
        except httpx.HTTPStatusError as e:
            raise AasWriteError(
                f"Failed to get element {element_path}: {e.response.text}",
                status_code=e.response.status_code,
            ) from e
        except httpx.HTTPError as e:
            raise AasWriteError(f"Failed to get element {element_path}: {e}") from e
