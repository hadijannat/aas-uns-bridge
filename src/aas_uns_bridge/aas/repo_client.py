"""REST client for AAS Repository API."""

import base64
import hashlib
import logging
from typing import Any

import httpx
from basyx.aas import model
from basyx.aas.adapter import json as aas_json

from aas_uns_bridge.config import RepoClientConfig

logger = logging.getLogger(__name__)


class AASRepoClientError(Exception):
    """Raised when AAS Repository operations fail."""

    pass


class AASRepoClient:
    """Client for AAS Repository REST API (DotAAS Part 2 compliant)."""

    def __init__(self, config: RepoClientConfig):
        """Initialize the repository client.

        Args:
            config: Repository client configuration.
        """
        self.config = config
        self.base_url = config.base_url.rstrip("/")
        self._etags: dict[str, str] = {}  # URL -> ETag for change detection
        self._hashes: dict[str, str] = {}  # URL -> content hash fallback

        headers: dict[str, str] = {"Accept": "application/json"}
        if config.auth_token:
            headers["Authorization"] = f"Bearer {config.auth_token.get_secret_value()}"

        self._client = httpx.Client(
            base_url=self.base_url,
            headers=headers,
            timeout=config.timeout_seconds,
        )

    def close(self) -> None:
        """Close the HTTP client."""
        self._client.close()

    def __enter__(self) -> "AASRepoClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def _encode_id(self, identifier: str) -> str:
        """Base64URL encode an identifier for API paths."""
        return base64.urlsafe_b64encode(identifier.encode()).decode().rstrip("=")

    def _compute_hash(self, content: bytes) -> str:
        """Compute SHA256 hash of content."""
        return hashlib.sha256(content).hexdigest()

    def _has_changed(self, url: str, response: httpx.Response) -> bool:
        """Check if content has changed since last fetch."""
        # Check ETag first
        etag = response.headers.get("ETag")
        if etag:
            old_etag = self._etags.get(url)
            self._etags[url] = etag
            if old_etag and old_etag == etag:
                return False

        # Fall back to content hash
        content_hash = self._compute_hash(response.content)
        old_hash = self._hashes.get(url)
        self._hashes[url] = content_hash
        return not (old_hash and old_hash == content_hash)

    def list_shells(self) -> list[dict[str, Any]]:
        """List all Asset Administration Shells in the repository.

        Returns:
            List of AAS descriptors.

        Raises:
            AASRepoClientError: If the request fails.
        """
        try:
            response = self._client.get("/shells")
            response.raise_for_status()
            data = response.json()
            # Handle paged response format
            if isinstance(data, dict) and "result" in data:
                return data["result"]
            return data if isinstance(data, list) else []
        except httpx.HTTPError as e:
            raise AASRepoClientError(f"Failed to list shells: {e}") from e

    def get_shell(self, aas_id: str) -> tuple[dict[str, Any], bool]:
        """Get a specific AAS by ID.

        Args:
            aas_id: The AAS identifier.

        Returns:
            Tuple of (AAS data dict, changed flag).

        Raises:
            AASRepoClientError: If the request fails.
        """
        url = f"/shells/{self._encode_id(aas_id)}"
        try:
            response = self._client.get(url)
            response.raise_for_status()
            changed = self._has_changed(url, response)
            return response.json(), changed
        except httpx.HTTPError as e:
            raise AASRepoClientError(f"Failed to get shell {aas_id}: {e}") from e

    def list_submodels(self) -> list[dict[str, Any]]:
        """List all Submodels in the repository.

        Returns:
            List of submodel descriptors.

        Raises:
            AASRepoClientError: If the request fails.
        """
        try:
            response = self._client.get("/submodels")
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict) and "result" in data:
                return data["result"]
            return data if isinstance(data, list) else []
        except httpx.HTTPError as e:
            raise AASRepoClientError(f"Failed to list submodels: {e}") from e

    def get_submodel(self, submodel_id: str) -> tuple[dict[str, Any], bool]:
        """Get a specific submodel by ID.

        Args:
            submodel_id: The submodel identifier.

        Returns:
            Tuple of (submodel data dict, changed flag).

        Raises:
            AASRepoClientError: If the request fails.
        """
        url = f"/submodels/{self._encode_id(submodel_id)}"
        try:
            response = self._client.get(url)
            response.raise_for_status()
            changed = self._has_changed(url, response)
            return response.json(), changed
        except httpx.HTTPError as e:
            raise AASRepoClientError(f"Failed to get submodel {submodel_id}: {e}") from e

    def fetch_all(self) -> tuple[model.DictObjectStore, bool]:
        """Fetch all AAS content from the repository.

        Returns:
            Tuple of (ObjectStore with all content, any_changed flag).

        Raises:
            AASRepoClientError: If fetching fails.
        """
        object_store: model.DictObjectStore = model.DictObjectStore()
        any_changed = False

        # Fetch all shells
        shells = self.list_shells()
        for shell_desc in shells:
            shell_id = shell_desc.get("id")
            if shell_id:
                shell_data, changed = self.get_shell(shell_id)
                any_changed = any_changed or changed
                try:
                    aas = aas_json.StrictAASFromJsonDecoder.decode(shell_data)
                    if isinstance(aas, model.AssetAdministrationShell):
                        object_store.add(aas)
                except Exception as e:
                    logger.warning("Failed to decode shell %s: %s", shell_id, e)

        # Fetch all submodels
        submodels = self.list_submodels()
        for sm_desc in submodels:
            sm_id = sm_desc.get("id")
            if sm_id:
                sm_data, changed = self.get_submodel(sm_id)
                any_changed = any_changed or changed
                try:
                    sm = aas_json.StrictAASFromJsonDecoder.decode(sm_data)
                    if isinstance(sm, model.Submodel):
                        object_store.add(sm)
                except Exception as e:
                    logger.warning("Failed to decode submodel %s: %s", sm_id, e)

        logger.info(
            "Fetched from repository: %d objects (changed: %s)",
            len(object_store),
            any_changed,
        )
        return object_store, any_changed
