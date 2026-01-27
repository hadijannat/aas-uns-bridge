"""Semantic context distribution publisher.

This module publishes full SemanticContext definitions to UNS system topics,
enabling pointer mode subscribers to resolve hash references to full contexts.

Context topics follow the pattern:
    UNS/Sys/Context/{dictionary}/{hash}

The contexts are published with retain=True so late-joining subscribers
can immediately resolve pointers without waiting for republication.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from aas_uns_bridge.mapping.sanitize import sanitize_segment
from aas_uns_bridge.semantic.models import SemanticContext, SemanticPointer

if TYPE_CHECKING:
    from aas_uns_bridge.mqtt.client import MqttClient

logger = logging.getLogger(__name__)


class ContextPublisher:
    """Publisher for semantic context distribution topics.

    Publishes full SemanticContext definitions to retained UNS system topics,
    enabling pointer mode resolution for subscribers.

    Topic structure:
        {prefix}/{dictionary}/{hash}

    Example:
        UNS/Sys/Context/ECLASS/a1b2c3d4e5f67890

    Payload structure (JSON):
        {
            "semanticId": "0173-1#02-AAO677#002",
            "dictionary": "ECLASS",
            "version": "002",
            "definition": "Manufacturer name",
            "unit": null,
            "hierarchy": ["0173-1#02-AAO677#002"]
        }
    """

    def __init__(
        self,
        mqtt_client: MqttClient,
        topic_prefix: str = "UNS/Sys/Context",
        qos: int = 1,
    ):
        """Initialize the context publisher.

        Args:
            mqtt_client: MQTT client for publishing.
            topic_prefix: Base topic prefix for context distribution.
            qos: MQTT QoS level for publishes.
        """
        self._client = mqtt_client
        self._prefix = topic_prefix
        self._qos = qos
        self._published_hashes: set[str] = set()
        self._publish_count = 0

    def publish_context(self, context: SemanticContext) -> SemanticPointer:
        """Publish a semantic context to its distribution topic.

        If the context has already been published (tracked by hash),
        skips republication unless force=True.

        Args:
            context: The semantic context to publish.

        Returns:
            The SemanticPointer for this context.
        """
        pointer = context.to_pointer()

        # Skip if already published
        if pointer.hash in self._published_hashes:
            return pointer

        topic = self._build_topic(pointer)
        payload = self._build_payload(context)

        self._client.publish(
            topic=topic,
            payload=payload,
            qos=self._qos,
            retain=True,
        )

        self._published_hashes.add(pointer.hash)
        self._publish_count += 1

        logger.debug(
            "Published semantic context %s to %s",
            pointer.hash,
            topic,
        )

        return pointer

    def publish_context_batch(
        self,
        contexts: list[SemanticContext],
        skip_published: bool = True,
    ) -> list[SemanticPointer]:
        """Publish multiple contexts efficiently.

        Args:
            contexts: List of semantic contexts to publish.
            skip_published: Skip contexts that have already been published.

        Returns:
            List of SemanticPointers for all contexts.
        """
        pointers: list[SemanticPointer] = []

        for context in contexts:
            pointer = context.to_pointer()
            pointers.append(pointer)

            if skip_published and pointer.hash in self._published_hashes:
                continue

            topic = self._build_topic(pointer)
            payload = self._build_payload(context)

            self._client.publish(
                topic=topic,
                payload=payload,
                qos=self._qos,
                retain=True,
            )

            self._published_hashes.add(pointer.hash)
            self._publish_count += 1

        logger.debug("Published %d semantic contexts", len(contexts))
        return pointers

    def republish_all(self, contexts: list[SemanticContext]) -> int:
        """Force republish all contexts (e.g., after reconnection).

        Args:
            contexts: List of semantic contexts to republish.

        Returns:
            Number of contexts published.
        """
        count = 0
        for context in contexts:
            pointer = context.to_pointer()
            topic = self._build_topic(pointer)
            payload = self._build_payload(context)

            self._client.publish(
                topic=topic,
                payload=payload,
                qos=self._qos,
                retain=True,
            )
            count += 1

        logger.info("Republished %d semantic contexts", count)
        return count

    def clear_context(self, pointer: SemanticPointer) -> None:
        """Clear a published context by sending empty retained message.

        Args:
            pointer: The pointer for the context to clear.
        """
        topic = self._build_topic(pointer)
        self._client.publish(
            topic=topic,
            payload=b"",
            qos=self._qos,
            retain=True,
        )
        self._published_hashes.discard(pointer.hash)
        logger.debug("Cleared semantic context %s", pointer.hash)

    def _build_topic(self, pointer: SemanticPointer) -> str:
        """Build the topic path for a pointer.

        Args:
            pointer: The semantic pointer.

        Returns:
            Full MQTT topic path.
        """
        # Sanitize dictionary name to handle special chars (/, +, #, whitespace)
        safe_dictionary = sanitize_segment(pointer.dictionary)
        return f"{self._prefix}/{safe_dictionary}/{pointer.hash}"

    def _build_payload(self, context: SemanticContext) -> bytes:
        """Build the JSON payload for a context.

        Args:
            context: The semantic context.

        Returns:
            UTF-8 encoded JSON bytes.
        """
        payload_dict = {
            "semanticId": context.semantic_id,
            "dictionary": context.dictionary,
            "version": context.version,
            "definition": context.definition,
            "preferredName": context.preferred_name,
            "unit": context.unit,
            "dataType": context.data_type,
            "hierarchy": list(context.hierarchy),
        }
        return json.dumps(payload_dict, ensure_ascii=False).encode("utf-8")

    @property
    def published_count(self) -> int:
        """Total number of contexts published since initialization."""
        return self._publish_count

    @property
    def unique_contexts(self) -> int:
        """Number of unique contexts currently published."""
        return len(self._published_hashes)

    def reset_tracking(self) -> None:
        """Reset the published hash tracking (e.g., after reconnect)."""
        self._published_hashes.clear()
