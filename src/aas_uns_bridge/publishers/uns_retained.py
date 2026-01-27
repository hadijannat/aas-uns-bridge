"""UNS retained topic publisher."""

from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING, Any, Literal

from aas_uns_bridge.config import SemanticConfig, UnsConfig
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.observability.metrics import METRICS

if TYPE_CHECKING:
    from aas_uns_bridge.mqtt.client import MqttClient
    from aas_uns_bridge.publishers.context_publisher import ContextPublisher
    from aas_uns_bridge.semantic.models import SemanticPointer
    from aas_uns_bridge.semantic.resolution_cache import SemanticResolutionCache

logger = logging.getLogger(__name__)

# User Property keys for MQTT v5 semantic metadata
USER_PROP_SEMANTIC_ID = "aas:semanticId"
USER_PROP_UNIT = "aas:unit"
USER_PROP_VALUE_TYPE = "aas:valueType"
USER_PROP_AAS_TYPE = "aas:aasType"
USER_PROP_SOURCE = "aas:source"

# Pointer mode User Property keys
USER_PROP_POINTER = "aas:ptr"
USER_PROP_DICTIONARY = "aas:dict"
USER_PROP_VERSION = "aas:ver"

# Payload mode types
PayloadMode = Literal["inline", "pointer", "hybrid"]


class UnsRetainedPublisher:
    """Publisher for UNS retained MQTT topics.

    Publishes AAS metrics as JSON payloads to retained MQTT topics,
    enabling late-subscriber discovery of current asset state.

    Supports three payload modes:
    - inline: Full metadata in payload (legacy, 150-250 bytes overhead)
    - pointer: Hash reference only (10-50 bytes overhead, 90% reduction)
    - hybrid: Pointer + minimal metadata for partial offline resolution

    Also supports MQTT v5 User Properties for semantic metadata when configured,
    reducing payload size and enabling header-based filtering on modern brokers.
    """

    def __init__(
        self,
        mqtt_client: MqttClient,
        config: UnsConfig,
        semantic_config: SemanticConfig | None = None,
        resolution_cache: SemanticResolutionCache | None = None,
        context_publisher: ContextPublisher | None = None,
        payload_mode: PayloadMode = "inline",
    ):
        """Initialize the UNS publisher.

        Args:
            mqtt_client: MQTT client for publishing.
            config: UNS publication configuration.
            semantic_config: Optional semantic enforcement configuration.
            resolution_cache: Optional semantic resolution cache for pointer mode.
            context_publisher: Optional context publisher for distributing contexts.
            payload_mode: Payload mode (inline, pointer, or hybrid).
        """
        self.client = mqtt_client
        self.config = config
        self.semantic_config = semantic_config or SemanticConfig()
        self._resolution_cache = resolution_cache
        self._context_publisher = context_publisher
        self._payload_mode = payload_mode
        self._published_count = 0

    @property
    def payload_mode(self) -> PayloadMode:
        """Current payload mode."""
        return self._payload_mode

    @payload_mode.setter
    def payload_mode(self, mode: PayloadMode) -> None:
        """Set payload mode."""
        self._payload_mode = mode

    def _build_user_properties(
        self, metric: ContextMetric, aas_uri: str | None = None
    ) -> dict[str, str]:
        """Build MQTT v5 User Properties for a metric (inline mode).

        Args:
            metric: The context metric.
            aas_uri: Optional AAS source URI.

        Returns:
            Dict of property key-value pairs (only non-None values included).
        """
        props: dict[str, str] = {}

        if metric.semantic_id:
            props[USER_PROP_SEMANTIC_ID] = metric.semantic_id
        if metric.unit:
            props[USER_PROP_UNIT] = metric.unit
        if metric.value_type:
            props[USER_PROP_VALUE_TYPE] = metric.value_type
        if metric.aas_type:
            props[USER_PROP_AAS_TYPE] = metric.aas_type

        source = aas_uri or metric.aas_source
        if source:
            props[USER_PROP_SOURCE] = source

        return props

    def _build_user_properties_pointer(self, pointer: SemanticPointer) -> dict[str, str]:
        """Build MQTT v5 User Properties for pointer mode.

        Args:
            pointer: The semantic pointer.

        Returns:
            Dict of property key-value pairs.
        """
        return {
            USER_PROP_POINTER: pointer.hash,
            USER_PROP_DICTIONARY: pointer.dictionary,
            USER_PROP_VERSION: pointer.version,
        }

    def _build_payload(
        self,
        metric: ContextMetric,
        aas_uri: str | None = None,
        include_metadata: bool = True,
    ) -> dict[str, Any]:
        """Build the JSON payload for a metric (inline mode).

        Args:
            metric: The context metric.
            aas_uri: Optional AAS source URI.
            include_metadata: Whether to include semantic metadata in payload.
                Set to False when metadata is in MQTT v5 User Properties.

        Returns:
            Payload dict ready for JSON serialization.
        """
        payload: dict[str, Any] = {
            "value": metric.value,
            "timestamp": metric.timestamp_ms,
        }

        # Include metadata in payload unless using User Properties exclusively
        if include_metadata:
            payload["semanticId"] = metric.semantic_id
            payload["unit"] = metric.unit
            payload["valueType"] = metric.value_type
            payload["source"] = "aas-uns-bridge"
            payload["aasUri"] = aas_uri or metric.aas_source

        return payload

    def _build_pointer_payload(
        self,
        metric: ContextMetric,
        pointer: SemanticPointer,
    ) -> dict[str, Any]:
        """Build minimal payload with semantic pointer (pointer mode).

        This achieves 90% payload overhead reduction by replacing inline
        metadata with a 16-character hash reference.

        Args:
            metric: The context metric.
            pointer: The semantic pointer.

        Returns:
            Minimal payload dict with pointer reference.
        """
        return {
            "value": metric.value,
            "timestamp": metric.timestamp_ms,
            "ptr": pointer.hash,
        }

    def _build_hybrid_payload(
        self,
        metric: ContextMetric,
        pointer: SemanticPointer,
        aas_uri: str | None = None,
    ) -> dict[str, Any]:
        """Build hybrid payload with pointer and minimal metadata.

        Includes pointer for full resolution plus unit/valueType for
        partial offline operation.

        Args:
            metric: The context metric.
            pointer: The semantic pointer.
            aas_uri: Optional AAS source URI.

        Returns:
            Hybrid payload dict.
        """
        payload: dict[str, Any] = {
            "value": metric.value,
            "timestamp": metric.timestamp_ms,
            "ptr": pointer.hash,
        }

        # Include minimal metadata for offline operation
        if metric.unit:
            payload["unit"] = metric.unit
        if metric.value_type:
            payload["valueType"] = metric.value_type

        return payload

    def _get_or_create_pointer(self, metric: ContextMetric) -> SemanticPointer | None:
        """Get or create a semantic pointer for a metric.

        Registers the semantic context in the resolution cache and
        optionally publishes it via the context publisher.

        Args:
            metric: The context metric.

        Returns:
            SemanticPointer if semantic_id exists, None otherwise.
        """
        if not metric.semantic_id:
            return None

        if not self._resolution_cache:
            # No cache - create pointer with auto-detected dictionary/version
            from aas_uns_bridge.semantic.models import SemanticContext

            context = SemanticContext.from_semantic_id(
                semantic_id=metric.semantic_id,
                unit=metric.unit,
                data_type=metric.value_type,
            )
            return context.to_pointer()

        # Check if already cached
        pointer = self._resolution_cache.get_pointer(metric.semantic_id)
        if pointer:
            return pointer

        # Create and register new context with auto-detected dictionary/version
        from aas_uns_bridge.semantic.models import SemanticContext

        # Get all semantic keys if available (poly-hierarchical)
        hierarchy = getattr(metric, "semantic_keys", ())

        # Use from_semantic_id to auto-detect dictionary and version
        context = SemanticContext.from_semantic_id(
            semantic_id=metric.semantic_id,
            unit=metric.unit,
            data_type=metric.value_type,
            additional_keys=hierarchy,
        )

        pointer = self._resolution_cache.register(context)

        # Publish context if publisher configured
        if self._context_publisher:
            self._context_publisher.publish_context(context)

        return pointer

    def publish_metric(
        self,
        topic: str,
        metric: ContextMetric,
        aas_uri: str | None = None,
    ) -> None:
        """Publish a single metric to its UNS topic.

        Payload mode determines the format:
        - inline: Full metadata in payload (legacy)
        - pointer: Hash reference only (90% smaller)
        - hybrid: Pointer + minimal metadata

        When semantic_config.use_user_properties is True, metadata/pointer
        is also included in MQTT v5 User Properties (headers).

        Args:
            topic: The full MQTT topic path.
            metric: The context metric to publish.
            aas_uri: Optional AAS source URI for provenance.
        """
        if not self.config.enabled:
            return

        use_props = self.semantic_config.use_user_properties
        user_properties: dict[str, str] | None = None

        # Build payload based on mode
        if self._payload_mode == "pointer" and metric.semantic_id:
            pointer = self._get_or_create_pointer(metric)
            if pointer:
                payload = self._build_pointer_payload(metric, pointer)
                if use_props:
                    user_properties = self._build_user_properties_pointer(pointer)
            else:
                # Fallback to inline if no semantic_id
                payload = self._build_payload(metric, aas_uri, include_metadata=True)
                if use_props:
                    user_properties = self._build_user_properties(metric, aas_uri)

        elif self._payload_mode == "hybrid" and metric.semantic_id:
            pointer = self._get_or_create_pointer(metric)
            if pointer:
                payload = self._build_hybrid_payload(metric, pointer, aas_uri)
                if use_props:
                    user_properties = self._build_user_properties_pointer(pointer)
            else:
                payload = self._build_payload(metric, aas_uri, include_metadata=True)
                if use_props:
                    user_properties = self._build_user_properties(metric, aas_uri)

        else:
            # Inline mode (legacy)
            include_payload_metadata = (
                not use_props or self.semantic_config.payload_metadata_fallback
            )
            payload = self._build_payload(
                metric, aas_uri, include_metadata=include_payload_metadata
            )
            if use_props:
                user_properties = self._build_user_properties(metric, aas_uri)

        payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")

        self.client.publish(
            topic=topic,
            payload=payload_bytes,
            qos=self.config.qos,
            retain=self.config.retain,
            user_properties=user_properties,
        )

        self._published_count += 1
        METRICS.uns_published_total.inc()
        METRICS.last_publish_timestamp.set(time.time())
        logger.debug("Published UNS metric to %s (mode=%s)", topic, self._payload_mode)

    def publish_batch(
        self,
        topic_metrics: dict[str, ContextMetric],
        aas_uri: str | None = None,
    ) -> int:
        """Publish multiple metrics.

        Args:
            topic_metrics: Mapping of topics to metrics.
            aas_uri: Optional AAS source URI for provenance.

        Returns:
            Number of metrics published.
        """
        if not self.config.enabled:
            return 0

        count = 0
        for topic, metric in topic_metrics.items():
            try:
                self.publish_metric(topic, metric, aas_uri)
                count += 1
            except Exception as e:
                logger.error("Failed to publish metric to %s: %s", topic, e)

        logger.info("Published %d UNS retained metrics (mode=%s)", count, self._payload_mode)
        return count

    @property
    def published_count(self) -> int:
        """Total number of metrics published since initialization."""
        return self._published_count
