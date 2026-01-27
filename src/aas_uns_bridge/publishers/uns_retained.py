"""UNS retained topic publisher."""

import json
import logging
import time
from typing import Any

from aas_uns_bridge.config import SemanticConfig, UnsConfig
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.mqtt.client import MqttClient
from aas_uns_bridge.observability.metrics import METRICS

logger = logging.getLogger(__name__)

# User Property keys for MQTT v5 semantic metadata
USER_PROP_SEMANTIC_ID = "aas:semanticId"
USER_PROP_UNIT = "aas:unit"
USER_PROP_VALUE_TYPE = "aas:valueType"
USER_PROP_AAS_TYPE = "aas:aasType"
USER_PROP_SOURCE = "aas:source"


class UnsRetainedPublisher:
    """Publisher for UNS retained MQTT topics.

    Publishes AAS metrics as JSON payloads to retained MQTT topics,
    enabling late-subscriber discovery of current asset state.

    Supports MQTT v5 User Properties for semantic metadata when configured,
    reducing payload size and enabling header-based filtering on modern brokers.
    """

    def __init__(
        self,
        mqtt_client: MqttClient,
        config: UnsConfig,
        semantic_config: SemanticConfig | None = None,
    ):
        """Initialize the UNS publisher.

        Args:
            mqtt_client: MQTT client for publishing.
            config: UNS publication configuration.
            semantic_config: Optional semantic enforcement configuration.
        """
        self.client = mqtt_client
        self.config = config
        self.semantic_config = semantic_config or SemanticConfig()
        self._published_count = 0

    def _build_user_properties(
        self, metric: ContextMetric, aas_uri: str | None = None
    ) -> dict[str, str]:
        """Build MQTT v5 User Properties for a metric.

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

    def _build_payload(
        self,
        metric: ContextMetric,
        aas_uri: str | None = None,
        include_metadata: bool = True,
    ) -> dict[str, Any]:
        """Build the JSON payload for a metric.

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

    def publish_metric(
        self,
        topic: str,
        metric: ContextMetric,
        aas_uri: str | None = None,
    ) -> None:
        """Publish a single metric to its UNS topic.

        When semantic_config.use_user_properties is True, semantic metadata
        is included in MQTT v5 User Properties (headers). When
        payload_metadata_fallback is also True, metadata is duplicated in
        the payload for compatibility with non-v5 subscribers.

        Args:
            topic: The full MQTT topic path.
            metric: The context metric to publish.
            aas_uri: Optional AAS source URI for provenance.
        """
        if not self.config.enabled:
            return

        # Determine whether to use User Properties and payload metadata
        use_props = self.semantic_config.use_user_properties
        include_payload_metadata = (
            not use_props or self.semantic_config.payload_metadata_fallback
        )

        payload = self._build_payload(
            metric, aas_uri, include_metadata=include_payload_metadata
        )
        payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")

        # Build User Properties if configured
        user_properties = None
        if use_props:
            user_properties = self._build_user_properties(metric, aas_uri)

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
        logger.debug("Published UNS metric to %s", topic)

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

        logger.info("Published %d UNS retained metrics", count)
        return count

    @property
    def published_count(self) -> int:
        """Total number of metrics published since initialization."""
        return self._published_count
