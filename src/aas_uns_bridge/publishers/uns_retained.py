"""UNS retained topic publisher."""

import json
import logging
from typing import Any

from aas_uns_bridge.config import UnsConfig
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.mqtt.client import MqttClient

logger = logging.getLogger(__name__)


class UnsRetainedPublisher:
    """Publisher for UNS retained MQTT topics.

    Publishes AAS metrics as JSON payloads to retained MQTT topics,
    enabling late-subscriber discovery of current asset state.
    """

    def __init__(self, mqtt_client: MqttClient, config: UnsConfig):
        """Initialize the UNS publisher.

        Args:
            mqtt_client: MQTT client for publishing.
            config: UNS publication configuration.
        """
        self.client = mqtt_client
        self.config = config
        self._published_count = 0

    def _build_payload(self, metric: ContextMetric, aas_uri: str | None = None) -> dict[str, Any]:
        """Build the JSON payload for a metric.

        Payload format:
        {
            "value": <metric value>,
            "timestamp": <unix ms>,
            "semanticId": <IRDI/IRI or null>,
            "unit": <unit string or null>,
            "valueType": <XSD type>,
            "source": "aas-uns-bridge",
            "aasUri": <source identifier>
        }
        """
        return {
            "value": metric.value,
            "timestamp": metric.timestamp_ms,
            "semanticId": metric.semantic_id,
            "unit": metric.unit,
            "valueType": metric.value_type,
            "source": "aas-uns-bridge",
            "aasUri": aas_uri or metric.aas_source,
        }

    def publish_metric(
        self,
        topic: str,
        metric: ContextMetric,
        aas_uri: str | None = None,
    ) -> None:
        """Publish a single metric to its UNS topic.

        Args:
            topic: The full MQTT topic path.
            metric: The context metric to publish.
            aas_uri: Optional AAS source URI for provenance.
        """
        if not self.config.enabled:
            return

        payload = self._build_payload(metric, aas_uri)
        payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")

        self.client.publish(
            topic=topic,
            payload=payload_bytes,
            qos=self.config.qos,
            retain=self.config.retain,
        )

        self._published_count += 1
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
