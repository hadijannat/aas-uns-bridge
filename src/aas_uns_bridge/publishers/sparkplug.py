"""Sparkplug B publisher for AAS metrics."""

import logging
import time
from typing import Any

from aas_uns_bridge.config import SparkplugConfig
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.mqtt.client import MqttClient

from ..state.alias_db import AliasDB

logger = logging.getLogger(__name__)

# Sparkplug B topic prefix
SPARKPLUG_NAMESPACE = "spBv1.0"


class SparkplugPublisher:
    """Publisher for Sparkplug B protocol.

    Implements the Sparkplug B specification for SCADA/historian integration:
    - NBIRTH/NDEATH for edge node lifecycle
    - DBIRTH/DDEATH for device lifecycle
    - NDATA/DDATA for metric updates
    - Handles NCMD for rebirth requests
    """

    def __init__(
        self,
        mqtt_client: MqttClient,
        config: SparkplugConfig,
        alias_db: AliasDB,
    ):
        """Initialize the Sparkplug publisher.

        Args:
            mqtt_client: MQTT client for publishing.
            config: Sparkplug configuration.
            alias_db: Database for metric alias persistence.
        """
        self.client = mqtt_client
        self.config = config
        self.alias_db = alias_db

        self._bd_seq = 0  # Birth/death sequence
        self._seq = 0  # Message sequence (0-255)
        self._is_online = False
        self._devices: set[str] = set()
        self._birth_count = 0

        # Set up LWT for NDEATH
        self._setup_lwt()

    def _setup_lwt(self) -> None:
        """Configure Last Will and Testament as NDEATH."""
        ndeath_topic = self._build_topic("NDEATH")
        ndeath_payload = self._build_ndeath_payload()
        self.client.set_lwt(ndeath_topic, ndeath_payload, qos=0, retain=False)
        logger.debug("Set NDEATH LWT on %s", ndeath_topic)

    def _build_topic(self, msg_type: str, device_id: str | None = None) -> str:
        """Build a Sparkplug B topic.

        Topic format: spBv1.0/{group_id}/{msg_type}/{edge_node_id}[/{device_id}]

        Args:
            msg_type: Message type (NBIRTH, DBIRTH, NDATA, etc.).
            device_id: Optional device identifier.

        Returns:
            Full MQTT topic path.
        """
        parts = [
            SPARKPLUG_NAMESPACE,
            self.config.group_id,
            msg_type,
            self.config.edge_node_id,
        ]
        if device_id:
            parts.append(device_id)
        return "/".join(parts)

    def _next_seq(self) -> int:
        """Get and increment the sequence number."""
        seq = self._seq
        self._seq = (self._seq + 1) % 256
        return seq

    def _build_ndeath_payload(self) -> bytes:
        """Build an NDEATH payload with bdSeq metric."""
        try:
            from aas_uns_bridge.publishers.sparkplug_payload import build_ndeath_payload

            return build_ndeath_payload(self._bd_seq)
        except ImportError:
            # Fallback: simple JSON-like structure for testing
            return b'{"bdSeq":' + str(self._bd_seq).encode() + b"}"

    def _build_payload_bytes(
        self,
        metrics: list[dict[str, Any]],
        seq: int,
        timestamp_ms: int | None = None,
    ) -> bytes:
        """Build a Sparkplug payload from metrics.

        Args:
            metrics: List of metric dicts with name, value, alias, etc.
            seq: Sequence number.
            timestamp_ms: Optional payload timestamp.

        Returns:
            Serialized payload bytes.
        """
        try:
            from aas_uns_bridge.publishers.sparkplug_payload import PayloadBuilder

            builder = PayloadBuilder()
            builder.set_seq(seq)
            if timestamp_ms:
                builder.set_timestamp(timestamp_ms)

            for m in metrics:
                properties = {}
                if m.get("semantic_id"):
                    properties["semanticId"] = m["semantic_id"]
                if m.get("unit"):
                    properties["unit"] = m["unit"]
                if m.get("aas_source"):
                    properties["aasSource"] = m["aas_source"]

                builder.add_metric_from_xsd(
                    name=m["name"],
                    value=m["value"],
                    xsd_type=m.get("value_type", "xs:string"),
                    timestamp_ms=m.get("timestamp_ms"),
                    alias=m.get("alias"),
                    properties=properties if properties else None,
                )

            return builder.build()

        except ImportError:
            # Fallback: JSON for testing without protobuf
            import json

            payload = {
                "timestamp": timestamp_ms or int(time.time() * 1000),
                "seq": seq,
                "metrics": metrics,
            }
            return json.dumps(payload).encode("utf-8")

    def publish_nbirth(self) -> None:
        """Publish Node Birth (NBIRTH) message.

        NBIRTH announces the edge node and includes:
        - bdSeq metric for death sequence correlation
        - Node Control/Rebirth metric for rebirth requests
        """
        if not self.config.enabled:
            return

        timestamp_ms = int(time.time() * 1000)

        metrics = [
            {
                "name": "bdSeq",
                "value": self._bd_seq,
                "value_type": "xs:long",
                "timestamp_ms": timestamp_ms,
            },
            {
                "name": "Node Control/Rebirth",
                "value": False,
                "value_type": "xs:boolean",
                "timestamp_ms": timestamp_ms,
            },
        ]

        payload = self._build_payload_bytes(metrics, self._next_seq(), timestamp_ms)
        topic = self._build_topic("NBIRTH")

        # Sparkplug requires retain=false, QoS=0 for births
        self.client.publish(topic, payload, qos=0, retain=False)

        self._is_online = True
        self._birth_count += 1
        logger.info("Published NBIRTH to %s (bdSeq=%d)", topic, self._bd_seq)

        # Subscribe to NCMD for rebirth requests
        ncmd_topic = self._build_topic("NCMD")
        self.client.subscribe(ncmd_topic, self._handle_ncmd)

    def publish_dbirth(
        self,
        device_id: str,
        metrics: list[ContextMetric],
        aas_uri: str | None = None,
    ) -> None:
        """Publish Device Birth (DBIRTH) message.

        Args:
            device_id: Device identifier (typically asset name).
            metrics: All metrics for the device.
            aas_uri: Optional AAS source URI.
        """
        if not self.config.enabled:
            return

        if not self._is_online:
            logger.warning("Cannot publish DBIRTH before NBIRTH")
            return

        timestamp_ms = int(time.time() * 1000)

        # Build metrics with aliases
        metric_dicts: list[dict[str, Any]] = []
        for m in metrics:
            alias = self.alias_db.get_alias(f"{device_id}/{m.path}", device_id)
            metric_dicts.append(
                {
                    "name": m.path,
                    "value": m.value,
                    "value_type": m.value_type,
                    "timestamp_ms": m.timestamp_ms or timestamp_ms,
                    "alias": alias,
                    "semantic_id": m.semantic_id,
                    "unit": m.unit,
                    "aas_source": aas_uri or m.aas_source,
                }
            )

        payload = self._build_payload_bytes(metric_dicts, self._next_seq(), timestamp_ms)
        topic = self._build_topic("DBIRTH", device_id)

        self.client.publish(topic, payload, qos=0, retain=False)

        self._devices.add(device_id)
        self._birth_count += 1
        logger.info("Published DBIRTH to %s (%d metrics)", topic, len(metrics))

    def publish_ddata(
        self,
        device_id: str,
        metrics: list[ContextMetric],
    ) -> None:
        """Publish Device Data (DDATA) message.

        Args:
            device_id: Device identifier.
            metrics: Changed metrics to publish.
        """
        if not self.config.enabled:
            return

        if device_id not in self._devices:
            # Device not born yet, need DBIRTH first
            self.publish_dbirth(device_id, metrics)
            return

        timestamp_ms = int(time.time() * 1000)

        # Build metrics using aliases (name not required after birth)
        metric_dicts: list[dict[str, Any]] = []
        for m in metrics:
            alias = self.alias_db.get_alias(f"{device_id}/{m.path}", device_id)
            metric_dicts.append(
                {
                    "name": "",  # Can be empty after birth
                    "value": m.value,
                    "value_type": m.value_type,
                    "timestamp_ms": m.timestamp_ms or timestamp_ms,
                    "alias": alias,
                }
            )

        payload = self._build_payload_bytes(metric_dicts, self._next_seq(), timestamp_ms)
        topic = self._build_topic("DDATA", device_id)

        self.client.publish(topic, payload, qos=self.config.qos, retain=False)
        logger.debug("Published DDATA to %s (%d metrics)", topic, len(metrics))

    def publish_ddeath(self, device_id: str) -> None:
        """Publish Device Death (DDEATH) message.

        Args:
            device_id: Device identifier.
        """
        if not self.config.enabled:
            return

        payload = self._build_payload_bytes([], self._next_seq())
        topic = self._build_topic("DDEATH", device_id)

        self.client.publish(topic, payload, qos=0, retain=False)

        self._devices.discard(device_id)
        logger.info("Published DDEATH to %s", topic)

    def _handle_ncmd(self, topic: str, payload: bytes) -> None:
        """Handle Node Command (NCMD) messages.

        Implements Rebirth request handling.

        Args:
            topic: MQTT topic.
            payload: Message payload.
        """
        logger.info("Received NCMD on %s", topic)

        # Parse payload to check for Rebirth command
        try:
            # Check for Node Control/Rebirth = true
            # In production, parse the protobuf properly
            if b"Rebirth" in payload:
                logger.info("Processing Rebirth command")
                self.rebirth()
        except Exception as e:
            logger.error("Error handling NCMD: %s", e)

    def rebirth(self) -> None:
        """Perform a rebirth sequence.

        Increments bdSeq and republishes NBIRTH and all DBIRTHs.
        """
        self._bd_seq += 1
        self._seq = 0
        self._is_online = False

        # Republish NBIRTH
        self.publish_nbirth()

        # Note: DBIRTHs need to be republished by the daemon
        # which has access to the current metrics
        logger.info("Rebirth initiated, bdSeq=%d", self._bd_seq)

    def shutdown(self) -> None:
        """Graceful shutdown - publish deaths for all devices."""
        for device_id in list(self._devices):
            self.publish_ddeath(device_id)

        # NDEATH will be sent by LWT

    @property
    def is_online(self) -> bool:
        """Check if the edge node is online (NBIRTH sent)."""
        return self._is_online

    @property
    def birth_count(self) -> int:
        """Total number of birth messages published."""
        return self._birth_count

    @property
    def active_devices(self) -> set[str]:
        """Set of device IDs with active DBIRTH."""
        return self._devices.copy()
