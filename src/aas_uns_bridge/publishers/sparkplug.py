"""Sparkplug B publisher for AAS metrics."""

import logging
import time
from typing import Any

from aas_uns_bridge.config import SparkplugConfig
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.mqtt.client import MqttClient
from aas_uns_bridge.observability.metrics import METRICS
from aas_uns_bridge.state.alias_db import AliasDB
from aas_uns_bridge.state.birth_cache import BirthCache

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
        birth_cache: BirthCache | None = None,
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
        self._birth_cache = birth_cache

        self._bd_seq = 0  # Birth/death sequence
        self._seq = 0  # Message sequence (0-255)
        self._is_online = False
        self._devices: set[str] = set()
        self._device_metrics: dict[str, dict[str, ContextMetric]] = {}
        self._birth_count = 0
        self._has_published_nbirth = False

        # Set up LWT for NDEATH
        self._setup_lwt()

    def _setup_lwt(self) -> None:
        """Configure Last Will and Testament as NDEATH."""
        ndeath_topic = self._build_topic("NDEATH")
        ndeath_payload = self._build_ndeath_payload()
        self.client.set_lwt(ndeath_topic, ndeath_payload, qos=0, retain=False)
        logger.debug("Set NDEATH LWT on %s", ndeath_topic)

    def mark_offline(self) -> None:
        """Mark the edge node as offline after disconnect."""
        self._is_online = False

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
            from aas_uns_bridge.publishers.sparkplug_payload import (
                SEMANTIC_PROPS,
                PayloadBuilder,
            )

            builder = PayloadBuilder()
            builder.set_seq(seq)
            if timestamp_ms:
                builder.set_timestamp(timestamp_ms)

            for m in metrics:
                # Build semantic properties using standardized aas:* namespaced keys
                properties: dict[str, Any] = {}
                if m.get("semantic_id"):
                    properties[SEMANTIC_PROPS["semanticId"]] = m["semantic_id"]
                if m.get("unit"):
                    properties[SEMANTIC_PROPS["unit"]] = m["unit"]
                if m.get("aas_source"):
                    properties[SEMANTIC_PROPS["aasSource"]] = m["aas_source"]

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

    def _decode_payload(self, payload: bytes) -> Any | None:
        """Decode a Sparkplug protobuf payload if available."""
        try:
            from aas_uns_bridge.proto import sparkplug_b_pb2 as spb

            decoded = spb.Payload()  # type: ignore[attr-defined]
            decoded.ParseFromString(payload)
            return decoded
        except Exception as exc:  # pragma: no cover - depends on optional proto
            logger.debug("Failed to decode Sparkplug payload: %s", exc)
            return None

    def _metric_truthy(self, metric: Any) -> bool:
        """Evaluate a Sparkplug metric value as truthy."""
        if getattr(metric, "boolean_value", False):
            return True
        if getattr(metric, "int_value", 0):
            return True
        if getattr(metric, "long_value", 0):
            return True
        if getattr(metric, "float_value", 0.0):
            return True
        if getattr(metric, "double_value", 0.0):
            return True
        if hasattr(metric, "string_value"):
            return str(metric.string_value).strip().lower() in {"true", "1", "yes", "y", "on"}
        return False

    def _payload_requests_rebirth(self, payload: bytes) -> bool:
        """Check if a payload contains a Rebirth request."""
        decoded = self._decode_payload(payload)
        if decoded is None:
            return b"Rebirth" in payload

        for metric in decoded.metrics:
            name = metric.name or ""
            if (name.endswith("Rebirth") or "Rebirth" in name) and self._metric_truthy(metric):
                return True
        return False

    def _store_device_metrics(self, device_id: str, metrics: list[ContextMetric]) -> None:
        """Store the full metric set for a device."""
        self._device_metrics[device_id] = {metric.path: metric for metric in metrics}

    def _merge_device_metrics(self, device_id: str, metrics: list[ContextMetric]) -> None:
        """Merge updated metrics into the stored device metrics."""
        if device_id not in self._device_metrics:
            self._device_metrics[device_id] = {}
        for metric in metrics:
            self._device_metrics[device_id][metric.path] = metric

    def _collect_device_metrics(self, device_id: str) -> list[ContextMetric]:
        """Return stored metrics for a device."""
        return list(self._device_metrics.get(device_id, {}).values())

    def _refresh_payload(self, payload: bytes, seq: int, timestamp_ms: int | None = None) -> bytes:
        """Refresh seq/timestamp fields in a cached payload."""
        decoded = self._decode_payload(payload)
        if decoded is None:
            return payload

        decoded.seq = seq
        decoded.timestamp = timestamp_ms or int(time.time() * 1000)
        result: bytes = decoded.SerializeToString()
        return result

    def _publish_cached_dbirth(self, device_id: str) -> bool:
        """Publish a cached DBIRTH payload if available."""
        if not self._birth_cache:
            return False

        cached = self._birth_cache.get_dbirth(device_id)
        if not cached:
            return False

        topic, payload = cached
        refreshed = self._refresh_payload(payload, self._next_seq())
        self.client.publish(topic, refreshed, qos=0, retain=False)
        self._devices.add(device_id)
        self._birth_count += 1
        METRICS.sparkplug_births_total.labels(birth_type="dbirth").inc()
        METRICS.active_devices.set(len(self._devices))
        METRICS.last_publish_timestamp.set(time.time())
        logger.info("Republished cached DBIRTH to %s", topic)
        return True

    def publish_nbirth(self) -> None:
        """Publish Node Birth (NBIRTH) message.

        NBIRTH announces the edge node and includes:
        - bdSeq metric for death sequence correlation
        - Node Control/Rebirth metric for rebirth requests
        """
        if not self.config.enabled:
            return

        if not self._is_online and self._has_published_nbirth:
            # New session after disconnect: increment birth/death sequence
            self._bd_seq += 1
            self._seq = 0

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

        if self._birth_cache:
            self._birth_cache.store_nbirth(topic, payload)

        self._is_online = True
        self._has_published_nbirth = True
        self._birth_count += 1
        METRICS.sparkplug_births_total.labels(birth_type="nbirth").inc()
        METRICS.last_publish_timestamp.set(time.time())
        logger.info("Published NBIRTH to %s (bdSeq=%d)", topic, self._bd_seq)

        # Subscribe to NCMD for rebirth requests
        ncmd_topic = self._build_topic("NCMD")
        self.client.subscribe(ncmd_topic, self._handle_ncmd)

        # Subscribe to DCMD for device-specific commands
        dcmd_topic = f"{self._build_topic('DCMD')}/#"
        self.client.subscribe(dcmd_topic, self._handle_dcmd)

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
        self._store_device_metrics(device_id, metrics)
        self._birth_count += 1
        METRICS.sparkplug_births_total.labels(birth_type="dbirth").inc()
        METRICS.active_devices.set(len(self._devices))
        METRICS.alias_count.set(self.alias_db.count)
        METRICS.last_publish_timestamp.set(time.time())
        logger.info("Published DBIRTH to %s (%d metrics)", topic, len(metrics))

        if self._birth_cache:
            self._birth_cache.store_dbirth(device_id, topic, payload)

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

        if not metrics:
            return

        self._merge_device_metrics(device_id, metrics)

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
        METRICS.sparkplug_data_total.inc()
        METRICS.last_publish_timestamp.set(time.time())
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
        METRICS.active_devices.set(len(self._devices))
        METRICS.last_publish_timestamp.set(time.time())
        logger.info("Published DDEATH to %s", topic)

    def _handle_ncmd(self, topic: str, payload: bytes) -> None:
        """Handle Node Command (NCMD) messages.

        Implements Rebirth request handling.

        Args:
            topic: MQTT topic.
            payload: Message payload.
        """
        logger.info("Received NCMD on %s", topic)

        try:
            if self._payload_requests_rebirth(payload):
                logger.info("Processing Node Rebirth command")
                self.rebirth()
        except Exception as e:
            logger.error("Error handling NCMD: %s", e)

    def _handle_dcmd(self, topic: str, payload: bytes) -> None:
        """Handle Device Command (DCMD) messages."""
        logger.info("Received DCMD on %s", topic)

        try:
            if not self._payload_requests_rebirth(payload):
                return

            parts = topic.split("/")
            device_id = parts[4] if len(parts) >= 5 else None
            if device_id:
                logger.info("Processing Device Rebirth for %s", device_id)
                self.rebirth_device(device_id)
        except Exception as e:
            logger.error("Error handling DCMD: %s", e)

    def rebirth(self) -> None:
        """Perform a rebirth sequence.

        Increments bdSeq and republishes NBIRTH and all DBIRTHs.
        """
        self._seq = 0
        self._is_online = False

        self.publish_nbirth()
        self.republish_dbirths()

        logger.info("Rebirth initiated, bdSeq=%d", self._bd_seq)

    def rebirth_device(self, device_id: str) -> None:
        """Rebirth a specific device by republishing its DBIRTH."""
        metrics = self._collect_device_metrics(device_id)
        if metrics:
            self.publish_dbirth(device_id, metrics)
            return

        if not self._publish_cached_dbirth(device_id):
            logger.warning("No cached metrics available for device rebirth: %s", device_id)

    def publish_device_metrics(
        self,
        device_id: str,
        metrics_all: list[ContextMetric],
        metrics_changed: list[ContextMetric],
        aas_uri: str | None = None,
    ) -> None:
        """Publish DBIRTH or DDATA based on device state."""
        if not self.config.enabled:
            return

        if device_id not in self._devices:
            self.publish_dbirth(device_id, metrics_all, aas_uri)
            return

        if metrics_changed:
            self.publish_ddata(device_id, metrics_changed)

        # Keep stored metrics updated even if nothing changed
        if metrics_all:
            self._store_device_metrics(device_id, metrics_all)

    def republish_dbirths(self) -> None:
        """Republish DBIRTHs for all known devices."""
        device_ids = set(self._device_metrics.keys())
        if self._birth_cache:
            device_ids.update(self._birth_cache.get_all_dbirth_device_ids())

        for device_id in sorted(device_ids):
            metrics = self._collect_device_metrics(device_id)
            if metrics:
                self.publish_dbirth(device_id, metrics)
                continue

            self._publish_cached_dbirth(device_id)

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
