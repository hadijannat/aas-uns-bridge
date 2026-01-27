"""Sparkplug B payload builder using protobuf."""

import importlib
import time
from typing import Any, cast

from aas_uns_bridge.publishers.sparkplug_types import (
    SparkplugDataType,
    python_to_sparkplug_type,
    xsd_to_sparkplug_type,
)

# Import generated protobuf classes
# These will be generated from sparkplug_b.proto
spb: Any | None
try:
    spb = importlib.import_module("aas_uns_bridge.proto.sparkplug_b_pb2")
except Exception:
    spb = None


class PayloadBuilder:
    """Builder for Sparkplug B payloads."""

    def __init__(self) -> None:
        """Initialize the payload builder."""
        if spb is None:
            raise ImportError(
                "Sparkplug protobuf not generated. Run: "
                "protoc --python_out=src/aas_uns_bridge/proto proto/sparkplug_b.proto"
            )
        self._payload = spb.Payload()
        self._payload.timestamp = int(time.time() * 1000)

    def set_timestamp(self, timestamp_ms: int) -> "PayloadBuilder":
        """Set the payload timestamp.

        Args:
            timestamp_ms: Unix timestamp in milliseconds.
        """
        self._payload.timestamp = timestamp_ms
        return self

    def set_seq(self, seq: int) -> "PayloadBuilder":
        """Set the sequence number.

        Args:
            seq: Sequence number (0-255).
        """
        self._payload.seq = seq % 256
        return self

    def add_metric(
        self,
        name: str,
        value: Any,
        timestamp_ms: int | None = None,
        alias: int | None = None,
        datatype: SparkplugDataType | None = None,
        is_null: bool = False,
        properties: dict[str, Any] | None = None,
    ) -> "PayloadBuilder":
        """Add a metric to the payload.

        Args:
            name: Metric name.
            value: Metric value.
            timestamp_ms: Optional metric timestamp.
            alias: Optional numeric alias.
            datatype: Optional explicit data type.
            is_null: Whether the value is null.
            properties: Optional property set for metadata.
        """
        metric = self._payload.metrics.add()
        metric.name = name

        if alias is not None:
            metric.alias = alias

        if timestamp_ms is not None:
            metric.timestamp = timestamp_ms
        else:
            metric.timestamp = int(time.time() * 1000)

        # Determine data type
        if datatype is None:
            datatype = python_to_sparkplug_type(value)
        metric.datatype = datatype.value

        # Set value based on type
        metric.is_null = is_null
        if not is_null and value is not None:
            self._set_metric_value(metric, value, datatype)

        # Add properties if provided
        if properties:
            self._add_properties(metric, properties)

        return self

    def _set_metric_value(
        self,
        metric: Any,
        value: Any,
        datatype: SparkplugDataType,
    ) -> None:
        """Set the value field on a metric."""
        if datatype == SparkplugDataType.Boolean:
            metric.boolean_value = bool(value)
        elif datatype in (
            SparkplugDataType.Int8,
            SparkplugDataType.Int16,
            SparkplugDataType.Int32,
            SparkplugDataType.UInt8,
            SparkplugDataType.UInt16,
            SparkplugDataType.UInt32,
        ):
            metric.int_value = int(value)
        elif datatype in (
            SparkplugDataType.Int64,
            SparkplugDataType.UInt64,
            SparkplugDataType.DateTime,
        ):
            metric.long_value = int(value)
        elif datatype == SparkplugDataType.Float:
            metric.float_value = float(value)
        elif datatype == SparkplugDataType.Double:
            metric.double_value = float(value)
        elif datatype == SparkplugDataType.Bytes:
            if isinstance(value, bytes):
                metric.bytes_value = value
            else:
                metric.bytes_value = str(value).encode("utf-8")
        else:
            # Default to string
            metric.string_value = str(value)

    def _add_properties(self, metric: Any, properties: dict[str, Any]) -> None:
        """Add a property set to a metric."""
        for key, value in properties.items():
            metric.properties.keys.append(key)
            prop_value = metric.properties.values.add()

            if value is None:
                prop_value.is_null = True
                prop_value.type = SparkplugDataType.Unknown.value
            elif isinstance(value, bool):
                prop_value.type = SparkplugDataType.Boolean.value
                prop_value.boolean_value = value
            elif isinstance(value, int):
                prop_value.type = SparkplugDataType.Int64.value
                prop_value.long_value = value
            elif isinstance(value, float):
                prop_value.type = SparkplugDataType.Double.value
                prop_value.double_value = value
            else:
                prop_value.type = SparkplugDataType.String.value
                prop_value.string_value = str(value)

    def add_metric_from_xsd(
        self,
        name: str,
        value: Any,
        xsd_type: str,
        timestamp_ms: int | None = None,
        alias: int | None = None,
        properties: dict[str, Any] | None = None,
    ) -> "PayloadBuilder":
        """Add a metric with XSD type conversion.

        Args:
            name: Metric name.
            value: Metric value.
            xsd_type: XSD type string (e.g., 'xs:int').
            timestamp_ms: Optional metric timestamp.
            alias: Optional numeric alias.
            properties: Optional property set for metadata.
        """
        datatype = xsd_to_sparkplug_type(xsd_type)
        return self.add_metric(
            name=name,
            value=value,
            timestamp_ms=timestamp_ms,
            alias=alias,
            datatype=datatype,
            properties=properties,
        )

    def build(self) -> bytes:
        """Build and serialize the payload.

        Returns:
            Serialized protobuf bytes.
        """
        return cast(bytes, self._payload.SerializeToString())

    def get_payload(self) -> Any:
        """Get the raw protobuf payload object."""
        return self._payload


def build_ndeath_payload(bd_seq: int) -> bytes:
    """Build an NDEATH payload.

    Args:
        bd_seq: Birth/death sequence number.

    Returns:
        Serialized NDEATH payload.
    """
    builder = PayloadBuilder()
    builder.add_metric("bdSeq", bd_seq, datatype=SparkplugDataType.Int64)
    return builder.build()
