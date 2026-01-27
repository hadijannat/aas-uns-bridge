"""Sparkplug B payload builder using protobuf.

This module provides builders for Sparkplug B payloads with support for
AAS semantic metadata through standardized PropertySet conventions.

The semantic PropertySet keys follow a namespace convention (aas:*) for
interoperability across AAS-aware Sparkplug consumers.
"""

from __future__ import annotations

import importlib
import json
import time
from typing import TYPE_CHECKING, Any, cast

from aas_uns_bridge.publishers.sparkplug_types import (
    SparkplugDataType,
    python_to_sparkplug_type,
    xsd_to_sparkplug_type,
)

if TYPE_CHECKING:
    from aas_uns_bridge.domain.models import ContextMetric
    from aas_uns_bridge.semantic.models import SemanticContext

# Import generated protobuf classes
# These will be generated from sparkplug_b.proto
spb: Any | None
try:
    spb = importlib.import_module("aas_uns_bridge.proto.sparkplug_b_pb2")
except Exception:
    spb = None


# =============================================================================
# Standardized Semantic PropertySet Keys for Sparkplug B
# =============================================================================
# These keys follow the AAS namespace convention for interoperability.
# Sparkplug consumers can filter/route based on these properties.

SEMANTIC_PROPS = {
    # Primary semantic identifier (IRDI or IRI)
    "semanticId": "aas:semanticId",
    # Source dictionary (ECLASS, IEC_CDD, IDTA, custom)
    "semanticDictionary": "aas:semanticDictionary",
    # Dictionary version
    "semanticVersion": "aas:semanticVersion",
    # Hash pointer for resolution cache lookup
    "semanticHash": "aas:semanticHash",
    # JSON array of all semantic keys (poly-hierarchical)
    "semanticKeys": "aas:semanticKeys",
    # Fidelity score for transformation quality
    "fidelityScore": "aas:fidelityScore",
    # Parent submodel semantic ID
    "submodelSemanticId": "aas:submodelSemanticId",
    # AAS element type
    "aasType": "aas:aasType",
    # Unit of measurement
    "unit": "aas:unit",
    # Source AAS URI
    "aasSource": "aas:aasSource",
}


def build_semantic_properties(
    metric: ContextMetric,
    fidelity_score: float | None = None,
) -> dict[str, Any]:
    """Build semantic PropertySet from a ContextMetric.

    Creates a dictionary of semantic metadata suitable for inclusion
    in Sparkplug B metric properties.

    Args:
        metric: The context metric with semantic metadata.
        fidelity_score: Optional fidelity score to include.

    Returns:
        Dictionary of property key-value pairs.
    """
    props: dict[str, Any] = {}

    if metric.semantic_id:
        props[SEMANTIC_PROPS["semanticId"]] = metric.semantic_id

    # Include all semantic keys if poly-hierarchical
    semantic_keys = getattr(metric, "semantic_keys", ())
    if semantic_keys and len(semantic_keys) > 1:
        props[SEMANTIC_PROPS["semanticKeys"]] = json.dumps(list(semantic_keys))

    if metric.unit:
        props[SEMANTIC_PROPS["unit"]] = metric.unit

    if metric.aas_type:
        props[SEMANTIC_PROPS["aasType"]] = metric.aas_type

    if metric.aas_source:
        props[SEMANTIC_PROPS["aasSource"]] = metric.aas_source

    # Include submodel context if available
    submodel_id = getattr(metric, "submodel_semantic_id", None)
    if submodel_id:
        props[SEMANTIC_PROPS["submodelSemanticId"]] = submodel_id

    if fidelity_score is not None:
        props[SEMANTIC_PROPS["fidelityScore"]] = fidelity_score

    return props


def build_semantic_properties_from_context(
    context: SemanticContext,
    include_hash: bool = True,
) -> dict[str, Any]:
    """Build semantic PropertySet from a SemanticContext.

    Args:
        context: The semantic context.
        include_hash: Whether to include the hash pointer.

    Returns:
        Dictionary of property key-value pairs.
    """
    props: dict[str, Any] = {
        SEMANTIC_PROPS["semanticId"]: context.semantic_id,
        SEMANTIC_PROPS["semanticDictionary"]: context.dictionary,
        SEMANTIC_PROPS["semanticVersion"]: context.version,
    }

    if include_hash:
        props[SEMANTIC_PROPS["semanticHash"]] = context.hash

    if context.unit:
        props[SEMANTIC_PROPS["unit"]] = context.unit

    if len(context.hierarchy) > 1:
        props[SEMANTIC_PROPS["semanticKeys"]] = json.dumps(list(context.hierarchy))

    return props


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

    def set_timestamp(self, timestamp_ms: int) -> PayloadBuilder:
        """Set the payload timestamp.

        Args:
            timestamp_ms: Unix timestamp in milliseconds.
        """
        self._payload.timestamp = timestamp_ms
        return self

    def set_seq(self, seq: int) -> PayloadBuilder:
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
    ) -> PayloadBuilder:
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
    ) -> PayloadBuilder:
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
            is_null=value is None,
            properties=properties,
        )

    def add_metric_with_semantic_props(
        self,
        metric: ContextMetric,
        alias: int | None = None,
        fidelity_score: float | None = None,
        additional_props: dict[str, Any] | None = None,
    ) -> PayloadBuilder:
        """Add a metric with standardized semantic PropertySet.

        Convenience method that extracts semantic metadata from a ContextMetric
        and includes it in the Sparkplug PropertySet using the AAS namespace
        convention.

        Args:
            metric: The context metric to add.
            alias: Optional numeric alias.
            fidelity_score: Optional fidelity score to include.
            additional_props: Additional properties to merge.

        Returns:
            Self for method chaining.
        """
        # Build semantic properties
        semantic_props = build_semantic_properties(metric, fidelity_score)

        # Merge additional properties if provided
        if additional_props:
            semantic_props.update(additional_props)

        # Determine datatype from XSD type
        datatype = xsd_to_sparkplug_type(metric.value_type)

        return self.add_metric(
            name=metric.path,
            value=metric.value,
            timestamp_ms=metric.timestamp_ms if metric.timestamp_ms else None,
            alias=alias,
            datatype=datatype,
            is_null=metric.value is None,
            properties=semantic_props if semantic_props else None,
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
