"""Unit tests for MQTT v5 User Properties support."""

from unittest.mock import MagicMock, patch

import pytest

from aas_uns_bridge.config import SemanticConfig, UnsConfig
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.mqtt.client import MqttClient
from aas_uns_bridge.publishers.uns_retained import (
    USER_PROP_AAS_TYPE,
    USER_PROP_SEMANTIC_ID,
    USER_PROP_SOURCE,
    USER_PROP_UNIT,
    USER_PROP_VALUE_TYPE,
    UnsRetainedPublisher,
)


@pytest.fixture
def sample_metric() -> ContextMetric:
    """Create a sample metric with full metadata."""
    return ContextMetric(
        path="TechnicalData.Temperature",
        value=25.5,
        aas_type="Property",
        value_type="xs:double",
        semantic_id="0173-1#02-AAO677#002",
        unit="degC",
        aas_source="test.aasx",
        timestamp_ms=1706400000000,
    )


@pytest.fixture
def minimal_metric() -> ContextMetric:
    """Create a metric with minimal metadata."""
    return ContextMetric(
        path="TechnicalData.Name",
        value="TestDevice",
        aas_type="Property",
        value_type="xs:string",
        timestamp_ms=1706400000000,
    )


class TestUnsRetainedPublisherUserProperties:
    """Tests for User Properties in UnsRetainedPublisher."""

    def test_build_user_properties_full(self, sample_metric: ContextMetric) -> None:
        """Test building User Properties with all metadata."""
        mqtt_client = MagicMock(spec=MqttClient)
        publisher = UnsRetainedPublisher(mqtt_client, UnsConfig())

        props = publisher._build_user_properties(sample_metric, "file://test.aasx")

        assert props[USER_PROP_SEMANTIC_ID] == "0173-1#02-AAO677#002"
        assert props[USER_PROP_UNIT] == "degC"
        assert props[USER_PROP_VALUE_TYPE] == "xs:double"
        assert props[USER_PROP_AAS_TYPE] == "Property"
        assert props[USER_PROP_SOURCE] == "file://test.aasx"

    def test_build_user_properties_minimal(self, minimal_metric: ContextMetric) -> None:
        """Test building User Properties with minimal metadata."""
        mqtt_client = MagicMock(spec=MqttClient)
        publisher = UnsRetainedPublisher(mqtt_client, UnsConfig())

        props = publisher._build_user_properties(minimal_metric)

        # Only non-None values should be included
        assert USER_PROP_SEMANTIC_ID not in props
        assert USER_PROP_UNIT not in props
        assert props[USER_PROP_VALUE_TYPE] == "xs:string"
        assert props[USER_PROP_AAS_TYPE] == "Property"

    def test_build_payload_with_metadata(self, sample_metric: ContextMetric) -> None:
        """Test building payload with metadata included."""
        mqtt_client = MagicMock(spec=MqttClient)
        publisher = UnsRetainedPublisher(mqtt_client, UnsConfig())

        payload = publisher._build_payload(sample_metric, include_metadata=True)

        assert payload["value"] == 25.5
        assert payload["timestamp"] == 1706400000000
        assert payload["semanticId"] == "0173-1#02-AAO677#002"
        assert payload["unit"] == "degC"
        assert payload["valueType"] == "xs:double"

    def test_build_payload_without_metadata(self, sample_metric: ContextMetric) -> None:
        """Test building payload without metadata (for User Properties mode)."""
        mqtt_client = MagicMock(spec=MqttClient)
        publisher = UnsRetainedPublisher(mqtt_client, UnsConfig())

        payload = publisher._build_payload(sample_metric, include_metadata=False)

        assert payload["value"] == 25.5
        assert payload["timestamp"] == 1706400000000
        assert "semanticId" not in payload
        assert "unit" not in payload
        assert "valueType" not in payload

    def test_publish_with_user_properties_enabled(
        self, sample_metric: ContextMetric
    ) -> None:
        """Test publishing with User Properties enabled."""
        mqtt_client = MagicMock(spec=MqttClient)
        semantic_config = SemanticConfig(
            use_user_properties=True,
            payload_metadata_fallback=False,
        )
        publisher = UnsRetainedPublisher(mqtt_client, UnsConfig(), semantic_config)

        publisher.publish_metric("test/topic", sample_metric)

        # Verify publish was called with user_properties
        mqtt_client.publish.assert_called_once()
        call_kwargs = mqtt_client.publish.call_args.kwargs
        assert call_kwargs["user_properties"] is not None
        assert USER_PROP_SEMANTIC_ID in call_kwargs["user_properties"]

    def test_publish_with_user_properties_and_fallback(
        self, sample_metric: ContextMetric
    ) -> None:
        """Test publishing with User Properties and payload fallback."""
        mqtt_client = MagicMock(spec=MqttClient)
        semantic_config = SemanticConfig(
            use_user_properties=True,
            payload_metadata_fallback=True,
        )
        publisher = UnsRetainedPublisher(mqtt_client, UnsConfig(), semantic_config)

        publisher.publish_metric("test/topic", sample_metric)

        # Verify both User Properties and payload metadata
        mqtt_client.publish.assert_called_once()
        call_kwargs = mqtt_client.publish.call_args.kwargs
        assert call_kwargs["user_properties"] is not None

        # Payload should contain metadata (since fallback=True)
        import json
        payload = json.loads(call_kwargs["payload"])
        assert "semanticId" in payload

    def test_publish_without_user_properties(
        self, sample_metric: ContextMetric
    ) -> None:
        """Test publishing without User Properties (default)."""
        mqtt_client = MagicMock(spec=MqttClient)
        publisher = UnsRetainedPublisher(mqtt_client, UnsConfig())

        publisher.publish_metric("test/topic", sample_metric)

        # Verify publish was called without user_properties
        mqtt_client.publish.assert_called_once()
        call_kwargs = mqtt_client.publish.call_args.kwargs
        assert call_kwargs.get("user_properties") is None


class TestMqttClientUserProperties:
    """Tests for User Properties in MqttClient."""

    def test_publish_builds_properties(self) -> None:
        """Test that publish method builds MQTT Properties correctly."""
        with patch("aas_uns_bridge.mqtt.client.mqtt.Client") as mock_client_cls:
            from aas_uns_bridge.config import MqttConfig

            mock_instance = mock_client_cls.return_value
            mock_instance.is_connected.return_value = True

            # Simulate successful publish
            mock_result = MagicMock()
            mock_result.rc = 0  # MQTT_ERR_SUCCESS
            mock_instance.publish.return_value = mock_result

            client = MqttClient(MqttConfig())
            # Manually set connected state
            client._connected.set()

            user_props = {
                "aas:semanticId": "0173-1#02-AAO677#002",
                "aas:unit": "degC",
            }

            client.publish(
                "test/topic",
                b"payload",
                user_properties=user_props,
            )

            # Verify publish was called with properties parameter
            mock_instance.publish.assert_called_once()
            call_args = mock_instance.publish.call_args
            assert call_args.kwargs.get("properties") is not None
