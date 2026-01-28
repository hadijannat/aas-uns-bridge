"""Unit tests for MQTT publish queue depth metric (backpressure monitoring)."""

from unittest.mock import MagicMock, patch

import pytest

from aas_uns_bridge.config import MqttConfig
from aas_uns_bridge.mqtt.client import MqttClient, MqttClientError
from aas_uns_bridge.observability.metrics import METRICS


class TestMqttPublishQueueDepthMetric:
    """Tests for the MQTT publish queue depth metric."""

    def test_queue_depth_metric_exists(self) -> None:
        """Verify the mqtt_publish_queue_depth metric is registered."""
        assert hasattr(METRICS, "mqtt_publish_queue_depth")
        # Verify it's a Gauge by checking it has the set method
        assert hasattr(METRICS.mqtt_publish_queue_depth, "set")
        # Verify metric name
        assert METRICS.mqtt_publish_queue_depth._name == "aas_bridge_mqtt_publish_queue_depth"

    def test_queue_depth_increments_on_publish(self) -> None:
        """Verify publish increments the queue depth metric."""
        with patch("aas_uns_bridge.mqtt.client.mqtt.Client") as mock_client_cls:
            mock_instance = mock_client_cls.return_value
            mock_instance.is_connected.return_value = True

            # Simulate successful publish (queued, awaiting ack)
            mock_result = MagicMock()
            mock_result.rc = 0  # MQTT_ERR_SUCCESS
            mock_instance.publish.return_value = mock_result

            client = MqttClient(MqttConfig())
            client._connected.set()

            # Reset metric to known state
            METRICS.mqtt_publish_queue_depth.set(0)
            client._pending_publish_count = 0

            # Publish a message
            client.publish("test/topic", b"payload", qos=1)

            # Verify pending count incremented
            assert client.get_pending_publish_count() == 1

            # Verify metric was updated
            # Get the current metric value
            metric_value = METRICS.mqtt_publish_queue_depth._value.get()
            assert metric_value == 1

    def test_queue_depth_decrements_on_ack(self) -> None:
        """Verify publish acknowledgment decrements the queue depth metric."""
        with patch("aas_uns_bridge.mqtt.client.mqtt.Client") as mock_client_cls:
            mock_instance = mock_client_cls.return_value
            mock_instance.is_connected.return_value = True

            # Simulate successful publish
            mock_result = MagicMock()
            mock_result.rc = 0
            mock_instance.publish.return_value = mock_result

            client = MqttClient(MqttConfig())
            client._connected.set()

            # Reset metric to known state
            METRICS.mqtt_publish_queue_depth.set(0)
            client._pending_publish_count = 0

            # Publish a message
            client.publish("test/topic", b"payload", qos=1)
            assert client.get_pending_publish_count() == 1

            # Simulate publish acknowledgment callback
            client._handle_publish_ack(
                mock_instance,
                None,
                mid=1,
                reason_code=None,
                properties=None,
            )

            # Verify pending count decremented
            assert client.get_pending_publish_count() == 0

            # Verify metric was updated
            metric_value = METRICS.mqtt_publish_queue_depth._value.get()
            assert metric_value == 0

    def test_queue_depth_handles_multiple_publishes(self) -> None:
        """Verify queue depth tracks multiple concurrent publishes."""
        with patch("aas_uns_bridge.mqtt.client.mqtt.Client") as mock_client_cls:
            mock_instance = mock_client_cls.return_value
            mock_instance.is_connected.return_value = True

            mock_result = MagicMock()
            mock_result.rc = 0
            mock_instance.publish.return_value = mock_result

            client = MqttClient(MqttConfig())
            client._connected.set()

            # Reset state
            METRICS.mqtt_publish_queue_depth.set(0)
            client._pending_publish_count = 0

            # Publish multiple messages
            for i in range(5):
                client.publish(f"test/topic/{i}", b"payload", qos=1)

            assert client.get_pending_publish_count() == 5
            assert METRICS.mqtt_publish_queue_depth._value.get() == 5

            # Acknowledge some messages
            for mid in range(3):
                client._handle_publish_ack(
                    mock_instance, None, mid=mid, reason_code=None, properties=None
                )

            assert client.get_pending_publish_count() == 2
            assert METRICS.mqtt_publish_queue_depth._value.get() == 2

    def test_queue_depth_does_not_go_negative(self) -> None:
        """Verify queue depth doesn't go negative on extra acks."""
        with patch("aas_uns_bridge.mqtt.client.mqtt.Client") as mock_client_cls:
            mock_instance = mock_client_cls.return_value

            client = MqttClient(MqttConfig())

            # Reset state
            METRICS.mqtt_publish_queue_depth.set(0)
            client._pending_publish_count = 0

            # Simulate spurious ack (shouldn't happen but guard against it)
            client._handle_publish_ack(
                mock_instance, None, mid=999, reason_code=None, properties=None
            )

            # Should remain at 0, not go negative
            assert client.get_pending_publish_count() == 0
            assert METRICS.mqtt_publish_queue_depth._value.get() == 0

    def test_queue_depth_decrements_on_publish_failure(self) -> None:
        """Verify failed publish decrements the pending count."""
        with patch("aas_uns_bridge.mqtt.client.mqtt.Client") as mock_client_cls:
            mock_instance = mock_client_cls.return_value
            mock_instance.is_connected.return_value = True

            # Simulate failed publish
            mock_result = MagicMock()
            mock_result.rc = 1  # Error code (not SUCCESS)
            mock_instance.publish.return_value = mock_result

            client = MqttClient(MqttConfig())
            client._connected.set()

            # Reset state
            METRICS.mqtt_publish_queue_depth.set(0)
            client._pending_publish_count = 0

            # Publish should fail
            with pytest.raises(MqttClientError):
                client.publish("test/topic", b"payload", qos=1)

            # Pending count should be back to 0 (incremented then decremented)
            assert client.get_pending_publish_count() == 0
            assert METRICS.mqtt_publish_queue_depth._value.get() == 0

    def test_get_pending_publish_count_method(self) -> None:
        """Verify the get_pending_publish_count helper method works correctly."""
        with patch("aas_uns_bridge.mqtt.client.mqtt.Client") as mock_client_cls:
            mock_instance = mock_client_cls.return_value
            mock_instance.is_connected.return_value = True

            mock_result = MagicMock()
            mock_result.rc = 0
            mock_instance.publish.return_value = mock_result

            client = MqttClient(MqttConfig())
            client._connected.set()

            # Reset state
            client._pending_publish_count = 0

            assert client.get_pending_publish_count() == 0

            client.publish("test/topic", b"payload")
            assert client.get_pending_publish_count() == 1

            client.publish("test/topic2", b"payload2")
            assert client.get_pending_publish_count() == 2
