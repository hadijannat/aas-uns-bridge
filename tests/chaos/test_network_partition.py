"""Chaos engineering tests for network partition scenarios.

Tests MQTT client and publisher behavior during network disconnections,
including publish failures, reconnection handling, and graceful degradation.
"""

from unittest.mock import MagicMock, patch

import pytest
from paho.mqtt.enums import MQTTErrorCode

from aas_uns_bridge.config import MqttConfig, UnsConfig
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.mqtt.client import MqttClient, MqttClientError
from aas_uns_bridge.publishers.uns_retained import UnsRetainedPublisher


@pytest.fixture
def mock_mqtt_config() -> MqttConfig:
    """Create a mock MQTT configuration for testing."""
    return MqttConfig(
        host="test-broker.local",
        port=1883,
        client_id="test-client",
        username="testuser",
        password=None,
        use_tls=False,
        reconnect_delay_min=0.1,
        reconnect_delay_max=1.0,
        keepalive=30,
    )


@pytest.fixture
def sample_metric() -> ContextMetric:
    """Create a sample metric for testing."""
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


@pytest.mark.chaos
class TestNetworkPartitionDuringPublish:
    """Tests for network partition scenarios during MQTT publish operations."""

    def test_publish_raises_when_disconnected(
        self,
        mock_mqtt_config: MqttConfig,
    ) -> None:
        """Verify publish raises MqttClientError when not connected."""
        with patch("aas_uns_bridge.mqtt.client.mqtt.Client"):
            client = MqttClient(mock_mqtt_config)
            # Ensure client is not connected
            assert not client.is_connected()

            with pytest.raises(MqttClientError) as exc_info:
                client.publish("test/topic", b"payload")

            assert "Not connected to broker" in str(exc_info.value)

    def test_publish_fails_mid_operation(
        self,
        mock_mqtt_config: MqttConfig,
    ) -> None:
        """Verify handling when connection drops during publish.

        Simulates the scenario where the broker becomes unreachable mid-operation
        by returning MQTT_ERR_NO_CONN from the publish call.
        """
        with patch("aas_uns_bridge.mqtt.client.mqtt.Client") as mock_client_cls:
            mock_paho = mock_client_cls.return_value

            client = MqttClient(mock_mqtt_config)
            # Simulate connected state
            client._connected.set()

            # Simulate publish failure due to connection loss
            mock_result = MagicMock()
            mock_result.rc = MQTTErrorCode.MQTT_ERR_NO_CONN
            mock_paho.publish.return_value = mock_result

            with pytest.raises(MqttClientError) as exc_info:
                client.publish("test/topic", b"payload")

            assert "Publish failed" in str(exc_info.value)

    def test_reconnect_after_disconnect(
        self,
        mock_mqtt_config: MqttConfig,
    ) -> None:
        """Verify client attempts reconnection after unexpected disconnect.

        Tests that the reconnect loop is triggered when an unexpected
        disconnection occurs (non-zero reason code).
        """
        with patch("aas_uns_bridge.mqtt.client.mqtt.Client") as mock_client_cls:
            mock_paho = mock_client_cls.return_value

            # Track if reconnect loop was started
            reconnect_started = False

            client = MqttClient(mock_mqtt_config)
            # Set up initial connected state
            client._connected.set()
            client._should_reconnect = True

            def mock_start_reconnect():
                nonlocal reconnect_started
                reconnect_started = True
                # Don't actually start the loop in tests

            client._start_reconnect_loop = mock_start_reconnect

            # Simulate unexpected disconnect with a failure reason code
            mock_reason_code = MagicMock()
            mock_reason_code.is_failure = True
            mock_reason_code.value = 1  # Non-zero indicates error

            mock_disconnect_flags = MagicMock()

            # Trigger the disconnect callback
            client._handle_disconnect(
                mock_paho,
                None,  # userdata
                mock_disconnect_flags,
                mock_reason_code,
                None,  # properties
            )

            # Verify reconnect was triggered
            assert reconnect_started
            # Verify connected state was cleared
            assert not client.is_connected()

    def test_no_reconnect_on_graceful_disconnect(
        self,
        mock_mqtt_config: MqttConfig,
    ) -> None:
        """Verify no reconnect attempt on graceful (expected) disconnect."""
        with patch("aas_uns_bridge.mqtt.client.mqtt.Client") as mock_client_cls:
            mock_paho = mock_client_cls.return_value

            reconnect_started = False

            client = MqttClient(mock_mqtt_config)
            client._connected.set()
            client._should_reconnect = True

            def mock_start_reconnect():
                nonlocal reconnect_started
                reconnect_started = True

            client._start_reconnect_loop = mock_start_reconnect

            # Simulate graceful disconnect (reason_code = 0, is_failure = False)
            mock_reason_code = MagicMock()
            mock_reason_code.is_failure = False
            mock_reason_code.value = 0

            mock_disconnect_flags = MagicMock()

            client._handle_disconnect(
                mock_paho,
                None,
                mock_disconnect_flags,
                mock_reason_code,
                None,
            )

            # Verify no reconnect was triggered
            assert not reconnect_started

    def test_publish_error_codes_propagated(
        self,
        mock_mqtt_config: MqttConfig,
    ) -> None:
        """Verify various MQTT error codes are properly propagated."""
        with patch("aas_uns_bridge.mqtt.client.mqtt.Client") as mock_client_cls:
            mock_paho = mock_client_cls.return_value

            client = MqttClient(mock_mqtt_config)
            client._connected.set()

            # Test various error codes
            error_codes = [
                MQTTErrorCode.MQTT_ERR_NO_CONN,
                MQTTErrorCode.MQTT_ERR_CONN_LOST,
                MQTTErrorCode.MQTT_ERR_PROTOCOL,
            ]

            for error_code in error_codes:
                mock_result = MagicMock()
                mock_result.rc = error_code
                mock_paho.publish.return_value = mock_result

                with pytest.raises(MqttClientError) as exc_info:
                    client.publish("test/topic", b"payload")

                assert "Publish failed" in str(exc_info.value)


@pytest.mark.chaos
class TestPublisherNetworkResilience:
    """Tests for UNS publisher behavior during network disconnections."""

    def test_uns_publisher_handles_disconnection(
        self,
        sample_metric: ContextMetric,
    ) -> None:
        """Verify UNS publisher behavior when MQTT disconnects.

        When the underlying MQTT client is disconnected, publish operations
        should raise MqttClientError which the publisher should propagate.
        """
        mock_mqtt_client = MagicMock(spec=MqttClient)

        # Configure mock to raise MqttClientError on publish
        mock_mqtt_client.publish.side_effect = MqttClientError("Not connected to broker")

        publisher = UnsRetainedPublisher(
            mqtt_client=mock_mqtt_client,
            config=UnsConfig(enabled=True),
        )

        # Attempting to publish should raise the error
        with pytest.raises(MqttClientError) as exc_info:
            publisher.publish_metric("test/topic", sample_metric)

        assert "Not connected" in str(exc_info.value)

    def test_uns_publisher_batch_handles_partial_failure(
        self,
        sample_metric: ContextMetric,
    ) -> None:
        """Verify batch publish handles partial failures gracefully.

        The batch publish method should continue publishing remaining metrics
        even if some fail, returning the count of successful publishes.
        """
        mock_mqtt_client = MagicMock(spec=MqttClient)

        # Configure mock to succeed on first call, fail on second
        call_count = 0

        def publish_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise MqttClientError("Connection lost mid-batch")

        mock_mqtt_client.publish.side_effect = publish_side_effect

        publisher = UnsRetainedPublisher(
            mqtt_client=mock_mqtt_client,
            config=UnsConfig(enabled=True),
        )

        # Create batch of metrics
        metrics = {
            "topic/1": sample_metric,
            "topic/2": sample_metric,
            "topic/3": sample_metric,
        }

        # Batch publish should succeed for 2 of 3 metrics
        count = publisher.publish_batch(metrics)

        # At least one should have succeeded before the failure
        # and one more after (batch continues on error)
        assert count == 2
        assert mock_mqtt_client.publish.call_count == 3

    def test_uns_publisher_disabled_ignores_disconnection(
        self,
        sample_metric: ContextMetric,
    ) -> None:
        """Verify disabled publisher ignores network issues."""
        mock_mqtt_client = MagicMock(spec=MqttClient)
        mock_mqtt_client.publish.side_effect = MqttClientError("Should not be called")

        publisher = UnsRetainedPublisher(
            mqtt_client=mock_mqtt_client,
            config=UnsConfig(enabled=False),
        )

        # Should not raise - publish is skipped when disabled
        publisher.publish_metric("test/topic", sample_metric)

        # Verify publish was never called
        mock_mqtt_client.publish.assert_not_called()

    def test_uns_publisher_increments_counter_on_success_only(
        self,
        sample_metric: ContextMetric,
    ) -> None:
        """Verify published count only increments on successful publish."""
        mock_mqtt_client = MagicMock(spec=MqttClient)

        publisher = UnsRetainedPublisher(
            mqtt_client=mock_mqtt_client,
            config=UnsConfig(enabled=True),
        )

        initial_count = publisher.published_count

        # Successful publish
        publisher.publish_metric("test/topic", sample_metric)
        assert publisher.published_count == initial_count + 1

        # Failed publish
        mock_mqtt_client.publish.side_effect = MqttClientError("Disconnected")

        with pytest.raises(MqttClientError):
            publisher.publish_metric("test/topic", sample_metric)

        # Count should not have incremented on failure
        assert publisher.published_count == initial_count + 1


@pytest.mark.chaos
class TestConnectionStateTransitions:
    """Tests for connection state transitions during network partitions."""

    def test_disconnect_callback_invoked(
        self,
        mock_mqtt_config: MqttConfig,
    ) -> None:
        """Verify on_disconnect callback is invoked on disconnection."""
        with patch("aas_uns_bridge.mqtt.client.mqtt.Client"):
            disconnect_callback_invoked = False

            def on_disconnect():
                nonlocal disconnect_callback_invoked
                disconnect_callback_invoked = True

            client = MqttClient(
                mock_mqtt_config,
                on_disconnect=on_disconnect,
            )
            client._connected.set()

            # Simulate disconnection
            mock_reason_code = MagicMock()
            mock_reason_code.is_failure = True

            client._handle_disconnect(
                MagicMock(),
                None,
                MagicMock(),
                mock_reason_code,
                None,
            )

            assert disconnect_callback_invoked

    def test_connect_callback_invoked_on_reconnect(
        self,
        mock_mqtt_config: MqttConfig,
    ) -> None:
        """Verify on_connect callback is invoked on successful reconnection."""
        with patch("aas_uns_bridge.mqtt.client.mqtt.Client"):
            connect_callback_invoked = False

            def on_connect():
                nonlocal connect_callback_invoked
                connect_callback_invoked = True

            client = MqttClient(
                mock_mqtt_config,
                on_connect=on_connect,
            )

            # Simulate successful connection with MQTTv5 ReasonCode
            mock_reason_code = MagicMock()
            mock_reason_code.value = 0
            mock_reason_code.is_failure = False

            mock_flags = MagicMock()

            client._handle_connect(
                MagicMock(),
                None,
                mock_flags,
                mock_reason_code,
                None,
            )

            assert connect_callback_invoked
            assert client.is_connected()

    def test_subscriptions_restored_on_reconnect(
        self,
        mock_mqtt_config: MqttConfig,
    ) -> None:
        """Verify subscriptions are restored after reconnection."""
        with patch("aas_uns_bridge.mqtt.client.mqtt.Client") as mock_client_cls:
            mock_paho = mock_client_cls.return_value
            mock_paho.subscribe.return_value = (MQTTErrorCode.MQTT_ERR_SUCCESS, None)

            client = MqttClient(mock_mqtt_config)

            # Add subscriptions while disconnected
            client._subscriptions = {
                "topic/one": lambda t, p: None,
                "topic/two": lambda t, p: None,
            }

            # Simulate reconnection
            mock_reason_code = MagicMock()
            mock_reason_code.value = 0
            mock_reason_code.is_failure = False

            client._handle_connect(
                mock_paho,
                None,
                MagicMock(),
                mock_reason_code,
                None,
            )

            # Verify all subscriptions were restored
            assert mock_paho.subscribe.call_count == 2
            subscribed_topics = [call[0][0] for call in mock_paho.subscribe.call_args_list]
            assert "topic/one" in subscribed_topics
            assert "topic/two" in subscribed_topics
