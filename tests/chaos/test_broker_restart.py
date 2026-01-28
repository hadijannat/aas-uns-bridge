"""Chaos engineering tests for broker restart recovery scenarios.

Tests MQTT client behavior during broker restarts, including subscription
restoration, LWT configuration, connection state tracking, and Sparkplug
rebirth handling after reconnection.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from paho.mqtt.enums import MQTTErrorCode

from aas_uns_bridge.config import MqttConfig
from aas_uns_bridge.mqtt.client import MqttClient
from aas_uns_bridge.state.birth_cache import BirthCache


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


@pytest.mark.chaos
class TestBrokerRestartRecovery:
    """Tests for broker restart recovery scenarios."""

    def test_resubscription_after_reconnect(
        self,
        mock_mqtt_config: MqttConfig,
    ) -> None:
        """Verify subscriptions are restored after reconnection.

        When the broker restarts and the client reconnects, all previously
        registered subscriptions should be automatically re-established by
        calling subscribe on the paho client for each stored subscription.
        """
        with patch("aas_uns_bridge.mqtt.client.mqtt.Client") as mock_client_cls:
            mock_paho = mock_client_cls.return_value
            mock_paho.subscribe.return_value = (MQTTErrorCode.MQTT_ERR_SUCCESS, None)

            client = MqttClient(mock_mqtt_config)

            # Register subscriptions while disconnected (stored internally)
            subscriptions = {
                "uns/enterprise/site/area/+/context/#": lambda t, p: None,
                "spBv1.0/AAS/NCMD/Bridge": lambda t, p: None,
                "spBv1.0/AAS/DCMD/Bridge/+": lambda t, p: None,
            }

            for topic, callback in subscriptions.items():
                client._subscriptions[topic] = callback

            # Simulate broker restart and reconnection
            mock_reason_code = MagicMock()
            mock_reason_code.value = 0
            mock_reason_code.is_failure = False

            mock_flags = MagicMock()

            # Trigger the connect callback (simulating reconnection after broker restart)
            client._handle_connect(
                mock_paho,
                None,  # userdata
                mock_flags,
                mock_reason_code,
                None,  # properties
            )

            # Verify all subscriptions were restored
            assert mock_paho.subscribe.call_count == len(subscriptions)

            # Verify each subscription topic was resubscribed
            subscribed_topics = [call[0][0] for call in mock_paho.subscribe.call_args_list]
            for topic in subscriptions:
                assert topic in subscribed_topics, f"Subscription for {topic} not restored"

    def test_lwt_published_on_unclean_disconnect(
        self,
        mock_mqtt_config: MqttConfig,
    ) -> None:
        """Verify LWT (Last Will and Testament) is configured via set_lwt method.

        The LWT message should be configured before connecting so that the broker
        will publish it on behalf of the client if an unclean disconnect occurs
        (e.g., broker restart without graceful client disconnect).
        """
        with patch("aas_uns_bridge.mqtt.client.mqtt.Client") as mock_client_cls:
            mock_paho = mock_client_cls.return_value

            client = MqttClient(mock_mqtt_config)

            # Configure LWT before connecting
            lwt_topic = "spBv1.0/AAS/NDEATH/Bridge"
            lwt_payload = b"\x00\x01\x02\x03"  # Simulated Sparkplug NDEATH payload
            lwt_qos = 1
            lwt_retain = False

            client.set_lwt(lwt_topic, lwt_payload, qos=lwt_qos, retain=lwt_retain)

            # Verify will_set was called on the paho client with correct parameters
            mock_paho.will_set.assert_called_once_with(
                lwt_topic,
                lwt_payload,
                lwt_qos,
                lwt_retain,
            )

    def test_connection_state_tracking(
        self,
        mock_mqtt_config: MqttConfig,
    ) -> None:
        """Verify is_connected() accurately tracks state through connect/disconnect cycles.

        Connection state should be False initially, True after successful connect,
        False after disconnect, and True again after reconnect.
        """
        with patch("aas_uns_bridge.mqtt.client.mqtt.Client") as mock_client_cls:
            mock_paho = mock_client_cls.return_value
            mock_paho.subscribe.return_value = (MQTTErrorCode.MQTT_ERR_SUCCESS, None)

            client = MqttClient(mock_mqtt_config)

            # Initially disconnected
            assert not client.is_connected(), "Client should start disconnected"

            # Simulate successful connection
            mock_reason_code_success = MagicMock()
            mock_reason_code_success.value = 0
            mock_reason_code_success.is_failure = False

            client._handle_connect(
                mock_paho,
                None,
                MagicMock(),
                mock_reason_code_success,
                None,
            )

            assert client.is_connected(), "Client should be connected after connect callback"

            # Simulate unexpected disconnect (broker restart)
            mock_reason_code_failure = MagicMock()
            mock_reason_code_failure.value = 1  # Non-zero indicates error
            mock_reason_code_failure.is_failure = True

            mock_disconnect_flags = MagicMock()

            # Prevent actual reconnect loop from starting
            client._should_reconnect = False

            client._handle_disconnect(
                mock_paho,
                None,
                mock_disconnect_flags,
                mock_reason_code_failure,
                None,
            )

            assert not client.is_connected(), (
                "Client should be disconnected after disconnect callback"
            )

            # Simulate reconnection after broker comes back
            client._should_reconnect = True

            client._handle_connect(
                mock_paho,
                None,
                MagicMock(),
                mock_reason_code_success,
                None,
            )

            assert client.is_connected(), "Client should be connected after reconnect"


@pytest.mark.chaos
class TestSparkplugRebirth:
    """Tests for Sparkplug rebirth handling after broker restart."""

    def test_nbirth_required_after_reconnect(self) -> None:
        """Verify BirthCache can store and retrieve NBIRTH for republishing after reconnect.

        After a broker restart, the Sparkplug specification requires that the edge node
        republish its NBIRTH message. The BirthCache should allow storing and retrieving
        the NBIRTH payload so it can be republished without re-traversing AAS content.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "birth_cache.db"
            cache = BirthCache(db_path)

            # Store NBIRTH payload (as would happen during initial publication)
            nbirth_topic = "spBv1.0/AAS/NBIRTH/Bridge"
            nbirth_payload = b"\x08\x00\x12\x0atest_metric"  # Simulated protobuf bytes

            cache.store_nbirth(nbirth_topic, nbirth_payload)

            # Simulate broker restart - retrieve cached NBIRTH for republishing
            result = cache.get_nbirth()

            assert result is not None, "NBIRTH should be cached"
            retrieved_topic, retrieved_payload = result
            assert retrieved_topic == nbirth_topic, "NBIRTH topic should match"
            assert retrieved_payload == nbirth_payload, "NBIRTH payload should match"

            # Verify DBIRTH storage and retrieval also works for device-level births
            device_id = "urn:example:aas:pump-001"
            dbirth_topic = f"spBv1.0/AAS/DBIRTH/Bridge/{device_id}"
            dbirth_payload = b"\x08\x01\x12\x0cdevice_data"

            cache.store_dbirth(device_id, dbirth_topic, dbirth_payload)

            dbirth_result = cache.get_dbirth(device_id)

            assert dbirth_result is not None, "DBIRTH should be cached"
            retrieved_dbirth_topic, retrieved_dbirth_payload = dbirth_result
            assert retrieved_dbirth_topic == dbirth_topic, "DBIRTH topic should match"
            assert retrieved_dbirth_payload == dbirth_payload, "DBIRTH payload should match"

            # Verify all device IDs can be enumerated for bulk rebirth
            device_ids = cache.get_all_dbirth_device_ids()
            assert device_id in device_ids, "Device ID should be in cached device list"
