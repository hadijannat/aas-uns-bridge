"""Integration tests for UNS retained publishing."""

import json
import threading
import time

import pytest

from aas_uns_bridge.config import MqttConfig, UnsConfig
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.mqtt.client import MqttClient
from aas_uns_bridge.publishers.uns_retained import UnsRetainedPublisher


@pytest.fixture
def mqtt_config() -> MqttConfig:
    """Create MQTT config for test broker."""
    return MqttConfig(
        host="localhost",
        port=1883,
        client_id=f"test-{time.time()}",
    )


@pytest.fixture
def uns_config() -> UnsConfig:
    """Create UNS config for testing."""
    return UnsConfig(
        enabled=True,
        root_topic="test",
        qos=1,
        retain=True,
    )


@pytest.fixture
def sample_metric() -> ContextMetric:
    """Create a sample metric for testing."""
    return ContextMetric(
        path="TechnicalData.GeneralInfo.Manufacturer",
        value="Test Corp",
        aas_type="Property",
        value_type="xs:string",
        semantic_id="0173-1#02-AAO677#002",
        unit=None,
        aas_source="test.aasx",
        timestamp_ms=int(time.time() * 1000),
    )


class TestUnsRetainedPublishIntegration:
    """Integration tests for UNS retained publishing.

    These tests require a running MQTT broker on localhost:1883.
    Skip with: pytest -m "not integration"
    """

    @pytest.mark.integration
    def test_published_message_received_by_late_subscriber(
        self,
        mqtt_config: MqttConfig,
        uns_config: UnsConfig,
        sample_metric: ContextMetric,
    ) -> None:
        """Test that retained messages are received by subscribers joining later."""
        topic = "test/AcmeCorp/PlantA/context/TechnicalData/Manufacturer"
        received_messages: list[tuple[str, bytes]] = []
        receive_event = threading.Event()

        # Publisher client
        pub_client = MqttClient(mqtt_config)
        publisher = UnsRetainedPublisher(pub_client, uns_config)

        try:
            pub_client.connect(timeout=10)

            # Publish retained message
            publisher.publish_metric(topic, sample_metric)

            # Wait for message to be stored by broker
            time.sleep(0.5)

            # Disconnect publisher
            pub_client.disconnect()

            # Create new subscriber (late joiner)
            sub_config = MqttConfig(
                host=mqtt_config.host,
                port=mqtt_config.port,
                client_id=f"sub-{time.time()}",
            )
            sub_client = MqttClient(sub_config)

            def on_message(t: str, payload: bytes) -> None:
                received_messages.append((t, payload))
                receive_event.set()

            sub_client.connect(timeout=10)
            sub_client.subscribe(topic, on_message)

            # Wait for retained message
            assert receive_event.wait(timeout=5), "Did not receive retained message"

            # Verify message content
            assert len(received_messages) == 1
            recv_topic, recv_payload = received_messages[0]
            assert recv_topic == topic

            payload_data = json.loads(recv_payload)
            assert payload_data["value"] == "Test Corp"
            assert payload_data["semanticId"] == "0173-1#02-AAO677#002"

        finally:
            pub_client.disconnect()
            if "sub_client" in locals():
                sub_client.disconnect()

    @pytest.mark.integration
    def test_batch_publish(
        self,
        mqtt_config: MqttConfig,
        uns_config: UnsConfig,
    ) -> None:
        """Test publishing multiple metrics in batch."""
        metrics = {
            "test/batch/metric1": ContextMetric(
                path="Data.Metric1",
                value=42,
                aas_type="Property",
                value_type="xs:int",
            ),
            "test/batch/metric2": ContextMetric(
                path="Data.Metric2",
                value=3.14,
                aas_type="Property",
                value_type="xs:double",
            ),
        }

        client = MqttClient(mqtt_config)
        publisher = UnsRetainedPublisher(client, uns_config)

        try:
            client.connect(timeout=10)

            count = publisher.publish_batch(metrics)

            assert count == 2
            assert publisher.published_count == 2

        finally:
            client.disconnect()
