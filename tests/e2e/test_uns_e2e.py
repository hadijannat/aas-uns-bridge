"""End-to-end tests for UNS retained plane."""

import json
import time
from pathlib import Path

import paho.mqtt.client as mqtt
import pytest

from aas_uns_bridge.aas.loader import load_json
from aas_uns_bridge.aas.traversal import flatten_submodel, iter_submodels
from aas_uns_bridge.config import MqttConfig, UnsConfig
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.mapping.isa95 import Isa95Mapper
from aas_uns_bridge.mqtt.client import MqttClient
from aas_uns_bridge.publishers.uns_retained import UnsRetainedPublisher


@pytest.mark.e2e
class TestUnsTopicCorrectness:
    """Tests for UNS topic structure correctness."""

    def test_topic_follows_isa95_hierarchy(
        self,
        mqtt_config: MqttConfig,
        uns_config: UnsConfig,
        sample_robot_json: Path,
        test_mappings_path: Path,
        clean_broker: None,
    ) -> None:
        """Verify topics match ISA-95 hierarchy from mappings."""
        # Load AAS and mappings
        object_store = load_json(sample_robot_json)
        mapper = Isa95Mapper.from_yaml(test_mappings_path)

        # Collect messages
        received: list[str] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            received.append(msg.topic)

        # Subscribe to all topics under expected enterprise
        sub_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"verify-{time.time()}",
        )
        sub_client.on_message = on_msg
        sub_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        sub_client.subscribe("AcmeCorp/#", qos=1)
        sub_client.loop_start()
        time.sleep(0.5)

        # Publish metrics
        client = MqttClient(mqtt_config)
        publisher = UnsRetainedPublisher(client, uns_config)
        client.connect(timeout=10)

        for submodel, asset_id in iter_submodels(object_store):
            if asset_id is None:
                continue
            metrics = flatten_submodel(submodel, str(sample_robot_json))
            identity = mapper.get_identity(asset_id)

            for metric in metrics:
                topic = f"{identity.topic_prefix()}/context/{submodel.id_short}/{metric.path}"
                publisher.publish_metric(topic, metric)

        time.sleep(1)

        # Verify topic structure
        sub_client.loop_stop()
        sub_client.disconnect()
        client.disconnect()

        assert len(received) > 0, "Should have received messages"

        # All topics should start with the mapped enterprise
        for topic in received:
            assert topic.startswith("AcmeCorp/PlantA/"), (
                f"Topic should follow ISA-95 hierarchy: {topic}"
            )
            assert "/context/" in topic, f"Topic should contain context segment: {topic}"

    def test_payload_schema_validation(
        self,
        mqtt_config: MqttConfig,
        uns_config: UnsConfig,
        sample_sensor_json: Path,
        test_mappings_path: Path,
        clean_broker: None,
    ) -> None:
        """Verify payload contains required schema fields."""
        object_store = load_json(sample_sensor_json)
        mapper = Isa95Mapper.from_yaml(test_mappings_path)

        received_payloads: list[tuple[str, bytes]] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            received_payloads.append((msg.topic, msg.payload))

        sub_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"schema-verify-{time.time()}",
        )
        sub_client.on_message = on_msg
        sub_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        sub_client.subscribe("AcmeCorp/#", qos=1)
        sub_client.loop_start()
        time.sleep(0.5)

        client = MqttClient(mqtt_config)
        publisher = UnsRetainedPublisher(client, uns_config)
        client.connect(timeout=10)

        for submodel, asset_id in iter_submodels(object_store):
            if asset_id is None:
                continue
            metrics = flatten_submodel(submodel, str(sample_sensor_json))
            identity = mapper.get_identity(asset_id)

            for metric in metrics:
                topic = f"{identity.topic_prefix()}/context/{submodel.id_short}/{metric.path}"
                publisher.publish_metric(topic, metric)

        time.sleep(1)
        sub_client.loop_stop()
        sub_client.disconnect()
        client.disconnect()

        assert len(received_payloads) > 0, "Should have received messages"

        # Verify payload schema
        for topic, payload in received_payloads:
            data = json.loads(payload)

            # Required fields
            assert "value" in data, f"Payload must have 'value': {topic}"
            assert "timestamp" in data, f"Payload must have 'timestamp': {topic}"
            assert "source" in data or "aasSource" in data, f"Payload must have source: {topic}"

            # Optional but should be present if available
            # semanticId is optional

            # Timestamp should be valid
            assert isinstance(data["timestamp"], int), f"Timestamp must be int: {topic}"
            assert data["timestamp"] > 0, f"Timestamp must be positive: {topic}"


@pytest.mark.e2e
class TestUnsLateSubscriber:
    """Tests for UNS retained message behavior with late subscribers."""

    def test_late_subscriber_receives_retained_messages(
        self,
        mqtt_config: MqttConfig,
        uns_config: UnsConfig,
        clean_broker: None,
    ) -> None:
        """Late subscriber should receive all retained messages."""
        test_topic = (
            f"TestEnterprise/TestSite/TestArea/TestLine/Asset/context/Data/Value-{time.time()}"
        )
        test_metric = ContextMetric(
            path="Data.Value",
            value="TestValue",
            aas_type="Property",
            value_type="xs:string",
            semantic_id="0173-1#02-TEST#001",
        )

        # Publish retained message
        pub_client = MqttClient(mqtt_config)
        publisher = UnsRetainedPublisher(pub_client, uns_config)
        pub_client.connect(timeout=10)
        publisher.publish_metric(test_topic, test_metric)
        time.sleep(0.5)
        pub_client.disconnect()

        # Subscribe AFTER publish (late subscriber)
        received: list[tuple[str, bytes, bool]] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            received.append((msg.topic, msg.payload, msg.retain))

        late_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"late-sub-{time.time()}",
        )
        late_client.on_message = on_msg
        late_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        late_client.subscribe(test_topic, qos=1)
        late_client.loop_start()

        # Wait for retained message delivery
        time.sleep(1)
        late_client.loop_stop()
        late_client.disconnect()

        # Should receive retained message
        assert len(received) == 1, "Late subscriber should receive retained message"
        topic, payload, is_retained = received[0]
        assert topic == test_topic
        assert is_retained, "Message should be marked as retained"

        data = json.loads(payload)
        assert data["value"] == "TestValue"

    def test_late_subscriber_receives_multiple_retained(
        self,
        mqtt_config: MqttConfig,
        uns_config: UnsConfig,
        clean_broker: None,
    ) -> None:
        """Late subscriber should receive all retained messages on wildcard subscribe."""
        base_topic = f"TestEnterprise/TestSite/TestArea/TestLine/MultiAsset-{int(time.time())}"
        num_messages = 5

        # Publish multiple retained messages
        pub_client = MqttClient(mqtt_config)
        publisher = UnsRetainedPublisher(pub_client, uns_config)
        pub_client.connect(timeout=10)

        for i in range(num_messages):
            topic = f"{base_topic}/context/Data/Property{i}"
            metric = ContextMetric(
                path=f"Data.Property{i}",
                value=f"Value{i}",
                aas_type="Property",
                value_type="xs:string",
            )
            publisher.publish_metric(topic, metric)

        time.sleep(0.5)
        pub_client.disconnect()

        # Late subscriber with wildcard
        received: list[str] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            received.append(msg.topic)

        late_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"late-wild-{time.time()}",
        )
        late_client.on_message = on_msg
        late_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        late_client.subscribe(f"{base_topic}/#", qos=1)
        late_client.loop_start()

        time.sleep(1)
        late_client.loop_stop()
        late_client.disconnect()

        # Should receive all retained messages
        assert len(received) == num_messages, (
            f"Should receive {num_messages} retained messages, got {len(received)}"
        )


@pytest.mark.e2e
class TestUnsBrokerRestart:
    """Tests for UNS retained message durability across broker restart.

    Note: These tests require external broker restart capability
    and are marked as slow/manual.
    """

    @pytest.mark.slow
    def test_retained_survives_broker_restart(
        self,
        mqtt_config: MqttConfig,
        uns_config: UnsConfig,
        clean_broker: None,
    ) -> None:
        """Retained messages should survive broker restart.

        This test publishes a message, then expects the broker to be
        restarted externally. Manual verification step.
        """
        test_topic = f"TestEnterprise/BrokerRestart/context/Data/Persistent-{time.time()}"
        test_value = f"Persistent-{time.time()}"

        # Publish retained message
        pub_client = MqttClient(mqtt_config)
        publisher = UnsRetainedPublisher(pub_client, uns_config)
        pub_client.connect(timeout=10)
        publisher.publish_metric(
            test_topic,
            ContextMetric(
                path="Data.Persistent",
                value=test_value,
                aas_type="Property",
                value_type="xs:string",
            ),
        )
        time.sleep(0.5)
        pub_client.disconnect()

        # Verify message is retained (pre-restart)
        received: list[tuple[str, bytes]] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            received.append((msg.topic, msg.payload))

        verify_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"verify-persist-{time.time()}",
        )
        verify_client.on_message = on_msg
        verify_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        verify_client.subscribe(test_topic, qos=1)
        verify_client.loop_start()
        time.sleep(1)
        verify_client.loop_stop()
        verify_client.disconnect()

        assert len(received) == 1, "Message should be retained"
        data = json.loads(received[0][1])
        assert data["value"] == test_value, "Value should match"

        # Note: Broker restart would happen here externally
        # After restart, the same subscription should still receive the message
