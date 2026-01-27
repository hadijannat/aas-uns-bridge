"""End-to-end tests for dual plane (UNS + Sparkplug) consistency."""

import time
from pathlib import Path

import paho.mqtt.client as mqtt
import pytest

from aas_uns_bridge.aas.loader import load_json
from aas_uns_bridge.aas.traversal import flatten_submodel, iter_submodels
from aas_uns_bridge.config import MqttConfig, SparkplugConfig, UnsConfig
from aas_uns_bridge.mapping.isa95 import ISA95Mapper, MappingConfig
from aas_uns_bridge.mqtt.client import MqttClient
from aas_uns_bridge.publishers.sparkplug import SPARKPLUG_NAMESPACE, SparkplugPublisher
from aas_uns_bridge.publishers.uns_retained import UnsRetainedPublisher
from aas_uns_bridge.state.alias_db import AliasDB


@pytest.mark.e2e
class TestDualPlaneConsistency:
    """Tests for consistency between UNS and Sparkplug planes."""

    def test_same_aas_produces_both_planes(
        self,
        mqtt_config: MqttConfig,
        uns_config: UnsConfig,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        sample_robot_json: Path,
        test_mappings_path: Path,
        clean_broker: None,
    ) -> None:
        """Same AAS file should produce both UNS and Sparkplug messages."""
        uns_topics: list[str] = []
        sparkplug_topics: list[str] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            if msg.topic.startswith("spBv1.0"):
                sparkplug_topics.append(msg.topic)
            elif msg.topic.startswith("AcmeCorp"):
                uns_topics.append(msg.topic)

        # Subscribe to both planes
        sub_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"dual-sub-{time.time()}",
        )
        sub_client.on_message = on_msg
        sub_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        sub_client.subscribe("AcmeCorp/#", qos=1)
        sub_client.subscribe(f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/#", qos=0)
        sub_client.loop_start()
        time.sleep(0.5)

        # Load AAS
        object_store = load_json(sample_robot_json)
        mapper = ISA95Mapper(MappingConfig.from_yaml(test_mappings_path))

        # Create publishers
        mqtt_client = MqttClient(mqtt_config)
        uns_publisher = UnsRetainedPublisher(mqtt_client, uns_config)
        sparkplug_publisher = SparkplugPublisher(mqtt_client, sparkplug_config, alias_db)

        mqtt_client.connect(timeout=10)
        sparkplug_publisher.publish_nbirth()

        # Publish to both planes
        for submodel, asset_id in iter_submodels(object_store):
            if asset_id is None:
                continue

            metrics = flatten_submodel(submodel, str(sample_robot_json))
            identity = mapper.get_identity(asset_id)
            device_id = asset_id.split("/")[-1] if "/" in asset_id else asset_id

            # UNS plane
            for metric in metrics:
                topic = f"{identity.topic_prefix()}/context/{submodel.id_short}/{metric.path}"
                uns_publisher.publish_metric(topic, metric)

            # Sparkplug plane
            sparkplug_publisher.publish_dbirth(device_id, metrics)

        time.sleep(1)
        sub_client.loop_stop()
        sub_client.disconnect()
        mqtt_client.disconnect()

        # Verify both planes received messages
        assert len(uns_topics) > 0, "Should receive UNS messages"
        assert len(sparkplug_topics) > 0, "Should receive Sparkplug messages"

        # Verify Sparkplug has DBIRTH
        dbirth_count = sum(1 for t in sparkplug_topics if "DBIRTH" in t)
        assert dbirth_count > 0, "Should receive DBIRTH messages"

    def test_metric_count_matches_across_planes(
        self,
        mqtt_config: MqttConfig,
        uns_config: UnsConfig,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        sample_sensor_json: Path,
        test_mappings_path: Path,
        clean_broker: None,
    ) -> None:
        """UNS topic count should match Sparkplug metric count in DBIRTH."""
        uns_topics: list[str] = []
        dbirth_payloads: list[bytes] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            if "DBIRTH" in msg.topic:
                dbirth_payloads.append(msg.payload)
            elif msg.topic.startswith("AcmeCorp") and "/context/" in msg.topic:
                uns_topics.append(msg.topic)

        sub_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"count-sub-{time.time()}",
        )
        sub_client.on_message = on_msg
        sub_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        sub_client.subscribe("AcmeCorp/#", qos=1)
        sub_client.subscribe(f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/#", qos=0)
        sub_client.loop_start()
        time.sleep(0.5)

        # Load AAS
        object_store = load_json(sample_sensor_json)
        mapper = ISA95Mapper(MappingConfig.from_yaml(test_mappings_path))

        mqtt_client = MqttClient(mqtt_config)
        uns_publisher = UnsRetainedPublisher(mqtt_client, uns_config)
        sparkplug_publisher = SparkplugPublisher(mqtt_client, sparkplug_config, alias_db)

        mqtt_client.connect(timeout=10)
        sparkplug_publisher.publish_nbirth()

        expected_metric_count = 0

        for submodel, asset_id in iter_submodels(object_store):
            if asset_id is None:
                continue

            metrics = flatten_submodel(submodel, str(sample_sensor_json))
            expected_metric_count += len(metrics)
            identity = mapper.get_identity(asset_id)
            device_id = asset_id.split("/")[-1] if "/" in asset_id else asset_id

            # UNS plane
            for metric in metrics:
                topic = f"{identity.topic_prefix()}/context/{submodel.id_short}/{metric.path}"
                uns_publisher.publish_metric(topic, metric)

            # Sparkplug plane
            sparkplug_publisher.publish_dbirth(device_id, metrics)

        time.sleep(1)
        sub_client.loop_stop()
        sub_client.disconnect()
        mqtt_client.disconnect()

        # Verify UNS topic count matches expected
        assert len(uns_topics) == expected_metric_count, (
            f"UNS topics ({len(uns_topics)}) should match metric count ({expected_metric_count})"
        )

        # Verify Sparkplug DBIRTH metric count
        if dbirth_payloads:
            try:
                from aas_uns_bridge.proto import sparkplug_b_pb2 as spb

                total_sparkplug_metrics = 0
                for payload_bytes in dbirth_payloads:
                    payload = spb.Payload()
                    payload.ParseFromString(payload_bytes)
                    total_sparkplug_metrics += len(payload.metrics)

                assert total_sparkplug_metrics == expected_metric_count, (
                    f"Sparkplug metrics ({total_sparkplug_metrics}) should match "
                    f"expected count ({expected_metric_count})"
                )
            except ImportError:
                # Can't verify Sparkplug metric count without protobuf
                pass


@pytest.mark.e2e
class TestDualPlaneIsolation:
    """Tests for proper isolation between UNS and Sparkplug planes."""

    def test_uns_retained_sparkplug_not(
        self,
        mqtt_config: MqttConfig,
        uns_config: UnsConfig,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        clean_broker: None,
    ) -> None:
        """UNS messages should be retained, Sparkplug should not."""
        from aas_uns_bridge.domain.models import ContextMetric

        uns_topic = f"TestEnterprise/TestSite/context/Data/RetainTest-{time.time()}"
        metric = ContextMetric(
            path="Data.RetainTest",
            value="test_value",
            aas_type="Property",
            value_type="xs:string",
        )

        mqtt_client = MqttClient(mqtt_config)
        uns_publisher = UnsRetainedPublisher(mqtt_client, uns_config)
        sparkplug_publisher = SparkplugPublisher(mqtt_client, sparkplug_config, alias_db)

        mqtt_client.connect(timeout=10)

        # Publish to both planes
        uns_publisher.publish_metric(uns_topic, metric)
        sparkplug_publisher.publish_nbirth()
        sparkplug_publisher.publish_dbirth("TestDevice", [metric])

        time.sleep(0.5)
        mqtt_client.disconnect()

        # Late subscriber
        uns_received: list[tuple[str, bool]] = []
        sparkplug_received: list[tuple[str, bool]] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            if msg.topic.startswith("spBv1.0"):
                sparkplug_received.append((msg.topic, msg.retain))
            else:
                uns_received.append((msg.topic, msg.retain))

        late_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"late-isolation-{time.time()}",
        )
        late_client.on_message = on_msg
        late_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        late_client.subscribe(uns_topic, qos=1)
        late_client.subscribe(f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/#", qos=0)
        late_client.loop_start()

        time.sleep(1)
        late_client.loop_stop()
        late_client.disconnect()

        # UNS should be received (retained)
        assert len(uns_received) == 1, "UNS message should be retained"
        assert uns_received[0][1] is True, "UNS message should have retain flag"

        # Sparkplug should NOT be received (not retained)
        assert len(sparkplug_received) == 0, "Sparkplug messages should not be retained"

    def test_planes_independent_of_each_other(
        self,
        mqtt_config: MqttConfig,
        uns_config: UnsConfig,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        clean_broker: None,
    ) -> None:
        """Publishing to one plane should not affect the other."""
        from aas_uns_bridge.domain.models import ContextMetric

        uns_only_topic = f"TestEnterprise/UnsOnly/context/Data/Value-{time.time()}"
        metric = ContextMetric(
            path="Data.Value",
            value="uns_only",
            aas_type="Property",
            value_type="xs:string",
        )

        uns_messages: list[str] = []
        sparkplug_messages: list[str] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            if msg.topic.startswith("spBv1.0"):
                sparkplug_messages.append(msg.topic)
            else:
                uns_messages.append(msg.topic)

        sub_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"indep-sub-{time.time()}",
        )
        sub_client.on_message = on_msg
        sub_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        sub_client.subscribe("TestEnterprise/#", qos=1)
        sub_client.subscribe(f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/#", qos=0)
        sub_client.loop_start()
        time.sleep(0.5)

        # Publish UNS only (no Sparkplug)
        mqtt_client = MqttClient(mqtt_config)
        uns_publisher = UnsRetainedPublisher(mqtt_client, uns_config)
        mqtt_client.connect(timeout=10)
        uns_publisher.publish_metric(uns_only_topic, metric)

        time.sleep(1)
        sub_client.loop_stop()
        sub_client.disconnect()
        mqtt_client.disconnect()

        # Should receive UNS message
        assert len(uns_messages) == 1, "Should receive UNS message"

        # Should NOT receive any Sparkplug messages
        assert len(sparkplug_messages) == 0, "Should not receive Sparkplug without explicit publish"
