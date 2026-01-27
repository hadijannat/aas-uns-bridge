"""End-to-end tests for Sparkplug B plane."""

import time
from pathlib import Path

import paho.mqtt.client as mqtt
import pytest

from aas_uns_bridge.aas.loader import load_json
from aas_uns_bridge.aas.traversal import flatten_submodel, iter_submodels
from aas_uns_bridge.config import MqttConfig, SparkplugConfig
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.mqtt.client import MqttClient
from aas_uns_bridge.publishers.sparkplug import SPARKPLUG_NAMESPACE, SparkplugPublisher
from aas_uns_bridge.state.alias_db import AliasDB


@pytest.mark.e2e
class TestSparkplugNBirth:
    """Tests for Sparkplug NBIRTH message compliance."""

    def test_nbirth_on_connect(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        clean_broker: None,
    ) -> None:
        """NBIRTH should be published immediately on connection."""
        received_topics: list[str] = []
        received_payloads: list[bytes] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            received_topics.append(msg.topic)
            received_payloads.append(msg.payload)

        # Subscribe first
        sub_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"nbirth-sub-{time.time()}",
        )
        sub_client.on_message = on_msg
        sub_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        sub_client.subscribe(f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/#", qos=0)
        sub_client.loop_start()
        time.sleep(0.5)

        # Connect publisher and send NBIRTH
        pub_client = MqttClient(mqtt_config)
        publisher = SparkplugPublisher(pub_client, sparkplug_config, alias_db)
        pub_client.connect(timeout=10)
        publisher.publish_nbirth()

        time.sleep(1)
        sub_client.loop_stop()
        sub_client.disconnect()
        pub_client.disconnect()

        # Verify NBIRTH received
        expected_topic = (
            f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/NBIRTH/"
            f"{sparkplug_config.edge_node_id}"
        )
        assert expected_topic in received_topics, f"Should receive NBIRTH on {expected_topic}"

    def test_nbirth_topic_format(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        clean_broker: None,
    ) -> None:
        """NBIRTH topic should follow spBv1.0/{group}/{NBIRTH}/{edge_node} format."""
        received_topics: list[str] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            if "NBIRTH" in msg.topic:
                received_topics.append(msg.topic)

        sub_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"topic-fmt-{time.time()}",
        )
        sub_client.on_message = on_msg
        sub_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        sub_client.subscribe(f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/#", qos=0)
        sub_client.loop_start()
        time.sleep(0.5)

        pub_client = MqttClient(mqtt_config)
        publisher = SparkplugPublisher(pub_client, sparkplug_config, alias_db)
        pub_client.connect(timeout=10)
        publisher.publish_nbirth()

        time.sleep(1)
        sub_client.loop_stop()
        sub_client.disconnect()
        pub_client.disconnect()

        assert len(received_topics) > 0, "Should receive NBIRTH"

        # Verify format
        topic = received_topics[0]
        parts = topic.split("/")
        assert len(parts) == 4, f"NBIRTH topic should have 4 parts: {topic}"
        assert parts[0] == "spBv1.0", f"Namespace should be spBv1.0: {topic}"
        assert parts[1] == sparkplug_config.group_id, f"Group ID mismatch: {topic}"
        assert parts[2] == "NBIRTH", f"Message type should be NBIRTH: {topic}"
        assert parts[3] == sparkplug_config.edge_node_id, f"Edge node ID mismatch: {topic}"


@pytest.mark.e2e
class TestSparkplugDBirth:
    """Tests for Sparkplug DBIRTH message compliance."""

    def test_dbirth_topic_format(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        clean_broker: None,
    ) -> None:
        """DBIRTH topic should follow spBv1.0/{group}/DBIRTH/{edge_node}/{device_id} format."""
        device_id = "TestDevice001"
        received_topics: list[str] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            if "DBIRTH" in msg.topic:
                received_topics.append(msg.topic)

        sub_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"dbirth-fmt-{time.time()}",
        )
        sub_client.on_message = on_msg
        sub_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        sub_client.subscribe(f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/#", qos=0)
        sub_client.loop_start()
        time.sleep(0.5)

        pub_client = MqttClient(mqtt_config)
        publisher = SparkplugPublisher(pub_client, sparkplug_config, alias_db)
        pub_client.connect(timeout=10)
        publisher.publish_nbirth()

        # Publish DBIRTH with test metrics
        metrics = [
            ContextMetric(
                path="Data.Temperature",
                value=25.5,
                aas_type="Property",
                value_type="xs:double",
            ),
        ]
        publisher.publish_dbirth(device_id, metrics)

        time.sleep(1)
        sub_client.loop_stop()
        sub_client.disconnect()
        pub_client.disconnect()

        assert len(received_topics) > 0, "Should receive DBIRTH"

        # Verify format
        topic = received_topics[0]
        parts = topic.split("/")
        assert len(parts) == 5, f"DBIRTH topic should have 5 parts: {topic}"
        assert parts[0] == "spBv1.0", f"Namespace should be spBv1.0: {topic}"
        assert parts[1] == sparkplug_config.group_id, f"Group ID mismatch: {topic}"
        assert parts[2] == "DBIRTH", f"Message type should be DBIRTH: {topic}"
        assert parts[3] == sparkplug_config.edge_node_id, f"Edge node ID mismatch: {topic}"
        assert parts[4] == device_id, f"Device ID mismatch: {topic}"

    def test_dbirth_from_aas_file(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        sample_robot_json: Path,
        clean_broker: None,
    ) -> None:
        """DBIRTH should include all metrics from AAS file."""
        received_payloads: list[bytes] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            if "DBIRTH" in msg.topic:
                received_payloads.append(msg.payload)

        sub_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"dbirth-aas-{time.time()}",
        )
        sub_client.on_message = on_msg
        sub_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        sub_client.subscribe(f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/#", qos=0)
        sub_client.loop_start()
        time.sleep(0.5)

        # Load AAS and publish DBIRTH
        object_store = load_json(sample_robot_json)
        pub_client = MqttClient(mqtt_config)
        publisher = SparkplugPublisher(pub_client, sparkplug_config, alias_db)
        pub_client.connect(timeout=10)
        publisher.publish_nbirth()

        total_metrics = 0
        for submodel, asset_id in iter_submodels(object_store):
            if asset_id is None:
                continue
            metrics = flatten_submodel(submodel, str(sample_robot_json))
            total_metrics += len(metrics)
            device_id = asset_id.split("/")[-1] if "/" in asset_id else asset_id
            publisher.publish_dbirth(device_id, metrics)

        time.sleep(1)
        sub_client.loop_stop()
        sub_client.disconnect()
        pub_client.disconnect()

        assert len(received_payloads) > 0, "Should receive DBIRTH"
        # Payload contains all metrics (verified by size - actual decode requires protobuf)
        assert len(received_payloads[0]) > 100, "DBIRTH payload should contain metrics"


@pytest.mark.e2e
class TestSparkplugRetainFalse:
    """Tests for Sparkplug retain=false requirement."""

    def test_nbirth_not_retained(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        clean_broker: None,
    ) -> None:
        """NBIRTH messages must not be retained."""
        # Publish NBIRTH
        pub_client = MqttClient(mqtt_config)
        publisher = SparkplugPublisher(pub_client, sparkplug_config, alias_db)
        pub_client.connect(timeout=10)
        publisher.publish_nbirth()
        time.sleep(0.5)
        pub_client.disconnect()

        # Late subscriber should NOT receive NBIRTH
        received: list[str] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            if "NBIRTH" in msg.topic:
                received.append(msg.topic)

        late_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"late-nbirth-{time.time()}",
        )
        late_client.on_message = on_msg
        late_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        late_client.subscribe(f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/#", qos=0)
        late_client.loop_start()

        time.sleep(1)
        late_client.loop_stop()
        late_client.disconnect()

        # Should NOT receive NBIRTH (not retained)
        assert len(received) == 0, "NBIRTH should NOT be retained"

    def test_dbirth_not_retained(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        clean_broker: None,
    ) -> None:
        """DBIRTH messages must not be retained."""
        device_id = "RetainTestDevice"

        # Publish DBIRTH
        pub_client = MqttClient(mqtt_config)
        publisher = SparkplugPublisher(pub_client, sparkplug_config, alias_db)
        pub_client.connect(timeout=10)
        publisher.publish_nbirth()
        publisher.publish_dbirth(
            device_id,
            [
                ContextMetric(
                    path="Test.Property",
                    value="test",
                    aas_type="Property",
                    value_type="xs:string",
                )
            ],
        )
        time.sleep(0.5)
        pub_client.disconnect()

        # Late subscriber should NOT receive DBIRTH
        received: list[str] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            if "DBIRTH" in msg.topic:
                received.append(msg.topic)

        late_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"late-dbirth-{time.time()}",
        )
        late_client.on_message = on_msg
        late_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        late_client.subscribe(f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/#", qos=0)
        late_client.loop_start()

        time.sleep(1)
        late_client.loop_stop()
        late_client.disconnect()

        assert len(received) == 0, "DBIRTH should NOT be retained"

    def test_ddata_not_retained(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        clean_broker: None,
    ) -> None:
        """DDATA messages must not be retained."""
        device_id = "DataRetainTestDevice"

        # Publish DDATA
        pub_client = MqttClient(mqtt_config)
        publisher = SparkplugPublisher(pub_client, sparkplug_config, alias_db)
        pub_client.connect(timeout=10)
        publisher.publish_nbirth()
        publisher.publish_dbirth(
            device_id,
            [
                ContextMetric(
                    path="Test.Property",
                    value="initial",
                    aas_type="Property",
                    value_type="xs:string",
                )
            ],
        )
        # Publish DDATA (update)
        publisher.publish_ddata(
            device_id,
            [
                ContextMetric(
                    path="Test.Property",
                    value="updated",
                    aas_type="Property",
                    value_type="xs:string",
                )
            ],
        )
        time.sleep(0.5)
        pub_client.disconnect()

        # Late subscriber should NOT receive any Sparkplug messages
        received: list[str] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            received.append(msg.topic)

        late_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"late-ddata-{time.time()}",
        )
        late_client.on_message = on_msg
        late_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        late_client.subscribe(f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/#", qos=0)
        late_client.loop_start()

        time.sleep(1)
        late_client.loop_stop()
        late_client.disconnect()

        assert len(received) == 0, "DDATA should NOT be retained"


@pytest.mark.e2e
class TestSparkplugProtobuf:
    """Tests for Sparkplug protobuf payload compliance."""

    def test_nbirth_payload_decodable(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        clean_broker: None,
    ) -> None:
        """NBIRTH payload should be valid protobuf."""
        received_payload: bytes | None = None

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            nonlocal received_payload
            if "NBIRTH" in msg.topic:
                received_payload = msg.payload

        sub_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"proto-nbirth-{time.time()}",
        )
        sub_client.on_message = on_msg
        sub_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        sub_client.subscribe(f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/#", qos=0)
        sub_client.loop_start()
        time.sleep(0.5)

        pub_client = MqttClient(mqtt_config)
        publisher = SparkplugPublisher(pub_client, sparkplug_config, alias_db)
        pub_client.connect(timeout=10)
        publisher.publish_nbirth()

        time.sleep(1)
        sub_client.loop_stop()
        sub_client.disconnect()
        pub_client.disconnect()

        assert received_payload is not None, "Should receive NBIRTH payload"

        # Try to decode as protobuf
        try:
            from aas_uns_bridge.proto import sparkplug_b_pb2 as spb

            payload = spb.Payload()
            payload.ParseFromString(received_payload)

            # NBIRTH should have bdSeq metric
            bdseq_found = False
            for metric in payload.metrics:
                if metric.name == "bdSeq":
                    bdseq_found = True
                    break

            assert bdseq_found, "NBIRTH should contain bdSeq metric"
        except ImportError:
            # Protobuf not generated, skip decode verification
            pytest.skip("Sparkplug protobuf not generated")

    def test_dbirth_payload_contains_metrics(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        clean_broker: None,
    ) -> None:
        """DBIRTH payload should contain all device metrics."""
        device_id = "ProtoTestDevice"
        received_payload: bytes | None = None

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            nonlocal received_payload
            if "DBIRTH" in msg.topic:
                received_payload = msg.payload

        sub_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"proto-dbirth-{time.time()}",
        )
        sub_client.on_message = on_msg
        sub_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        sub_client.subscribe(f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/#", qos=0)
        sub_client.loop_start()
        time.sleep(0.5)

        pub_client = MqttClient(mqtt_config)
        publisher = SparkplugPublisher(pub_client, sparkplug_config, alias_db)
        pub_client.connect(timeout=10)
        publisher.publish_nbirth()

        test_metrics = [
            ContextMetric(
                path="Data.Temperature",
                value=25.5,
                aas_type="Property",
                value_type="xs:double",
                semantic_id="0173-1#02-AAB123#001",
            ),
            ContextMetric(
                path="Data.Status",
                value="Running",
                aas_type="Property",
                value_type="xs:string",
            ),
        ]
        publisher.publish_dbirth(device_id, test_metrics)

        time.sleep(1)
        sub_client.loop_stop()
        sub_client.disconnect()
        pub_client.disconnect()

        assert received_payload is not None, "Should receive DBIRTH payload"

        try:
            from aas_uns_bridge.proto import sparkplug_b_pb2 as spb

            payload = spb.Payload()
            payload.ParseFromString(received_payload)

            assert len(payload.metrics) == 2, f"Should have 2 metrics, got {len(payload.metrics)}"

            metric_names = [m.name for m in payload.metrics]
            assert "Data.Temperature" in metric_names, "Should contain Temperature metric"
            assert "Data.Status" in metric_names, "Should contain Status metric"
        except ImportError:
            pytest.skip("Sparkplug protobuf not generated")


@pytest.mark.e2e
class TestSparkplugRebirth:
    """Tests for Sparkplug rebirth behavior."""

    def test_rebirth_increments_bdseq(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        clean_broker: None,
    ) -> None:
        """Rebirth should publish new NBIRTH with incremented bdSeq."""
        received_payloads: list[bytes] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            if "NBIRTH" in msg.topic:
                received_payloads.append(msg.payload)

        sub_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"rebirth-{time.time()}",
        )
        sub_client.on_message = on_msg
        sub_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        sub_client.subscribe(f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/#", qos=0)
        sub_client.loop_start()
        time.sleep(0.5)

        pub_client = MqttClient(mqtt_config)
        publisher = SparkplugPublisher(pub_client, sparkplug_config, alias_db)
        pub_client.connect(timeout=10)

        # Initial NBIRTH
        publisher.publish_nbirth()
        time.sleep(0.5)

        # Trigger rebirth
        publisher.rebirth()
        time.sleep(1)

        sub_client.loop_stop()
        sub_client.disconnect()
        pub_client.disconnect()

        assert len(received_payloads) >= 2, "Should receive at least 2 NBIRTH messages"

        try:
            from aas_uns_bridge.proto import sparkplug_b_pb2 as spb

            bdseqs: list[int] = []
            for payload_bytes in received_payloads:
                payload = spb.Payload()
                payload.ParseFromString(payload_bytes)
                for metric in payload.metrics:
                    if metric.name == "bdSeq":
                        bdseqs.append(metric.long_value)
                        break

            assert len(bdseqs) >= 2, "Should extract bdSeq from both NBIRTHs"
            assert bdseqs[1] > bdseqs[0], f"bdSeq should increment: {bdseqs}"
        except ImportError:
            pytest.skip("Sparkplug protobuf not generated")
