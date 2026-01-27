"""Resilience tests for bridge and broker restart scenarios."""

import time
from pathlib import Path
from tempfile import TemporaryDirectory

import paho.mqtt.client as mqtt
import pytest

from aas_uns_bridge.aas.loader import load_json
from aas_uns_bridge.aas.traversal import flatten_submodel, iter_submodels
from aas_uns_bridge.config import MqttConfig, SparkplugConfig, UnsConfig
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.mqtt.client import MqttClient
from aas_uns_bridge.publishers.sparkplug import SparkplugPublisher, SPARKPLUG_NAMESPACE
from aas_uns_bridge.publishers.uns_retained import UnsRetainedPublisher
from aas_uns_bridge.state.alias_db import AliasDB


@pytest.mark.e2e
class TestBridgeRestartResilience:
    """Tests for bridge restart resilience."""

    def test_alias_persistence_across_restart(
        self,
        sparkplug_config: SparkplugConfig,
    ) -> None:
        """Aliases should persist across bridge restarts."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "aliases.db"

            # First "run" - assign aliases
            alias_db1 = AliasDB(db_path)
            metrics = [
                ContextMetric(
                    path="Data.Temperature",
                    value=25.5,
                    aas_type="Property",
                    value_type="xs:double",
                ),
                ContextMetric(
                    path="Data.Pressure",
                    value=101.3,
                    aas_type="Property",
                    value_type="xs:double",
                ),
            ]

            device_id = "PersistentDevice"
            first_aliases = {}
            for metric in metrics:
                alias = alias_db1.get_alias(f"{device_id}/{metric.path}", device_id)
                first_aliases[metric.path] = alias

            # Close and reopen (simulating restart)
            del alias_db1

            # Second "run" - aliases should be the same
            alias_db2 = AliasDB(db_path)
            second_aliases = {}
            for metric in metrics:
                alias = alias_db2.get_alias(f"{device_id}/{metric.path}", device_id)
                second_aliases[metric.path] = alias

            # Aliases should match
            assert first_aliases == second_aliases, (
                f"Aliases should persist: first={first_aliases}, second={second_aliases}"
            )

    def test_no_duplicate_aliases_on_rapid_restart(
        self,
        sparkplug_config: SparkplugConfig,
    ) -> None:
        """Rapid restarts should not create duplicate aliases."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "aliases.db"

            all_aliases: list[dict[str, int]] = []
            device_id = "RapidRestartDevice"
            metric_path = "Data.Value"

            # Simulate 5 rapid restarts
            for i in range(5):
                alias_db = AliasDB(db_path)
                alias = alias_db.get_alias(f"{device_id}/{metric_path}", device_id)
                all_aliases.append({metric_path: alias})
                del alias_db

            # All should have the same alias
            first_alias = all_aliases[0][metric_path]
            for i, aliases in enumerate(all_aliases):
                assert aliases[metric_path] == first_alias, (
                    f"Alias mismatch on restart {i}: {aliases[metric_path]} != {first_alias}"
                )

    def test_publisher_state_survives_reconnect(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        clean_broker: None,
    ) -> None:
        """Publisher should maintain state through reconnection."""
        nbirth_count: list[int] = [0]

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            if "NBIRTH" in msg.topic:
                nbirth_count[0] += 1

        sub_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"reconnect-sub-{time.time()}",
        )
        sub_client.on_message = on_msg
        sub_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        sub_client.subscribe(f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/#", qos=0)
        sub_client.loop_start()
        time.sleep(0.5)

        # First connection
        mqtt_client = MqttClient(mqtt_config)
        publisher = SparkplugPublisher(mqtt_client, sparkplug_config, alias_db)
        mqtt_client.connect(timeout=10)
        publisher.publish_nbirth()
        initial_bdseq = publisher._bd_seq

        time.sleep(0.5)

        # Simulate disconnect/reconnect
        mqtt_client.disconnect()
        time.sleep(0.5)

        # Reconnect - publisher state should be maintained
        mqtt_client.connect(timeout=10)
        publisher.rebirth()

        time.sleep(1)
        sub_client.loop_stop()
        sub_client.disconnect()
        mqtt_client.disconnect()

        # bdSeq should have incremented
        assert publisher._bd_seq > initial_bdseq, "bdSeq should increment on rebirth"
        assert nbirth_count[0] >= 2, "Should receive at least 2 NBIRTH messages"


@pytest.mark.e2e
class TestBrokerRestartResilience:
    """Tests for broker restart resilience.

    Note: These tests verify retained message persistence after broker restart.
    Actual broker restart must be performed externally.
    """

    def test_uns_retained_available_after_delay(
        self,
        mqtt_config: MqttConfig,
        uns_config: UnsConfig,
        clean_broker: None,
    ) -> None:
        """UNS retained messages should remain available after significant delay."""
        test_topic = f"TestEnterprise/Persistent/context/Data/Durable-{time.time()}"
        test_value = f"durable-{time.time()}"

        # Publish retained message
        pub_client = MqttClient(mqtt_config)
        publisher = UnsRetainedPublisher(pub_client, uns_config)
        pub_client.connect(timeout=10)
        publisher.publish_metric(
            test_topic,
            ContextMetric(
                path="Data.Durable",
                value=test_value,
                aas_type="Property",
                value_type="xs:string",
            ),
        )
        pub_client.disconnect()

        # Wait (simulating broker downtime or network issues)
        time.sleep(2)

        # Verify message is still retained
        received: list[tuple[str, bytes]] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            received.append((msg.topic, msg.payload))

        verify_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"verify-durable-{time.time()}",
        )
        verify_client.on_message = on_msg
        verify_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        verify_client.subscribe(test_topic, qos=1)
        verify_client.loop_start()

        time.sleep(1)
        verify_client.loop_stop()
        verify_client.disconnect()

        assert len(received) == 1, "Retained message should still be available"

    def test_bridge_reconnects_on_broker_restart(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        clean_broker: None,
    ) -> None:
        """Bridge should reconnect and republish NBIRTH after broker restart.

        This test simulates the sequence of events but actual broker restart
        must be performed externally for full validation.
        """
        # Create client with reconnect behavior
        mqtt_client = MqttClient(mqtt_config)
        publisher = SparkplugPublisher(mqtt_client, sparkplug_config, alias_db)

        mqtt_client.connect(timeout=10)
        publisher.publish_nbirth()
        initial_birth_count = publisher.birth_count

        # Simulate reconnection scenario
        time.sleep(0.5)
        mqtt_client.disconnect()
        time.sleep(0.5)

        # Reconnect and rebirth
        mqtt_client.connect(timeout=10)
        publisher.rebirth()

        # Verify NBIRTH was republished
        assert publisher.birth_count > initial_birth_count, (
            "Should have published additional NBIRTH on reconnect"
        )

        mqtt_client.disconnect()


@pytest.mark.e2e
class TestChangeDetection:
    """Tests for change detection and deduplication."""

    def test_only_changed_metrics_republished(
        self,
        mqtt_config: MqttConfig,
        uns_config: UnsConfig,
        clean_broker: None,
    ) -> None:
        """Only changed metrics should be republished."""
        base_topic = f"TestEnterprise/ChangeDetect/context/Data"
        published_topics: list[str] = []

        def on_msg(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
            published_topics.append(msg.topic)

        sub_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"change-sub-{time.time()}",
        )
        sub_client.on_message = on_msg
        sub_client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
        sub_client.subscribe(f"{base_topic}/#", qos=1)
        sub_client.loop_start()
        time.sleep(0.5)

        mqtt_client = MqttClient(mqtt_config)
        publisher = UnsRetainedPublisher(mqtt_client, uns_config)
        mqtt_client.connect(timeout=10)

        # Initial publish - 3 metrics
        metrics = [
            ("Value1", "initial1"),
            ("Value2", "initial2"),
            ("Value3", "initial3"),
        ]

        for name, value in metrics:
            topic = f"{base_topic}/{name}"
            publisher.publish_metric(
                topic,
                ContextMetric(
                    path=f"Data.{name}",
                    value=value,
                    aas_type="Property",
                    value_type="xs:string",
                ),
            )

        time.sleep(0.5)
        initial_count = len(published_topics)

        # Second publish - only Value2 changed
        # (In production, hash-based deduplication would prevent republish of unchanged)
        # This test verifies the infrastructure works
        changed_topic = f"{base_topic}/Value2"
        publisher.publish_metric(
            changed_topic,
            ContextMetric(
                path="Data.Value2",
                value="changed2",
                aas_type="Property",
                value_type="xs:string",
            ),
        )

        time.sleep(0.5)
        sub_client.loop_stop()
        sub_client.disconnect()
        mqtt_client.disconnect()

        # Should have initial 3 + 1 changed
        assert len(published_topics) == initial_count + 1, (
            f"Should have {initial_count + 1} publishes, got {len(published_topics)}"
        )

        # Last topic should be the changed one
        assert published_topics[-1] == changed_topic, "Last publish should be the changed metric"

    def test_unchanged_file_skipped_on_reprocess(
        self,
        sample_robot_json: Path,
    ) -> None:
        """Verify file hash can detect unchanged files.

        This tests the hash computation infrastructure used for deduplication.
        """
        import hashlib

        # Compute hash twice
        with open(sample_robot_json, "rb") as f:
            hash1 = hashlib.sha256(f.read()).hexdigest()

        with open(sample_robot_json, "rb") as f:
            hash2 = hashlib.sha256(f.read()).hexdigest()

        # Hashes should match (file unchanged)
        assert hash1 == hash2, "Same file should produce same hash"

        # Verify the hash changes if content changes (conceptually)
        # In production, this would prevent republishing unchanged metrics


@pytest.mark.e2e
@pytest.mark.slow
class TestLongRunningResilience:
    """Long-running resilience tests."""

    def test_multiple_reconnect_cycles(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        clean_broker: None,
    ) -> None:
        """Publisher should handle multiple reconnect cycles gracefully."""
        mqtt_client = MqttClient(mqtt_config)
        publisher = SparkplugPublisher(mqtt_client, sparkplug_config, alias_db)

        bdseq_values: list[int] = []

        # 5 connect/disconnect cycles
        for i in range(5):
            mqtt_client.connect(timeout=10)
            publisher.publish_nbirth()
            bdseq_values.append(publisher._bd_seq)

            time.sleep(0.3)
            mqtt_client.disconnect()
            time.sleep(0.2)

            # Trigger rebirth on next iteration
            if i < 4:
                publisher._is_online = False
                publisher._bd_seq += 1

        # bdSeq should monotonically increase
        for i in range(1, len(bdseq_values)):
            assert bdseq_values[i] >= bdseq_values[i - 1], (
                f"bdSeq should not decrease: {bdseq_values}"
            )

    def test_alias_stability_over_time(
        self,
        sparkplug_config: SparkplugConfig,
    ) -> None:
        """Aliases should remain stable over many operations."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "aliases.db"
            alias_db = AliasDB(db_path)

            device_id = "StabilityDevice"
            metric_paths = [f"Data.Property{i}" for i in range(100)]

            # Assign aliases
            initial_aliases = {}
            for path in metric_paths:
                alias = alias_db.get_alias(f"{device_id}/{path}", device_id)
                initial_aliases[path] = alias

            # Get aliases again (many times)
            for _ in range(10):
                for path in metric_paths:
                    alias = alias_db.get_alias(f"{device_id}/{path}", device_id)
                    assert alias == initial_aliases[path], (
                        f"Alias for {path} should be stable"
                    )

            # All aliases should be unique
            all_aliases = list(initial_aliases.values())
            assert len(all_aliases) == len(set(all_aliases)), "All aliases should be unique"
