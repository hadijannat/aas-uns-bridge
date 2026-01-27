"""Integration tests for Sparkplug B birth messages."""

import json
import threading
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from aas_uns_bridge.config import MqttConfig, SparkplugConfig
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.mqtt.client import MqttClient
from aas_uns_bridge.publishers.sparkplug import SparkplugPublisher, SPARKPLUG_NAMESPACE
from aas_uns_bridge.state.alias_db import AliasDB


@pytest.fixture
def mqtt_config() -> MqttConfig:
    """Create MQTT config for test broker."""
    return MqttConfig(
        host="localhost",
        port=1883,
        client_id=f"sparkplug-test-{time.time()}",
    )


@pytest.fixture
def sparkplug_config() -> SparkplugConfig:
    """Create Sparkplug config for testing."""
    return SparkplugConfig(
        enabled=True,
        group_id="TestGroup",
        edge_node_id="TestNode",
    )


@pytest.fixture
def temp_db() -> Path:
    """Create a temporary database directory."""
    with TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "aliases.db"


@pytest.fixture
def sample_metrics() -> list[ContextMetric]:
    """Create sample metrics for DBIRTH."""
    return [
        ContextMetric(
            path="TechnicalData.Temperature",
            value=25.5,
            aas_type="Property",
            value_type="xs:double",
            semantic_id="0173-1#02-AAB123#001",
            unit="°C",
        ),
        ContextMetric(
            path="TechnicalData.Status",
            value="Running",
            aas_type="Property",
            value_type="xs:string",
        ),
    ]


class TestSparkplugBirthIntegration:
    """Integration tests for Sparkplug B birth messages.

    These tests require a running MQTT broker on localhost:1883.
    Skip with: pytest -m "not integration"
    """

    @pytest.mark.integration
    def test_nbirth_topic_format(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        temp_db: Path,
    ) -> None:
        """Test that NBIRTH uses correct topic format."""
        received_topics: list[str] = []
        receive_event = threading.Event()

        # Subscribe first
        sub_client = MqttClient(MqttConfig(
            host=mqtt_config.host,
            port=mqtt_config.port,
            client_id=f"sub-{time.time()}",
        ))

        def on_message(topic: str, payload: bytes) -> None:
            received_topics.append(topic)
            receive_event.set()

        try:
            sub_client.connect(timeout=10)
            sub_client.subscribe(f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/#", on_message)
            time.sleep(0.5)

            # Now publish NBIRTH
            alias_db = AliasDB(temp_db)
            pub_client = MqttClient(mqtt_config)
            publisher = SparkplugPublisher(pub_client, sparkplug_config, alias_db)

            pub_client.connect(timeout=10)
            publisher.publish_nbirth()

            # Wait for message
            assert receive_event.wait(timeout=5), "Did not receive NBIRTH"

            # Verify topic format
            expected_topic = f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/NBIRTH/{sparkplug_config.edge_node_id}"
            assert expected_topic in received_topics

        finally:
            sub_client.disconnect()
            if "pub_client" in locals():
                pub_client.disconnect()

    @pytest.mark.integration
    def test_dbirth_topic_format(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        temp_db: Path,
        sample_metrics: list[ContextMetric],
    ) -> None:
        """Test that DBIRTH uses correct topic format with device ID."""
        received_topics: list[str] = []
        receive_event = threading.Event()
        device_id = "TestDevice001"

        sub_client = MqttClient(MqttConfig(
            host=mqtt_config.host,
            port=mqtt_config.port,
            client_id=f"sub-{time.time()}",
        ))

        def on_message(topic: str, payload: bytes) -> None:
            received_topics.append(topic)
            if "DBIRTH" in topic:
                receive_event.set()

        try:
            sub_client.connect(timeout=10)
            sub_client.subscribe(f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/#", on_message)
            time.sleep(0.5)

            alias_db = AliasDB(temp_db)
            pub_client = MqttClient(mqtt_config)
            publisher = SparkplugPublisher(pub_client, sparkplug_config, alias_db)

            pub_client.connect(timeout=10)
            publisher.publish_nbirth()
            publisher.publish_dbirth(device_id, sample_metrics)

            assert receive_event.wait(timeout=5), "Did not receive DBIRTH"

            expected_topic = f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/DBIRTH/{sparkplug_config.edge_node_id}/{device_id}"
            assert expected_topic in received_topics

        finally:
            sub_client.disconnect()
            if "pub_client" in locals():
                pub_client.disconnect()

    @pytest.mark.integration
    def test_sparkplug_retain_false(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        temp_db: Path,
    ) -> None:
        """Test that Sparkplug messages have retain=false.

        Sparkplug spec requires retain=false for all messages.
        Late subscribers should NOT receive birth messages.
        """
        alias_db = AliasDB(temp_db)
        pub_client = MqttClient(mqtt_config)
        publisher = SparkplugPublisher(pub_client, sparkplug_config, alias_db)

        try:
            pub_client.connect(timeout=10)
            publisher.publish_nbirth()
            time.sleep(0.5)
            pub_client.disconnect()

            # Subscribe AFTER publish
            received_messages: list[str] = []
            sub_client = MqttClient(MqttConfig(
                host=mqtt_config.host,
                port=mqtt_config.port,
                client_id=f"late-sub-{time.time()}",
            ))

            def on_message(topic: str, payload: bytes) -> None:
                received_messages.append(topic)

            sub_client.connect(timeout=10)
            sub_client.subscribe(f"{SPARKPLUG_NAMESPACE}/{sparkplug_config.group_id}/#", on_message)

            # Wait briefly - should NOT receive retained message
            time.sleep(1)

            # Sparkplug messages should NOT be retained
            nbirth_topics = [t for t in received_messages if "NBIRTH" in t]
            assert len(nbirth_topics) == 0, "NBIRTH should not be retained"

        finally:
            if "pub_client" in locals():
                try:
                    pub_client.disconnect()
                except Exception:
                    pass
            if "sub_client" in locals():
                sub_client.disconnect()

    @pytest.mark.integration
    def test_alias_persistence(
        self,
        sparkplug_config: SparkplugConfig,
        temp_db: Path,
        sample_metrics: list[ContextMetric],
    ) -> None:
        """Test that aliases persist across restarts."""
        device_id = "PersistentDevice"

        # First run - assign aliases
        alias_db1 = AliasDB(temp_db)
        for metric in sample_metrics:
            alias_db1.get_alias(f"{device_id}/{metric.path}", device_id)

        first_aliases = dict(alias_db1.iter_all())

        # Simulate restart - new DB instance
        alias_db2 = AliasDB(temp_db)
        second_aliases = dict(alias_db2.iter_all())

        # Aliases should be identical
        assert first_aliases == second_aliases
