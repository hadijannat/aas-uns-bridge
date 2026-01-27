"""Shared fixtures for E2E tests."""

import os
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator

import paho.mqtt.client as mqtt
import pytest

from aas_uns_bridge.config import MqttConfig, UnsConfig, SparkplugConfig, BridgeConfig
from aas_uns_bridge.mqtt.client import MqttClient
from aas_uns_bridge.state.alias_db import AliasDB

# Test fixtures directory
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line("markers", "e2e: end-to-end integration tests")
    config.addinivalue_line("markers", "slow: slow tests")
    config.addinivalue_line("markers", "load: load/performance tests")


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    """Return the fixtures directory path."""
    return FIXTURES_DIR


@pytest.fixture(scope="session")
def sample_robot_json(fixtures_dir: Path) -> Path:
    """Return path to sample robot AAS JSON."""
    return fixtures_dir / "sample_robot.json"


@pytest.fixture(scope="session")
def sample_sensor_json(fixtures_dir: Path) -> Path:
    """Return path to sample sensor AAS JSON."""
    return fixtures_dir / "sample_sensor.json"


@pytest.fixture(scope="session")
def large_aas_json(fixtures_dir: Path) -> Path:
    """Return path to large AAS JSON for load testing."""
    return fixtures_dir / "large_aas_5k_properties.json"


@pytest.fixture(scope="session")
def test_config_path(fixtures_dir: Path) -> Path:
    """Return path to test configuration."""
    return fixtures_dir / "test_config.yaml"


@pytest.fixture(scope="session")
def test_mappings_path(fixtures_dir: Path) -> Path:
    """Return path to test mappings."""
    return fixtures_dir / "test_mappings.yaml"


@pytest.fixture
def unique_client_id() -> str:
    """Generate a unique MQTT client ID."""
    return f"test-{time.time()}-{os.getpid()}"


@pytest.fixture
def mqtt_config(unique_client_id: str) -> MqttConfig:
    """Create MQTT config for test broker."""
    return MqttConfig(
        host=os.environ.get("TEST_MQTT_HOST", "localhost"),
        port=int(os.environ.get("TEST_MQTT_PORT", "1883")),
        client_id=unique_client_id,
        keepalive=30,
        reconnect_delay_min=0.5,
        reconnect_delay_max=5.0,
    )


@pytest.fixture
def uns_config() -> UnsConfig:
    """Create UNS config for testing."""
    return UnsConfig(
        enabled=True,
        root_topic="",
        qos=1,
        retain=True,
    )


@pytest.fixture
def sparkplug_config() -> SparkplugConfig:
    """Create Sparkplug config for testing."""
    return SparkplugConfig(
        enabled=True,
        group_id="TestGroup",
        edge_node_id="TestNode",
        qos=0,
    )


@pytest.fixture
def temp_db_dir() -> Generator[Path, None, None]:
    """Create a temporary database directory."""
    with TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def alias_db(temp_db_dir: Path) -> AliasDB:
    """Create a temporary alias database."""
    return AliasDB(temp_db_dir / "aliases.db")


@pytest.fixture
def mqtt_client(mqtt_config: MqttConfig, require_broker: None) -> Generator[MqttClient, None, None]:
    """Create and connect an MQTT client.

    Automatically skips test if broker is not available.
    """
    client = MqttClient(mqtt_config)
    client.connect(timeout=10)
    yield client
    client.disconnect()


def is_broker_available(host: str = "localhost", port: int = 1883) -> bool:
    """Check if MQTT broker is available."""
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"health-check-{time.time()}",
    )
    try:
        client.connect(host, port, keepalive=5)
        client.disconnect()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def broker_available() -> bool:
    """Check if MQTT broker is available."""
    host = os.environ.get("TEST_MQTT_HOST", "localhost")
    port = int(os.environ.get("TEST_MQTT_PORT", "1883"))
    return is_broker_available(host, port)


@pytest.fixture
def require_broker(broker_available: bool) -> None:
    """Skip test if broker is not available."""
    if not broker_available:
        host = os.environ.get("TEST_MQTT_HOST", "localhost")
        port = os.environ.get("TEST_MQTT_PORT", "1883")
        pytest.skip(f"MQTT broker not available at {host}:{port}")


class MessageCollector:
    """Collects MQTT messages for test verification."""

    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes, bool]] = []

    def on_message(
        self, client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage
    ) -> None:
        """Handle received message."""
        self.messages.append((msg.topic, msg.payload, msg.retain))

    def clear(self) -> None:
        """Clear collected messages."""
        self.messages.clear()

    def wait_for_messages(
        self, count: int = 1, timeout: float = 5.0
    ) -> list[tuple[str, bytes, bool]]:
        """Wait for specified number of messages."""
        start = time.time()
        while len(self.messages) < count:
            if time.time() - start > timeout:
                break
            time.sleep(0.1)
        return self.messages.copy()


@pytest.fixture
def message_collector() -> MessageCollector:
    """Create a message collector."""
    return MessageCollector()


@pytest.fixture
def subscriber_client(mqtt_config: MqttConfig, require_broker: None) -> Generator[mqtt.Client, None, None]:
    """Create a raw paho MQTT client for subscribing.

    Automatically skips test if broker is not available.
    """
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"sub-{time.time()}-{os.getpid()}",
    )
    client.connect(mqtt_config.host, mqtt_config.port, keepalive=30)
    client.loop_start()
    yield client
    client.loop_stop()
    client.disconnect()


def cleanup_retained_messages(
    host: str = "localhost",
    port: int = 1883,
    topic_prefix: str = "#",
) -> None:
    """Clean up retained messages from broker.

    This helps ensure test isolation by removing retained messages
    from previous test runs.
    """
    retained_topics: list[str] = []

    def on_message(
        client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage
    ) -> None:
        if msg.retain:
            retained_topics.append(msg.topic)

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"cleanup-{time.time()}",
    )
    client.on_message = on_message
    client.connect(host, port, keepalive=10)
    client.subscribe(topic_prefix, qos=0)
    client.loop_start()

    # Wait to collect retained messages
    time.sleep(1)

    # Clear retained messages
    for topic in retained_topics:
        client.publish(topic, payload=None, retain=True)

    time.sleep(0.5)
    client.loop_stop()
    client.disconnect()


@pytest.fixture
def clean_broker(mqtt_config: MqttConfig, require_broker: None) -> Generator[None, None, None]:
    """Fixture to clean up test topics before and after test.

    Automatically skips test if broker is not available.
    """
    # Clean before test
    cleanup_retained_messages(
        mqtt_config.host,
        mqtt_config.port,
        "AcmeCorp/#",
    )
    cleanup_retained_messages(
        mqtt_config.host,
        mqtt_config.port,
        "TestEnterprise/#",
    )
    cleanup_retained_messages(
        mqtt_config.host,
        mqtt_config.port,
        "spBv1.0/TestGroup/#",
    )
    yield
    # Clean after test
    cleanup_retained_messages(
        mqtt_config.host,
        mqtt_config.port,
        "AcmeCorp/#",
    )
    cleanup_retained_messages(
        mqtt_config.host,
        mqtt_config.port,
        "TestEnterprise/#",
    )
    cleanup_retained_messages(
        mqtt_config.host,
        mqtt_config.port,
        "spBv1.0/TestGroup/#",
    )
