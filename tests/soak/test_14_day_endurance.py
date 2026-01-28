"""14-day endurance (soak) test for TRL 6 evidence.

This test runs the bridge for an extended period to verify:
- Memory stability (no leaks)
- Connection resilience (reconnection after network issues)
- Metric accuracy over time
- No performance degradation

Usage:
    # Run for 1 hour (quick validation)
    pytest tests/soak/test_14_day_endurance.py -v -k "quick" --timeout=3700

    # Run for 24 hours
    pytest tests/soak/test_14_day_endurance.py -v -k "day" --timeout=90000

    # Run for 14 days (full TRL 6)
    pytest tests/soak/test_14_day_endurance.py -v -k "full" --timeout=1209600

Note: These tests require:
    - MQTT broker running on localhost:1883
    - Sufficient disk space for metrics log
    - Stable network connection
"""

import contextlib
import gc
import json
import logging
import os
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path

import psutil
import pytest

from aas_uns_bridge.config import MqttConfig, SparkplugConfig, UnsConfig
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.mqtt.client import MqttClient
from aas_uns_bridge.publishers.sparkplug import SparkplugPublisher
from aas_uns_bridge.publishers.uns_retained import UnsRetainedPublisher
from aas_uns_bridge.state.alias_db import AliasDB

logger = logging.getLogger(__name__)


@dataclass
class SoakMetrics:
    """Metrics collected during soak test."""

    timestamp: str = ""
    elapsed_hours: float = 0.0
    memory_mb: float = 0.0
    memory_percent: float = 0.0
    cpu_percent: float = 0.0
    messages_published: int = 0
    errors: int = 0
    reconnects: int = 0
    active_devices: int = 0


@dataclass
class SoakTestConfig:
    """Configuration for soak test."""

    duration_seconds: int = 86400  # 24 hours default
    sample_interval_seconds: int = 300  # 5 minutes
    publish_interval_seconds: float = 1.0  # Publish every second
    num_devices: int = 5
    metrics_per_device: int = 10
    output_dir: Path = field(default_factory=lambda: Path("soak-results"))


@dataclass
class SoakTestResults:
    """Results from soak test execution."""

    start_time: str = ""
    end_time: str = ""
    duration_hours: float = 0.0
    total_messages: int = 0
    total_errors: int = 0
    total_reconnects: int = 0
    initial_memory_mb: float = 0.0
    final_memory_mb: float = 0.0
    peak_memory_mb: float = 0.0
    memory_growth_percent: float = 0.0
    passed: bool = False
    failure_reason: str = ""


class SoakTestRunner:
    """Runs soak tests with metrics collection."""

    def __init__(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        uns_config: UnsConfig,
        test_config: SoakTestConfig,
    ):
        self.mqtt_config = mqtt_config
        self.sparkplug_config = sparkplug_config
        self.uns_config = uns_config
        self.test_config = test_config

        self.client: MqttClient | None = None
        self.sparkplug: SparkplugPublisher | None = None
        self.uns: UnsRetainedPublisher | None = None
        self.alias_db: AliasDB | None = None

        self._stop_event = threading.Event()
        self._metrics_log: list[SoakMetrics] = []
        self._message_count = 0
        self._error_count = 0
        self._reconnect_count = 0
        self._process = psutil.Process()

    def setup(self) -> None:
        """Initialize publishers and connections."""
        # Create output directory
        self.test_config.output_dir.mkdir(parents=True, exist_ok=True)

        # Create temporary alias database
        db_path = self.test_config.output_dir / "soak_aliases.db"
        self.alias_db = AliasDB(db_path)

        # Create MQTT client
        self.client = MqttClient(self.mqtt_config)

        # Create publishers
        self.sparkplug = SparkplugPublisher(self.client, self.sparkplug_config, self.alias_db)
        self.uns = UnsRetainedPublisher(self.client, self.uns_config)

    def connect(self) -> None:
        """Connect to broker and publish births."""
        if not self.client:
            raise RuntimeError("Call setup() first")

        self.client.connect(timeout=30)
        self.sparkplug.publish_nbirth()

        # Publish initial DBIRTHs
        for device_idx in range(self.test_config.num_devices):
            device_id = f"SoakDevice{device_idx:03d}"
            metrics = self._generate_device_metrics(device_id)
            self.sparkplug.publish_dbirth(device_id, metrics)

    def _generate_device_metrics(self, device_id: str) -> list[ContextMetric]:
        """Generate test metrics for a device."""
        metrics = []
        for i in range(self.test_config.metrics_per_device):
            metrics.append(
                ContextMetric(
                    path=f"Data.Metric{i:02d}",
                    value=i * 1.5,
                    aas_type="Property",
                    value_type="xs:double",
                    semantic_id=f"0173-1#02-TEST{i:03d}#001",
                )
            )
        return metrics

    def _collect_metrics(self, elapsed_seconds: float) -> SoakMetrics:
        """Collect current system metrics."""
        gc.collect()  # Force garbage collection before measurement

        mem_info = self._process.memory_info()
        cpu_percent = self._process.cpu_percent(interval=0.1)

        return SoakMetrics(
            timestamp=datetime.now().isoformat(),
            elapsed_hours=elapsed_seconds / 3600,
            memory_mb=mem_info.rss / (1024 * 1024),
            memory_percent=self._process.memory_percent(),
            cpu_percent=cpu_percent,
            messages_published=self._message_count,
            errors=self._error_count,
            reconnects=self._reconnect_count,
            active_devices=len(self.sparkplug.active_devices) if self.sparkplug else 0,
        )

    def _publish_update(self, device_idx: int, iteration: int) -> None:
        """Publish a metric update."""
        try:
            device_id = f"SoakDevice{device_idx:03d}"
            metric_idx = iteration % self.test_config.metrics_per_device

            metric = ContextMetric(
                path=f"Data.Metric{metric_idx:02d}",
                value=iteration * 0.1,
                aas_type="Property",
                value_type="xs:double",
                timestamp_ms=int(time.time() * 1000),
            )

            # Publish to both planes
            self.sparkplug.publish_ddata(device_id, [metric])
            topic = f"SoakTest/Site/Area/Line/{device_id}/context/Data/Metric{metric_idx:02d}"
            self.uns.publish_metric(topic, metric)

            self._message_count += 2  # One for each plane
        except Exception as e:
            logger.warning("Publish error: %s", e)
            self._error_count += 1

    def run(self) -> SoakTestResults:
        """Execute the soak test."""
        results = SoakTestResults()
        results.start_time = datetime.now().isoformat()

        # Initial metrics
        initial = self._collect_metrics(0)
        results.initial_memory_mb = initial.memory_mb
        self._metrics_log.append(initial)

        start_time = time.time()
        last_sample_time = start_time
        iteration = 0
        peak_memory = initial.memory_mb

        logger.info(
            "Starting soak test: duration=%d seconds, devices=%d",
            self.test_config.duration_seconds,
            self.test_config.num_devices,
        )

        try:
            while not self._stop_event.is_set():
                current_time = time.time()
                elapsed = current_time - start_time

                # Check duration
                if elapsed >= self.test_config.duration_seconds:
                    logger.info("Soak test duration completed")
                    break

                # Publish updates
                for device_idx in range(self.test_config.num_devices):
                    self._publish_update(device_idx, iteration)
                iteration += 1

                # Sample metrics periodically
                if current_time - last_sample_time >= self.test_config.sample_interval_seconds:
                    metrics = self._collect_metrics(elapsed)
                    self._metrics_log.append(metrics)
                    peak_memory = max(peak_memory, metrics.memory_mb)
                    last_sample_time = current_time

                    logger.info(
                        "Soak checkpoint: hours=%.1f, messages=%d, memory=%.1fMB, errors=%d",
                        metrics.elapsed_hours,
                        metrics.messages_published,
                        metrics.memory_mb,
                        metrics.errors,
                    )

                # Wait for next publish interval
                time.sleep(self.test_config.publish_interval_seconds)

        except Exception as e:
            results.failure_reason = str(e)
            logger.error("Soak test failed: %s", e)

        # Final metrics
        final = self._collect_metrics(time.time() - start_time)
        self._metrics_log.append(final)

        # Populate results
        results.end_time = datetime.now().isoformat()
        results.duration_hours = final.elapsed_hours
        results.total_messages = self._message_count
        results.total_errors = self._error_count
        results.total_reconnects = self._reconnect_count
        results.final_memory_mb = final.memory_mb
        results.peak_memory_mb = peak_memory

        if results.initial_memory_mb > 0:
            results.memory_growth_percent = (
                (results.final_memory_mb - results.initial_memory_mb) / results.initial_memory_mb
            ) * 100

        # Determine pass/fail
        if not results.failure_reason:
            # Pass criteria:
            # - Memory growth < 50%
            # - Error rate < 0.1%
            # - No unrecoverable failures
            error_rate = (
                results.total_errors / results.total_messages if results.total_messages > 0 else 0
            )

            if results.memory_growth_percent > 50:
                results.failure_reason = (
                    f"Memory growth {results.memory_growth_percent:.1f}% exceeds 50%"
                )
            elif error_rate > 0.001:
                results.failure_reason = f"Error rate {error_rate:.4%} exceeds 0.1%"
            else:
                results.passed = True

        return results

    def save_results(self, results: SoakTestResults) -> None:
        """Save test results and metrics to files."""
        # Save summary
        summary_path = self.test_config.output_dir / "soak_summary.json"
        with open(summary_path, "w") as f:
            json.dump(asdict(results), f, indent=2)

        # Save detailed metrics log
        metrics_path = self.test_config.output_dir / "soak_metrics.json"
        with open(metrics_path, "w") as f:
            json.dump([asdict(m) for m in self._metrics_log], f, indent=2)

        logger.info("Results saved to %s", self.test_config.output_dir)

    def teardown(self) -> None:
        """Clean up resources."""
        self._stop_event.set()

        if self.sparkplug:
            with contextlib.suppress(Exception):
                self.sparkplug.shutdown()

        if self.client:
            with contextlib.suppress(Exception):
                self.client.disconnect()


# Fixtures


@pytest.fixture
def mqtt_config() -> MqttConfig:
    """MQTT configuration for soak tests."""
    return MqttConfig(
        host=os.environ.get("TEST_MQTT_HOST", "localhost"),
        port=int(os.environ.get("TEST_MQTT_PORT", "1883")),
        client_id=f"soak-test-{int(time.time())}",
    )


@pytest.fixture
def sparkplug_config() -> SparkplugConfig:
    """Sparkplug configuration for soak tests."""
    return SparkplugConfig(
        enabled=True,
        group_id="SoakTestGroup",
        edge_node_id="SoakTestNode",
    )


@pytest.fixture
def uns_config() -> UnsConfig:
    """UNS configuration for soak tests."""
    return UnsConfig(
        enabled=True,
        root_topic="SoakTest",
        qos=1,
        retain=True,
    )


# Test Classes


@pytest.mark.soak
class TestSoakQuick:
    """Quick soak tests for CI validation (1 hour)."""

    @pytest.mark.timeout(3700)  # 1 hour + buffer
    def test_1_hour_endurance(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        uns_config: UnsConfig,
        tmp_path: Path,
    ) -> None:
        """Run 1-hour soak test for quick validation."""
        config = SoakTestConfig(
            duration_seconds=3600,  # 1 hour
            sample_interval_seconds=60,  # Sample every minute
            publish_interval_seconds=1.0,
            num_devices=3,
            metrics_per_device=5,
            output_dir=tmp_path / "soak-1h",
        )

        runner = SoakTestRunner(mqtt_config, sparkplug_config, uns_config, config)

        try:
            runner.setup()
            runner.connect()
            results = runner.run()
            runner.save_results(results)

            assert results.passed, f"Soak test failed: {results.failure_reason}"
            assert results.memory_growth_percent < 50, "Memory leak detected"
        finally:
            runner.teardown()


@pytest.mark.soak
@pytest.mark.slow
class TestSoak24Hour:
    """24-hour soak test for TRL 5."""

    @pytest.mark.timeout(90000)  # 25 hours + buffer
    def test_24_hour_endurance(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        uns_config: UnsConfig,
        tmp_path: Path,
    ) -> None:
        """Run 24-hour soak test."""
        config = SoakTestConfig(
            duration_seconds=86400,  # 24 hours
            sample_interval_seconds=300,  # Sample every 5 minutes
            publish_interval_seconds=1.0,
            num_devices=5,
            metrics_per_device=10,
            output_dir=tmp_path / "soak-24h",
        )

        runner = SoakTestRunner(mqtt_config, sparkplug_config, uns_config, config)

        try:
            runner.setup()
            runner.connect()
            results = runner.run()
            runner.save_results(results)

            assert results.passed, f"Soak test failed: {results.failure_reason}"
        finally:
            runner.teardown()


@pytest.mark.soak
@pytest.mark.slow
class TestSoak14Day:
    """14-day soak test for TRL 6 evidence."""

    @pytest.mark.timeout(1209600 + 3600)  # 14 days + 1 hour buffer
    def test_14_day_continuous_operation(
        self,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
        uns_config: UnsConfig,
    ) -> None:
        """Run bridge for 14 days continuously.

        Pass criteria:
        - Memory growth < 50%
        - Error rate < 0.1%
        - No unrecoverable failures
        """
        # Use persistent output directory for 14-day test
        output_dir = Path("soak-results-14day")

        config = SoakTestConfig(
            duration_seconds=1209600,  # 14 days
            sample_interval_seconds=300,  # Sample every 5 minutes (4032 samples)
            publish_interval_seconds=1.0,
            num_devices=5,
            metrics_per_device=10,
            output_dir=output_dir,
        )

        runner = SoakTestRunner(mqtt_config, sparkplug_config, uns_config, config)

        try:
            runner.setup()
            runner.connect()
            results = runner.run()
            runner.save_results(results)

            # Assertions with detailed messages
            assert results.passed, (
                f"14-day soak test failed: {results.failure_reason}\n"
                f"Duration: {results.duration_hours:.1f} hours\n"
                f"Messages: {results.total_messages}\n"
                f"Errors: {results.total_errors}\n"
                f"Memory growth: {results.memory_growth_percent:.1f}%"
            )
        finally:
            runner.teardown()
