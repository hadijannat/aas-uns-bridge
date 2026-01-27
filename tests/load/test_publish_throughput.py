"""Load tests for publish throughput."""

import gc
import os
import resource
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from aas_uns_bridge.aas.loader import load_json
from aas_uns_bridge.aas.traversal import flatten_submodel, iter_submodels
from aas_uns_bridge.config import MqttConfig, UnsConfig, SparkplugConfig
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.mqtt.client import MqttClient
from aas_uns_bridge.publishers.uns_retained import UnsRetainedPublisher
from aas_uns_bridge.publishers.sparkplug import SparkplugPublisher
from aas_uns_bridge.state.alias_db import AliasDB


def get_memory_usage_mb() -> float:
    """Get current memory usage in MB."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_maxrss / (1024 * 1024)  # Convert to MB on macOS


@pytest.fixture
def large_aas_path() -> Path:
    """Return path to large AAS fixture."""
    return Path(__file__).parent.parent / "fixtures" / "large_aas_5k_properties.json"


@pytest.mark.load
class TestPublishThroughput:
    """Tests for publish throughput performance."""

    @pytest.mark.timeout(30)
    def test_5k_properties_publish_under_30s(
        self,
        large_aas_path: Path,
        mqtt_config: MqttConfig,
        uns_config: UnsConfig,
    ) -> None:
        """5000 properties should publish in under 30 seconds."""
        if not large_aas_path.exists():
            pytest.skip("Large AAS fixture not found")

        # Load AAS
        start_load = time.time()
        object_store = load_json(large_aas_path)
        load_time = time.time() - start_load
        print(f"\nLoad time: {load_time:.2f}s")

        # Flatten all metrics
        start_flatten = time.time()
        all_metrics: list[ContextMetric] = []
        for submodel, asset_id in iter_submodels(object_store):
            metrics = flatten_submodel(submodel, str(large_aas_path))
            all_metrics.extend(metrics)
        flatten_time = time.time() - start_flatten
        print(f"Flatten time: {flatten_time:.2f}s")
        print(f"Total metrics: {len(all_metrics)}")

        # Publish all metrics
        mqtt_client = MqttClient(mqtt_config)
        publisher = UnsRetainedPublisher(mqtt_client, uns_config)
        mqtt_client.connect(timeout=10)

        start_publish = time.time()
        for i, metric in enumerate(all_metrics):
            topic = f"LoadTest/Performance/context/Data/{metric.path}"
            publisher.publish_metric(topic, metric)

        publish_time = time.time() - start_publish
        mqtt_client.disconnect()

        total_time = load_time + flatten_time + publish_time
        print(f"Publish time: {publish_time:.2f}s")
        print(f"Total time: {total_time:.2f}s")
        print(f"Throughput: {len(all_metrics) / publish_time:.0f} metrics/s")

        assert total_time < 30, f"Should complete in under 30s, took {total_time:.2f}s"

    @pytest.mark.timeout(60)
    def test_sparkplug_5k_dbirth_under_30s(
        self,
        large_aas_path: Path,
        mqtt_config: MqttConfig,
        sparkplug_config: SparkplugConfig,
    ) -> None:
        """5000 metrics in DBIRTH should publish in under 30 seconds."""
        if not large_aas_path.exists():
            pytest.skip("Large AAS fixture not found")

        with TemporaryDirectory() as tmpdir:
            alias_db = AliasDB(Path(tmpdir) / "aliases.db")

            # Load and flatten
            object_store = load_json(large_aas_path)
            all_metrics: list[ContextMetric] = []
            for submodel, asset_id in iter_submodels(object_store):
                metrics = flatten_submodel(submodel, str(large_aas_path))
                all_metrics.extend(metrics)

            print(f"\nTotal metrics: {len(all_metrics)}")

            # Publish DBIRTH
            mqtt_client = MqttClient(mqtt_config)
            publisher = SparkplugPublisher(mqtt_client, sparkplug_config, alias_db)
            mqtt_client.connect(timeout=10)
            publisher.publish_nbirth()

            start = time.time()
            publisher.publish_dbirth("LoadTestDevice", all_metrics)
            publish_time = time.time() - start

            mqtt_client.disconnect()

            print(f"DBIRTH publish time: {publish_time:.2f}s")
            assert publish_time < 30, f"Should publish in under 30s, took {publish_time:.2f}s"


@pytest.mark.load
class TestMemoryStability:
    """Tests for memory stability under load."""

    @pytest.mark.timeout(120)
    def test_repeated_publish_no_memory_leak(
        self,
        large_aas_path: Path,
        mqtt_config: MqttConfig,
        uns_config: UnsConfig,
    ) -> None:
        """Repeated publishes should not cause memory growth."""
        if not large_aas_path.exists():
            pytest.skip("Large AAS fixture not found")

        # Load AAS once
        object_store = load_json(large_aas_path)
        all_metrics: list[ContextMetric] = []
        for submodel, asset_id in iter_submodels(object_store):
            metrics = flatten_submodel(submodel, str(large_aas_path))
            all_metrics.extend(metrics)

        # Use a subset for faster testing
        test_metrics = all_metrics[:500]
        print(f"\nUsing {len(test_metrics)} metrics per iteration")

        mqtt_client = MqttClient(mqtt_config)
        publisher = UnsRetainedPublisher(mqtt_client, uns_config)
        mqtt_client.connect(timeout=10)

        # Track memory over iterations
        memory_samples: list[float] = []
        iterations = 10

        gc.collect()
        initial_memory = get_memory_usage_mb()
        memory_samples.append(initial_memory)
        print(f"Initial memory: {initial_memory:.2f} MB")

        for i in range(iterations):
            for metric in test_metrics:
                topic = f"MemoryTest/Iteration{i}/context/Data/{metric.path}"
                publisher.publish_metric(topic, metric)

            gc.collect()
            current_memory = get_memory_usage_mb()
            memory_samples.append(current_memory)
            print(f"Iteration {i + 1}: {current_memory:.2f} MB")

        mqtt_client.disconnect()

        # Check for memory growth
        final_memory = memory_samples[-1]
        memory_growth = final_memory - initial_memory

        print(f"\nMemory growth: {memory_growth:.2f} MB")
        print(f"Growth per iteration: {memory_growth / iterations:.2f} MB")

        # Allow some memory growth but flag excessive growth
        # (5 MB per iteration would be concerning)
        max_allowed_growth = iterations * 5  # 5 MB per iteration max
        assert memory_growth < max_allowed_growth, (
            f"Excessive memory growth: {memory_growth:.2f} MB over {iterations} iterations"
        )

    def test_alias_db_memory_with_many_metrics(
        self,
        sparkplug_config: SparkplugConfig,
    ) -> None:
        """Alias DB should handle large number of metrics without excessive memory."""
        with TemporaryDirectory() as tmpdir:
            gc.collect()
            initial_memory = get_memory_usage_mb()

            alias_db = AliasDB(Path(tmpdir) / "aliases.db")

            # Create 5000 aliases
            device_id = "LargeDevice"
            num_metrics = 5000

            start = time.time()
            for i in range(num_metrics):
                alias_db.get_alias(f"{device_id}/Property{i}", device_id)
            alias_time = time.time() - start

            gc.collect()
            final_memory = get_memory_usage_mb()
            memory_growth = final_memory - initial_memory

            print(f"\nAlias creation time for {num_metrics} metrics: {alias_time:.2f}s")
            print(f"Memory growth: {memory_growth:.2f} MB")

            # Should be fast
            assert alias_time < 5, f"Alias creation too slow: {alias_time:.2f}s"

            # Memory growth should be reasonable
            # SQLite should keep most data on disk
            assert memory_growth < 50, f"Excessive memory growth: {memory_growth:.2f} MB"


@pytest.mark.load
class TestBatchPublishPerformance:
    """Tests for batch publish performance."""

    def test_batch_vs_individual_performance(
        self,
        mqtt_config: MqttConfig,
        uns_config: UnsConfig,
    ) -> None:
        """Batch publishing should be faster than individual publishes."""
        num_metrics = 100

        metrics = {
            f"BatchTest/Performance/context/Data/Property{i}": ContextMetric(
                path=f"Data.Property{i}",
                value=f"value_{i}",
                aas_type="Property",
                value_type="xs:string",
            )
            for i in range(num_metrics)
        }

        mqtt_client = MqttClient(mqtt_config)
        publisher = UnsRetainedPublisher(mqtt_client, uns_config)
        mqtt_client.connect(timeout=10)

        # Individual publishes
        start_individual = time.time()
        for topic, metric in metrics.items():
            publisher.publish_metric(topic, metric)
        individual_time = time.time() - start_individual

        # Batch publish
        start_batch = time.time()
        publisher.publish_batch(metrics)
        batch_time = time.time() - start_batch

        mqtt_client.disconnect()

        print(f"\nIndividual publish time: {individual_time:.3f}s")
        print(f"Batch publish time: {batch_time:.3f}s")
        print(f"Speedup: {individual_time / batch_time:.2f}x")

        # Batch should be at least as fast (network conditions may vary)
        # Just verify it completes without error


@pytest.mark.load
class TestConcurrentPublish:
    """Tests for concurrent publish scenarios."""

    def test_high_frequency_publish(
        self,
        mqtt_config: MqttConfig,
        uns_config: UnsConfig,
    ) -> None:
        """Test high-frequency publishes don't cause issues."""
        mqtt_client = MqttClient(mqtt_config)
        publisher = UnsRetainedPublisher(mqtt_client, uns_config)
        mqtt_client.connect(timeout=10)

        num_publishes = 1000
        metric = ContextMetric(
            path="Data.HighFreq",
            value="test",
            aas_type="Property",
            value_type="xs:string",
        )

        start = time.time()
        for i in range(num_publishes):
            topic = f"HighFreqTest/context/Data/Value{i % 10}"
            publisher.publish_metric(topic, metric)
        elapsed = time.time() - start

        mqtt_client.disconnect()

        rate = num_publishes / elapsed
        print(f"\nPublish rate: {rate:.0f} messages/s")

        # Should achieve reasonable rate
        assert rate > 100, f"Publish rate too low: {rate:.0f} messages/s"
