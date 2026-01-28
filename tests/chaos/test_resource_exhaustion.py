"""Chaos engineering tests for resource exhaustion scenarios.

Tests system behavior under high load, concurrent access, and memory pressure,
including concurrent metric processing, alias database access, and hash cache growth.
"""

import tempfile
import threading
import time
from pathlib import Path

import pytest

from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.state.alias_db import AliasDB
from aas_uns_bridge.state.last_published import LastPublishedHashes


@pytest.fixture
def sample_metric_factory():
    """Factory for creating sample metrics with unique values."""

    def _create(index: int, value: float = 25.5) -> ContextMetric:
        return ContextMetric(
            path=f"TechnicalData.Sensor{index}.Temperature",
            value=value,
            aas_type="Property",
            value_type="xs:double",
            semantic_id=f"0173-1#02-AAO677#{index:03d}",
            unit="degC",
            aas_source="test.aasx",
            timestamp_ms=1706400000000 + index,
        )

    return _create


@pytest.fixture
def temp_db_path(tmp_path: Path) -> Path:
    """Create a temporary database path."""
    return tmp_path / "test_alias.db"


@pytest.fixture
def temp_hash_db_path(tmp_path: Path) -> Path:
    """Create a temporary database path for hash storage."""
    return tmp_path / "test_hashes.db"


@pytest.mark.chaos
class TestConcurrentFileWatchEvents:
    """Tests for concurrent file watch event processing scenarios."""

    def test_concurrent_metric_processing(
        self,
        sample_metric_factory,
        temp_hash_db_path: Path,
    ) -> None:
        """Process 100 metrics concurrently using threads, verify all report changed on first time.

        Simulates a burst of file watch events triggering metric processing
        from multiple threads simultaneously.
        """
        hash_tracker = LastPublishedHashes(db_path=temp_hash_db_path)
        threads: list[threading.Thread] = []
        results: list[tuple[int, bool]] = []
        lock = threading.Lock()

        def worker(i: int) -> None:
            metric = sample_metric_factory(i, value=float(i))
            topic = f"uns/test/sensor{i}/temperature"
            # First check should always report changed
            changed = hash_tracker.has_changed(topic, metric)
            with lock:
                results.append((i, changed))

        # Launch 100 concurrent threads
        for i in range(100):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join(timeout=5.0)

        # Verify all threads completed
        assert len(results) == 100, f"Expected 100 results, got {len(results)}"

        # All metrics should report as changed (first time seen)
        changed_count = sum(1 for _, changed in results if changed)
        assert changed_count == 100, f"Expected all 100 to report changed, got {changed_count}"

    def test_alias_db_concurrent_access(
        self,
        temp_db_path: Path,
    ) -> None:
        """Test 50 concurrent alias requests, verify all aliases are unique.

        Ensures the AliasDB correctly handles concurrent access and maintains
        unique alias assignments under load.
        """
        alias_db = AliasDB(db_path=temp_db_path)
        threads: list[threading.Thread] = []
        results: list[tuple[int, int]] = []
        lock = threading.Lock()

        def worker(i: int) -> None:
            metric_path = f"device/sensor{i}/temperature"
            alias = alias_db.get_alias(metric_path, device_id="test-device")
            with lock:
                results.append((i, alias))

        # Launch 50 concurrent threads
        for i in range(50):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join(timeout=5.0)

        # Verify all threads completed
        assert len(results) == 50, f"Expected 50 results, got {len(results)}"

        # Verify all aliases are unique
        aliases = [alias for _, alias in results]
        assert len(set(aliases)) == 50, f"Expected 50 unique aliases, got {len(set(aliases))}"

        # Verify alias count in database
        assert alias_db.count == 50, f"Expected 50 aliases in DB, got {alias_db.count}"


@pytest.mark.chaos
class TestHighThroughputPublishing:
    """Tests for high throughput publishing scenarios."""

    def test_deduplication_under_load(
        self,
        sample_metric_factory,
        temp_hash_db_path: Path,
    ) -> None:
        """Publish same value 1000 times, verify only published once (deduplication works).

        Tests that the hash-based deduplication correctly identifies unchanged
        values even under high-frequency repeated publish attempts.
        """
        hash_tracker = LastPublishedHashes(db_path=temp_hash_db_path)

        # Create a metric with a fixed value
        metric = sample_metric_factory(0, value=42.0)
        topic = "uns/test/sensor/temperature"

        # First check should indicate change (never seen before)
        assert hash_tracker.has_changed(topic, metric) is True

        # Update the hash tracker with the value
        hash_tracker.update(topic, metric)

        # Now publish the same value 1000 times
        change_reported_count = 0
        for _ in range(1000):
            if hash_tracker.has_changed(topic, metric):
                change_reported_count += 1
                hash_tracker.update(topic, metric)

        # No changes should be reported after the first update
        assert change_reported_count == 0, (
            f"Expected 0 changes after initial update, got {change_reported_count}"
        )

    def test_batch_filter_performance(
        self,
        sample_metric_factory,
        temp_hash_db_path: Path,
    ) -> None:
        """Filter 500 metrics, verify completes in <1 second.

        Ensures the batch filtering operation maintains acceptable performance
        under moderate load.
        """
        hash_tracker = LastPublishedHashes(db_path=temp_hash_db_path)

        # Create 500 metrics with unique topics
        topic_metrics: dict[str, ContextMetric] = {}
        for i in range(500):
            metric = sample_metric_factory(i, value=float(i))
            topic = f"uns/test/sensor{i}/temperature"
            topic_metrics[topic] = metric

        # Measure time to filter all metrics
        start_time = time.perf_counter()
        changed_metrics = hash_tracker.filter_changed(topic_metrics)
        elapsed_time = time.perf_counter() - start_time

        # Verify performance
        assert elapsed_time < 1.0, f"Filter took {elapsed_time:.3f}s, expected <1s"

        # All metrics should be changed on first filter
        assert len(changed_metrics) == 500, (
            f"Expected 500 changed metrics, got {len(changed_metrics)}"
        )

        # Update all metrics
        hash_tracker.update_batch(topic_metrics)

        # Second filter should return no changed metrics
        start_time = time.perf_counter()
        changed_metrics = hash_tracker.filter_changed(topic_metrics)
        elapsed_time = time.perf_counter() - start_time

        assert elapsed_time < 1.0, f"Second filter took {elapsed_time:.3f}s, expected <1s"
        assert len(changed_metrics) == 0, (
            f"Expected 0 changed metrics after update, got {len(changed_metrics)}"
        )


@pytest.mark.chaos
class TestMemoryBounds:
    """Tests for memory bounds and cache management."""

    def test_hash_cache_memory_growth(
        self,
        sample_metric_factory,
    ) -> None:
        """Add 10000 entries, verify count, then clear and verify count is 0.

        Tests that the hash cache correctly tracks entries and can be cleared
        to free memory.
        """
        # Use in-memory storage for this test
        hash_tracker = LastPublishedHashes(db_path=None)

        # Add 10000 unique entries
        for i in range(10000):
            metric = sample_metric_factory(i, value=float(i))
            topic = f"uns/test/sensor{i}/temperature"
            hash_tracker.update(topic, metric)

        # Verify count
        assert hash_tracker.count == 10000, (
            f"Expected 10000 entries, got {hash_tracker.count}"
        )

        # Clear the cache
        hash_tracker.clear()

        # Verify count is 0 after clear
        assert hash_tracker.count == 0, (
            f"Expected 0 entries after clear, got {hash_tracker.count}"
        )

    def test_alias_db_memory_growth(
        self,
        temp_db_path: Path,
    ) -> None:
        """Add 10000 alias entries, verify count, then clear and verify count is 0.

        Tests that the alias database correctly tracks entries and can be cleared.
        """
        alias_db = AliasDB(db_path=temp_db_path)

        # Add 10000 unique aliases
        for i in range(10000):
            metric_path = f"device/sensor{i}/temperature"
            alias_db.get_alias(metric_path, device_id="test-device")

        # Verify count
        assert alias_db.count == 10000, (
            f"Expected 10000 aliases, got {alias_db.count}"
        )

        # Clear all aliases
        alias_db.clear_all()

        # Verify count is 0 after clear
        assert alias_db.count == 0, (
            f"Expected 0 aliases after clear, got {alias_db.count}"
        )

    def test_concurrent_hash_updates_and_clears(
        self,
        sample_metric_factory,
    ) -> None:
        """Test concurrent hash updates don't corrupt state during clear.

        Ensures thread safety when updates and clear operations happen concurrently.
        """
        hash_tracker = LastPublishedHashes(db_path=None)
        errors: list[Exception] = []
        lock = threading.Lock()

        def updater(batch_id: int) -> None:
            """Add entries in a batch."""
            try:
                for i in range(100):
                    metric = sample_metric_factory(batch_id * 100 + i, value=float(i))
                    topic = f"uns/test/batch{batch_id}/sensor{i}/temperature"
                    hash_tracker.update(topic, metric)
            except Exception as e:
                with lock:
                    errors.append(e)

        def clearer() -> None:
            """Periodically clear the cache."""
            try:
                for _ in range(5):
                    time.sleep(0.01)  # Small delay
                    hash_tracker.clear()
            except Exception as e:
                with lock:
                    errors.append(e)

        # Launch concurrent updaters and clearer
        threads: list[threading.Thread] = []

        for batch_id in range(10):
            t = threading.Thread(target=updater, args=(batch_id,))
            threads.append(t)
            t.start()

        clearer_thread = threading.Thread(target=clearer)
        threads.append(clearer_thread)
        clearer_thread.start()

        # Wait for all threads to complete
        for t in threads:
            t.join(timeout=10.0)

        # No exceptions should have occurred
        assert len(errors) == 0, f"Errors occurred during concurrent operations: {errors}"

        # Final state should be valid (count >= 0)
        assert hash_tracker.count >= 0, "Invalid cache count after concurrent operations"
