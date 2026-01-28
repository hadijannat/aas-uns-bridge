"""Memory stability tests for the AAS-UNS Bridge.

These tests verify that core components do not leak memory under sustained load.
Memory leak detection works by:
1. Running a warm-up phase to populate caches
2. Taking an initial snapshot after warm-up
3. Running many cycles of operations on stable datasets
4. Verifying memory does not grow beyond expected bounds

A true memory leak would show continuous growth even with a fixed dataset.
"""

import gc
import tempfile
import time
from pathlib import Path

import pytest

from aas_uns_bridge.config import LifecycleConfig
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.state.alias_db import AliasDB
from aas_uns_bridge.state.asset_lifecycle import AssetLifecycleTracker
from aas_uns_bridge.state.last_published import LastPublishedHashes


@pytest.mark.stability
class TestMemoryStability:
    """Memory stability tests for state management components."""

    @pytest.mark.timeout(60)
    def test_hash_tracking_no_memory_leak(self, memory_tracker) -> None:
        """Verify hash tracker does not leak memory under sustained load.

        Creates LastPublishedHashes with in-memory storage and processes
        100 cycles of 100 metrics with the same topics but changing values.
        After the cache is populated, memory should stabilize.
        """
        hashes = LastPublishedHashes(db_path=None)

        # Pre-create a list of topics and reuse them
        topics = [f"test/topic/{i}" for i in range(100)]

        # Warm-up phase: populate the cache with 100 topics
        for i, topic in enumerate(topics):
            metric = ContextMetric(
                path=f"test.metric.{i}",
                value=f"warmup_value_{i}",
                aas_type="Property",
                value_type="xs:string",
                timestamp_ms=int(time.time() * 1000),
            )
            hashes.update(topic, metric)

        # Force garbage collection and take initial snapshot
        gc.collect()
        gc.collect()  # Double collect to ensure all generations are cleaned
        memory_tracker.snapshot(time.time())

        for cycle in range(100):
            # Process metrics - reuse same topic strings, only value changes
            for i, topic in enumerate(topics):
                metric = ContextMetric(
                    path=f"test.metric.{i}",
                    value=f"value_{cycle}_{i}",  # Value changes each cycle
                    aas_type="Property",
                    value_type="xs:string",
                    timestamp_ms=int(time.time() * 1000),
                )
                if hashes.has_changed(topic, metric):
                    hashes.update(topic, metric)

            # Take snapshot every 10 cycles
            if (cycle + 1) % 10 == 0:
                gc.collect()
                gc.collect()
                memory_tracker.snapshot(time.time())

        growth_ratio = memory_tracker.get_growth_ratio()
        assert growth_ratio < 1.5, (
            f"Memory grew by {(growth_ratio - 1) * 100:.1f}% "
            f"(expected < 50%)"
        )

    @pytest.mark.timeout(60)
    def test_alias_db_no_memory_leak(self, memory_tracker) -> None:
        """Verify alias database does not leak memory with repeated access.

        Uses temporary directory for AliasDB and repeatedly accesses the
        same 50 aliases (should use cache). Memory growth should not
        exceed 30%.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "aliases.db"
            alias_db = AliasDB(db_path)

            # Pre-create metric paths to reuse
            metric_paths = [f"metric/path/{i}" for i in range(50)]
            device_ids = [f"device_{i % 5}" for i in range(50)]

            # Pre-create 50 aliases
            for path, device_id in zip(metric_paths, device_ids):
                alias_db.get_alias(path, device_id=device_id)

            # Take initial snapshot after setup
            gc.collect()
            gc.collect()
            memory_tracker.snapshot(time.time())

            for cycle in range(100):
                # Access the same 50 aliases repeatedly (should hit cache)
                for path, device_id in zip(metric_paths, device_ids):
                    alias_db.get_alias(path, device_id=device_id)

                # Take snapshot every 10 cycles
                if (cycle + 1) % 10 == 0:
                    gc.collect()
                    gc.collect()
                    memory_tracker.snapshot(time.time())

            growth_ratio = memory_tracker.get_growth_ratio()
            assert growth_ratio < 1.3, (
                f"Memory grew by {(growth_ratio - 1) * 100:.1f}% "
                f"(expected < 30%)"
            )

    @pytest.mark.timeout(60)
    def test_lifecycle_tracker_no_memory_leak(self, memory_tracker) -> None:
        """Verify lifecycle tracker does not leak memory under churn.

        Creates AssetLifecycleTracker with a fixed set of 10 assets.
        Simulates 100 cycles of marking assets online and removing/re-adding
        them. Uses fixed topics to avoid unbounded growth in the topics set.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "lifecycle.db"
            config = LifecycleConfig(
                enabled=True,
                stale_threshold_seconds=60,  # 60 seconds
            )
            tracker = AssetLifecycleTracker(db_path, config)

            # Fixed set of 10 assets and their topics
            asset_ids = [f"https://example.com/asset/{i}" for i in range(10)]
            asset_topics = [f"uns/test/asset/{i}" for i in range(10)]

            # Warm-up: add all assets initially with their fixed topics
            for asset_id, topic in zip(asset_ids, asset_topics):
                tracker.mark_online(asset_id, topic=topic)

            # Take initial snapshot after warm-up
            gc.collect()
            gc.collect()
            memory_tracker.snapshot(time.time())

            for cycle in range(100):
                # Mark all 10 assets online with their same fixed topics
                for asset_id, topic in zip(asset_ids, asset_topics):
                    tracker.mark_online(asset_id, topic=topic)

                # Remove 5 assets
                for asset_id in asset_ids[:5]:
                    tracker.remove_asset(asset_id)

                # Re-add the removed assets with their same fixed topics
                for asset_id, topic in zip(asset_ids[:5], asset_topics[:5]):
                    tracker.mark_online(asset_id, topic=topic)

                # Take snapshot every 10 cycles
                if (cycle + 1) % 10 == 0:
                    gc.collect()
                    gc.collect()
                    memory_tracker.snapshot(time.time())

            growth_ratio = memory_tracker.get_growth_ratio()
            assert growth_ratio < 1.5, (
                f"Memory grew by {(growth_ratio - 1) * 100:.1f}% "
                f"(expected < 50%)"
            )
