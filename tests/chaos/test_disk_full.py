"""Chaos engineering tests for disk full / SQLite failure scenarios.

Tests verify that state persistence components (AliasDB, LastPublishedHashes,
BirthCache) handle disk write failures gracefully, including:
- Read-only database files (simulating disk full)
- Permission errors during persistence
- Cache survival during disk failures
"""

import sqlite3
import tempfile
from pathlib import Path

import pytest

from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.state.alias_db import AliasDB
from aas_uns_bridge.state.birth_cache import BirthCache
from aas_uns_bridge.state.last_published import LastPublishedHashes


@pytest.fixture
def sample_metric() -> ContextMetric:
    """Create a sample metric for testing."""
    return ContextMetric(
        path="TechnicalData.Temperature",
        value=25.5,
        aas_type="Property",
        value_type="xs:double",
        semantic_id="0173-1#02-AAO677#002",
        unit="degC",
        aas_source="test.aasx",
        timestamp_ms=1706400000000,
    )


@pytest.mark.chaos
class TestAliasDBDiskFailure:
    """Tests for AliasDB behavior during disk write failures."""

    def test_alias_db_handles_write_failure(self) -> None:
        """Verify OperationalError or PermissionError is raised on write attempt to read-only db.

        Simulates disk full by making the database file read-only, then verifying
        that write operations fail with the expected exception.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "aliases.db"

            # Initialize the database with write permissions
            alias_db = AliasDB(db_path)

            # Assign an initial alias to ensure db is properly initialized
            alias1 = alias_db.get_alias("test/metric/1", device_id="device1")
            assert alias1 == 1

            # Make database file read-only to simulate disk full
            db_path.chmod(0o444)

            try:
                # Attempting to assign a new alias should fail
                with pytest.raises((sqlite3.OperationalError, PermissionError)):
                    alias_db.get_alias("test/metric/2", device_id="device2")
            finally:
                # Restore write permissions for cleanup
                db_path.chmod(0o644)

    def test_alias_db_cache_survives_disk_failure(self) -> None:
        """Verify cached lookups still work after initial alias is assigned.

        Even when the disk becomes read-only, previously cached aliases should
        remain accessible from memory.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "aliases.db"

            # Initialize the database
            alias_db = AliasDB(db_path)

            # Pre-populate cache with some aliases
            alias1 = alias_db.get_alias("test/metric/1", device_id="device1")
            alias2 = alias_db.get_alias("test/metric/2", device_id="device1")
            alias3 = alias_db.get_alias("test/metric/3", device_id="device2")

            # Make database read-only
            db_path.chmod(0o444)

            try:
                # Cached lookups should still work from memory
                assert alias_db.get_alias("test/metric/1", device_id="device1") == alias1
                assert alias_db.get_alias("test/metric/2", device_id="device1") == alias2
                assert alias_db.get_alias("test/metric/3", device_id="device2") == alias3

                # Reverse lookup should also work
                assert alias_db.get_path(alias1) == "test/metric/1"
                assert alias_db.get_path(alias2) == "test/metric/2"

                # Count should reflect cached entries
                assert alias_db.count == 3

                # Iteration should work from cache
                cached_entries = list(alias_db.iter_all())
                assert len(cached_entries) == 3
            finally:
                # Restore write permissions for cleanup
                db_path.chmod(0o644)


@pytest.mark.chaos
class TestLastPublishedHashesDiskFailure:
    """Tests for LastPublishedHashes behavior during disk failures."""

    def test_hashes_cache_works_without_persistence(
        self,
        sample_metric: ContextMetric,
    ) -> None:
        """Verify hash tracking works in memory-only mode (db_path=None).

        When no db_path is provided, the hash tracker should operate purely
        in-memory without any disk I/O.
        """
        # Initialize without persistence
        hashes = LastPublishedHashes(db_path=None)

        topic = "test/enterprise/site/area/line/asset/context/submodel/path"

        # First check - should indicate changed (never published)
        assert hashes.has_changed(topic, sample_metric) is True

        # Update the hash
        hashes.update(topic, sample_metric)

        # Now should indicate not changed
        assert hashes.has_changed(topic, sample_metric) is False

        # Verify count
        assert hashes.count == 1

        # Create a different metric
        changed_metric = ContextMetric(
            path=sample_metric.path,
            value=30.0,  # Different value
            aas_type=sample_metric.aas_type,
            value_type=sample_metric.value_type,
            semantic_id=sample_metric.semantic_id,
            unit=sample_metric.unit,
            aas_source=sample_metric.aas_source,
            timestamp_ms=sample_metric.timestamp_ms,
        )

        # Should detect the change
        assert hashes.has_changed(topic, changed_metric) is True

        # Clear should work
        hashes.clear()
        assert hashes.count == 0
        assert hashes.has_changed(topic, sample_metric) is True

    def test_hashes_graceful_persistence_failure(
        self,
        sample_metric: ContextMetric,
    ) -> None:
        """Verify memory cache continues working after disk becomes read-only.

        Tests that even when persistence fails, the in-memory cache continues
        to function correctly for deduplication.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "hashes.db"

            # Initialize with persistence
            hashes = LastPublishedHashes(db_path=db_path)

            topic1 = "test/topic/1"
            topic2 = "test/topic/2"

            # Store some initial hashes (with persistence)
            hashes.update(topic1, sample_metric)
            assert hashes.has_changed(topic1, sample_metric) is False

            # Make database read-only
            db_path.chmod(0o444)

            try:
                # Memory cache should still work for existing entries
                assert hashes.has_changed(topic1, sample_metric) is False

                # The in-memory check for a new topic should work
                assert hashes.has_changed(topic2, sample_metric) is True

                # Attempting to update should fail due to read-only db,
                # but the memory cache update happens first
                # Note: The actual behavior depends on implementation -
                # if it updates memory first, cache will be updated even on db failure
                try:
                    hashes.update(topic2, sample_metric)
                    # If we get here, memory was updated before db error
                    # Check that memory cache reflects the update
                    assert hashes.has_changed(topic2, sample_metric) is False
                except (sqlite3.OperationalError, PermissionError):
                    # If error is raised, memory cache may or may not be updated
                    # depending on implementation order
                    pass

                # Filter operation should work on memory cache
                metrics = {topic1: sample_metric}
                filtered = hashes.filter_changed(metrics)
                assert len(filtered) == 0  # topic1 hasn't changed

            finally:
                # Restore write permissions for cleanup
                db_path.chmod(0o644)

    def test_hashes_batch_operations_in_memory(
        self,
        sample_metric: ContextMetric,
    ) -> None:
        """Verify batch operations work correctly in memory-only mode."""
        hashes = LastPublishedHashes(db_path=None)

        # Create multiple metrics
        metrics = {
            f"test/topic/{i}": ContextMetric(
                path=f"path.{i}",
                value=i * 10.0,
                aas_type="Property",
                value_type="xs:double",
                semantic_id=sample_metric.semantic_id,
                unit="unit",
                aas_source="test.aasx",
                timestamp_ms=1706400000000,
            )
            for i in range(5)
        }

        # All should be marked as changed initially
        filtered = hashes.filter_changed(metrics)
        assert len(filtered) == 5

        # Batch update
        hashes.update_batch(metrics)

        # Now none should be marked as changed
        filtered = hashes.filter_changed(metrics)
        assert len(filtered) == 0

        assert hashes.count == 5


@pytest.mark.chaos
class TestBirthCacheDiskFailure:
    """Tests for BirthCache behavior during disk failures."""

    def test_birth_cache_read_after_write_failure(self) -> None:
        """Verify stored births can still be read after disk becomes read-only.

        Tests that cached birth messages remain readable even when the database
        becomes read-only.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "births.db"

            # Initialize and store some birth messages
            cache = BirthCache(db_path)

            nbirth_topic = "spBv1.0/group/NBIRTH/edge_node"
            nbirth_payload = b"\x08\x01\x10\x02\x18\x03"  # Sample protobuf bytes

            dbirth_topic = "spBv1.0/group/DBIRTH/edge_node/device1"
            dbirth_payload = b"\x08\x04\x10\x05\x18\x06"  # Sample protobuf bytes

            cache.store_nbirth(nbirth_topic, nbirth_payload)
            cache.store_dbirth("device1", dbirth_topic, dbirth_payload)

            # Make database read-only
            db_path.chmod(0o444)

            try:
                # Should still be able to read cached births
                result = cache.get_nbirth()
                assert result is not None
                assert result[0] == nbirth_topic
                assert result[1] == nbirth_payload

                result = cache.get_dbirth("device1")
                assert result is not None
                assert result[0] == dbirth_topic
                assert result[1] == dbirth_payload

                # Should be able to list device IDs
                device_ids = cache.get_all_dbirth_device_ids()
                assert "device1" in device_ids

                # Non-existent device should return None
                assert cache.get_dbirth("nonexistent") is None

            finally:
                # Restore write permissions for cleanup
                db_path.chmod(0o644)

    def test_birth_cache_write_fails_on_readonly(self) -> None:
        """Verify write operations fail appropriately on read-only database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "births.db"

            # Initialize the cache
            cache = BirthCache(db_path)

            # Store initial data
            cache.store_nbirth("topic1", b"payload1")

            # Make database read-only
            db_path.chmod(0o444)

            try:
                # Write operations should fail
                with pytest.raises((sqlite3.OperationalError, PermissionError)):
                    cache.store_nbirth("topic2", b"payload2")

                with pytest.raises((sqlite3.OperationalError, PermissionError)):
                    cache.store_dbirth("device2", "topic3", b"payload3")

            finally:
                # Restore write permissions for cleanup
                db_path.chmod(0o644)

    def test_birth_cache_survives_partial_writes(self) -> None:
        """Verify cache integrity after failed write attempts.

        Ensures that failed write operations don't corrupt existing cached data.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "births.db"

            # Initialize and populate cache
            cache = BirthCache(db_path)

            # Store multiple birth messages
            cache.store_nbirth("spBv1.0/group/NBIRTH/node", b"nbirth_data")
            cache.store_dbirth("device1", "spBv1.0/group/DBIRTH/node/device1", b"dbirth1")
            cache.store_dbirth("device2", "spBv1.0/group/DBIRTH/node/device2", b"dbirth2")

            # Make read-only
            db_path.chmod(0o444)

            try:
                # Attempt writes that will fail
                try:
                    cache.store_dbirth("device3", "topic", b"data")
                except (sqlite3.OperationalError, PermissionError):
                    pass

                # Verify original data is intact
                assert cache.get_nbirth() is not None
                assert cache.get_dbirth("device1") is not None
                assert cache.get_dbirth("device2") is not None

                device_ids = cache.get_all_dbirth_device_ids()
                assert len(device_ids) == 2
                assert "device1" in device_ids
                assert "device2" in device_ids

            finally:
                db_path.chmod(0o644)

    def test_birth_cache_database_recreation(self) -> None:
        """Verify cache can be recreated from disk after restart."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "births.db"

            # Create and populate first cache instance
            cache1 = BirthCache(db_path)
            cache1.store_nbirth("spBv1.0/group/NBIRTH/node", b"nbirth_persistent")
            cache1.store_dbirth("device1", "spBv1.0/group/DBIRTH/node/device1", b"dbirth_persistent")

            # Simulate restart by creating new cache instance
            cache2 = BirthCache(db_path)

            # Data should persist across instances
            result = cache2.get_nbirth()
            assert result is not None
            assert result[1] == b"nbirth_persistent"

            result = cache2.get_dbirth("device1")
            assert result is not None
            assert result[1] == b"dbirth_persistent"
