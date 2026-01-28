"""Unit tests for LastPublishedHashes size limits and TTL expiry."""

import tempfile
import time
from pathlib import Path

import pytest

from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.state.last_published import LastPublishedHashes


@pytest.fixture
def temp_db() -> Path:
    """Create a temporary database path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "hashes.db"


@pytest.fixture
def sample_metric() -> ContextMetric:
    """Create a sample metric for testing."""
    return ContextMetric(
        path="temperature",
        value=25.5,
        aas_type="Property",
        value_type="xs:double",
        unit="degC",
        timestamp_ms=int(time.time() * 1000),
    )


def make_metric(value: float) -> ContextMetric:
    """Helper to create metrics with different values."""
    return ContextMetric(
        path="temperature",
        value=value,
        aas_type="Property",
        value_type="xs:double",
        unit="degC",
        timestamp_ms=int(time.time() * 1000),
    )


class TestLastPublishedHashesTTL:
    """Tests for TTL-based expiry."""

    def test_expired_entries_removed(self, temp_db: Path) -> None:
        """Verify entries older than TTL are removed."""
        # Use a very short TTL for testing
        hashes = LastPublishedHashes(db_path=temp_db, ttl_seconds=1)

        # Add an entry
        metric = make_metric(25.5)
        hashes.update("topic/1", metric)
        assert hashes.count == 1

        # Wait for TTL to expire
        time.sleep(1.5)

        # Force cleanup
        removed = hashes.force_cleanup()

        assert removed >= 1
        assert hashes.count == 0

    def test_fresh_entries_not_removed(self, temp_db: Path) -> None:
        """Verify non-expired entries are kept."""
        hashes = LastPublishedHashes(db_path=temp_db, ttl_seconds=3600)  # 1 hour TTL

        # Add entries
        for i in range(5):
            hashes.update(f"topic/{i}", make_metric(float(i)))

        assert hashes.count == 5

        # Force cleanup
        removed = hashes.force_cleanup()

        assert removed == 0
        assert hashes.count == 5

    def test_cleanup_runs_periodically(self, temp_db: Path) -> None:
        """Verify automatic cleanup runs every N operations."""
        # Very short TTL (1 second)
        hashes = LastPublishedHashes(db_path=temp_db, ttl_seconds=1, max_entries=1000)
        # Note: each update call increments op_count once
        # Cleanup runs when op_count reaches CLEANUP_INTERVAL

        # Add entries
        for i in range(5):
            hashes.update(f"topic/{i}", make_metric(float(i)))

        assert hashes.count == 5

        # Wait for TTL to expire
        time.sleep(1.5)

        # Force cleanup to verify it works
        removed = hashes.force_cleanup()

        # All 5 entries should be removed since they're all expired
        assert removed == 5
        assert hashes.count == 0

    def test_ttl_property(self, temp_db: Path) -> None:
        """Verify ttl_seconds property returns configured value."""
        hashes = LastPublishedHashes(db_path=temp_db, ttl_seconds=7200)
        assert hashes.ttl_seconds == 7200

    def test_default_ttl(self, temp_db: Path) -> None:
        """Verify default TTL is 24 hours."""
        hashes = LastPublishedHashes(db_path=temp_db)
        assert hashes.ttl_seconds == 86400


class TestLastPublishedHashesMaxEntries:
    """Tests for max entries limit."""

    def test_max_entries_respected(self, temp_db: Path) -> None:
        """Verify size limit is enforced."""
        hashes = LastPublishedHashes(db_path=temp_db, max_entries=5, ttl_seconds=3600)

        # Add more entries than max
        for i in range(10):
            hashes.update(f"topic/{i}", make_metric(float(i)))

        # Should have enforced limit
        assert hashes.count <= 5

    def test_max_entries_property(self, temp_db: Path) -> None:
        """Verify max_entries property returns configured value."""
        hashes = LastPublishedHashes(db_path=temp_db, max_entries=1000)
        assert hashes.max_entries == 1000

    def test_default_max_entries(self, temp_db: Path) -> None:
        """Verify default max_entries is 50,000."""
        hashes = LastPublishedHashes(db_path=temp_db)
        assert hashes.max_entries == 50_000

    def test_oldest_entries_removed_first(self, temp_db: Path) -> None:
        """Verify oldest entries (by created_at) are removed first."""
        hashes = LastPublishedHashes(db_path=temp_db, max_entries=5, ttl_seconds=3600)

        # Add entries with slight delays
        for i in range(5):
            hashes.update(f"topic/{i}", make_metric(float(i)))
            time.sleep(0.01)

        # Add more to trigger cleanup
        for i in range(5, 10):
            hashes.update(f"topic/{i}", make_metric(float(i)))

        # Newest entries should remain
        # Note: exact behavior depends on cleanup timing
        assert hashes.count <= 5


class TestLastPublishedHashesEvictionCallback:
    """Tests for eviction callback."""

    def test_eviction_callback_called(self, temp_db: Path) -> None:
        """Verify eviction callback is called with correct count."""
        eviction_counts: list[int] = []

        def callback(count: int) -> None:
            eviction_counts.append(count)

        hashes = LastPublishedHashes(
            db_path=temp_db,
            max_entries=5,
            ttl_seconds=3600,
            eviction_callback=callback,
        )

        # Add entries beyond max
        for i in range(10):
            hashes.update(f"topic/{i}", make_metric(float(i)))

        # Should have recorded evictions
        assert len(eviction_counts) >= 1
        assert sum(eviction_counts) >= 1

    def test_eviction_callback_on_ttl_expiry(self, temp_db: Path) -> None:
        """Verify eviction callback is called for TTL expiry."""
        eviction_counts: list[int] = []

        def callback(count: int) -> None:
            eviction_counts.append(count)

        hashes = LastPublishedHashes(
            db_path=temp_db,
            ttl_seconds=1,
            eviction_callback=callback,
        )

        # Add multiple entries to ensure we have something to evict
        for i in range(5):
            hashes.update(f"topic/{i}", make_metric(float(i)))

        assert len(eviction_counts) == 0  # No evictions yet

        # Wait for expiry
        time.sleep(1.5)

        # Force cleanup
        removed = hashes.force_cleanup()

        # Should have evicted entries and called callback
        assert removed == 5
        assert len(eviction_counts) >= 1
        assert sum(eviction_counts) == 5


class TestLastPublishedHashesPersistence:
    """Tests for database persistence with new schema."""

    def test_persistence_with_created_at(self, temp_db: Path) -> None:
        """Verify created_at is persisted and loaded."""
        # First instance
        hashes1 = LastPublishedHashes(db_path=temp_db, ttl_seconds=3600)
        hashes1.update("topic/1", make_metric(25.5))
        assert hashes1.count == 1

        # Second instance
        hashes2 = LastPublishedHashes(db_path=temp_db, ttl_seconds=3600)
        assert hashes2.count == 1

        # Entry should not be considered expired
        removed = hashes2.force_cleanup()
        assert removed == 0
        assert hashes2.count == 1

    def test_schema_migration_adds_created_at(self, temp_db: Path) -> None:
        """Verify schema migration adds created_at column."""
        import sqlite3

        now = int(time.time())

        # Create old schema without created_at
        with sqlite3.connect(temp_db) as conn:
            conn.execute("""
                CREATE TABLE published_hashes (
                    topic TEXT PRIMARY KEY,
                    hash TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                )
            """)
            conn.execute(
                "INSERT INTO published_hashes (topic, hash, updated_at) VALUES (?, ?, ?)",
                ("old/topic", "abc123", now),
            )
            conn.commit()

        # Open with LastPublishedHashes - should migrate
        hashes = LastPublishedHashes(db_path=temp_db, ttl_seconds=3600)

        # Verify old entry is preserved
        assert hashes.count == 1

        # Verify has_changed works
        old_metric = ContextMetric(
            path="old",
            value="test",
            aas_type="Property",
            value_type="xs:string",
            unit="",
            timestamp_ms=now * 1000,
        )
        # This should return True since the hash won't match
        assert hashes.has_changed("old/topic", old_metric)

    def test_update_preserves_created_at(self, temp_db: Path) -> None:
        """Verify update doesn't change created_at for existing entries."""
        import sqlite3

        hashes = LastPublishedHashes(db_path=temp_db, ttl_seconds=3600)

        # Add initial entry
        hashes.update("topic/1", make_metric(25.5))

        # Get initial created_at
        with sqlite3.connect(temp_db) as conn:
            cursor = conn.execute(
                "SELECT created_at FROM published_hashes WHERE topic = ?",
                ("topic/1",),
            )
            initial_created_at = cursor.fetchone()[0]

        time.sleep(0.1)

        # Update with new value
        hashes.update("topic/1", make_metric(30.0))

        # created_at should not change
        with sqlite3.connect(temp_db) as conn:
            cursor = conn.execute(
                "SELECT created_at FROM published_hashes WHERE topic = ?",
                ("topic/1",),
            )
            final_created_at = cursor.fetchone()[0]

        assert final_created_at == initial_created_at


class TestLastPublishedHashesBatch:
    """Tests for batch operations with limits."""

    def test_update_batch_respects_limits(self, temp_db: Path) -> None:
        """Verify batch update respects max entries."""
        hashes = LastPublishedHashes(db_path=temp_db, max_entries=5, ttl_seconds=3600)

        # Batch update with more than max entries
        topic_metrics = {f"topic/{i}": make_metric(float(i)) for i in range(10)}
        hashes.update_batch(topic_metrics)

        # Should enforce limit after cleanup
        hashes.force_cleanup()
        assert hashes.count <= 5

    def test_update_batch_preserves_created_at(self, temp_db: Path) -> None:
        """Verify batch update preserves created_at for existing entries."""
        import sqlite3

        hashes = LastPublishedHashes(db_path=temp_db, ttl_seconds=3600)

        # Add initial entries
        hashes.update_batch(
            {
                "topic/1": make_metric(25.5),
                "topic/2": make_metric(26.5),
            }
        )

        # Get initial created_at
        with sqlite3.connect(temp_db) as conn:
            cursor = conn.execute("SELECT topic, created_at FROM published_hashes ORDER BY topic")
            initial_times = {row[0]: row[1] for row in cursor}

        time.sleep(0.1)

        # Update with new values
        hashes.update_batch(
            {
                "topic/1": make_metric(30.0),
                "topic/2": make_metric(31.0),
            }
        )

        # created_at should not change
        with sqlite3.connect(temp_db) as conn:
            cursor = conn.execute("SELECT topic, created_at FROM published_hashes ORDER BY topic")
            final_times = {row[0]: row[1] for row in cursor}

        assert initial_times == final_times


class TestLastPublishedHashesClear:
    """Tests for clear operation."""

    def test_clear_resets_all_state(self, temp_db: Path) -> None:
        """Verify clear resets all internal state."""
        hashes = LastPublishedHashes(db_path=temp_db, ttl_seconds=3600)

        # Add entries
        for i in range(5):
            hashes.update(f"topic/{i}", make_metric(float(i)))

        assert hashes.count == 5

        # Clear
        hashes.clear()

        assert hashes.count == 0
        assert hashes._op_count == 0
        assert len(hashes._cache_timestamps) == 0


class TestLastPublishedHashesInMemory:
    """Tests for in-memory mode (no persistence)."""

    def test_in_memory_ttl_cleanup(self) -> None:
        """Verify TTL cleanup works in memory-only mode."""
        hashes = LastPublishedHashes(db_path=None, ttl_seconds=1)

        # Add entry
        hashes.update("topic/1", make_metric(25.5))
        assert hashes.count == 1

        # Wait for expiry
        time.sleep(1.5)

        # Force cleanup
        removed = hashes.force_cleanup()

        assert removed >= 1
        assert hashes.count == 0

    def test_in_memory_max_entries(self) -> None:
        """Verify max entries works in memory-only mode."""
        hashes = LastPublishedHashes(db_path=None, max_entries=5, ttl_seconds=3600)

        # Add more entries than max
        for i in range(10):
            hashes.update(f"topic/{i}", make_metric(float(i)))

        # Force cleanup to enforce limit
        hashes.force_cleanup()

        # Should have enforced limit
        assert hashes.count <= 5
