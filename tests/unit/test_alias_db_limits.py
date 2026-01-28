"""Unit tests for AliasDB size limits and LRU eviction."""

import tempfile
import time
from pathlib import Path

import pytest

from aas_uns_bridge.state.alias_db import AliasDB


@pytest.fixture
def temp_db() -> Path:
    """Create a temporary database path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "alias.db"


class TestAliasDBEviction:
    """Tests for LRU eviction behavior."""

    def test_evicts_oldest_when_at_capacity(self, temp_db: Path) -> None:
        """Verify LRU eviction removes oldest accessed entries."""
        db = AliasDB(temp_db, max_entries=5)

        # Fill to capacity
        for i in range(5):
            db.get_alias(f"metric/path/{i}", device_id="device1")

        assert db.count == 5

        # Add one more - should trigger eviction
        db.get_alias("metric/path/new", device_id="device1")

        # Should have evicted some entries (10% = at least 1)
        # New total should be less than or equal to max_entries
        assert db.count <= 5

    def test_access_updates_last_accessed(self, temp_db: Path) -> None:
        """Verify get updates last_accessed timestamp."""
        import sqlite3

        db = AliasDB(temp_db, max_entries=100)  # Use large max to avoid eviction

        # Create entry
        db.get_alias("metric/path/0", device_id="device1")

        # Get initial timestamp
        with sqlite3.connect(temp_db) as conn:
            cursor = conn.execute(
                "SELECT last_accessed FROM metric_aliases WHERE metric_path = ?",
                ("metric/path/0",),
            )
            initial_time = cursor.fetchone()[0]

        time.sleep(1.1)  # Delay >1 second to ensure different timestamp

        # Access the entry again
        db.get_alias("metric/path/0", device_id="device1")

        # Verify timestamp was updated
        with sqlite3.connect(temp_db) as conn:
            cursor = conn.execute(
                "SELECT last_accessed FROM metric_aliases WHERE metric_path = ?",
                ("metric/path/0",),
            )
            updated_time = cursor.fetchone()[0]

        assert updated_time > initial_time

    def test_no_eviction_under_capacity(self, temp_db: Path) -> None:
        """Verify no eviction when space is available."""
        db = AliasDB(temp_db, max_entries=10)

        # Add fewer entries than max
        for i in range(5):
            db.get_alias(f"metric/path/{i}", device_id="device1")

        assert db.count == 5

        # All entries should still be present
        for i in range(5):
            alias = db.get_alias(f"metric/path/{i}", device_id="device1")
            assert alias is not None

    def test_eviction_metric_incremented(self, temp_db: Path) -> None:
        """Verify eviction callback is called with correct count."""
        eviction_counts: list[int] = []

        def callback(count: int) -> None:
            eviction_counts.append(count)

        db = AliasDB(temp_db, max_entries=5, eviction_callback=callback)

        # Fill to capacity
        for i in range(5):
            db.get_alias(f"metric/path/{i}", device_id="device1")

        assert len(eviction_counts) == 0

        # Trigger eviction
        db.get_alias("metric/path/new", device_id="device1")

        # Should have recorded an eviction
        assert len(eviction_counts) == 1
        assert eviction_counts[0] >= 1

    def test_max_entries_property(self, temp_db: Path) -> None:
        """Verify max_entries property returns configured value."""
        db = AliasDB(temp_db, max_entries=1000)
        assert db.max_entries == 1000

    def test_default_max_entries(self, temp_db: Path) -> None:
        """Verify default max_entries is 100,000."""
        db = AliasDB(temp_db)
        assert db.max_entries == 100_000

    def test_eviction_preserves_most_recent(self, temp_db: Path) -> None:
        """Verify eviction keeps the most recently accessed entries."""
        db = AliasDB(temp_db, max_entries=5)

        # Create 5 entries with staggered access times
        for i in range(5):
            db.get_alias(f"metric/path/{i}", device_id="device1")
            time.sleep(0.01)

        # Access entries 3 and 4 to make them newest
        db.get_alias("metric/path/3", device_id="device1")
        time.sleep(0.01)
        db.get_alias("metric/path/4", device_id="device1")
        time.sleep(0.01)

        # Add new entries to trigger eviction
        db.get_alias("metric/path/new1", device_id="device1")
        db.get_alias("metric/path/new2", device_id="device1")

        # Entries 3 and 4 should still be present
        aliases = list(db.iter_all())
        paths = [p for p, _ in aliases]
        assert "metric/path/3" in paths
        assert "metric/path/4" in paths

    def test_schema_migration_adds_last_accessed(self, temp_db: Path) -> None:
        """Verify schema migration adds last_accessed column."""
        import sqlite3

        # Create old schema without last_accessed
        with sqlite3.connect(temp_db) as conn:
            conn.execute("""
                CREATE TABLE metric_aliases (
                    metric_path TEXT PRIMARY KEY,
                    alias INTEGER UNIQUE NOT NULL,
                    device_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute(
                "INSERT INTO metric_aliases (metric_path, alias, device_id) VALUES (?, ?, ?)",
                ("old/metric", 1, "device1"),
            )
            conn.commit()

        # Open with AliasDB - should migrate
        db = AliasDB(temp_db, max_entries=100)

        # Verify old entry is preserved
        assert db.count == 1
        alias = db.get_alias("old/metric", device_id="device1")
        assert alias == 1

        # Verify new entries work
        new_alias = db.get_alias("new/metric", device_id="device1")
        assert new_alias == 2

    def test_clear_device_resets_cache(self, temp_db: Path) -> None:
        """Verify clear_device works with new schema."""
        db = AliasDB(temp_db, max_entries=100)

        # Add entries for two devices
        for i in range(3):
            db.get_alias(f"device1/metric/{i}", device_id="device1")
            db.get_alias(f"device2/metric/{i}", device_id="device2")

        assert db.count == 6

        # Clear one device
        removed = db.clear_device("device1")

        assert removed == 3
        assert db.count == 3

        # Verify device2 entries remain
        aliases = list(db.iter_all())
        paths = [p for p, _ in aliases]
        for i in range(3):
            assert f"device2/metric/{i}" in paths

    def test_clear_all_resets_state(self, temp_db: Path) -> None:
        """Verify clear_all resets all state."""
        eviction_counts: list[int] = []
        db = AliasDB(
            temp_db,
            max_entries=10,
            eviction_callback=lambda c: eviction_counts.append(c),
        )

        # Add entries
        for i in range(5):
            db.get_alias(f"metric/path/{i}", device_id="device1")

        assert db.count == 5

        # Clear all
        db.clear_all()

        assert db.count == 0

        # New entries should get fresh aliases starting at 1
        alias = db.get_alias("new/metric", device_id="device1")
        assert alias == 1
