"""SQLite-backed alias database for Sparkplug metric aliases."""

import logging
import os
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path

from aas_uns_bridge.observability.metrics import METRICS

logger = logging.getLogger(__name__)


class AliasDB:
    """Persistent storage for Sparkplug metric aliases.

    Sparkplug B uses numeric aliases to reduce bandwidth after birth messages.
    This database maintains stable alias assignments across bridge restarts.

    Implements LRU eviction when max_entries is reached.
    """

    def __init__(
        self,
        db_path: Path,
        max_entries: int = 100_000,
        eviction_callback: Callable[[int], None] | None = None,
    ):
        """Initialize the alias database.

        Args:
            db_path: Path to the SQLite database file.
            max_entries: Maximum number of entries before LRU eviction.
            eviction_callback: Optional callback to record eviction counts.
        """
        self.db_path = db_path
        self._max_entries = max_entries
        self._eviction_callback = eviction_callback
        self._lock = threading.Lock()
        self._cache: dict[str, int] = {}
        self._next_alias: int = 1

        # Ensure parent directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self._init_db()
        self._load_cache()
        self._report_db_size()

    def _init_db(self) -> None:
        """Initialize the database schema."""
        with sqlite3.connect(self.db_path) as conn:
            # Check if table exists
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='metric_aliases'"
            )
            table_exists = cursor.fetchone() is not None

            if not table_exists:
                # Create new table with full schema
                conn.execute("""
                    CREATE TABLE metric_aliases (
                        metric_path TEXT PRIMARY KEY,
                        alias INTEGER UNIQUE NOT NULL,
                        device_id TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_accessed INTEGER NOT NULL DEFAULT 0
                    )
                """)
                conn.execute("""
                    CREATE INDEX idx_alias ON metric_aliases(alias)
                """)
                conn.execute("""
                    CREATE INDEX idx_device ON metric_aliases(device_id)
                """)
                conn.execute("""
                    CREATE INDEX idx_last_accessed ON metric_aliases(last_accessed)
                """)
            else:
                # Migrate existing schema if needed
                self._migrate_schema(conn)

            conn.commit()

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        """Migrate schema if last_accessed column doesn't exist."""
        cursor = conn.execute("PRAGMA table_info(metric_aliases)")
        columns = {row[1] for row in cursor}
        if "last_accessed" not in columns:
            logger.info("Migrating alias_db schema: adding last_accessed column")
            conn.execute(
                "ALTER TABLE metric_aliases ADD COLUMN last_accessed INTEGER NOT NULL DEFAULT 0"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_last_accessed ON metric_aliases(last_accessed)"
            )
            conn.commit()

    def _load_cache(self) -> None:
        """Load existing aliases into memory cache."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT metric_path, alias FROM metric_aliases")
            for path, alias in cursor:
                self._cache[path] = alias
                if alias >= self._next_alias:
                    self._next_alias = alias + 1

        logger.info("Loaded %d metric aliases from database", len(self._cache))

    def _report_db_size(self) -> None:
        """Report the database file size to Prometheus metrics."""
        try:
            size = os.path.getsize(self.db_path)
            METRICS.state_db_size_bytes.labels(db_type="alias").set(size)
        except OSError:
            pass

    def _evict_if_needed(self, conn: sqlite3.Connection) -> int:
        """Evict oldest entries if at capacity.

        Args:
            conn: Active database connection.

        Returns:
            Number of entries evicted.
        """
        current_count = len(self._cache)
        if current_count < self._max_entries:
            return 0

        # Calculate how many to evict (evict 10% to avoid frequent evictions)
        evict_count = max(1, self._max_entries // 10)

        # Find oldest entries by last_accessed
        cursor = conn.execute(
            "SELECT metric_path FROM metric_aliases ORDER BY last_accessed ASC LIMIT ?",
            (evict_count,),
        )
        paths_to_evict = [row[0] for row in cursor]

        if paths_to_evict:
            placeholders = ",".join("?" * len(paths_to_evict))
            conn.execute(
                f"DELETE FROM metric_aliases WHERE metric_path IN ({placeholders})",
                paths_to_evict,
            )
            conn.commit()

            # Update cache
            for path in paths_to_evict:
                self._cache.pop(path, None)

            logger.debug("Evicted %d oldest aliases from database", len(paths_to_evict))

            # Record eviction metric
            if self._eviction_callback:
                self._eviction_callback(len(paths_to_evict))

        return len(paths_to_evict)

    def _update_access_time(self, conn: sqlite3.Connection, metric_path: str) -> None:
        """Update the last_accessed timestamp for a metric.

        Args:
            conn: Active database connection.
            metric_path: The metric path to update.
        """
        now = int(time.time())
        conn.execute(
            "UPDATE metric_aliases SET last_accessed = ? WHERE metric_path = ?",
            (now, metric_path),
        )
        conn.commit()

    def get_alias(self, metric_path: str, device_id: str | None = None) -> int:
        """Get or create an alias for a metric path.

        Args:
            metric_path: The full metric path (including device).
            device_id: Optional device identifier.

        Returns:
            Numeric alias for the metric.
        """
        with self._lock:
            if metric_path in self._cache:
                # Update access time for existing entry
                with sqlite3.connect(self.db_path) as conn:
                    self._update_access_time(conn, metric_path)
                return self._cache[metric_path]

            # Check capacity and evict if needed before inserting
            with sqlite3.connect(self.db_path) as conn:
                self._evict_if_needed(conn)

            # Assign new alias
            alias = self._next_alias
            self._next_alias += 1
            now = int(time.time())

            # Persist to database
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT INTO metric_aliases (metric_path, alias, device_id, last_accessed) "
                    "VALUES (?, ?, ?, ?)",
                    (metric_path, alias, device_id, now),
                )
                conn.commit()

            self._cache[metric_path] = alias
            self._report_db_size()
            logger.debug("Assigned alias %d to %s", alias, metric_path)
            return alias

    def get_path(self, alias: int) -> str | None:
        """Look up a metric path by alias.

        Args:
            alias: The numeric alias.

        Returns:
            Metric path or None if not found.
        """
        with self._lock:
            for path, a in self._cache.items():
                if a == alias:
                    return path
            return None

    def get_device_aliases(self, device_id: str) -> dict[str, int]:
        """Get all aliases for a specific device.

        Args:
            device_id: The device identifier.

        Returns:
            Dict mapping metric paths to aliases.
        """
        result: dict[str, int] = {}
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT metric_path, alias FROM metric_aliases WHERE device_id = ?",
                (device_id,),
            )
            for path, alias in cursor:
                result[path] = alias
        return result

    def iter_all(self) -> Iterator[tuple[str, int]]:
        """Iterate over all (path, alias) pairs.

        Yields:
            Tuples of (metric_path, alias).
        """
        with self._lock:
            yield from self._cache.items()

    def clear_device(self, device_id: str) -> int:
        """Remove all aliases for a device.

        Args:
            device_id: The device identifier.

        Returns:
            Number of aliases removed.
        """
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "DELETE FROM metric_aliases WHERE device_id = ?",
                    (device_id,),
                )
                conn.commit()
                count = cursor.rowcount

            # Reload cache
            self._cache.clear()
            self._next_alias = 1
            self._load_cache()

            return count

    def clear_all(self) -> None:
        """Remove all aliases from the database."""
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM metric_aliases")
                conn.commit()

            self._cache.clear()
            self._next_alias = 1

        self._report_db_size()
        logger.info("Cleared all metric aliases")

    @property
    def count(self) -> int:
        """Number of aliases in the database."""
        with self._lock:
            return len(self._cache)

    @property
    def max_entries(self) -> int:
        """Maximum number of entries before eviction."""
        return self._max_entries
