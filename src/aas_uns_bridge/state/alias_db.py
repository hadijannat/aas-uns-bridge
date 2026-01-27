"""SQLite-backed alias database for Sparkplug metric aliases."""

import logging
import sqlite3
import threading
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)


class AliasDB:
    """Persistent storage for Sparkplug metric aliases.

    Sparkplug B uses numeric aliases to reduce bandwidth after birth messages.
    This database maintains stable alias assignments across bridge restarts.
    """

    def __init__(self, db_path: Path):
        """Initialize the alias database.

        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = db_path
        self._lock = threading.Lock()
        self._cache: dict[str, int] = {}
        self._next_alias: int = 1

        # Ensure parent directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self._init_db()
        self._load_cache()

    def _init_db(self) -> None:
        """Initialize the database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metric_aliases (
                    metric_path TEXT PRIMARY KEY,
                    alias INTEGER UNIQUE NOT NULL,
                    device_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_alias ON metric_aliases(alias)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_device ON metric_aliases(device_id)
            """)
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
                return self._cache[metric_path]

            # Assign new alias
            alias = self._next_alias
            self._next_alias += 1

            # Persist to database
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT INTO metric_aliases (metric_path, alias, device_id) VALUES (?, ?, ?)",
                    (metric_path, alias, device_id),
                )
                conn.commit()

            self._cache[metric_path] = alias
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

        logger.info("Cleared all metric aliases")

    @property
    def count(self) -> int:
        """Number of aliases in the database."""
        with self._lock:
            return len(self._cache)
