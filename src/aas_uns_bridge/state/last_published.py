"""Hash-based deduplication for published metrics."""

import hashlib
import json
import logging
import sqlite3
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from aas_uns_bridge.domain.models import ContextMetric

logger = logging.getLogger(__name__)


class LastPublishedHashes:
    """Track hashes of published metrics to avoid redundant publishes.

    Computes SHA256 hashes of metric payloads and skips publishing
    if the value hasn't changed since the last publish.

    Implements TTL-based expiry and max entries limit.
    """

    # Run cleanup every N operations
    CLEANUP_INTERVAL = 100

    def __init__(
        self,
        db_path: Path | None = None,
        max_entries: int = 50_000,
        ttl_seconds: int = 86400,
        eviction_callback: Callable[[int], None] | None = None,
    ):
        """Initialize the hash tracker.

        Args:
            db_path: Optional path for persistent storage.
                    If None, uses in-memory storage only.
            max_entries: Maximum number of entries before cleanup.
            ttl_seconds: Time-to-live in seconds (default: 24 hours).
            eviction_callback: Optional callback to record eviction counts.
        """
        self.db_path = db_path
        self._max_entries = max_entries
        self._ttl_seconds = ttl_seconds
        self._eviction_callback = eviction_callback
        self._cache: dict[str, str] = {}
        self._cache_timestamps: dict[str, int] = {}
        self._persist = db_path is not None
        self._op_count = 0

        if self._persist:
            assert self.db_path is not None
            db_path = self.db_path
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self._init_db()
            self._load_cache()

    def _init_db(self) -> None:
        """Initialize the database schema."""
        assert self.db_path is not None
        with sqlite3.connect(self.db_path) as conn:
            # Check if table exists
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='published_hashes'"
            )
            table_exists = cursor.fetchone() is not None

            if not table_exists:
                # Create new table with full schema
                conn.execute("""
                    CREATE TABLE published_hashes (
                        topic TEXT PRIMARY KEY,
                        hash TEXT NOT NULL,
                        updated_at INTEGER NOT NULL,
                        created_at INTEGER NOT NULL DEFAULT 0
                    )
                """)
                conn.execute("""
                    CREATE INDEX idx_created_at ON published_hashes(created_at)
                """)
            else:
                # Migrate schema if created_at column doesn't exist
                self._migrate_schema(conn)

            conn.commit()

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        """Migrate schema if created_at column doesn't exist."""
        cursor = conn.execute("PRAGMA table_info(published_hashes)")
        columns = {row[1] for row in cursor}
        if "created_at" not in columns:
            logger.info("Migrating last_published schema: adding created_at column")
            conn.execute(
                "ALTER TABLE published_hashes ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_created_at ON published_hashes(created_at)"
            )
            # Set created_at to updated_at for existing entries
            conn.execute("UPDATE published_hashes SET created_at = updated_at")
            conn.commit()

    def _load_cache(self) -> None:
        """Load hashes from database into memory."""
        assert self.db_path is not None
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT topic, hash, created_at FROM published_hashes")
            for topic, hash_value, created_at in cursor:
                self._cache[topic] = hash_value
                self._cache_timestamps[topic] = created_at or 0
        logger.debug("Loaded %d hashes from database", len(self._cache))

    def _compute_hash(self, value: Any) -> str:
        """Compute SHA256 hash of a value."""
        # Serialize to JSON for consistent hashing
        serialized = json.dumps(value, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def _maybe_cleanup(self) -> None:
        """Run cleanup periodically based on operation count."""
        self._op_count += 1
        if self._op_count >= self.CLEANUP_INTERVAL:
            self._op_count = 0
            self._cleanup_expired()

    def _cleanup_expired(self) -> int:
        """Remove expired entries based on TTL.

        Returns:
            Number of entries removed.
        """
        now = int(time.time())
        cutoff = now - self._ttl_seconds
        expired_topics = []

        # Find expired entries in cache
        for topic, created_at in list(self._cache_timestamps.items()):
            if created_at > 0 and created_at <= cutoff:
                expired_topics.append(topic)

        # Remove from cache
        for topic in expired_topics:
            self._cache.pop(topic, None)
            self._cache_timestamps.pop(topic, None)

        # Remove from database
        if self._persist and expired_topics:
            assert self.db_path is not None
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "DELETE FROM published_hashes WHERE created_at > 0 AND created_at <= ?",
                    (cutoff,),
                )
                conn.commit()

        if expired_topics:
            logger.debug("Cleaned up %d expired hash entries", len(expired_topics))
            if self._eviction_callback:
                self._eviction_callback(len(expired_topics))

        # Also check max entries limit
        self._enforce_max_entries()

        return len(expired_topics)

    def _enforce_max_entries(self) -> int:
        """Enforce max entries limit by removing oldest entries.

        Returns:
            Number of entries removed.
        """
        if len(self._cache) <= self._max_entries:
            return 0

        # Calculate how many to remove
        excess = len(self._cache) - self._max_entries

        # Sort by created_at and remove oldest
        sorted_topics = sorted(
            self._cache_timestamps.items(),
            key=lambda x: x[1],
        )
        topics_to_remove = [t[0] for t in sorted_topics[:excess]]

        for topic in topics_to_remove:
            self._cache.pop(topic, None)
            self._cache_timestamps.pop(topic, None)

        if self._persist and topics_to_remove:
            assert self.db_path is not None
            placeholders = ",".join("?" * len(topics_to_remove))
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    f"DELETE FROM published_hashes WHERE topic IN ({placeholders})",
                    topics_to_remove,
                )
                conn.commit()

        if topics_to_remove:
            logger.debug("Enforced max entries limit, removed %d entries", len(topics_to_remove))
            if self._eviction_callback:
                self._eviction_callback(len(topics_to_remove))

        return len(topics_to_remove)

    def has_changed(self, topic: str, metric: ContextMetric) -> bool:
        """Check if a metric has changed since last publish.

        Args:
            topic: MQTT topic for the metric.
            metric: The metric to check.

        Returns:
            True if the value has changed or never published.
        """
        self._maybe_cleanup()

        # Compute hash of value only (not timestamp)
        current_hash = self._compute_hash(metric.value)
        previous_hash = self._cache.get(topic)

        return previous_hash != current_hash

    def update(self, topic: str, metric: ContextMetric) -> None:
        """Update the stored hash for a metric.

        Args:
            topic: MQTT topic for the metric.
            metric: The published metric.
        """
        self._maybe_cleanup()

        now = int(time.time())
        current_hash = self._compute_hash(metric.value)

        # Check if this is a new entry
        is_new = topic not in self._cache

        self._cache[topic] = current_hash

        # Only set created_at for new entries
        if is_new:
            self._cache_timestamps[topic] = now
            # Check max entries when adding new entries
            self._enforce_max_entries()

        if self._persist:
            assert self.db_path is not None
            with sqlite3.connect(self.db_path) as conn:
                # Check if entry exists
                cursor = conn.execute(
                    "SELECT created_at FROM published_hashes WHERE topic = ?",
                    (topic,),
                )
                row = cursor.fetchone()
                created_at = row[0] if row else now

                conn.execute(
                    """
                    INSERT OR REPLACE INTO published_hashes (topic, hash, updated_at, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (topic, current_hash, now, created_at),
                )
                conn.commit()

    def filter_changed(
        self,
        topic_metrics: dict[str, ContextMetric],
    ) -> dict[str, ContextMetric]:
        """Filter to only metrics that have changed.

        Args:
            topic_metrics: Mapping of topics to metrics.

        Returns:
            Filtered dict with only changed metrics.
        """
        return {
            topic: metric
            for topic, metric in topic_metrics.items()
            if self.has_changed(topic, metric)
        }

    def update_batch(self, topic_metrics: dict[str, ContextMetric]) -> None:
        """Update hashes for multiple metrics.

        Args:
            topic_metrics: Mapping of topics to published metrics.
        """
        self._maybe_cleanup()

        now = int(time.time())
        new_entries_added = False

        for topic, metric in topic_metrics.items():
            current_hash = self._compute_hash(metric.value)
            is_new = topic not in self._cache
            self._cache[topic] = current_hash
            # Only set created_at for new entries
            if is_new:
                self._cache_timestamps[topic] = now
                new_entries_added = True

        if self._persist and topic_metrics:
            assert self.db_path is not None
            with sqlite3.connect(self.db_path) as conn:
                # Get existing created_at values
                topics = list(topic_metrics.keys())
                placeholders = ",".join("?" * len(topics))
                query = (
                    "SELECT topic, created_at FROM published_hashes "
                    f"WHERE topic IN ({placeholders})"
                )
                cursor = conn.execute(query, topics)
                existing_created = {row[0]: row[1] for row in cursor}

                conn.executemany(
                    """
                    INSERT OR REPLACE INTO published_hashes (topic, hash, updated_at, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    [
                        (
                            topic,
                            self._compute_hash(metric.value),
                            now,
                            existing_created.get(topic, now),
                        )
                        for topic, metric in topic_metrics.items()
                    ],
                )
                conn.commit()

        # Check max entries if we added new entries
        if new_entries_added:
            self._enforce_max_entries()

    def clear(self) -> None:
        """Clear all stored hashes."""
        self._cache.clear()
        self._cache_timestamps.clear()
        self._op_count = 0
        if self._persist:
            assert self.db_path is not None
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM published_hashes")
                conn.commit()
        logger.info("Cleared published hashes")

    def force_cleanup(self) -> int:
        """Force immediate cleanup of expired entries.

        Returns:
            Number of entries removed.
        """
        return self._cleanup_expired()

    @property
    def count(self) -> int:
        """Number of tracked topics."""
        return len(self._cache)

    @property
    def max_entries(self) -> int:
        """Maximum number of entries before cleanup."""
        return self._max_entries

    @property
    def ttl_seconds(self) -> int:
        """Time-to-live in seconds."""
        return self._ttl_seconds
