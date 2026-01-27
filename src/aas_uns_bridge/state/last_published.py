"""Hash-based deduplication for published metrics."""

import hashlib
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

from aas_uns_bridge.domain.models import ContextMetric

logger = logging.getLogger(__name__)


class LastPublishedHashes:
    """Track hashes of published metrics to avoid redundant publishes.

    Computes SHA256 hashes of metric payloads and skips publishing
    if the value hasn't changed since the last publish.
    """

    def __init__(self, db_path: Path | None = None):
        """Initialize the hash tracker.

        Args:
            db_path: Optional path for persistent storage.
                    If None, uses in-memory storage only.
        """
        self.db_path = db_path
        self._cache: dict[str, str] = {}
        self._persist = db_path is not None

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
            conn.execute("""
                CREATE TABLE IF NOT EXISTS published_hashes (
                    topic TEXT PRIMARY KEY,
                    hash TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                )
            """)
            conn.commit()

    def _load_cache(self) -> None:
        """Load hashes from database into memory."""
        assert self.db_path is not None
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT topic, hash FROM published_hashes")
            for topic, hash_value in cursor:
                self._cache[topic] = hash_value
        logger.debug("Loaded %d hashes from database", len(self._cache))

    def _compute_hash(self, value: Any) -> str:
        """Compute SHA256 hash of a value."""
        # Serialize to JSON for consistent hashing
        serialized = json.dumps(value, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def has_changed(self, topic: str, metric: ContextMetric) -> bool:
        """Check if a metric has changed since last publish.

        Args:
            topic: MQTT topic for the metric.
            metric: The metric to check.

        Returns:
            True if the value has changed or never published.
        """
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
        import time

        current_hash = self._compute_hash(metric.value)
        self._cache[topic] = current_hash

        if self._persist:
            assert self.db_path is not None
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO published_hashes (topic, hash, updated_at)
                    VALUES (?, ?, ?)
                    """,
                    (topic, current_hash, int(time.time())),
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
        import time

        now = int(time.time())

        for topic, metric in topic_metrics.items():
            current_hash = self._compute_hash(metric.value)
            self._cache[topic] = current_hash

        if self._persist and topic_metrics:
            assert self.db_path is not None
            with sqlite3.connect(self.db_path) as conn:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO published_hashes (topic, hash, updated_at)
                    VALUES (?, ?, ?)
                    """,
                    [
                        (topic, self._compute_hash(metric.value), now)
                        for topic, metric in topic_metrics.items()
                    ],
                )
                conn.commit()

    def clear(self) -> None:
        """Clear all stored hashes."""
        self._cache.clear()
        if self._persist:
            assert self.db_path is not None
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM published_hashes")
                conn.commit()
        logger.info("Cleared published hashes")

    @property
    def count(self) -> int:
        """Number of tracked topics."""
        return len(self._cache)
