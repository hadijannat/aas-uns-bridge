"""Birth message caching for fast reconnection."""

import json
import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)


class BirthCache:
    """Cache for NBIRTH and DBIRTH payloads.

    Enables fast reconnection by storing serialized birth messages
    that can be republished without re-traversing AAS content.
    """

    def __init__(self, db_path: Path):
        """Initialize the birth cache.

        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS birth_cache (
                    key TEXT PRIMARY KEY,
                    payload BLOB NOT NULL,
                    topic TEXT NOT NULL,
                    timestamp INTEGER NOT NULL
                )
            """)
            conn.commit()

    def store_nbirth(self, topic: str, payload: bytes) -> None:
        """Store an NBIRTH payload.

        Args:
            topic: MQTT topic for the birth message.
            payload: Serialized payload bytes.
        """
        self._store("nbirth", topic, payload)
        logger.debug("Cached NBIRTH for topic %s", topic)

    def store_dbirth(self, device_id: str, topic: str, payload: bytes) -> None:
        """Store a DBIRTH payload.

        Args:
            device_id: Device identifier.
            topic: MQTT topic for the birth message.
            payload: Serialized payload bytes.
        """
        self._store(f"dbirth:{device_id}", topic, payload)
        logger.debug("Cached DBIRTH for device %s", device_id)

    def _store(self, key: str, topic: str, payload: bytes) -> None:
        """Store a payload in the cache."""
        import time
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO birth_cache (key, payload, topic, timestamp)
                VALUES (?, ?, ?, ?)
                """,
                (key, payload, topic, int(time.time())),
            )
            conn.commit()

    def get_nbirth(self) -> tuple[str, bytes] | None:
        """Get cached NBIRTH payload.

        Returns:
            Tuple of (topic, payload) or None if not cached.
        """
        return self._get("nbirth")

    def get_dbirth(self, device_id: str) -> tuple[str, bytes] | None:
        """Get cached DBIRTH payload.

        Args:
            device_id: Device identifier.

        Returns:
            Tuple of (topic, payload) or None if not cached.
        """
        return self._get(f"dbirth:{device_id}")

    def _get(self, key: str) -> tuple[str, bytes] | None:
        """Get a cached payload."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT topic, payload FROM birth_cache WHERE key = ?",
                (key,),
            )
            row = cursor.fetchone()
            if row:
                return (row[0], row[1])
            return None

    def get_all_dbirth_device_ids(self) -> list[str]:
        """Get all device IDs with cached DBIRTHs.

        Returns:
            List of device identifiers.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT key FROM birth_cache WHERE key LIKE 'dbirth:%'"
            )
            return [row[0].replace("dbirth:", "") for row in cursor]

    def remove_dbirth(self, device_id: str) -> None:
        """Remove a cached DBIRTH.

        Args:
            device_id: Device identifier.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM birth_cache WHERE key = ?",
                (f"dbirth:{device_id}",),
            )
            conn.commit()

    def clear(self) -> None:
        """Clear all cached births."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM birth_cache")
            conn.commit()
        logger.info("Cleared birth cache")
