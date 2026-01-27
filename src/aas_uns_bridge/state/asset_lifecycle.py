"""Asset lifecycle tracking for the AAS-UNS Bridge."""

import json
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from aas_uns_bridge.config import LifecycleConfig

logger = logging.getLogger(__name__)


class AssetState(Enum):
    """Lifecycle states for an asset."""

    ONLINE = "online"
    STALE = "stale"
    OFFLINE = "offline"


@dataclass
class AssetStatus:
    """Current status of a tracked asset."""

    asset_id: str
    """The asset identifier."""

    state: AssetState
    """Current lifecycle state."""

    last_seen_ms: int
    """Timestamp of last data publication (Unix ms)."""

    first_seen_ms: int
    """Timestamp of first data publication (Unix ms)."""

    topics: set[str] = field(default_factory=set)
    """Set of topics published by this asset."""

    @property
    def age_seconds(self) -> float:
        """Time since last seen in seconds."""
        return (time.time() * 1000 - self.last_seen_ms) / 1000


@dataclass(frozen=True, slots=True)
class LifecycleEvent:
    """A lifecycle state change event."""

    asset_id: str
    """The affected asset."""

    previous_state: AssetState | None
    """Previous state (None for first online)."""

    new_state: AssetState
    """New state."""

    timestamp_ms: int
    """When the transition occurred (Unix ms)."""

    reason: str = ""
    """Reason for the transition."""

    def to_dict(self) -> dict[str, str | int | None]:
        """Convert to dict for JSON serialization."""
        return {
            "assetId": self.asset_id,
            "previousState": self.previous_state.value if self.previous_state else None,
            "newState": self.new_state.value,
            "timestamp": self.timestamp_ms,
            "reason": self.reason,
        }


class AssetLifecycleTracker:
    """Tracks asset lifecycle states.

    Monitors asset activity and detects:
    - New assets coming online
    - Assets becoming stale (no data for threshold period)
    - Assets going offline (explicit DDEATH or timeout)

    Optionally publishes lifecycle events and can clear retained messages
    when assets go offline.
    """

    def __init__(self, db_path: Path, config: LifecycleConfig):
        """Initialize the lifecycle tracker.

        Args:
            db_path: Path to SQLite database for state persistence.
            config: Lifecycle tracking configuration.
        """
        self.db_path = db_path
        self.config = config
        self._lock = threading.Lock()
        self._assets: dict[str, AssetStatus] = {}

        # Ensure parent directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self._init_db()
        self._load_state()

    def _init_db(self) -> None:
        """Initialize the database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS asset_lifecycle (
                    asset_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    last_seen_ms INTEGER NOT NULL,
                    first_seen_ms INTEGER NOT NULL,
                    topics TEXT
                )
            """)
            conn.commit()

    def _load_state(self) -> None:
        """Load persisted asset states from database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT asset_id, state, last_seen_ms, first_seen_ms, topics FROM asset_lifecycle"
            )

            for row in cursor:
                asset_id, state_str, last_seen, first_seen, topics_json = row
                topics = set(json.loads(topics_json)) if topics_json else set()
                self._assets[asset_id] = AssetStatus(
                    asset_id=asset_id,
                    state=AssetState(state_str),
                    last_seen_ms=last_seen,
                    first_seen_ms=first_seen,
                    topics=topics,
                )

        logger.info("Loaded lifecycle state for %d assets", len(self._assets))

    def _persist_asset(self, asset: AssetStatus) -> None:
        """Persist asset state to database."""
        topics_json = json.dumps(list(asset.topics))

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO asset_lifecycle
                (asset_id, state, last_seen_ms, first_seen_ms, topics)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    asset.asset_id,
                    asset.state.value,
                    asset.last_seen_ms,
                    asset.first_seen_ms,
                    topics_json,
                ),
            )
            conn.commit()

    def mark_online(self, asset_id: str, topic: str | None = None) -> LifecycleEvent | None:
        """Mark an asset as online (received data).

        Args:
            asset_id: The asset identifier.
            topic: Optional topic that was published to.

        Returns:
            LifecycleEvent if state changed, None otherwise.
        """
        now_ms = int(time.time() * 1000)
        event = None

        with self._lock:
            if asset_id in self._assets:
                asset = self._assets[asset_id]
                previous_state = asset.state

                # Update last seen time
                asset.last_seen_ms = now_ms
                if topic:
                    asset.topics.add(topic)

                # Transition from STALE/OFFLINE to ONLINE
                if asset.state != AssetState.ONLINE:
                    asset.state = AssetState.ONLINE
                    event = LifecycleEvent(
                        asset_id=asset_id,
                        previous_state=previous_state,
                        new_state=AssetState.ONLINE,
                        timestamp_ms=now_ms,
                        reason="data_received",
                    )

                # Update reference (since AssetStatus is mutable)
                self._assets[asset_id] = asset
            else:
                # New asset
                asset = AssetStatus(
                    asset_id=asset_id,
                    state=AssetState.ONLINE,
                    last_seen_ms=now_ms,
                    first_seen_ms=now_ms,
                    topics={topic} if topic else set(),
                )
                self._assets[asset_id] = asset
                event = LifecycleEvent(
                    asset_id=asset_id,
                    previous_state=None,
                    new_state=AssetState.ONLINE,
                    timestamp_ms=now_ms,
                    reason="first_seen",
                )

            self._persist_asset(asset)

        if event:
            logger.info(
                "Asset %s transitioned to ONLINE (reason: %s)",
                asset_id,
                event.reason,
            )

        return event

    def mark_offline(self, asset_id: str, reason: str = "explicit") -> LifecycleEvent | None:
        """Mark an asset as offline.

        Args:
            asset_id: The asset identifier.
            reason: Reason for going offline (e.g., 'ddeath', 'explicit', 'timeout').

        Returns:
            LifecycleEvent if state changed, None if already offline or unknown.
        """
        now_ms = int(time.time() * 1000)

        with self._lock:
            if asset_id not in self._assets:
                return None

            asset = self._assets[asset_id]
            if asset.state == AssetState.OFFLINE:
                return None

            previous_state = asset.state
            asset.state = AssetState.OFFLINE
            self._assets[asset_id] = asset
            self._persist_asset(asset)

        event = LifecycleEvent(
            asset_id=asset_id,
            previous_state=previous_state,
            new_state=AssetState.OFFLINE,
            timestamp_ms=now_ms,
            reason=reason,
        )

        logger.info(
            "Asset %s transitioned to OFFLINE (reason: %s)",
            asset_id,
            reason,
        )

        return event

    def check_stale_assets(self) -> list[LifecycleEvent]:
        """Check for assets that have become stale.

        Returns:
            List of state change events for newly stale assets.
        """
        events = []
        now_ms = int(time.time() * 1000)
        threshold_ms = self.config.stale_threshold_seconds * 1000

        with self._lock:
            for asset_id, asset in list(self._assets.items()):
                if asset.state != AssetState.ONLINE:
                    continue

                age_ms = now_ms - asset.last_seen_ms
                if age_ms > threshold_ms:
                    asset.state = AssetState.STALE
                    self._assets[asset_id] = asset
                    self._persist_asset(asset)

                    event = LifecycleEvent(
                        asset_id=asset_id,
                        previous_state=AssetState.ONLINE,
                        new_state=AssetState.STALE,
                        timestamp_ms=now_ms,
                        reason=f"no_data_for_{int(age_ms / 1000)}s",
                    )
                    events.append(event)

                    logger.warning(
                        "Asset %s became stale (no data for %.1fs)",
                        asset_id,
                        age_ms / 1000,
                    )

        return events

    def get_asset_status(self, asset_id: str) -> AssetStatus | None:
        """Get the current status of an asset.

        Args:
            asset_id: The asset identifier.

        Returns:
            AssetStatus or None if not tracked.
        """
        with self._lock:
            return self._assets.get(asset_id)

    def get_all_assets(self) -> dict[str, AssetStatus]:
        """Get all tracked assets.

        Returns:
            Dict mapping asset IDs to their status.
        """
        with self._lock:
            return dict(self._assets)

    def get_assets_by_state(self, state: AssetState) -> list[AssetStatus]:
        """Get all assets in a given state.

        Args:
            state: The state to filter by.

        Returns:
            List of assets in that state.
        """
        with self._lock:
            return [a for a in self._assets.values() if a.state == state]

    def get_topics_for_asset(self, asset_id: str) -> set[str]:
        """Get all topics published by an asset.

        Args:
            asset_id: The asset identifier.

        Returns:
            Set of topic strings (empty if asset not tracked).
        """
        with self._lock:
            asset = self._assets.get(asset_id)
            return set(asset.topics) if asset else set()

    def remove_asset(self, asset_id: str) -> bool:
        """Remove an asset from tracking.

        Args:
            asset_id: The asset identifier.

        Returns:
            True if removed, False if not found.
        """
        with self._lock:
            if asset_id not in self._assets:
                return False

            del self._assets[asset_id]

            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "DELETE FROM asset_lifecycle WHERE asset_id = ?",
                    (asset_id,),
                )
                conn.commit()

        logger.info("Removed asset %s from lifecycle tracking", asset_id)
        return True

    def clear_all(self) -> int:
        """Remove all tracked assets.

        Returns:
            Number of assets removed.
        """
        with self._lock:
            count = len(self._assets)
            self._assets.clear()

            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM asset_lifecycle")
                conn.commit()

        logger.info("Cleared %d assets from lifecycle tracking", count)
        return count

    def build_lifecycle_topic(self, asset_id: str) -> str:
        """Build the lifecycle event topic for an asset.

        Args:
            asset_id: The asset identifier.

        Returns:
            MQTT topic for lifecycle events.
        """
        # Sanitize asset_id for use in topic
        sanitized = asset_id.replace("https://", "").replace("http://", "")
        sanitized = sanitized.replace("/", "_").replace(" ", "_")
        return f"UNS/Sys/Lifecycle/{sanitized}"

    def build_event_payload(self, event: LifecycleEvent) -> bytes:
        """Build JSON payload for a lifecycle event.

        Args:
            event: The lifecycle event.

        Returns:
            JSON-encoded bytes.
        """
        return json.dumps(event.to_dict(), ensure_ascii=False).encode("utf-8")

    @property
    def online_count(self) -> int:
        """Number of assets currently online."""
        with self._lock:
            return sum(1 for a in self._assets.values() if a.state == AssetState.ONLINE)

    @property
    def stale_count(self) -> int:
        """Number of assets currently stale."""
        with self._lock:
            return sum(1 for a in self._assets.values() if a.state == AssetState.STALE)

    @property
    def offline_count(self) -> int:
        """Number of assets currently offline."""
        with self._lock:
            return sum(1 for a in self._assets.values() if a.state == AssetState.OFFLINE)

    @property
    def total_count(self) -> int:
        """Total number of tracked assets."""
        with self._lock:
            return len(self._assets)
