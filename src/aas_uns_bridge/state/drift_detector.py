"""Schema drift detection for AAS metrics."""

import hashlib
import json
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from aas_uns_bridge.config import DriftConfig
from aas_uns_bridge.domain.models import ContextMetric

logger = logging.getLogger(__name__)


class DriftEventType(Enum):
    """Types of schema drift events."""

    ADDED = "added"
    REMOVED = "removed"
    TYPE_CHANGED = "type_changed"
    UNIT_CHANGED = "unit_changed"
    SEMANTIC_CHANGED = "semantic_changed"


@dataclass(frozen=True, slots=True)
class MetricFingerprint:
    """Structural fingerprint of a metric (excludes value)."""

    path: str
    """Metric path within the submodel."""

    aas_type: str
    """AAS element type (Property, Range, etc.)."""

    value_type: str
    """XSD value type (xs:string, xs:int, etc.)."""

    semantic_id: str | None = None
    """IRDI/IRI semantic identifier."""

    unit: str | None = None
    """Unit of measurement."""

    @property
    def hash(self) -> str:
        """Compute a stable hash of the fingerprint."""
        data = f"{self.path}|{self.aas_type}|{self.value_type}|{self.semantic_id}|{self.unit}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    @classmethod
    def from_metric(cls, metric: ContextMetric) -> "MetricFingerprint":
        """Create a fingerprint from a ContextMetric."""
        return cls(
            path=metric.path,
            aas_type=metric.aas_type,
            value_type=metric.value_type,
            semantic_id=metric.semantic_id,
            unit=metric.unit,
        )


@dataclass(frozen=True, slots=True)
class DriftEvent:
    """A detected schema drift event."""

    event_type: DriftEventType
    """Type of drift detected."""

    asset_id: str
    """The asset where drift was detected."""

    metric_path: str
    """Path of the affected metric."""

    timestamp_ms: int
    """When the drift was detected (Unix ms)."""

    previous: MetricFingerprint | None = None
    """Previous fingerprint (for changes/removals)."""

    current: MetricFingerprint | None = None
    """Current fingerprint (for changes/additions)."""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for JSON serialization."""
        result: dict[str, Any] = {
            "eventType": self.event_type.value,
            "assetId": self.asset_id,
            "metricPath": self.metric_path,
            "timestamp": self.timestamp_ms,
        }
        if self.previous:
            result["previous"] = {
                "aasType": self.previous.aas_type,
                "valueType": self.previous.value_type,
                "semanticId": self.previous.semantic_id,
                "unit": self.previous.unit,
            }
        if self.current:
            result["current"] = {
                "aasType": self.current.aas_type,
                "valueType": self.current.value_type,
                "semanticId": self.current.semantic_id,
                "unit": self.current.unit,
            }
        return result


@dataclass
class DriftDetectionResult:
    """Result of drift detection for an asset."""

    asset_id: str
    """The asset that was checked."""

    events: list[DriftEvent] = field(default_factory=list)
    """List of drift events detected."""

    @property
    def has_drift(self) -> bool:
        """Check if any drift was detected."""
        return len(self.events) > 0

    @property
    def additions(self) -> list[DriftEvent]:
        """Get all addition events."""
        return [e for e in self.events if e.event_type == DriftEventType.ADDED]

    @property
    def removals(self) -> list[DriftEvent]:
        """Get all removal events."""
        return [e for e in self.events if e.event_type == DriftEventType.REMOVED]

    @property
    def changes(self) -> list[DriftEvent]:
        """Get all change events (type, unit, semantic)."""
        change_types = {
            DriftEventType.TYPE_CHANGED,
            DriftEventType.UNIT_CHANGED,
            DriftEventType.SEMANTIC_CHANGED,
        }
        return [e for e in self.events if e.event_type in change_types]


class DriftDetector:
    """Detects schema drift in AAS metrics.

    Compares current metric fingerprints against previously stored fingerprints
    to detect structural changes like:
    - New metrics added
    - Existing metrics removed
    - Changes in value_type, unit, or semantic_id
    """

    def __init__(self, db_path: Path, config: DriftConfig):
        """Initialize the drift detector.

        Args:
            db_path: Path to SQLite database for fingerprint storage.
            config: Drift detection configuration.
        """
        self.db_path = db_path
        self.config = config
        self._lock = threading.Lock()

        # Ensure parent directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self._init_db()

    def _init_db(self) -> None:
        """Initialize the database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metric_fingerprints (
                    asset_id TEXT NOT NULL,
                    metric_path TEXT NOT NULL,
                    aas_type TEXT,
                    value_type TEXT,
                    semantic_id TEXT,
                    unit TEXT,
                    fingerprint_hash TEXT,
                    updated_at INTEGER,
                    PRIMARY KEY (asset_id, metric_path)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_fingerprint_asset
                ON metric_fingerprints(asset_id)
            """)
            conn.commit()

    def detect_drift(self, asset_id: str, metrics: list[ContextMetric]) -> DriftDetectionResult:
        """Detect drift for an asset's metrics.

        Compares provided metrics against stored fingerprints and returns
        any drift events. Does NOT update stored fingerprints - call
        update_fingerprints() after handling drift events.

        Args:
            asset_id: The asset identifier.
            metrics: Current metrics for the asset.

        Returns:
            DriftDetectionResult with any detected drift events.
        """
        events: list[DriftEvent] = []
        now_ms = int(time.time() * 1000)

        # Build current fingerprints
        current_fps = {m.path: MetricFingerprint.from_metric(m) for m in metrics}

        # Load stored fingerprints
        stored_fps = self._load_fingerprints(asset_id)

        current_paths = set(current_fps.keys())
        stored_paths = set(stored_fps.keys())

        # Detect additions
        if self.config.track_additions:
            for path in current_paths - stored_paths:
                events.append(
                    DriftEvent(
                        event_type=DriftEventType.ADDED,
                        asset_id=asset_id,
                        metric_path=path,
                        timestamp_ms=now_ms,
                        current=current_fps[path],
                    )
                )

        # Detect removals
        if self.config.track_removals:
            for path in stored_paths - current_paths:
                events.append(
                    DriftEvent(
                        event_type=DriftEventType.REMOVED,
                        asset_id=asset_id,
                        metric_path=path,
                        timestamp_ms=now_ms,
                        previous=stored_fps[path],
                    )
                )

        # Detect changes in existing metrics
        if self.config.track_type_changes:
            for path in current_paths & stored_paths:
                current_fp = current_fps[path]
                stored_fp = stored_fps[path]

                # Check for value type change
                if current_fp.value_type != stored_fp.value_type:
                    events.append(
                        DriftEvent(
                            event_type=DriftEventType.TYPE_CHANGED,
                            asset_id=asset_id,
                            metric_path=path,
                            timestamp_ms=now_ms,
                            previous=stored_fp,
                            current=current_fp,
                        )
                    )
                # Check for unit change
                elif current_fp.unit != stored_fp.unit:
                    events.append(
                        DriftEvent(
                            event_type=DriftEventType.UNIT_CHANGED,
                            asset_id=asset_id,
                            metric_path=path,
                            timestamp_ms=now_ms,
                            previous=stored_fp,
                            current=current_fp,
                        )
                    )
                # Check for semantic ID change
                elif current_fp.semantic_id != stored_fp.semantic_id:
                    events.append(
                        DriftEvent(
                            event_type=DriftEventType.SEMANTIC_CHANGED,
                            asset_id=asset_id,
                            metric_path=path,
                            timestamp_ms=now_ms,
                            previous=stored_fp,
                            current=current_fp,
                        )
                    )

        return DriftDetectionResult(asset_id=asset_id, events=events)

    def update_fingerprints(self, asset_id: str, metrics: list[ContextMetric]) -> None:
        """Update stored fingerprints for an asset.

        Replaces all stored fingerprints for the asset with current metrics.

        Args:
            asset_id: The asset identifier.
            metrics: Current metrics to store.
        """
        now = int(time.time())

        with self._lock, sqlite3.connect(self.db_path) as conn:
            # Delete existing fingerprints for this asset
            conn.execute(
                "DELETE FROM metric_fingerprints WHERE asset_id = ?",
                (asset_id,),
            )

            # Insert current fingerprints
            for metric in metrics:
                fp = MetricFingerprint.from_metric(metric)
                conn.execute(
                    """
                    INSERT INTO metric_fingerprints
                    (asset_id, metric_path, aas_type, value_type,
                     semantic_id, unit, fingerprint_hash, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        asset_id,
                        fp.path,
                        fp.aas_type,
                        fp.value_type,
                        fp.semantic_id,
                        fp.unit,
                        fp.hash,
                        now,
                    ),
                )

            conn.commit()

        logger.debug("Updated %d fingerprints for asset %s", len(metrics), asset_id)

    def _load_fingerprints(self, asset_id: str) -> dict[str, MetricFingerprint]:
        """Load stored fingerprints for an asset.

        Args:
            asset_id: The asset identifier.

        Returns:
            Dict mapping metric paths to fingerprints.
        """
        result: dict[str, MetricFingerprint] = {}

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT metric_path, aas_type, value_type, semantic_id, unit
                FROM metric_fingerprints
                WHERE asset_id = ?
                """,
                (asset_id,),
            )

            for row in cursor:
                path, aas_type, value_type, semantic_id, unit = row
                result[path] = MetricFingerprint(
                    path=path,
                    aas_type=aas_type,
                    value_type=value_type,
                    semantic_id=semantic_id,
                    unit=unit,
                )

        return result

    def get_all_assets(self) -> list[str]:
        """Get all asset IDs with stored fingerprints.

        Returns:
            List of asset identifiers.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT DISTINCT asset_id FROM metric_fingerprints")
            return [row[0] for row in cursor]

    def clear_asset(self, asset_id: str) -> int:
        """Clear all fingerprints for an asset.

        Args:
            asset_id: The asset identifier.

        Returns:
            Number of fingerprints deleted.
        """
        with self._lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM metric_fingerprints WHERE asset_id = ?",
                (asset_id,),
            )
            conn.commit()
            return cursor.rowcount

    def clear_all(self) -> int:
        """Clear all stored fingerprints.

        Returns:
            Number of fingerprints deleted.
        """
        with self._lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM metric_fingerprints")
            conn.commit()
            return cursor.rowcount

    def build_alert_topic(self, asset_id: str) -> str:
        """Build the alert topic for an asset.

        Args:
            asset_id: The asset identifier.

        Returns:
            MQTT topic for drift alerts.
        """
        # Sanitize asset_id for use in topic
        sanitized = asset_id.replace("https://", "").replace("http://", "")
        sanitized = sanitized.replace("/", "_").replace(" ", "_")
        return self.config.alert_topic_template.format(asset_id=sanitized)

    def build_alert_payload(self, event: DriftEvent) -> bytes:
        """Build JSON payload for a drift alert.

        Args:
            event: The drift event.

        Returns:
            JSON-encoded bytes.
        """
        return json.dumps(event.to_dict(), ensure_ascii=False).encode("utf-8")
