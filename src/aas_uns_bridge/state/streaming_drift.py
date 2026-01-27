"""Streaming drift detection with Half-Space Trees.

This module provides incremental anomaly detection for AAS metrics using
Half-Space Trees (HST), enabling O(1) per-observation scoring without
storing historical data. Supports severity-aware classification and
confidence scoring for drift events.

References:
- Tan, Ting, & Liu (2011): Fast Anomaly Detection for Streaming Data
- MASS algorithm for streaming anomaly detection
"""

from __future__ import annotations

import hashlib
import logging
import math
import random
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.observability.metrics import METRICS

logger = logging.getLogger(__name__)


class DriftType(Enum):
    """Types of drift detected by the streaming detector."""

    CONCEPT_DRIFT = "concept_drift"
    """Gradual change in value distribution over time."""

    SCHEMA_EVOLUTION = "schema_evolution"
    """Structural change (new/removed metrics, type changes)."""

    VALUE_ANOMALY = "value_anomaly"
    """Single observation outside normal range."""

    FREQUENCY_ANOMALY = "frequency_anomaly"
    """Change in metric publication frequency."""


class DriftSeverity(Enum):
    """Severity levels for drift events."""

    LOW = "low"
    """Minor deviation, likely noise."""

    MEDIUM = "medium"
    """Noticeable change, worth monitoring."""

    HIGH = "high"
    """Significant drift, may require attention."""

    CRITICAL = "critical"
    """Major anomaly, likely requires action."""


@dataclass(frozen=True, slots=True)
class DriftResult:
    """Result of drift detection for a single observation."""

    is_drift: bool
    """Whether drift was detected."""

    drift_type: DriftType
    """Classification of drift type."""

    severity: DriftSeverity
    """Severity level of the drift."""

    confidence: float
    """Confidence score 0.0 - 1.0."""

    anomaly_score: float
    """Raw anomaly score from detector."""

    suggested_action: str
    """Recommended action (alert, quarantine, auto_accept)."""

    metric_path: str = ""
    """Path of the metric that triggered drift."""

    details: dict[str, Any] = field(default_factory=dict)
    """Additional context about the drift."""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "isDrift": self.is_drift,
            "driftType": self.drift_type.value,
            "severity": self.severity.value,
            "confidence": self.confidence,
            "anomalyScore": self.anomaly_score,
            "suggestedAction": self.suggested_action,
            "metricPath": self.metric_path,
            "details": self.details,
        }


class HalfSpaceTree:
    """Single Half-Space Tree for streaming anomaly detection.

    A Half-Space Tree recursively partitions the feature space using
    random hyperplanes. Anomaly scores are based on the depth at which
    an observation is isolated - anomalies tend to be isolated at
    shallower depths.

    This is a simplified implementation optimized for low-dimensional
    feature spaces typical of AAS metrics.
    """

    def __init__(
        self,
        max_depth: int = 10,
        window_size: int = 1000,
        seed: int | None = None,
    ):
        """Initialize a Half-Space Tree.

        Args:
            max_depth: Maximum tree depth.
            window_size: Reference window size for mass estimation.
            seed: Random seed for reproducibility.
        """
        self.max_depth = max_depth
        self.window_size = window_size
        self._rng = random.Random(seed)

        # Tree structure: list of (dimension, split_value, mass) tuples
        # Index 0 is root, children of node i are at 2i+1 (left) and 2i+2 (right)
        self._nodes: list[tuple[int, float, int]] = []
        self._feature_ranges: dict[int, tuple[float, float]] = {}
        self._initialized = False

    def _init_tree(self, num_features: int) -> None:
        """Initialize tree structure with random splits."""
        num_nodes = (1 << (self.max_depth + 1)) - 1  # 2^(d+1) - 1

        self._nodes = []
        for _ in range(num_nodes):
            dim = self._rng.randint(0, num_features - 1)
            # Split value will be updated as data comes in
            split = self._rng.random()
            self._nodes.append((dim, split, 0))

        self._initialized = True

    def update(self, features: list[float]) -> None:
        """Update the tree with a new observation.

        Args:
            features: Feature vector for the observation.
        """
        if not self._initialized:
            self._init_tree(len(features))

        # Update feature ranges
        for i, val in enumerate(features):
            if i not in self._feature_ranges:
                self._feature_ranges[i] = (val, val)
            else:
                lo, hi = self._feature_ranges[i]
                self._feature_ranges[i] = (min(lo, val), max(hi, val))

        # Traverse tree and increment mass counters
        node_idx = 0
        for _depth in range(self.max_depth):
            if node_idx >= len(self._nodes):
                break

            dim, split, mass = self._nodes[node_idx]

            # Normalize feature to [0, 1] using observed range
            if dim in self._feature_ranges:
                lo, hi = self._feature_ranges[dim]
                norm_val = (features[dim] - lo) / (hi - lo) if hi > lo else 0.5
            else:
                norm_val = features[dim] if 0 <= features[dim] <= 1 else 0.5

            # Update mass
            self._nodes[node_idx] = (dim, split, min(mass + 1, self.window_size))

            # Traverse left or right
            node_idx = 2 * node_idx + 1 if norm_val < split else 2 * node_idx + 2

    def score(self, features: list[float]) -> float:
        """Compute anomaly score for an observation.

        Lower mass at isolation depth = higher anomaly score.

        Args:
            features: Feature vector to score.

        Returns:
            Anomaly score in [0, 1], higher means more anomalous.
        """
        if not self._initialized or not self._nodes:
            return 0.5  # Uncertain when tree not initialized

        node_idx = 0
        total_mass = 0
        nodes_visited = 0

        for _depth in range(self.max_depth):
            if node_idx >= len(self._nodes):
                break

            dim, split, mass = self._nodes[node_idx]
            total_mass += mass
            nodes_visited += 1

            # Normalize feature
            if dim in self._feature_ranges:
                lo, hi = self._feature_ranges[dim]
                norm_val = (features[dim] - lo) / (hi - lo) if hi > lo else 0.5
            else:
                norm_val = features[dim] if 0 <= features[dim] <= 1 else 0.5

            # Traverse
            node_idx = 2 * node_idx + 1 if norm_val < split else 2 * node_idx + 2

        if nodes_visited == 0:
            return 0.5

        # Score based on average mass encountered
        avg_mass = total_mass / nodes_visited
        # Lower mass = higher anomaly score
        # Normalize by window size
        score = 1.0 - (avg_mass / self.window_size)
        return max(0.0, min(1.0, score))


class HalfSpaceForest:
    """Ensemble of Half-Space Trees for robust anomaly detection.

    Combines multiple trees to reduce variance and provide more
    reliable anomaly scores.
    """

    def __init__(
        self,
        num_trees: int = 25,
        max_depth: int = 10,
        window_size: int = 1000,
        seed: int | None = None,
    ):
        """Initialize the forest.

        Args:
            num_trees: Number of trees in the ensemble.
            max_depth: Maximum depth per tree.
            window_size: Reference window size for mass estimation.
            seed: Random seed for reproducibility.
        """
        self.num_trees = num_trees
        base_seed = seed if seed is not None else int(time.time())
        self.trees = [
            HalfSpaceTree(max_depth, window_size, base_seed + i) for i in range(num_trees)
        ]

    def update(self, features: list[float]) -> None:
        """Update all trees with a new observation."""
        for tree in self.trees:
            tree.update(features)

    def score(self, features: list[float]) -> float:
        """Compute ensemble anomaly score.

        Returns:
            Average anomaly score across all trees.
        """
        if not self.trees:
            return 0.5
        scores = [tree.score(features) for tree in self.trees]
        return sum(scores) / len(scores)


class IncrementalDriftDetector:
    """Severity-aware drift detection with confidence scoring.

    Combines Half-Space Trees for value anomaly detection with
    schema tracking for structural drift detection. Provides
    confidence-calibrated results with suggested actions.
    """

    def __init__(
        self,
        db_path: Path,
        window_size: int = 1000,
        num_trees: int = 25,
        severity_thresholds: dict[str, float] | None = None,
    ):
        """Initialize the drift detector.

        Args:
            db_path: Path to SQLite database for state persistence.
            window_size: Window size for streaming detection.
            num_trees: Number of Half-Space Trees.
            severity_thresholds: Score thresholds for severity levels.
        """
        self.db_path = db_path
        self.window_size = window_size
        self.num_trees = num_trees
        self._lock = threading.Lock()

        # Default severity thresholds
        self.severity_thresholds = severity_thresholds or {
            "low": 0.3,
            "medium": 0.5,
            "high": 0.7,
            "critical": 0.9,
        }

        # Per-asset forests for value anomaly detection
        self._forests: dict[str, HalfSpaceForest] = {}

        # Schema tracking
        self._schema_hashes: dict[str, str] = {}  # asset_id -> schema_hash

        # Ensure parent directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self._load_state_from_db()

    def _init_db(self) -> None:
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS drift_state (
                    asset_id TEXT PRIMARY KEY,
                    schema_hash TEXT,
                    observation_count INTEGER DEFAULT 0,
                    last_drift_timestamp INTEGER,
                    last_drift_type TEXT,
                    updated_at INTEGER
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS drift_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_id TEXT NOT NULL,
                    timestamp_ms INTEGER NOT NULL,
                    drift_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    confidence REAL,
                    anomaly_score REAL,
                    metric_path TEXT,
                    details_json TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_drift_history_asset
                ON drift_history(asset_id, timestamp_ms DESC)
            """)
            conn.commit()

    def _load_state_from_db(self) -> None:
        """Load persisted schema hashes from database.

        Restores schema hash state from previous runs to prevent spurious
        drift alerts on daemon restarts.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT asset_id, schema_hash FROM drift_state WHERE schema_hash IS NOT NULL"
            )
            for row in cursor:
                self._schema_hashes[row[0]] = row[1]

        if self._schema_hashes:
            logger.info("Loaded %d schema hashes from persistence", len(self._schema_hashes))

    def _get_or_create_forest(self, asset_id: str) -> HalfSpaceForest:
        """Get or create a forest for an asset."""
        if asset_id not in self._forests:
            self._forests[asset_id] = HalfSpaceForest(
                num_trees=self.num_trees,
                window_size=self.window_size,
            )
        return self._forests[asset_id]

    def _featurize(self, metric: ContextMetric) -> list[float]:
        """Convert a metric to a feature vector.

        Args:
            metric: The context metric.

        Returns:
            Feature vector for anomaly detection.
        """
        features: list[float] = []

        # Feature 1: Numeric value (if applicable)
        if isinstance(metric.value, (int, float)):
            features.append(float(metric.value))
        else:
            features.append(0.0)

        # Feature 2: Value type hash (normalized)
        type_hash = int(hashlib.md5(metric.value_type.encode()).hexdigest()[:8], 16)
        features.append((type_hash % 1000) / 1000.0)

        # Feature 3: Path depth
        path_depth = metric.path.count(".") + metric.path.count("[")
        features.append(min(path_depth / 10.0, 1.0))

        # Feature 4: Has semantic ID
        features.append(1.0 if metric.semantic_id else 0.0)

        # Feature 5: Has unit
        features.append(1.0 if metric.unit else 0.0)

        return features

    def _compute_schema_hash(self, metrics: list[ContextMetric]) -> str:
        """Compute a hash of the schema structure.

        Args:
            metrics: List of metrics.

        Returns:
            Hash representing the schema.
        """
        # Sort by path for deterministic hash
        schema_parts = sorted(
            f"{m.path}|{m.aas_type}|{m.value_type}|{m.semantic_id}" for m in metrics
        )
        return hashlib.sha256("|".join(schema_parts).encode()).hexdigest()[:16]

    def _score_to_severity(self, score: float) -> DriftSeverity:
        """Convert anomaly score to severity level."""
        if score >= self.severity_thresholds["critical"]:
            return DriftSeverity.CRITICAL
        elif score >= self.severity_thresholds["high"]:
            return DriftSeverity.HIGH
        elif score >= self.severity_thresholds["medium"]:
            return DriftSeverity.MEDIUM
        elif score >= self.severity_thresholds["low"]:
            return DriftSeverity.LOW
        return DriftSeverity.LOW

    def _calculate_confidence(self, score: float, observation_count: int) -> float:
        """Calculate confidence in the drift detection.

        Confidence increases with more observations (better model)
        and more extreme scores.

        Args:
            score: Raw anomaly score.
            observation_count: Number of observations seen.

        Returns:
            Confidence score 0.0 - 1.0.
        """
        # Model confidence based on observation count
        # Approaches 1.0 as observations increase
        model_confidence = 1.0 - math.exp(-observation_count / self.window_size)

        # Score confidence based on distance from 0.5 (uncertain)
        score_confidence = abs(score - 0.5) * 2

        # Combined confidence
        return model_confidence * score_confidence

    def _suggest_action(
        self,
        drift_type: DriftType,
        severity: DriftSeverity,
        confidence: float,
    ) -> str:
        """Suggest an action based on drift characteristics.

        Args:
            drift_type: Type of drift detected.
            severity: Severity level.
            confidence: Detection confidence.

        Returns:
            Suggested action string.
        """
        if confidence < 0.3:
            return "monitor"  # Low confidence, keep watching

        if drift_type == DriftType.SCHEMA_EVOLUTION:
            if severity in (DriftSeverity.HIGH, DriftSeverity.CRITICAL):
                return "quarantine"  # Major schema change needs review
            return "alert"

        if drift_type == DriftType.VALUE_ANOMALY:
            if severity == DriftSeverity.CRITICAL:
                return "quarantine"
            elif severity == DriftSeverity.HIGH:
                return "alert"
            return "auto_accept"

        if drift_type == DriftType.CONCEPT_DRIFT:
            return "alert"  # Always alert on concept drift

        return "monitor"

    def detect(
        self,
        asset_id: str,
        metric: ContextMetric,
    ) -> DriftResult:
        """Detect drift for a single metric observation.

        Args:
            asset_id: The asset identifier.
            metric: The metric to check.

        Returns:
            DriftResult with detection details.
        """
        with self._lock:
            forest = self._get_or_create_forest(asset_id)

        features = self._featurize(metric)
        anomaly_score = forest.score(features)

        # Update the forest with this observation
        forest.update(features)

        # Get observation count for confidence calculation
        observation_count = sum(
            sum(n[2] for n in tree._nodes) // len(tree._nodes)
            for tree in forest.trees
            if tree._nodes
        ) // max(len(forest.trees), 1)

        # Update observation count metric
        METRICS.streaming_drift_forest_observations.labels(asset_id=asset_id).set(observation_count)

        # Determine if this is drift
        threshold = self.severity_thresholds["low"]
        is_drift = anomaly_score >= threshold

        if is_drift:
            severity = self._score_to_severity(anomaly_score)
            confidence = self._calculate_confidence(anomaly_score, observation_count)
            drift_type = DriftType.VALUE_ANOMALY
            suggested_action = self._suggest_action(drift_type, severity, confidence)

            return DriftResult(
                is_drift=True,
                drift_type=drift_type,
                severity=severity,
                confidence=confidence,
                anomaly_score=anomaly_score,
                suggested_action=suggested_action,
                metric_path=metric.path,
                details={
                    "value": metric.value,
                    "observationCount": observation_count,
                },
            )

        return DriftResult(
            is_drift=False,
            drift_type=DriftType.VALUE_ANOMALY,
            severity=DriftSeverity.LOW,
            confidence=self._calculate_confidence(anomaly_score, observation_count),
            anomaly_score=anomaly_score,
            suggested_action="none",
            metric_path=metric.path,
        )

    def detect_schema_drift(
        self,
        asset_id: str,
        metrics: list[ContextMetric],
    ) -> DriftResult | None:
        """Detect schema-level drift for an asset.

        Args:
            asset_id: The asset identifier.
            metrics: Current metrics for the asset.

        Returns:
            DriftResult if schema drift detected, None otherwise.
        """
        current_hash = self._compute_schema_hash(metrics)

        with self._lock:
            previous_hash = self._schema_hashes.get(asset_id)
            self._schema_hashes[asset_id] = current_hash

        if previous_hash is None:
            # First observation, no drift
            return None

        if current_hash != previous_hash:
            # Schema changed
            return DriftResult(
                is_drift=True,
                drift_type=DriftType.SCHEMA_EVOLUTION,
                severity=DriftSeverity.MEDIUM,
                confidence=0.95,  # Schema hash is deterministic
                anomaly_score=1.0,
                suggested_action="alert",
                details={
                    "previousHash": previous_hash,
                    "currentHash": current_hash,
                    "metricCount": len(metrics),
                },
            )

        return None

    def detect_batch(
        self,
        asset_id: str,
        metrics: list[ContextMetric],
    ) -> list[DriftResult]:
        """Detect drift for a batch of metrics.

        Combines value-level and schema-level detection.

        Args:
            asset_id: The asset identifier.
            metrics: List of metrics to check.

        Returns:
            List of DriftResults (only includes detected drift).
        """
        results: list[DriftResult] = []

        # Schema drift check first
        schema_result = self.detect_schema_drift(asset_id, metrics)
        if schema_result:
            results.append(schema_result)
            self._persist_drift(asset_id, schema_result)

        # Value drift check for numeric metrics
        for metric in metrics:
            if isinstance(metric.value, (int, float)):
                result = self.detect(asset_id, metric)
                if result.is_drift:
                    results.append(result)
                    self._persist_drift(asset_id, result)

        return results

    def _persist_drift(self, asset_id: str, result: DriftResult) -> None:
        """Persist drift detection to database."""
        now = int(time.time() * 1000)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO drift_history
                (asset_id, timestamp_ms, drift_type, severity, confidence,
                 anomaly_score, metric_path, details_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    asset_id,
                    now,
                    result.drift_type.value,
                    result.severity.value,
                    result.confidence,
                    result.anomaly_score,
                    result.metric_path,
                    str(result.details),
                ),
            )

            conn.execute(
                """
                INSERT OR REPLACE INTO drift_state
                (asset_id, schema_hash, last_drift_timestamp,
                 last_drift_type, updated_at)
                VALUES (?, ?, ?, ?, ?)
            """,
                (
                    asset_id,
                    self._schema_hashes.get(asset_id),
                    now,
                    result.drift_type.value,
                    int(time.time()),
                ),
            )
            conn.commit()

    def get_drift_history(
        self,
        asset_id: str,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Get drift history for an asset.

        Args:
            asset_id: The asset identifier.
            limit: Maximum number of records to return.

        Returns:
            List of drift event dictionaries.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT timestamp_ms, drift_type, severity, confidence,
                       anomaly_score, metric_path, details_json
                FROM drift_history
                WHERE asset_id = ?
                ORDER BY timestamp_ms DESC
                LIMIT ?
            """,
                (asset_id, limit),
            )

            return [
                {
                    "timestamp": row[0],
                    "driftType": row[1],
                    "severity": row[2],
                    "confidence": row[3],
                    "anomalyScore": row[4],
                    "metricPath": row[5],
                    "details": row[6],
                }
                for row in cursor
            ]

    def clear_asset(self, asset_id: str) -> None:
        """Clear all state for an asset.

        Args:
            asset_id: The asset identifier.
        """
        with self._lock:
            self._forests.pop(asset_id, None)
            self._schema_hashes.pop(asset_id, None)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM drift_state WHERE asset_id = ?", (asset_id,))
            conn.execute("DELETE FROM drift_history WHERE asset_id = ?", (asset_id,))
            conn.commit()
