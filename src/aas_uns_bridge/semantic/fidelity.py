"""Information-theoretic fidelity metrics for semantic transformation.

This module provides metrics to quantify the fidelity of AAS-to-UNS
transformations. It measures how much semantic information is preserved
or lost during the graph-to-tree projection process.

Key metrics:
- Entropy loss: Information lost when reducing poly-hierarchical refs to single keys
- Structural fidelity: Path preservation through the transformation
- Semantic fidelity: Semantic key preservation ratio
"""

from __future__ import annotations

import logging
import math
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aas_uns_bridge.domain.models import ContextMetric

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class FidelityReport:
    """Comprehensive fidelity report for an asset.

    Captures multiple dimensions of transformation fidelity
    with actionable recommendations.
    """

    asset_id: str
    """The asset being evaluated."""

    overall_score: float
    """Weighted average fidelity score (0.0 - 1.0)."""

    structural_fidelity: float
    """Path structure preservation score (0.0 - 1.0)."""

    semantic_fidelity: float
    """Semantic key preservation score (0.0 - 1.0)."""

    entropy_loss: float
    """Information entropy lost in transformation (0.0 - 1.0)."""

    metric_count: int
    """Number of metrics evaluated."""

    recommendations: tuple[str, ...] = field(default_factory=tuple)
    """Actionable recommendations for improving fidelity."""

    timestamp_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    """When the report was generated."""

    details: dict[str, Any] = field(default_factory=dict)
    """Additional diagnostic details."""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "assetId": self.asset_id,
            "overallScore": self.overall_score,
            "structuralFidelity": self.structural_fidelity,
            "semanticFidelity": self.semantic_fidelity,
            "entropyLoss": self.entropy_loss,
            "metricCount": self.metric_count,
            "recommendations": list(self.recommendations),
            "timestamp": self.timestamp_ms,
            "details": self.details,
        }

    @property
    def grade(self) -> str:
        """Letter grade based on overall score."""
        if self.overall_score >= 0.95:
            return "A+"
        elif self.overall_score >= 0.90:
            return "A"
        elif self.overall_score >= 0.85:
            return "B+"
        elif self.overall_score >= 0.80:
            return "B"
        elif self.overall_score >= 0.70:
            return "C"
        elif self.overall_score >= 0.60:
            return "D"
        return "F"


@dataclass
class MetricFidelityStats:
    """Statistics for a single metric's fidelity."""

    path: str
    has_semantic_id: bool
    semantic_key_count: int
    has_unit: bool
    has_submodel_context: bool
    path_depth: int


class FidelityCalculator:
    """Calculator for information-theoretic fidelity metrics.

    Measures the fidelity of AAS-to-UNS transformation by quantifying:
    - How much semantic information is preserved
    - How structural relationships are maintained
    - What information is lost in the graph-to-tree projection

    Optionally persists historical fidelity data for trend analysis.
    """

    def __init__(
        self,
        db_path: Path | None = None,
        weights: dict[str, float] | None = None,
    ):
        """Initialize the fidelity calculator.

        Args:
            db_path: Optional path to SQLite database for persistence.
            weights: Optional weights for fidelity components.
        """
        self.db_path = db_path
        self._lock = threading.Lock()

        # Default weights for overall score
        self.weights = weights or {
            "structural": 0.3,
            "semantic": 0.5,
            "entropy": 0.2,
        }

        if db_path:
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self._init_db()

    def _init_db(self) -> None:
        """Initialize database schema."""
        if not self.db_path:
            return

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fidelity_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_id TEXT NOT NULL,
                    timestamp_ms INTEGER NOT NULL,
                    overall_score REAL,
                    structural_fidelity REAL,
                    semantic_fidelity REAL,
                    entropy_loss REAL,
                    metric_count INTEGER,
                    grade TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_fidelity_asset_time
                ON fidelity_history(asset_id, timestamp_ms DESC)
            """)
            conn.commit()

    def calculate_entropy_loss(
        self,
        original_key_count: int,
        preserved_key_count: int,
    ) -> float:
        """Calculate Shannon entropy loss ratio.

        Measures information loss when reducing multiple semantic keys
        to a smaller set (or single key) in the transformation.

        Args:
            original_key_count: Total semantic keys in AAS source.
            preserved_key_count: Keys preserved in UNS output.

        Returns:
            Entropy loss ratio (0.0 = no loss, 1.0 = total loss).
        """
        if original_key_count == 0:
            return 0.0  # No keys to lose

        if preserved_key_count >= original_key_count:
            return 0.0  # No loss (or gain)

        # Calculate entropy reduction
        # H = log2(n) for uniform distribution over n keys
        original_entropy = math.log2(original_key_count) if original_key_count > 1 else 0
        preserved_entropy = math.log2(preserved_key_count) if preserved_key_count > 1 else 0

        if original_entropy == 0:
            return 0.0

        return 1.0 - (preserved_entropy / original_entropy)

    def calculate_semantic_fidelity(
        self,
        metrics: list[ContextMetric],
    ) -> float:
        """Calculate semantic key preservation fidelity.

        Measures what fraction of metrics have semantic identifiers
        and how many of their poly-hierarchical keys are preserved.

        Args:
            metrics: List of context metrics.

        Returns:
            Semantic fidelity score (0.0 - 1.0).
        """
        if not metrics:
            return 1.0

        total_possible_keys = 0
        preserved_keys = 0
        metrics_with_semantic_id = 0

        for metric in metrics:
            # Check if metric has primary semantic ID
            if metric.semantic_id:
                metrics_with_semantic_id += 1

            # Check poly-hierarchical keys
            semantic_keys = getattr(metric, "semantic_keys", ())
            if semantic_keys:
                # Has poly-hierarchical keys - count them
                total_possible_keys += len(semantic_keys)
                preserved_keys += len(semantic_keys)
            elif metric.semantic_id:
                # Only has primary semantic_id - count it once (not double-counted)
                total_possible_keys += 1
                preserved_keys += 1

        # Combine two factors:
        # 1. Fraction of metrics with any semantic ID
        coverage = metrics_with_semantic_id / len(metrics)

        # 2. Key preservation ratio (if any keys exist)
        key_ratio = preserved_keys / total_possible_keys if total_possible_keys > 0 else 0.0

        # Weight coverage more heavily (semantic IDs should be present)
        return 0.7 * coverage + 0.3 * key_ratio

    def calculate_structural_fidelity(
        self,
        metrics: list[ContextMetric],
    ) -> float:
        """Calculate structural path preservation fidelity.

        Measures how well the AAS submodel structure is preserved
        in the flattened metric paths.

        Args:
            metrics: List of context metrics.

        Returns:
            Structural fidelity score (0.0 - 1.0).
        """
        if not metrics:
            return 1.0

        scores: list[float] = []

        for metric in metrics:
            path_score = 1.0

            # Check path depth (deeper = more structure preserved)
            depth = metric.path.count(".") + metric.path.count("[")
            if depth == 0:
                path_score *= 0.5  # Flat path loses structure

            # Check if submodel context is preserved
            if hasattr(metric, "submodel_semantic_id") and metric.submodel_semantic_id:
                path_score *= 1.0
            else:
                path_score *= 0.9  # Minor penalty for missing submodel context

            # Check for meaningful path segments (not just "unnamed")
            if "unnamed" in metric.path.lower():
                path_score *= 0.8

            scores.append(path_score)

        return sum(scores) / len(scores)

    def calculate_asset_fidelity(
        self,
        asset_id: str,
        metrics: list[ContextMetric],
    ) -> FidelityReport:
        """Calculate comprehensive fidelity report for an asset.

        Combines multiple fidelity dimensions into an overall score
        with actionable recommendations.

        Args:
            asset_id: The asset identifier.
            metrics: Current metrics for the asset.

        Returns:
            FidelityReport with all metrics and recommendations.
        """
        if not metrics:
            return FidelityReport(
                asset_id=asset_id,
                overall_score=1.0,
                structural_fidelity=1.0,
                semantic_fidelity=1.0,
                entropy_loss=0.0,
                metric_count=0,
                recommendations=(),
            )

        # Calculate component fidelities
        structural = self.calculate_structural_fidelity(metrics)
        semantic = self.calculate_semantic_fidelity(metrics)

        # Calculate entropy loss from poly-hierarchical reduction
        total_original_keys = sum(
            len(getattr(m, "semantic_keys", ()) or (m.semantic_id,) if m.semantic_id else ())
            for m in metrics
        )
        # In current mode, we preserve all keys via semantic_keys field
        total_preserved_keys = sum(
            len(getattr(m, "semantic_keys", ()) or ((m.semantic_id,) if m.semantic_id else ()))
            for m in metrics
        )
        entropy_loss = self.calculate_entropy_loss(total_original_keys, total_preserved_keys)

        # Calculate overall score
        overall = (
            self.weights["structural"] * structural
            + self.weights["semantic"] * semantic
            + self.weights["entropy"] * (1.0 - entropy_loss)  # Invert: lower loss = higher score
        )

        # Generate recommendations
        recommendations = self._generate_recommendations(
            structural, semantic, entropy_loss, metrics
        )

        # Build details
        details = {
            "metricsWithSemanticId": sum(1 for m in metrics if m.semantic_id),
            "metricsWithUnit": sum(1 for m in metrics if m.unit),
            "metricsWithSubmodelContext": sum(
                1 for m in metrics if hasattr(m, "submodel_semantic_id") and m.submodel_semantic_id
            ),
            "avgPathDepth": sum(m.path.count(".") + m.path.count("[") for m in metrics)
            / len(metrics),
            "totalSemanticKeys": total_original_keys,
        }

        report = FidelityReport(
            asset_id=asset_id,
            overall_score=overall,
            structural_fidelity=structural,
            semantic_fidelity=semantic,
            entropy_loss=entropy_loss,
            metric_count=len(metrics),
            recommendations=tuple(recommendations),
            details=details,
        )

        # Persist if database configured
        if self.db_path:
            self._persist_report(report)

        return report

    def _generate_recommendations(
        self,
        structural: float,
        semantic: float,
        entropy_loss: float,
        metrics: list[ContextMetric],
    ) -> list[str]:
        """Generate actionable recommendations based on fidelity scores.

        Args:
            structural: Structural fidelity score.
            semantic: Semantic fidelity score.
            entropy_loss: Entropy loss ratio.
            metrics: The evaluated metrics.

        Returns:
            List of recommendation strings.
        """
        recommendations: list[str] = []

        # Check semantic coverage
        metrics_without_semantic = sum(1 for m in metrics if not m.semantic_id)
        if metrics_without_semantic > 0:
            pct = (metrics_without_semantic / len(metrics)) * 100
            recommendations.append(
                f"Add semantic IDs to {metrics_without_semantic} metrics ({pct:.0f}% missing)"
            )

        # Check for unnamed elements
        unnamed_count = sum(1 for m in metrics if "unnamed" in m.path.lower())
        if unnamed_count > 0:
            recommendations.append(f"Assign id_short to {unnamed_count} unnamed elements")

        # Check unit coverage for numeric values
        numeric_without_unit = sum(
            1 for m in metrics if isinstance(m.value, (int, float)) and not m.unit
        )
        if numeric_without_unit > 0:
            recommendations.append(f"Add units to {numeric_without_unit} numeric metrics")

        # Check submodel context
        without_submodel_ctx = sum(
            1
            for m in metrics
            if not (hasattr(m, "submodel_semantic_id") and m.submodel_semantic_id)
        )
        if without_submodel_ctx > len(metrics) * 0.5:
            recommendations.append("Add semantic IDs to submodels for better context")

        # General score-based recommendations
        if structural < 0.7:
            recommendations.append("Review AAS structure - significant path flattening detected")

        if semantic < 0.7:
            recommendations.append("Improve semantic coverage for better interoperability")

        if entropy_loss > 0.3:
            recommendations.append(
                "Enable extract_all_semantic_keys to preserve poly-hierarchical refs"
            )

        return recommendations

    def _persist_report(self, report: FidelityReport) -> None:
        """Persist a fidelity report to database."""
        if not self.db_path:
            return

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO fidelity_history
                (asset_id, timestamp_ms, overall_score, structural_fidelity,
                 semantic_fidelity, entropy_loss, metric_count, grade)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    report.asset_id,
                    report.timestamp_ms,
                    report.overall_score,
                    report.structural_fidelity,
                    report.semantic_fidelity,
                    report.entropy_loss,
                    report.metric_count,
                    report.grade,
                ),
            )
            conn.commit()

    def get_fidelity_trend(
        self,
        asset_id: str,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Get historical fidelity trend for an asset.

        Args:
            asset_id: The asset identifier.
            limit: Maximum records to return.

        Returns:
            List of historical fidelity records.
        """
        if not self.db_path:
            return []

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT timestamp_ms, overall_score, structural_fidelity,
                       semantic_fidelity, entropy_loss, metric_count, grade
                FROM fidelity_history
                WHERE asset_id = ?
                ORDER BY timestamp_ms DESC
                LIMIT ?
            """,
                (asset_id, limit),
            )

            return [
                {
                    "timestamp": row[0],
                    "overallScore": row[1],
                    "structuralFidelity": row[2],
                    "semanticFidelity": row[3],
                    "entropyLoss": row[4],
                    "metricCount": row[5],
                    "grade": row[6],
                }
                for row in cursor
            ]

    def calculate_batch_fidelity(
        self,
        asset_metrics: dict[str, list[ContextMetric]],
    ) -> dict[str, FidelityReport]:
        """Calculate fidelity for multiple assets.

        Args:
            asset_metrics: Mapping of asset IDs to their metrics.

        Returns:
            Mapping of asset IDs to fidelity reports.
        """
        return {
            asset_id: self.calculate_asset_fidelity(asset_id, metrics)
            for asset_id, metrics in asset_metrics.items()
        }
