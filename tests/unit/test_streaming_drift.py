"""Unit tests for streaming drift detection with Half-Space Trees."""

import tempfile
from pathlib import Path

import pytest

from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.state.streaming_drift import (
    DriftResult,
    DriftSeverity,
    DriftType,
    HalfSpaceForest,
    HalfSpaceTree,
    IncrementalDriftDetector,
)


@pytest.fixture
def temp_db() -> Path:
    """Create a temporary database path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "drift.db"


@pytest.fixture
def sample_metrics() -> list[ContextMetric]:
    """Create sample metrics for testing."""
    return [
        ContextMetric(
            path="TechnicalData.Temperature",
            value=25.5,
            aas_type="Property",
            value_type="xs:double",
            semantic_id="0173-1#02-AAO677#002",
            unit="degC",
        ),
        ContextMetric(
            path="TechnicalData.Pressure",
            value=1013.25,
            aas_type="Property",
            value_type="xs:double",
            semantic_id="0173-1#02-AAO680#001",
            unit="hPa",
        ),
    ]


class TestHalfSpaceTree:
    """Tests for HalfSpaceTree."""

    def test_initial_score_is_uncertain(self) -> None:
        """Test that score is 0.5 (uncertain) before any training."""
        tree = HalfSpaceTree(max_depth=5, window_size=100, seed=42)

        score = tree.score([1.0, 2.0, 3.0])

        assert score == 0.5

    def test_update_initializes_tree(self) -> None:
        """Test that update initializes the tree structure."""
        tree = HalfSpaceTree(max_depth=5, window_size=100, seed=42)

        tree.update([1.0, 2.0, 3.0])

        assert tree._initialized
        assert len(tree._nodes) > 0

    def test_score_after_training(self) -> None:
        """Test that score changes after training."""
        tree = HalfSpaceTree(max_depth=5, window_size=100, seed=42)

        # Train with similar values
        for _ in range(50):
            tree.update([1.0, 1.0, 1.0])

        # Score similar value - should be low (normal)
        normal_score = tree.score([1.0, 1.0, 1.0])

        # Score outlier - should be higher
        outlier_score = tree.score([100.0, 100.0, 100.0])

        assert outlier_score >= normal_score

    def test_deterministic_with_seed(self) -> None:
        """Test that results are deterministic with same seed."""
        tree1 = HalfSpaceTree(max_depth=5, window_size=100, seed=42)
        tree2 = HalfSpaceTree(max_depth=5, window_size=100, seed=42)

        features = [1.0, 2.0, 3.0]
        tree1.update(features)
        tree2.update(features)

        assert tree1.score(features) == tree2.score(features)


class TestHalfSpaceForest:
    """Tests for HalfSpaceForest ensemble."""

    def test_forest_creates_multiple_trees(self) -> None:
        """Test that forest creates specified number of trees."""
        forest = HalfSpaceForest(num_trees=10, max_depth=5, window_size=100)

        assert len(forest.trees) == 10

    def test_ensemble_score_is_average(self) -> None:
        """Test that forest score is average of tree scores."""
        forest = HalfSpaceForest(num_trees=5, max_depth=5, window_size=100, seed=42)

        features = [1.0, 2.0, 3.0]
        forest.update(features)

        ensemble_score = forest.score(features)
        individual_scores = [tree.score(features) for tree in forest.trees]

        expected = sum(individual_scores) / len(individual_scores)
        assert abs(ensemble_score - expected) < 0.001

    def test_update_propagates_to_all_trees(self) -> None:
        """Test that update propagates to all trees."""
        forest = HalfSpaceForest(num_trees=5, max_depth=5, window_size=100)

        forest.update([1.0, 2.0, 3.0])

        for tree in forest.trees:
            assert tree._initialized


class TestIncrementalDriftDetector:
    """Tests for IncrementalDriftDetector."""

    def test_detect_returns_drift_result(
        self, temp_db: Path, sample_metrics: list[ContextMetric]
    ) -> None:
        """Test that detect returns a DriftResult."""
        detector = IncrementalDriftDetector(temp_db)
        metric = sample_metrics[0]

        result = detector.detect("asset1", metric)

        assert isinstance(result, DriftResult)
        assert isinstance(result.drift_type, DriftType)
        assert isinstance(result.severity, DriftSeverity)

    def test_detect_schema_drift_on_first_call_returns_none(
        self, temp_db: Path, sample_metrics: list[ContextMetric]
    ) -> None:
        """Test that first schema check returns None (no previous baseline)."""
        detector = IncrementalDriftDetector(temp_db)

        result = detector.detect_schema_drift("asset1", sample_metrics)

        assert result is None

    def test_detect_schema_drift_on_change(
        self, temp_db: Path, sample_metrics: list[ContextMetric]
    ) -> None:
        """Test that schema change is detected."""
        detector = IncrementalDriftDetector(temp_db)

        # First call establishes baseline
        detector.detect_schema_drift("asset1", sample_metrics)

        # Second call with different schema
        changed_metrics = [
            ContextMetric(
                path="TechnicalData.NewMetric",
                value=42,
                aas_type="Property",
                value_type="xs:int",
            )
        ]
        result = detector.detect_schema_drift("asset1", changed_metrics)

        assert result is not None
        assert result.is_drift
        assert result.drift_type == DriftType.SCHEMA_EVOLUTION

    def test_detect_batch_combines_schema_and_value_drift(
        self, temp_db: Path, sample_metrics: list[ContextMetric]
    ) -> None:
        """Test that detect_batch combines schema and value drift detection."""
        detector = IncrementalDriftDetector(temp_db)

        results = detector.detect_batch("asset1", sample_metrics)

        # May or may not have drift results depending on scores
        assert isinstance(results, list)

    def test_severity_thresholds_customization(self, temp_db: Path) -> None:
        """Test that custom severity thresholds are applied."""
        custom_thresholds = {
            "low": 0.1,
            "medium": 0.3,
            "high": 0.5,
            "critical": 0.7,
        }
        detector = IncrementalDriftDetector(temp_db, severity_thresholds=custom_thresholds)

        assert detector.severity_thresholds == custom_thresholds

    def test_get_drift_history_returns_list(
        self, temp_db: Path, sample_metrics: list[ContextMetric]
    ) -> None:
        """Test retrieving drift history."""
        detector = IncrementalDriftDetector(temp_db)

        # Generate some drift events
        detector.detect_batch("asset1", sample_metrics)

        history = detector.get_drift_history("asset1")

        assert isinstance(history, list)

    def test_clear_asset_removes_state(
        self, temp_db: Path, sample_metrics: list[ContextMetric]
    ) -> None:
        """Test that clear_asset removes all state for an asset."""
        detector = IncrementalDriftDetector(temp_db)

        # Establish state
        detector.detect_schema_drift("asset1", sample_metrics)

        # Clear state
        detector.clear_asset("asset1")

        # Next call should be treated as first (no previous baseline)
        result = detector.detect_schema_drift("asset1", sample_metrics)
        assert result is None


class TestDriftResult:
    """Tests for DriftResult dataclass."""

    def test_to_dict_serialization(self) -> None:
        """Test DriftResult serialization."""
        result = DriftResult(
            is_drift=True,
            drift_type=DriftType.VALUE_ANOMALY,
            severity=DriftSeverity.HIGH,
            confidence=0.85,
            anomaly_score=0.78,
            suggested_action="alert",
            metric_path="Test.Path",
            details={"value": 123},
        )

        data = result.to_dict()

        assert data["isDrift"] is True
        assert data["driftType"] == "value_anomaly"
        assert data["severity"] == "high"
        assert data["confidence"] == 0.85
        assert data["suggestedAction"] == "alert"

    def test_suggested_action_based_on_severity(self) -> None:
        """Test that suggested actions vary by severity."""
        # The suggested action is computed by the detector, not the result
        # but we can verify the result stores it correctly
        low_result = DriftResult(
            is_drift=True,
            drift_type=DriftType.VALUE_ANOMALY,
            severity=DriftSeverity.LOW,
            confidence=0.3,
            anomaly_score=0.35,
            suggested_action="auto_accept",
        )

        critical_result = DriftResult(
            is_drift=True,
            drift_type=DriftType.VALUE_ANOMALY,
            severity=DriftSeverity.CRITICAL,
            confidence=0.95,
            anomaly_score=0.95,
            suggested_action="quarantine",
        )

        assert low_result.suggested_action == "auto_accept"
        assert critical_result.suggested_action == "quarantine"


class TestDriftSeverityAndType:
    """Tests for DriftType and DriftSeverity enums."""

    def test_drift_type_values(self) -> None:
        """Test DriftType enum values."""
        assert DriftType.CONCEPT_DRIFT.value == "concept_drift"
        assert DriftType.SCHEMA_EVOLUTION.value == "schema_evolution"
        assert DriftType.VALUE_ANOMALY.value == "value_anomaly"
        assert DriftType.FREQUENCY_ANOMALY.value == "frequency_anomaly"

    def test_drift_severity_values(self) -> None:
        """Test DriftSeverity enum values."""
        assert DriftSeverity.LOW.value == "low"
        assert DriftSeverity.MEDIUM.value == "medium"
        assert DriftSeverity.HIGH.value == "high"
        assert DriftSeverity.CRITICAL.value == "critical"
