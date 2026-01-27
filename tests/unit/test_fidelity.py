"""Unit tests for fidelity calculation."""

import tempfile
from pathlib import Path

import pytest

from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.semantic.fidelity import (
    FidelityCalculator,
    FidelityReport,
)


@pytest.fixture
def temp_db() -> Path:
    """Create a temporary database path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "fidelity.db"


@pytest.fixture
def sample_metrics_high_fidelity() -> list[ContextMetric]:
    """Create sample metrics with high semantic fidelity."""
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
        ContextMetric(
            path="Identification.SerialNumber",
            value="SN-12345",
            aas_type="Property",
            value_type="xs:string",
            semantic_id="0173-1#02-AAM556#002",
        ),
    ]


@pytest.fixture
def sample_metrics_low_fidelity() -> list[ContextMetric]:
    """Create sample metrics with low semantic fidelity."""
    return [
        ContextMetric(
            path="unnamed",  # Poor structure
            value=25.5,
            aas_type="Property",
            value_type="xs:double",
            # No semantic_id
            # No unit
        ),
        ContextMetric(
            path="Data",  # Flat path
            value=1013.25,
            aas_type="Property",
            value_type="xs:double",
            # No semantic_id
        ),
    ]


@pytest.fixture
def calculator() -> FidelityCalculator:
    """Create a fidelity calculator without persistence."""
    return FidelityCalculator(db_path=None)


class TestFidelityCalculatorEntropyLoss:
    """Tests for entropy loss calculation."""

    def test_no_loss_when_all_keys_preserved(self, calculator: FidelityCalculator) -> None:
        """Test zero entropy loss when all keys are preserved."""
        loss = calculator.calculate_entropy_loss(
            original_key_count=5,
            preserved_key_count=5,
        )

        assert loss == 0.0

    def test_total_loss_when_no_keys_preserved(self, calculator: FidelityCalculator) -> None:
        """Test entropy loss approaches 1.0 when keys are reduced."""
        loss = calculator.calculate_entropy_loss(
            original_key_count=10,
            preserved_key_count=1,
        )

        assert loss == 1.0  # log2(1) = 0, so 1 - 0/log2(10) = 1

    def test_partial_loss(self, calculator: FidelityCalculator) -> None:
        """Test partial entropy loss."""
        loss = calculator.calculate_entropy_loss(
            original_key_count=8,
            preserved_key_count=4,
        )

        # log2(4)/log2(8) = 2/3, so loss = 1 - 2/3 ≈ 0.333
        assert 0.3 < loss < 0.4

    def test_no_keys_means_no_loss(self, calculator: FidelityCalculator) -> None:
        """Test that zero original keys means no loss."""
        loss = calculator.calculate_entropy_loss(
            original_key_count=0,
            preserved_key_count=0,
        )

        assert loss == 0.0


class TestFidelityCalculatorSemanticFidelity:
    """Tests for semantic fidelity calculation."""

    def test_high_fidelity_with_semantic_ids(
        self,
        calculator: FidelityCalculator,
        sample_metrics_high_fidelity: list[ContextMetric],
    ) -> None:
        """Test high fidelity when all metrics have semantic IDs."""
        fidelity = calculator.calculate_semantic_fidelity(sample_metrics_high_fidelity)

        assert fidelity > 0.5  # High coverage

    def test_low_fidelity_without_semantic_ids(
        self,
        calculator: FidelityCalculator,
        sample_metrics_low_fidelity: list[ContextMetric],
    ) -> None:
        """Test low fidelity when metrics lack semantic IDs."""
        fidelity = calculator.calculate_semantic_fidelity(sample_metrics_low_fidelity)

        assert fidelity < 0.5  # Low coverage

    def test_empty_metrics_returns_perfect(self, calculator: FidelityCalculator) -> None:
        """Test that empty metrics returns 1.0 (no loss)."""
        fidelity = calculator.calculate_semantic_fidelity([])

        assert fidelity == 1.0


class TestFidelityCalculatorStructuralFidelity:
    """Tests for structural fidelity calculation."""

    def test_high_fidelity_with_deep_paths(
        self,
        calculator: FidelityCalculator,
        sample_metrics_high_fidelity: list[ContextMetric],
    ) -> None:
        """Test high fidelity with well-structured paths."""
        fidelity = calculator.calculate_structural_fidelity(sample_metrics_high_fidelity)

        assert fidelity > 0.7  # Good structure preserved

    def test_low_fidelity_with_flat_paths(
        self,
        calculator: FidelityCalculator,
        sample_metrics_low_fidelity: list[ContextMetric],
    ) -> None:
        """Test lower fidelity with flat/unnamed paths."""
        fidelity = calculator.calculate_structural_fidelity(sample_metrics_low_fidelity)

        assert fidelity < 0.7  # Structure loss detected

    def test_unnamed_elements_reduce_fidelity(self, calculator: FidelityCalculator) -> None:
        """Test that unnamed elements reduce structural fidelity."""
        with_names = [
            ContextMetric(
                path="TechnicalData.Temperature",
                value=25.5,
                aas_type="Property",
                value_type="xs:double",
            ),
        ]
        with_unnamed = [
            ContextMetric(
                path="unnamed.Temperature",
                value=25.5,
                aas_type="Property",
                value_type="xs:double",
            ),
        ]

        fidelity_named = calculator.calculate_structural_fidelity(with_names)
        fidelity_unnamed = calculator.calculate_structural_fidelity(with_unnamed)

        assert fidelity_named > fidelity_unnamed


class TestFidelityReport:
    """Tests for FidelityReport."""

    def test_grade_calculation_a_plus(self) -> None:
        """Test A+ grade for very high fidelity."""
        report = FidelityReport(
            asset_id="test",
            overall_score=0.98,
            structural_fidelity=0.95,
            semantic_fidelity=0.99,
            entropy_loss=0.02,
            metric_count=10,
        )

        assert report.grade == "A+"

    def test_grade_calculation_a(self) -> None:
        """Test A grade for high fidelity."""
        report = FidelityReport(
            asset_id="test",
            overall_score=0.92,
            structural_fidelity=0.90,
            semantic_fidelity=0.95,
            entropy_loss=0.08,
            metric_count=10,
        )

        assert report.grade == "A"

    def test_grade_calculation_b(self) -> None:
        """Test B grade for good fidelity."""
        report = FidelityReport(
            asset_id="test",
            overall_score=0.82,
            structural_fidelity=0.80,
            semantic_fidelity=0.85,
            entropy_loss=0.18,
            metric_count=10,
        )

        assert report.grade == "B"

    def test_grade_calculation_f(self) -> None:
        """Test F grade for poor fidelity."""
        report = FidelityReport(
            asset_id="test",
            overall_score=0.50,
            structural_fidelity=0.45,
            semantic_fidelity=0.55,
            entropy_loss=0.50,
            metric_count=10,
        )

        assert report.grade == "F"

    def test_to_dict_serialization(self) -> None:
        """Test FidelityReport serialization."""
        report = FidelityReport(
            asset_id="test-asset",
            overall_score=0.85,
            structural_fidelity=0.80,
            semantic_fidelity=0.90,
            entropy_loss=0.15,
            metric_count=5,
            recommendations=("Add semantic IDs", "Review structure"),
        )

        data = report.to_dict()

        assert data["assetId"] == "test-asset"
        assert data["overallScore"] == 0.85
        assert len(data["recommendations"]) == 2


class TestFidelityCalculatorAssetFidelity:
    """Tests for calculate_asset_fidelity."""

    def test_calculate_asset_fidelity_high(
        self,
        calculator: FidelityCalculator,
        sample_metrics_high_fidelity: list[ContextMetric],
    ) -> None:
        """Test asset fidelity calculation with good metrics."""
        report = calculator.calculate_asset_fidelity("asset1", sample_metrics_high_fidelity)

        assert isinstance(report, FidelityReport)
        assert report.asset_id == "asset1"
        assert report.overall_score > 0.5
        assert report.metric_count == 3

    def test_calculate_asset_fidelity_low(
        self,
        calculator: FidelityCalculator,
        sample_metrics_low_fidelity: list[ContextMetric],
    ) -> None:
        """Test asset fidelity calculation with poor metrics."""
        report = calculator.calculate_asset_fidelity("asset1", sample_metrics_low_fidelity)

        assert report.overall_score < 0.7  # Should detect issues
        assert len(report.recommendations) > 0  # Should suggest improvements

    def test_calculate_asset_fidelity_empty(self, calculator: FidelityCalculator) -> None:
        """Test asset fidelity with no metrics."""
        report = calculator.calculate_asset_fidelity("asset1", [])

        assert report.overall_score == 1.0  # No degradation when no metrics
        assert report.metric_count == 0


class TestFidelityCalculatorRecommendations:
    """Tests for recommendation generation."""

    def test_recommends_adding_semantic_ids(
        self,
        calculator: FidelityCalculator,
        sample_metrics_low_fidelity: list[ContextMetric],
    ) -> None:
        """Test that missing semantic IDs generate recommendations."""
        report = calculator.calculate_asset_fidelity("asset1", sample_metrics_low_fidelity)

        assert any("semantic" in r.lower() for r in report.recommendations)

    def test_recommends_fixing_unnamed_elements(self, calculator: FidelityCalculator) -> None:
        """Test that unnamed elements generate recommendations."""
        metrics = [
            ContextMetric(
                path="unnamed.Value",
                value=123,
                aas_type="Property",
                value_type="xs:int",
            ),
        ]

        report = calculator.calculate_asset_fidelity("asset1", metrics)

        assert any("unnamed" in r.lower() for r in report.recommendations)

    def test_recommends_adding_units(self, calculator: FidelityCalculator) -> None:
        """Test that missing units on numeric values generate recommendations."""
        metrics = [
            ContextMetric(
                path="TechnicalData.Temperature",
                value=25.5,  # Numeric without unit
                aas_type="Property",
                value_type="xs:double",
                semantic_id="test",
                # No unit
            ),
        ]

        report = calculator.calculate_asset_fidelity("asset1", metrics)

        assert any("unit" in r.lower() for r in report.recommendations)


class TestFidelityCalculatorPersistence:
    """Tests for fidelity persistence."""

    def test_persistence_stores_reports(self, temp_db: Path) -> None:
        """Test that reports are stored in database."""
        calculator = FidelityCalculator(db_path=temp_db)

        metrics = [
            ContextMetric(
                path="Test.Path",
                value=123,
                aas_type="Property",
                value_type="xs:int",
                semantic_id="test",
            ),
        ]

        # Generate report
        calculator.calculate_asset_fidelity("asset1", metrics)

        # Check history
        history = calculator.get_fidelity_trend("asset1")

        assert len(history) == 1
        assert "overallScore" in history[0]

    def test_get_fidelity_trend(self, temp_db: Path) -> None:
        """Test retrieving fidelity trend over time."""
        calculator = FidelityCalculator(db_path=temp_db)

        metrics = [
            ContextMetric(
                path="Test.Path",
                value=123,
                aas_type="Property",
                value_type="xs:int",
                semantic_id="test",
            ),
        ]

        # Generate multiple reports
        for _ in range(3):
            calculator.calculate_asset_fidelity("asset1", metrics)

        history = calculator.get_fidelity_trend("asset1")

        assert len(history) == 3


class TestFidelityCalculatorBatch:
    """Tests for batch fidelity calculation."""

    def test_calculate_batch_fidelity(
        self,
        calculator: FidelityCalculator,
        sample_metrics_high_fidelity: list[ContextMetric],
    ) -> None:
        """Test calculating fidelity for multiple assets."""
        asset_metrics = {
            "asset1": sample_metrics_high_fidelity[:1],
            "asset2": sample_metrics_high_fidelity[1:],
        }

        reports = calculator.calculate_batch_fidelity(asset_metrics)

        assert "asset1" in reports
        assert "asset2" in reports
        assert isinstance(reports["asset1"], FidelityReport)
