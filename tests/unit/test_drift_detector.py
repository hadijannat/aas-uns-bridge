"""Unit tests for schema drift detection."""

import json
import tempfile
from pathlib import Path

import pytest

from aas_uns_bridge.config import DriftConfig
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.state.drift_detector import (
    DriftDetector,
    DriftEventType,
    MetricFingerprint,
)


@pytest.fixture
def temp_db() -> Path:
    """Create a temporary database path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "drift.db"


@pytest.fixture
def basic_config() -> DriftConfig:
    """Create a basic drift detection config."""
    return DriftConfig(
        enabled=True,
        track_additions=True,
        track_removals=True,
        track_type_changes=True,
        alert_topic_template="UNS/Sys/DriftAlerts/{asset_id}",
    )


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


class TestMetricFingerprint:
    """Tests for MetricFingerprint."""

    def test_from_metric(self) -> None:
        """Test creating fingerprint from metric."""
        metric = ContextMetric(
            path="TechnicalData.Temperature",
            value=25.5,
            aas_type="Property",
            value_type="xs:double",
            semantic_id="0173-1#02-AAO677#002",
            unit="degC",
        )

        fp = MetricFingerprint.from_metric(metric)

        assert fp.path == "TechnicalData.Temperature"
        assert fp.aas_type == "Property"
        assert fp.value_type == "xs:double"
        assert fp.semantic_id == "0173-1#02-AAO677#002"
        assert fp.unit == "degC"

    def test_hash_stable(self) -> None:
        """Test that hash is stable for same data."""
        fp1 = MetricFingerprint(
            path="Test.Path",
            aas_type="Property",
            value_type="xs:string",
        )
        fp2 = MetricFingerprint(
            path="Test.Path",
            aas_type="Property",
            value_type="xs:string",
        )

        assert fp1.hash == fp2.hash

    def test_hash_differs_for_different_data(self) -> None:
        """Test that hash differs for different data."""
        fp1 = MetricFingerprint(
            path="Test.Path",
            aas_type="Property",
            value_type="xs:string",
        )
        fp2 = MetricFingerprint(
            path="Test.Path",
            aas_type="Property",
            value_type="xs:double",  # Different type
        )

        assert fp1.hash != fp2.hash


class TestDriftDetectorAdditions:
    """Tests for detecting metric additions."""

    def test_detect_new_metrics(
        self, temp_db: Path, basic_config: DriftConfig, sample_metrics: list[ContextMetric]
    ) -> None:
        """Test detecting newly added metrics."""
        detector = DriftDetector(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        # First detection should find all metrics as additions
        result = detector.detect_drift(asset_id, sample_metrics)

        assert result.has_drift
        assert len(result.additions) == 2
        assert all(e.event_type == DriftEventType.ADDED for e in result.events)

    def test_no_additions_after_update(
        self, temp_db: Path, basic_config: DriftConfig, sample_metrics: list[ContextMetric]
    ) -> None:
        """Test that no additions detected after fingerprint update."""
        detector = DriftDetector(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        # Update fingerprints
        detector.update_fingerprints(asset_id, sample_metrics)

        # Second detection should find no drift
        result = detector.detect_drift(asset_id, sample_metrics)

        assert not result.has_drift

    def test_detect_single_addition(
        self, temp_db: Path, basic_config: DriftConfig, sample_metrics: list[ContextMetric]
    ) -> None:
        """Test detecting a single new metric."""
        detector = DriftDetector(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        # Store initial metrics
        detector.update_fingerprints(asset_id, sample_metrics[:1])

        # Detect with additional metric
        result = detector.detect_drift(asset_id, sample_metrics)

        assert result.has_drift
        assert len(result.additions) == 1
        assert result.additions[0].metric_path == "TechnicalData.Pressure"


class TestDriftDetectorRemovals:
    """Tests for detecting metric removals."""

    def test_detect_removed_metrics(
        self, temp_db: Path, basic_config: DriftConfig, sample_metrics: list[ContextMetric]
    ) -> None:
        """Test detecting removed metrics."""
        detector = DriftDetector(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        # Store all metrics
        detector.update_fingerprints(asset_id, sample_metrics)

        # Detect with only one metric (one removed)
        result = detector.detect_drift(asset_id, sample_metrics[:1])

        assert result.has_drift
        assert len(result.removals) == 1
        assert result.removals[0].metric_path == "TechnicalData.Pressure"

    def test_removal_includes_previous_fingerprint(
        self, temp_db: Path, basic_config: DriftConfig, sample_metrics: list[ContextMetric]
    ) -> None:
        """Test that removal event includes previous fingerprint."""
        detector = DriftDetector(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        detector.update_fingerprints(asset_id, sample_metrics)
        result = detector.detect_drift(asset_id, sample_metrics[:1])

        removal = result.removals[0]
        assert removal.previous is not None
        assert removal.previous.semantic_id == "0173-1#02-AAO680#001"


class TestDriftDetectorChanges:
    """Tests for detecting metric changes."""

    def test_detect_type_change(self, temp_db: Path, basic_config: DriftConfig) -> None:
        """Test detecting value_type change."""
        detector = DriftDetector(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        original = [
            ContextMetric(
                path="TechnicalData.Value",
                value=25,
                aas_type="Property",
                value_type="xs:int",
            ),
        ]
        changed = [
            ContextMetric(
                path="TechnicalData.Value",
                value=25.0,
                aas_type="Property",
                value_type="xs:double",  # Changed from int to double
            ),
        ]

        detector.update_fingerprints(asset_id, original)
        result = detector.detect_drift(asset_id, changed)

        assert result.has_drift
        assert len(result.changes) == 1
        assert result.events[0].event_type == DriftEventType.TYPE_CHANGED

    def test_detect_unit_change(self, temp_db: Path, basic_config: DriftConfig) -> None:
        """Test detecting unit change."""
        detector = DriftDetector(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        original = [
            ContextMetric(
                path="TechnicalData.Temperature",
                value=25.5,
                aas_type="Property",
                value_type="xs:double",
                unit="degC",
            ),
        ]
        changed = [
            ContextMetric(
                path="TechnicalData.Temperature",
                value=77.9,
                aas_type="Property",
                value_type="xs:double",
                unit="degF",  # Changed unit
            ),
        ]

        detector.update_fingerprints(asset_id, original)
        result = detector.detect_drift(asset_id, changed)

        assert result.has_drift
        assert result.events[0].event_type == DriftEventType.UNIT_CHANGED

    def test_detect_semantic_id_change(self, temp_db: Path, basic_config: DriftConfig) -> None:
        """Test detecting semantic ID change."""
        detector = DriftDetector(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        original = [
            ContextMetric(
                path="TechnicalData.Temperature",
                value=25.5,
                aas_type="Property",
                value_type="xs:double",
                semantic_id="old-semantic-id",
            ),
        ]
        changed = [
            ContextMetric(
                path="TechnicalData.Temperature",
                value=25.5,
                aas_type="Property",
                value_type="xs:double",
                semantic_id="new-semantic-id",  # Changed
            ),
        ]

        detector.update_fingerprints(asset_id, original)
        result = detector.detect_drift(asset_id, changed)

        assert result.has_drift
        assert result.events[0].event_type == DriftEventType.SEMANTIC_CHANGED


class TestDriftDetectorConfiguration:
    """Tests for drift detector configuration."""

    def test_track_additions_disabled(self, temp_db: Path) -> None:
        """Test that additions are not tracked when disabled."""
        config = DriftConfig(
            enabled=True,
            track_additions=False,
            track_removals=True,
            track_type_changes=True,
        )
        detector = DriftDetector(temp_db, config)
        asset_id = "https://example.com/asset/001"

        metrics = [
            ContextMetric(
                path="Test.Path",
                value="test",
                aas_type="Property",
                value_type="xs:string",
            ),
        ]

        result = detector.detect_drift(asset_id, metrics)

        assert len(result.additions) == 0

    def test_track_removals_disabled(self, temp_db: Path) -> None:
        """Test that removals are not tracked when disabled."""
        config = DriftConfig(
            enabled=True,
            track_additions=True,
            track_removals=False,
            track_type_changes=True,
        )
        detector = DriftDetector(temp_db, config)
        asset_id = "https://example.com/asset/001"

        initial = [
            ContextMetric(
                path="Test.Path",
                value="test",
                aas_type="Property",
                value_type="xs:string",
            ),
        ]

        detector.update_fingerprints(asset_id, initial)
        result = detector.detect_drift(asset_id, [])  # All removed

        assert len(result.removals) == 0


class TestDriftDetectorAlerts:
    """Tests for drift alert generation."""

    def test_build_alert_topic(self, temp_db: Path, basic_config: DriftConfig) -> None:
        """Test building alert topic from asset ID."""
        detector = DriftDetector(temp_db, basic_config)

        topic = detector.build_alert_topic("https://example.com/asset/001")

        assert topic == "UNS/Sys/DriftAlerts/example.com_asset_001"

    def test_build_alert_payload(
        self, temp_db: Path, basic_config: DriftConfig, sample_metrics: list[ContextMetric]
    ) -> None:
        """Test building alert payload."""
        detector = DriftDetector(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        result = detector.detect_drift(asset_id, sample_metrics)
        event = result.events[0]

        payload = detector.build_alert_payload(event)
        data = json.loads(payload)

        assert data["eventType"] == "added"
        assert data["assetId"] == asset_id
        assert "timestamp" in data


class TestDriftDetectorPersistence:
    """Tests for drift detector persistence."""

    def test_fingerprints_persist_across_instances(
        self, temp_db: Path, basic_config: DriftConfig, sample_metrics: list[ContextMetric]
    ) -> None:
        """Test that fingerprints persist across detector instances."""
        asset_id = "https://example.com/asset/001"

        # First instance stores fingerprints
        detector1 = DriftDetector(temp_db, basic_config)
        detector1.update_fingerprints(asset_id, sample_metrics)

        # Second instance should see them
        detector2 = DriftDetector(temp_db, basic_config)
        result = detector2.detect_drift(asset_id, sample_metrics)

        assert not result.has_drift

    def test_clear_asset(
        self, temp_db: Path, basic_config: DriftConfig, sample_metrics: list[ContextMetric]
    ) -> None:
        """Test clearing fingerprints for an asset."""
        detector = DriftDetector(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        detector.update_fingerprints(asset_id, sample_metrics)
        count = detector.clear_asset(asset_id)

        assert count == 2

        # Should detect all as additions again
        result = detector.detect_drift(asset_id, sample_metrics)
        assert len(result.additions) == 2

    def test_get_all_assets(
        self, temp_db: Path, basic_config: DriftConfig, sample_metrics: list[ContextMetric]
    ) -> None:
        """Test getting all tracked asset IDs."""
        detector = DriftDetector(temp_db, basic_config)

        detector.update_fingerprints("asset1", sample_metrics[:1])
        detector.update_fingerprints("asset2", sample_metrics[1:])

        assets = detector.get_all_assets()

        assert set(assets) == {"asset1", "asset2"}
