"""Unit tests for performance metrics (TRL 8 Task 19)."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

from prometheus_client import REGISTRY

from aas_uns_bridge.observability.metrics import METRICS


def _get_histogram_count(name: str, labels: dict[str, str] | None = None) -> float:
    """Get histogram count from registry."""
    for metric in REGISTRY.collect():
        if metric.name == name:
            for sample in metric.samples:
                if sample.name == f"{name}_count" and (
                    labels is None or all(sample.labels.get(k) == v for k, v in labels.items())
                ):
                    return sample.value
    return 0.0


class TestMetricDefinitions:
    """Test that all required metrics are registered."""

    def test_publish_latency_metric_exists(self) -> None:
        """Verify publish_latency_seconds histogram is registered."""
        assert hasattr(METRICS, "publish_latency_seconds")
        # Verify it's a Histogram by checking for observe method
        assert hasattr(METRICS.publish_latency_seconds.labels(publisher_type="uns"), "observe")

    def test_state_db_size_metric_exists(self) -> None:
        """Verify state_db_size_bytes gauge is registered."""
        assert hasattr(METRICS, "state_db_size_bytes")
        # Verify it's a Gauge by checking for set method
        assert hasattr(METRICS.state_db_size_bytes.labels(db_type="alias"), "set")

    def test_traversal_duration_metric_exists(self) -> None:
        """Verify traversal_duration_seconds histogram is registered."""
        assert hasattr(METRICS, "traversal_duration_seconds")
        # Verify it's a Histogram by checking for observe method
        assert hasattr(METRICS.traversal_duration_seconds, "observe")

    def test_aas_load_duration_metric_exists(self) -> None:
        """Verify aas_load_duration_seconds histogram is registered."""
        assert hasattr(METRICS, "aas_load_duration_seconds")
        # Verify it's a Histogram by checking for observe method
        assert hasattr(METRICS.aas_load_duration_seconds.labels(source_type="file"), "observe")


class TestStateDbSizeReporting:
    """Test that state databases report their file size."""

    def test_alias_db_reports_size(self) -> None:
        """Verify AliasDB reports its file size on initialization."""
        from aas_uns_bridge.state.alias_db import AliasDB

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "alias.db"

            # Create the database
            db = AliasDB(db_path, max_entries=100)

            # Add some data to ensure the file has content
            db.get_alias("metric/test/path", device_id="device1")

            # Get the reported metric value
            # The metric should be set after the add operation
            metric_value = METRICS.state_db_size_bytes.labels(db_type="alias")._value.get()

            # Verify size is greater than 0 (SQLite has overhead even for small DBs)
            assert metric_value > 0

    def test_last_published_reports_size(self) -> None:
        """Verify LastPublishedHashes reports its file size on initialization."""
        from aas_uns_bridge.domain.models import ContextMetric
        from aas_uns_bridge.state.last_published import LastPublishedHashes

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "hashes.db"

            # Create the database with persistence
            hashes = LastPublishedHashes(db_path=db_path, max_entries=100)

            # Add some data
            metric = ContextMetric(
                path="test/path",
                value=42,
                aas_type="Property",
                value_type="xs:int",
                semantic_id=None,
                unit=None,
                aas_source="test",
                timestamp_ms=1234567890,
            )
            hashes.update("test/topic", metric)

            # Get the reported metric value
            metric_value = METRICS.state_db_size_bytes.labels(db_type="hash")._value.get()

            # Verify size is greater than 0
            assert metric_value > 0


class TestTraversalDurationMetric:
    """Test that traversal duration is tracked."""

    def test_traversal_records_duration(self) -> None:
        """Verify flatten_submodel records traversal duration."""
        from basyx.aas import model

        from aas_uns_bridge.aas.traversal import flatten_submodel

        # Create a simple submodel
        submodel = model.Submodel(
            id_="https://example.com/submodel",
            id_short="TestSubmodel",
        )
        prop = model.Property(
            id_short="TestProperty",
            value_type=str,
            value="test_value",
        )
        submodel.submodel_element = {prop}

        initial_count = _get_histogram_count("aas_bridge_traversal_duration_seconds")

        # Call flatten_submodel
        metrics = flatten_submodel(submodel, aas_source="test.json")

        # Verify the duration was recorded (count should have increased)
        new_count = _get_histogram_count("aas_bridge_traversal_duration_seconds")
        assert new_count > initial_count
        assert len(metrics) > 0


class TestPublishLatencyMetric:
    """Test that publish latency is tracked."""

    def test_uns_publish_records_latency(self) -> None:
        """Verify UNS publisher records publish latency."""
        from aas_uns_bridge.config import SemanticConfig, UnsConfig
        from aas_uns_bridge.domain.models import ContextMetric
        from aas_uns_bridge.publishers.uns_retained import UnsRetainedPublisher

        # Create mock MQTT client
        mock_client = MagicMock()
        mock_client.is_connected.return_value = True

        config = UnsConfig(enabled=True, retain=True, qos=0)
        semantic_config = SemanticConfig()

        publisher = UnsRetainedPublisher(
            mqtt_client=mock_client,
            config=config,
            semantic_config=semantic_config,
        )

        metric = ContextMetric(
            path="test/path",
            value=42,
            aas_type="Property",
            value_type="xs:int",
            semantic_id=None,
            unit=None,
            aas_source="test",
            timestamp_ms=1234567890,
        )

        # Record initial count
        initial_count = _get_histogram_count(
            "aas_bridge_publish_latency_seconds", {"publisher_type": "uns"}
        )

        # Publish a metric
        publisher.publish_metric("test/topic", metric)

        # Verify latency was recorded
        new_count = _get_histogram_count(
            "aas_bridge_publish_latency_seconds", {"publisher_type": "uns"}
        )
        assert new_count > initial_count

    def test_sparkplug_publish_records_latency(self) -> None:
        """Verify Sparkplug publisher records publish latency."""
        from aas_uns_bridge.config import SparkplugConfig
        from aas_uns_bridge.publishers.sparkplug import SparkplugPublisher
        from aas_uns_bridge.state.alias_db import AliasDB

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "alias.db"
            alias_db = AliasDB(db_path, max_entries=100)

            # Create mock MQTT client
            mock_client = MagicMock()
            mock_client.is_connected.return_value = True

            config = SparkplugConfig(
                enabled=True,
                group_id="test_group",
                edge_node_id="test_node",
                qos=0,
            )

            publisher = SparkplugPublisher(
                mqtt_client=mock_client,
                config=config,
                alias_db=alias_db,
            )

            # Record initial count
            initial_count = _get_histogram_count(
                "aas_bridge_publish_latency_seconds", {"publisher_type": "sparkplug"}
            )

            # Publish NBIRTH
            publisher.publish_nbirth()

            # Verify latency was recorded
            new_count = _get_histogram_count(
                "aas_bridge_publish_latency_seconds", {"publisher_type": "sparkplug"}
            )
            assert new_count > initial_count


class TestAasLoadDurationMetric:
    """Test that AAS load duration is tracked."""

    def test_load_json_records_duration(self) -> None:
        """Verify load_json records load duration."""
        import json as json_lib

        from aas_uns_bridge.aas.loader import load_json

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a minimal AAS JSON file
            json_path = Path(tmpdir) / "test.json"
            aas_content = {
                "assetAdministrationShells": [],
                "submodels": [],
                "conceptDescriptions": [],
            }
            with open(json_path, "w") as f:
                json_lib.dump(aas_content, f)

            # Record initial count
            initial_count = _get_histogram_count(
                "aas_bridge_aas_load_duration_seconds", {"source_type": "file"}
            )

            # Load the file
            load_json(json_path)

            # Verify duration was recorded
            new_count = _get_histogram_count(
                "aas_bridge_aas_load_duration_seconds", {"source_type": "file"}
            )
            assert new_count > initial_count
