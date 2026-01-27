"""Unit tests for ISA-95 mapping."""

import pytest

from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.mapping.isa95 import (
    HierarchyLevel,
    ISA95Mapper,
    MappingConfig,
    PatternMapping,
)


@pytest.fixture
def sample_config() -> MappingConfig:
    """Create a sample mapping configuration."""
    return MappingConfig(
        default=HierarchyLevel(
            enterprise="DefaultCorp",
            site="DefaultSite",
        ),
        assets={
            "https://example.com/aas/robot-001": HierarchyLevel(
                enterprise="AcmeCorp",
                site="PlantA",
                area="Assembly",
                line="Line1",
                asset="Robot001",
            ),
        },
        patterns=[
            PatternMapping(
                pattern="https://example.com/aas/sensor-*",
                enterprise="AcmeCorp",
                site="PlantA",
                area="Sensors",
            ),
        ],
    )


@pytest.fixture
def sample_metric() -> ContextMetric:
    """Create a sample context metric."""
    return ContextMetric(
        path="TechnicalData.GeneralInfo.ManufacturerName",
        value="Acme",
        aas_type="Property",
        value_type="xs:string",
        aas_source="test.aasx",
        timestamp_ms=1234567890000,
    )


class TestISA95Mapper:
    """Tests for ISA95Mapper class."""

    def test_exact_match(self, sample_config: MappingConfig) -> None:
        """Test that exact asset ID matches work."""
        mapper = ISA95Mapper(sample_config)
        identity = mapper.get_identity("https://example.com/aas/robot-001")

        assert identity.enterprise == "AcmeCorp"
        assert identity.site == "PlantA"
        assert identity.area == "Assembly"
        assert identity.line == "Line1"
        assert identity.asset == "Robot001"

    def test_pattern_match(self, sample_config: MappingConfig) -> None:
        """Test that pattern matching works."""
        mapper = ISA95Mapper(sample_config)
        identity = mapper.get_identity("https://example.com/aas/sensor-temperature-42")

        assert identity.enterprise == "AcmeCorp"
        assert identity.site == "PlantA"
        assert identity.area == "Sensors"
        # Asset extracted from URL
        assert identity.asset == "sensor-temperature-42"

    def test_default_fallback(self, sample_config: MappingConfig) -> None:
        """Test that default mapping is used for unknown assets."""
        mapper = ISA95Mapper(sample_config)
        identity = mapper.get_identity("https://unknown.com/asset/xyz")

        assert identity.enterprise == "DefaultCorp"
        assert identity.site == "DefaultSite"

    def test_build_topic(self, sample_config: MappingConfig, sample_metric: ContextMetric) -> None:
        """Test topic construction."""
        mapper = ISA95Mapper(sample_config)
        topic = mapper.build_topic(
            sample_metric,
            global_asset_id="https://example.com/aas/robot-001",
            submodel_id_short="TechnicalData",
        )

        expected = (
            "AcmeCorp/PlantA/Assembly/Line1/Robot001/context/TechnicalData/"
            "GeneralInfo/ManufacturerName"
        )
        assert topic == expected

    def test_build_topic_with_root(
        self, sample_config: MappingConfig, sample_metric: ContextMetric
    ) -> None:
        """Test topic construction with root prefix."""
        mapper = ISA95Mapper(sample_config, root_topic="uns")
        topic = mapper.build_topic(
            sample_metric,
            global_asset_id="https://example.com/aas/robot-001",
            submodel_id_short="TechnicalData",
        )

        assert topic.startswith("uns/")
        assert "AcmeCorp" in topic

    def test_build_topic_unknown_asset(
        self, sample_config: MappingConfig, sample_metric: ContextMetric
    ) -> None:
        """Test topic construction with unknown asset."""
        mapper = ISA95Mapper(sample_config)
        topic = mapper.build_topic(
            sample_metric,
            global_asset_id=None,
            submodel_id_short="TechnicalData",
        )

        assert topic.startswith("DefaultCorp/")
        assert "context/TechnicalData" in topic

    def test_identity_caching(self, sample_config: MappingConfig) -> None:
        """Test that identities are cached."""
        mapper = ISA95Mapper(sample_config)
        asset_id = "https://example.com/aas/robot-001"

        identity1 = mapper.get_identity(asset_id)
        identity2 = mapper.get_identity(asset_id)

        assert identity1 is identity2  # Same object from cache

    def test_topic_sanitization(self, sample_config: MappingConfig) -> None:
        """Test that topics are properly sanitized."""
        # Create config with special characters
        config = MappingConfig(
            default=HierarchyLevel(
                enterprise="Acme Corp",  # Space
                site="Plant+A",  # Plus sign
            ),
        )
        mapper = ISA95Mapper(config)

        metric = ContextMetric(
            path="Data.Element Name",
            value="test",
            aas_type="Property",
            value_type="xs:string",
        )

        topic = mapper.build_topic(metric, None, "Data")

        # Verify sanitization
        assert "+" not in topic
        assert " " not in topic
        assert "_" in topic  # Spaces converted to underscores

    def test_build_topics_for_submodel(self, sample_config: MappingConfig) -> None:
        """Test building topics for multiple metrics."""
        mapper = ISA95Mapper(sample_config)
        metrics = [
            ContextMetric(
                path="Submodel.Prop1",
                value="val1",
                aas_type="Property",
                value_type="xs:string",
            ),
            ContextMetric(
                path="Submodel.Prop2",
                value="val2",
                aas_type="Property",
                value_type="xs:string",
            ),
        ]

        topic_map = mapper.build_topics_for_submodel(
            metrics,
            global_asset_id="https://example.com/aas/robot-001",
            submodel_id_short="Submodel",
        )

        assert len(topic_map) == 2
        assert all("context/Submodel" in topic for topic in topic_map)
