"""ISA-95 hierarchy mapping for UNS topic construction."""

import fnmatch
import logging
from pathlib import Path

import yaml
from pydantic import BaseModel

from aas_uns_bridge.domain.models import AssetIdentity, ContextMetric
from aas_uns_bridge.mapping.sanitize import sanitize_metric_path, sanitize_segment

logger = logging.getLogger(__name__)


class HierarchyLevel(BaseModel):
    """ISA-95 hierarchy level configuration."""

    enterprise: str
    site: str = ""
    area: str = ""
    line: str = ""
    asset: str = ""


class PatternMapping(BaseModel):
    """Pattern-based hierarchy mapping."""

    pattern: str
    enterprise: str
    site: str = ""
    area: str = ""
    line: str = ""
    asset: str = ""


class MappingConfig(BaseModel):
    """Configuration for ISA-95 hierarchy mappings."""

    default: HierarchyLevel
    assets: dict[str, HierarchyLevel] = {}
    patterns: list[PatternMapping] = []

    @classmethod
    def from_yaml(cls, path: Path) -> "MappingConfig":
        """Load mapping configuration from YAML file."""
        if not path.exists():
            logger.warning("Mappings file not found: %s, using defaults", path)
            return cls(default=HierarchyLevel(enterprise="Default"))

        with open(path) as f:
            data = yaml.safe_load(f)

        if not data:
            return cls(default=HierarchyLevel(enterprise="Default"))

        # Parse default
        default_data = data.get("default", {"enterprise": "Default"})
        default = HierarchyLevel.model_validate(default_data)

        # Parse asset mappings
        assets: dict[str, HierarchyLevel] = {}
        for asset_id, mapping in data.get("assets", {}).items():
            assets[asset_id] = HierarchyLevel.model_validate(mapping)

        # Parse pattern mappings
        patterns: list[PatternMapping] = []
        for p in data.get("patterns", []):
            patterns.append(PatternMapping.model_validate(p))

        return cls(default=default, assets=assets, patterns=patterns)


class ISA95Mapper:
    """Maps AAS assets to ISA-95 hierarchy for UNS topic construction."""

    def __init__(self, config: MappingConfig, root_topic: str = ""):
        """Initialize the mapper.

        Args:
            config: Hierarchy mapping configuration.
            root_topic: Optional root topic prefix (e.g., "uns").
        """
        self.config = config
        self.root_topic = root_topic.strip("/") if root_topic else ""
        self._cache: dict[str, AssetIdentity] = {}

    def _match_pattern(self, global_asset_id: str) -> PatternMapping | None:
        """Find first matching pattern for an asset ID."""
        for pattern in self.config.patterns:
            if fnmatch.fnmatch(global_asset_id, pattern.pattern):
                return pattern
        return None

    def get_identity(self, global_asset_id: str) -> AssetIdentity:
        """Get the ISA-95 identity for an asset.

        Looks up mappings in order:
        1. Exact match in assets dict
        2. Pattern match
        3. Default mapping

        Args:
            global_asset_id: The AAS globalAssetId.

        Returns:
            AssetIdentity with hierarchy levels populated.
        """
        # Check cache
        if global_asset_id in self._cache:
            return self._cache[global_asset_id]

        # Try exact match
        if global_asset_id in self.config.assets:
            mapping = self.config.assets[global_asset_id]
        else:
            # Try pattern match
            pattern = self._match_pattern(global_asset_id)
            if pattern:
                mapping = HierarchyLevel(
                    enterprise=pattern.enterprise,
                    site=pattern.site,
                    area=pattern.area,
                    line=pattern.line,
                    asset=pattern.asset,
                )
            else:
                # Fall back to default
                mapping = self.config.default

        # Extract asset name from ID if not specified
        asset_name = mapping.asset
        if not asset_name:
            # Try to extract meaningful name from globalAssetId
            asset_name = global_asset_id.rsplit("/", 1)[-1].rsplit("#", 1)[-1]

        identity = AssetIdentity(
            global_asset_id=global_asset_id,
            enterprise=sanitize_segment(mapping.enterprise),
            site=sanitize_segment(mapping.site) if mapping.site else "",
            area=sanitize_segment(mapping.area) if mapping.area else "",
            line=sanitize_segment(mapping.line) if mapping.line else "",
            asset=sanitize_segment(asset_name) if asset_name else "",
        )

        self._cache[global_asset_id] = identity
        return identity

    def build_topic(
        self,
        metric: ContextMetric,
        global_asset_id: str | None,
        submodel_id_short: str,
    ) -> str:
        """Build the full UNS topic path for a metric.

        Topic structure:
        {root}/{enterprise}/{site}/{area}/{line}/{asset}/context/{submodel}/{element_path}

        Args:
            metric: The context metric to publish.
            global_asset_id: The asset's globalAssetId.
            submodel_id_short: The submodel's idShort.

        Returns:
            Full sanitized MQTT topic path.
        """
        parts: list[str] = []

        # Add root topic if configured
        if self.root_topic:
            parts.append(self.root_topic)

        # Add hierarchy levels if asset ID is known
        if global_asset_id:
            identity = self.get_identity(global_asset_id)
            for level in [
                identity.enterprise,
                identity.site,
                identity.area,
                identity.line,
                identity.asset,
            ]:
                if level:
                    parts.append(level)
        else:
            # Fall back to default enterprise
            parts.append(sanitize_segment(self.config.default.enterprise))

        # Add context marker
        parts.append("context")

        # Add submodel name
        parts.append(sanitize_segment(submodel_id_short))

        # Add element path (convert dot-separated to slash-separated)
        # Skip the submodel prefix if it's already in the path
        element_path = metric.path
        if element_path.startswith(f"{submodel_id_short}."):
            element_path = element_path[len(submodel_id_short) + 1 :]

        sanitized_path = sanitize_metric_path(element_path)
        parts.append(sanitized_path)

        return "/".join(parts)

    def build_topics_for_submodel(
        self,
        metrics: list[ContextMetric],
        global_asset_id: str | None,
        submodel_id_short: str,
    ) -> dict[str, ContextMetric]:
        """Build topics for all metrics in a submodel.

        Args:
            metrics: List of context metrics from the submodel.
            global_asset_id: The asset's globalAssetId.
            submodel_id_short: The submodel's idShort.

        Returns:
            Dict mapping topic paths to their metrics.
        """
        return {self.build_topic(m, global_asset_id, submodel_id_short): m for m in metrics}
