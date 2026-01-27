"""Core domain models for the AAS-UNS Bridge."""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ContextMetric:
    """Immutable representation of a flattened AAS metric for UNS publication.

    A ContextMetric captures a single value extracted from an AAS submodel element,
    along with all metadata needed to publish it to both UNS retained topics and
    Sparkplug B payloads.

    Supports poly-hierarchical semantic references through the semantic_keys tuple,
    which captures all semantic identifiers from composite references. The semantic_id
    field provides backward compatibility by returning the primary (first) key.
    """

    path: str
    """Dot-separated path from submodel root.

    Example: 'TechnicalData.GeneralInformation.ManufacturerName'.
    """

    value: Any
    """The metric value (string, int, float, bool, or None)."""

    aas_type: str
    """AAS element type (e.g., 'Property', 'MultiLanguageProperty', 'Range')."""

    value_type: str
    """XSD value type (e.g., 'xs:string', 'xs:int', 'xs:double')."""

    semantic_id: str | None = None
    """Primary IRDI or IRI semantic identifier (e.g., '0173-1#02-AAO677#002').

    For poly-hierarchical references, this is the first key. Use semantic_keys
    for access to all keys.
    """

    unit: str | None = None
    """Unit of measurement from DataSpecificationIEC61360 (e.g., 'mm', 'kg')."""

    aas_source: str = ""
    """Source identifier (file path or repository URL)."""

    timestamp_ms: int = 0
    """Unix timestamp in milliseconds when the metric was extracted."""

    semantic_keys: tuple[str, ...] = ()
    """All semantic keys for poly-hierarchical references.

    AAS elements can have composite semantic references pointing to multiple
    concepts in different dictionaries. This tuple captures all keys in order.
    If empty, semantic_id is the only reference.
    """

    submodel_semantic_id: str | None = None
    """Semantic identifier of the parent submodel.

    Provides context about which submodel template this metric belongs to,
    useful for understanding the metric's role in the overall asset model.
    """


@dataclass(frozen=True, slots=True)
class AssetIdentity:
    """ISA-95 hierarchy mapping for an AAS asset.

    Maps a globalAssetId to the ISA-95 equipment hierarchy levels,
    enabling construction of proper UNS topic paths.
    """

    global_asset_id: str
    """The AAS globalAssetId (typically an IRI)."""

    enterprise: str
    """ISA-95 Level 4: Enterprise name."""

    site: str = ""
    """ISA-95 Level 3: Site/Plant name."""

    area: str = ""
    """ISA-95 Level 2: Area name."""

    line: str = ""
    """ISA-95 Level 1: Production line name."""

    asset: str = ""
    """Asset/equipment identifier within the line."""

    def topic_prefix(self) -> str:
        """Build the UNS topic prefix from non-empty hierarchy levels."""
        parts = [p for p in [self.enterprise, self.site, self.area, self.line, self.asset] if p]
        return "/".join(parts)


@dataclass(frozen=True, slots=True)
class SubmodelInfo:
    """Metadata about a submodel being processed."""

    id_short: str
    """Short identifier of the submodel."""

    semantic_id: str | None
    """Semantic identifier of the submodel."""

    global_asset_id: str
    """The asset this submodel belongs to."""
