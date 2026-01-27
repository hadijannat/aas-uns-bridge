"""Core domain models for semantic context management.

This module defines the foundational types for the Semantic Hypervisor:
- SemanticPointer: Compact hash-based reference to semantic context (16 chars)
- SemanticContext: Full semantic definition for caching and distribution

The pointer mechanism enables 90% payload overhead reduction by replacing
inline metadata (150-250 bytes) with hash references (10-50 bytes).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class SemanticPointer:
    """Hash-based reference to a semantic context.

    A SemanticPointer provides a compact representation of semantic identity
    that can be included in MQTT payloads and User Properties. Subscribers
    can resolve the pointer to full SemanticContext via the resolution cache
    or UNS/Sys/Context/{dictionary}/{hash} topics.

    Attributes:
        hash: SHA256 hash of semantic_id, truncated to 16 characters.
        dictionary: Source dictionary identifier (e.g., ECLASS, IEC_CDD, custom).
        version: Dictionary version string.
    """

    hash: str
    dictionary: str
    version: str

    @classmethod
    def from_semantic_id(
        cls,
        semantic_id: str,
        dictionary: str = "unknown",
        version: str = "1.0",
    ) -> SemanticPointer:
        """Create a pointer from a semantic ID.

        Args:
            semantic_id: The full semantic identifier (IRDI or IRI).
            dictionary: Source dictionary name.
            version: Dictionary version.

        Returns:
            A SemanticPointer with computed hash.
        """
        hash_value = hashlib.sha256(semantic_id.encode("utf-8")).hexdigest()[:16]
        return cls(hash=hash_value, dictionary=dictionary, version=version)

    def to_dict(self) -> dict[str, str]:
        """Convert to dictionary for JSON serialization."""
        return {
            "hash": self.hash,
            "dictionary": self.dictionary,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> SemanticPointer:
        """Create from dictionary."""
        return cls(
            hash=data["hash"],
            dictionary=data.get("dictionary", "unknown"),
            version=data.get("version", "1.0"),
        )


def _detect_dictionary(semantic_id: str) -> str:
    """Detect the semantic dictionary from an identifier.

    Args:
        semantic_id: The semantic identifier (IRDI or IRI).

    Returns:
        Dictionary name (ECLASS, IEC_CDD, or custom).
    """
    if not semantic_id:
        return "unknown"

    # ECLASS IRDIs: 0173-1#02-XXX#NNN format
    if semantic_id.startswith("0173-1#") or semantic_id.startswith("0173-1---"):
        return "ECLASS"

    # IEC CDD IRDIs: 0112/2///XXX format
    if semantic_id.startswith("0112/"):
        return "IEC_CDD"

    # IRI-based semantic IDs
    if semantic_id.startswith("http://") or semantic_id.startswith("https://"):
        if "eclass" in semantic_id.lower():
            return "ECLASS"
        if "iec" in semantic_id.lower() or "cdd" in semantic_id.lower():
            return "IEC_CDD"
        if "admin-shell.io" in semantic_id:
            return "IDTA"
        return "IRI"

    return "custom"


def _extract_version(semantic_id: str) -> str:
    """Extract version from a semantic ID if present.

    Args:
        semantic_id: The semantic identifier.

    Returns:
        Extracted version or "1.0" default.
    """
    if not semantic_id:
        return "1.0"

    # ECLASS version in #NNN suffix
    if "#" in semantic_id:
        parts = semantic_id.split("#")
        if len(parts) >= 3:
            return parts[-1]

    return "1.0"


@dataclass(frozen=True, slots=True)
class SemanticContext:
    """Full semantic definition for caching and distribution.

    SemanticContext captures all semantic metadata for an AAS element,
    enabling subscribers to understand the meaning, constraints, and
    relationships of metric values. Contexts are cached locally and
    distributed via UNS/Sys/Context topics.

    Attributes:
        semantic_id: Primary semantic identifier (IRDI or IRI).
        dictionary: Source dictionary (ECLASS, IEC_CDD, custom).
        version: Dictionary version.
        definition: Human-readable definition of the concept.
        preferred_name: Preferred display name.
        unit: Unit of measurement (UCUM code).
        data_type: Expected data type.
        hierarchy: All semantic keys for poly-hierarchical references.
    """

    semantic_id: str
    dictionary: str = field(default="unknown")
    version: str = field(default="1.0")
    definition: str | None = field(default=None)
    preferred_name: str | None = field(default=None)
    unit: str | None = field(default=None)
    data_type: str | None = field(default=None)
    hierarchy: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        """Validate and normalize fields after initialization."""
        # Ensure hierarchy includes semantic_id if not already present
        if self.semantic_id and self.semantic_id not in self.hierarchy:
            # Use object.__setattr__ since frozen
            object.__setattr__(self, "hierarchy", (self.semantic_id, *self.hierarchy))

    @classmethod
    def from_semantic_id(
        cls,
        semantic_id: str,
        unit: str | None = None,
        data_type: str | None = None,
        additional_keys: tuple[str, ...] = (),
    ) -> SemanticContext:
        """Create a context from a semantic ID with auto-detected dictionary.

        Args:
            semantic_id: The semantic identifier.
            unit: Optional unit of measurement.
            data_type: Optional data type.
            additional_keys: Additional semantic keys for poly-hierarchical refs.

        Returns:
            A SemanticContext with detected dictionary and version.
        """
        dictionary = _detect_dictionary(semantic_id)
        version = _extract_version(semantic_id)

        return cls(
            semantic_id=semantic_id,
            dictionary=dictionary,
            version=version,
            unit=unit,
            data_type=data_type,
            hierarchy=additional_keys,
        )

    def to_pointer(self) -> SemanticPointer:
        """Create a SemanticPointer from this context.

        Returns:
            A SemanticPointer referencing this context.
        """
        return SemanticPointer.from_semantic_id(
            self.semantic_id,
            self.dictionary,
            self.version,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "semanticId": self.semantic_id,
            "dictionary": self.dictionary,
            "version": self.version,
            "definition": self.definition,
            "preferredName": self.preferred_name,
            "unit": self.unit,
            "dataType": self.data_type,
            "hierarchy": list(self.hierarchy),
        }

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SemanticContext:
        """Create from dictionary."""
        return cls(
            semantic_id=data["semanticId"],
            dictionary=data.get("dictionary", "unknown"),
            version=data.get("version", "1.0"),
            definition=data.get("definition"),
            preferred_name=data.get("preferredName"),
            unit=data.get("unit"),
            data_type=data.get("dataType"),
            hierarchy=tuple(data.get("hierarchy", [])),
        )

    @classmethod
    def from_json(cls, json_str: str) -> SemanticContext:
        """Create from JSON string."""
        return cls.from_dict(json.loads(json_str))

    @property
    def hash(self) -> str:
        """Get the hash for this context's semantic ID."""
        return hashlib.sha256(self.semantic_id.encode("utf-8")).hexdigest()[:16]
