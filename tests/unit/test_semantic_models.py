"""Unit tests for semantic models (SemanticPointer, SemanticContext)."""

from aas_uns_bridge.semantic.models import (
    SemanticContext,
    SemanticPointer,
    _detect_dictionary,
    _extract_version,
)


class TestSemanticPointer:
    """Tests for SemanticPointer."""

    def test_from_semantic_id_creates_hash(self) -> None:
        """Test that from_semantic_id creates a 16-character hash."""
        pointer = SemanticPointer.from_semantic_id(
            semantic_id="0173-1#02-AAO677#002",
            dictionary="ECLASS",
            version="14.0",
        )

        assert len(pointer.hash) == 16
        assert pointer.dictionary == "ECLASS"
        assert pointer.version == "14.0"

    def test_hash_is_deterministic(self) -> None:
        """Test that the same semantic ID produces the same hash."""
        p1 = SemanticPointer.from_semantic_id("0173-1#02-AAO677#002")
        p2 = SemanticPointer.from_semantic_id("0173-1#02-AAO677#002")

        assert p1.hash == p2.hash

    def test_different_ids_produce_different_hashes(self) -> None:
        """Test that different semantic IDs produce different hashes."""
        p1 = SemanticPointer.from_semantic_id("0173-1#02-AAO677#002")
        p2 = SemanticPointer.from_semantic_id("0173-1#02-AAO680#001")

        assert p1.hash != p2.hash

    def test_to_dict_roundtrip(self) -> None:
        """Test serialization and deserialization."""
        original = SemanticPointer.from_semantic_id(
            semantic_id="0173-1#02-AAO677#002",
            dictionary="ECLASS",
            version="14.0",
        )

        data = original.to_dict()
        restored = SemanticPointer.from_dict(data)

        assert restored == original
        assert restored.hash == original.hash

    def test_default_dictionary_and_version(self) -> None:
        """Test default values when not specified."""
        pointer = SemanticPointer.from_semantic_id("some-id")

        assert pointer.dictionary == "unknown"
        assert pointer.version == "1.0"


class TestSemanticContext:
    """Tests for SemanticContext."""

    def test_from_semantic_id_auto_detects_eclass(self) -> None:
        """Test auto-detection of ECLASS dictionary."""
        context = SemanticContext.from_semantic_id("0173-1#02-AAO677#002")

        assert context.dictionary == "ECLASS"
        assert context.version == "002"

    def test_from_semantic_id_auto_detects_iec_cdd(self) -> None:
        """Test auto-detection of IEC CDD dictionary."""
        context = SemanticContext.from_semantic_id("0112/2///12345")

        assert context.dictionary == "IEC_CDD"

    def test_from_semantic_id_auto_detects_iri(self) -> None:
        """Test auto-detection of IRI-based semantic IDs."""
        context = SemanticContext.from_semantic_id("https://example.org/vocab/property")

        assert context.dictionary == "IRI"

    def test_from_semantic_id_detects_idta(self) -> None:
        """Test detection of IDTA semantic IDs."""
        context = SemanticContext.from_semantic_id(
            "https://admin-shell.io/idta/carbonFootprint/1/0"
        )

        assert context.dictionary == "IDTA"

    def test_hierarchy_includes_semantic_id(self) -> None:
        """Test that hierarchy always includes the primary semantic_id."""
        context = SemanticContext(
            semantic_id="0173-1#02-AAO677#002",
            hierarchy=("other-key-1", "other-key-2"),
        )

        assert "0173-1#02-AAO677#002" in context.hierarchy
        assert len(context.hierarchy) == 3

    def test_hierarchy_does_not_duplicate(self) -> None:
        """Test that semantic_id is not duplicated if already in hierarchy."""
        context = SemanticContext(
            semantic_id="0173-1#02-AAO677#002",
            hierarchy=("0173-1#02-AAO677#002", "other-key"),
        )

        assert context.hierarchy.count("0173-1#02-AAO677#002") == 1

    def test_to_pointer(self) -> None:
        """Test creating a pointer from context."""
        context = SemanticContext(
            semantic_id="0173-1#02-AAO677#002",
            dictionary="ECLASS",
            version="14.0",
        )

        pointer = context.to_pointer()

        assert pointer.dictionary == "ECLASS"
        assert pointer.version == "14.0"
        assert len(pointer.hash) == 16

    def test_to_dict_roundtrip(self) -> None:
        """Test serialization and deserialization."""
        original = SemanticContext(
            semantic_id="0173-1#02-AAO677#002",
            dictionary="ECLASS",
            version="14.0",
            definition="Operating temperature",
            preferred_name="Temperature",
            unit="degC",
            data_type="xs:double",
            hierarchy=("parent-key",),
        )

        data = original.to_dict()
        restored = SemanticContext.from_dict(data)

        assert restored.semantic_id == original.semantic_id
        assert restored.dictionary == original.dictionary
        assert restored.definition == original.definition
        assert restored.unit == original.unit

    def test_to_json_roundtrip(self) -> None:
        """Test JSON serialization and deserialization."""
        original = SemanticContext(
            semantic_id="0173-1#02-AAO677#002",
            dictionary="ECLASS",
            version="14.0",
            unit="degC",
        )

        json_str = original.to_json()
        restored = SemanticContext.from_json(json_str)

        assert restored == original

    def test_hash_property(self) -> None:
        """Test the hash property matches pointer hash."""
        context = SemanticContext.from_semantic_id("0173-1#02-AAO677#002")
        pointer = context.to_pointer()

        assert context.hash == pointer.hash


class TestDetectDictionary:
    """Tests for _detect_dictionary helper."""

    def test_eclass_irdi_format(self) -> None:
        """Test ECLASS IRDI format detection."""
        assert _detect_dictionary("0173-1#02-AAO677#002") == "ECLASS"
        assert _detect_dictionary("0173-1---XXXXX---") == "ECLASS"

    def test_iec_cdd_format(self) -> None:
        """Test IEC CDD format detection."""
        assert _detect_dictionary("0112/2///12345") == "IEC_CDD"

    def test_iri_format(self) -> None:
        """Test generic IRI detection."""
        assert _detect_dictionary("https://example.org/vocab") == "IRI"
        assert _detect_dictionary("http://example.org/vocab") == "IRI"

    def test_eclass_iri_format(self) -> None:
        """Test ECLASS IRI format detection."""
        assert _detect_dictionary("https://eclass.org/property/123") == "ECLASS"

    def test_unknown_format(self) -> None:
        """Test unknown format fallback."""
        assert _detect_dictionary("some-random-string") == "custom"

    def test_empty_string(self) -> None:
        """Test empty string handling."""
        assert _detect_dictionary("") == "unknown"


class TestExtractVersion:
    """Tests for _extract_version helper."""

    def test_eclass_version_extraction(self) -> None:
        """Test version extraction from ECLASS IRDI."""
        assert _extract_version("0173-1#02-AAO677#002") == "002"
        assert _extract_version("0173-1#02-AAO677#014") == "014"

    def test_no_version_default(self) -> None:
        """Test default version when none present."""
        assert _extract_version("https://example.org/vocab") == "1.0"

    def test_empty_string(self) -> None:
        """Test empty string handling."""
        assert _extract_version("") == "1.0"
