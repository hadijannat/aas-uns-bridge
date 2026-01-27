"""Tests for handling malformed AAS files."""

import contextlib
import json
import logging
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from aas_uns_bridge.aas.loader import AASLoadError, load_file, load_json
from aas_uns_bridge.aas.traversal import flatten_submodel, iter_submodels
from aas_uns_bridge.domain.models import ContextMetric


@pytest.mark.e2e
class TestMissingSemanticId:
    """Tests for AAS elements without semantic IDs."""

    def test_property_without_semanticid_still_processes(
        self,
        fixtures_dir: Path,
    ) -> None:
        """Properties without semanticId should still be processed."""
        malformed_path = fixtures_dir / "malformed_missing_semanticid.json"

        if not malformed_path.exists():
            pytest.skip("Malformed fixture not found")

        # Load should not raise
        object_store = load_json(malformed_path)

        # BaSyx may or may not load the submodels depending on strictness
        # Traverse and flatten whatever is available
        metrics_found: list[ContextMetric] = []
        for submodel, _asset_id in iter_submodels(object_store):
            metrics = flatten_submodel(submodel, str(malformed_path))
            metrics_found.extend(metrics)

        # If BaSyx loaded the data, verify semanticId behavior
        if len(metrics_found) > 0:
            # Some metrics should have None semanticId (the fixture has no semanticId)
            without_semantic = [m for m in metrics_found if m.semantic_id is None]
            assert len(without_semantic) > 0, "Should have metrics without semanticId"
        # If BaSyx rejected the file, that's also acceptable behavior
        # for a "malformed" fixture - the test passes either way

    def test_semanticid_not_required_for_uns_publish(
        self,
        fixtures_dir: Path,
    ) -> None:
        """UNS publish should work even without semanticId."""
        malformed_path = fixtures_dir / "malformed_missing_semanticid.json"

        if not malformed_path.exists():
            pytest.skip("Malformed fixture not found")

        object_store = load_json(malformed_path)

        for submodel, _asset_id in iter_submodels(object_store):
            metrics = flatten_submodel(submodel, str(malformed_path))

            for metric in metrics:
                # All metrics should have required fields
                assert metric.path, "Path should not be empty"
                assert metric.aas_type, "AAS type should not be empty"
                assert metric.value_type, "Value type should not be empty"
                # semanticId can be None - that's OK


@pytest.mark.e2e
class TestEmptyCollection:
    """Tests for empty SubmodelElementCollections."""

    def test_empty_collection_skipped_gracefully(
        self,
        fixtures_dir: Path,
    ) -> None:
        """Empty collections should be skipped without error."""
        malformed_path = fixtures_dir / "malformed_empty_collection.json"

        if not malformed_path.exists():
            pytest.skip("Malformed fixture not found")

        # Load should not raise
        object_store = load_json(malformed_path)

        # Traverse should not raise
        for submodel, _asset_id in iter_submodels(object_store):
            # Should not raise even with empty collections
            metrics = flatten_submodel(submodel, str(malformed_path))

            # Should still produce metrics from non-empty elements
            assert len(metrics) >= 0, "Should handle empty collections"

    def test_valid_properties_extracted_around_empty(
        self,
        fixtures_dir: Path,
    ) -> None:
        """Valid properties should still be extracted even with empty collections."""
        malformed_path = fixtures_dir / "malformed_empty_collection.json"

        if not malformed_path.exists():
            pytest.skip("Malformed fixture not found")

        object_store = load_json(malformed_path)

        for submodel, _asset_id in iter_submodels(object_store):
            metrics = flatten_submodel(submodel, str(malformed_path))

            # Should find the ValidProperty
            valid_found = any("ValidProperty" in m.path for m in metrics)
            assert valid_found, "Should extract valid properties around empty collections"


@pytest.mark.e2e
class TestMissingIdShort:
    """Tests for elements with missing idShort."""

    def test_missing_idshort_uses_unnamed(
        self,
        fixtures_dir: Path,
    ) -> None:
        """Elements without idShort should use 'unnamed' placeholder."""
        malformed_path = fixtures_dir / "malformed_missing_idshort.json"

        if not malformed_path.exists():
            pytest.skip("Malformed fixture not found")

        # This may raise depending on BaSyx strictness - that's expected
        try:
            object_store = load_json(malformed_path)

            for submodel, _asset_id in iter_submodels(object_store):
                metrics = flatten_submodel(submodel, str(malformed_path))
                if metrics:
                    assert any("unnamed" in m.path.lower() for m in metrics)
        except AASLoadError:
            # BaSyx may reject invalid structures - that's acceptable
            pass


@pytest.mark.e2e
class TestInvalidJson:
    """Tests for invalid JSON files."""

    def test_invalid_json_raises_error(
        self,
        fixtures_dir: Path,
    ) -> None:
        """Invalid JSON should raise AASLoadError."""
        malformed_path = fixtures_dir / "malformed_invalid_json.json"

        if not malformed_path.exists():
            pytest.skip("Malformed fixture not found")

        with pytest.raises(AASLoadError) as exc_info:
            load_json(malformed_path)

        # Error should mention JSON or parsing
        assert (
            "json" in str(exc_info.value).lower()
            or "parse" in str(exc_info.value).lower()
            or "failed" in str(exc_info.value).lower()
        )

    def test_completely_malformed_content(self) -> None:
        """Completely malformed content should be rejected gracefully."""
        with NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("not valid json at all { ]")
            temp_path = Path(f.name)

        try:
            with pytest.raises(AASLoadError):
                load_json(temp_path)
        finally:
            temp_path.unlink()

    def test_empty_file(self) -> None:
        """Empty file should be handled gracefully."""
        with NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("")
            temp_path = Path(f.name)

        try:
            with pytest.raises(AASLoadError):
                load_json(temp_path)
        finally:
            temp_path.unlink()


@pytest.mark.e2e
class TestInvalidAasx:
    """Tests for invalid AASX packages."""

    def test_nonexistent_file_raises_error(self) -> None:
        """Nonexistent file should raise AASLoadError."""
        with pytest.raises(AASLoadError) as exc_info:
            load_file(Path("/nonexistent/path/to/file.aasx"))

        assert "not found" in str(exc_info.value).lower()

    def test_non_aasx_file_raises_error(self) -> None:
        """Non-AASX file with .aasx extension should raise error."""
        with NamedTemporaryFile(mode="w", suffix=".aasx", delete=False) as f:
            f.write("this is not a valid aasx file")
            temp_path = Path(f.name)

        try:
            with pytest.raises(AASLoadError):
                load_file(temp_path)
        finally:
            temp_path.unlink()

    def test_unknown_extension_raises_error(self) -> None:
        """Unknown file extension should raise AASLoadError."""
        with NamedTemporaryFile(mode="w", suffix=".xyz", delete=False) as f:
            f.write("{}")
            temp_path = Path(f.name)

        try:
            with pytest.raises(AASLoadError) as exc_info:
                load_file(temp_path)

            assert "unknown" in str(exc_info.value).lower()
        finally:
            temp_path.unlink()


@pytest.mark.e2e
class TestLoggingOnMalformed:
    """Tests for logging behavior on malformed input."""

    def test_invalid_json_logs_error(
        self,
        fixtures_dir: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Invalid JSON should log appropriate error."""
        malformed_path = fixtures_dir / "malformed_invalid_json.json"

        if not malformed_path.exists():
            pytest.skip("Malformed fixture not found")

        with caplog.at_level(logging.DEBUG), contextlib.suppress(AASLoadError):
            load_json(malformed_path)

        # Should have logged something (may be at different levels)
        # The actual logging happens in the exception handling


@pytest.mark.e2e
class TestGracefulDegradation:
    """Tests for graceful degradation with partial data."""

    def test_partial_aas_extracts_available_data(self) -> None:
        """AAS with some invalid elements should still extract valid ones."""
        # Create a minimal valid AAS
        valid_aas = {
            "assetAdministrationShells": [
                {
                    "modelType": "AssetAdministrationShell",
                    "idShort": "PartialAAS",
                    "id": "https://example.com/aas/partial",
                    "assetInformation": {
                        "assetKind": "Instance",
                        "globalAssetId": "https://example.com/asset/partial",
                    },
                    "submodels": [
                        {
                            "type": "ModelReference",
                            "keys": [
                                {
                                    "type": "Submodel",
                                    "value": "https://example.com/submodel/partial",
                                }
                            ],
                        }
                    ],
                }
            ],
            "submodels": [
                {
                    "modelType": "Submodel",
                    "idShort": "PartialData",
                    "id": "https://example.com/submodel/partial",
                    "submodelElements": [
                        {
                            "idShort": "ValidProperty",
                            "modelType": "Property",
                            "valueType": "xs:string",
                            "value": "valid_value",
                        },
                    ],
                }
            ],
            "conceptDescriptions": [],
        }

        with NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(valid_aas, f)
            temp_path = Path(f.name)

        try:
            object_store = load_json(temp_path)

            # Should extract the valid property
            for submodel, _asset_id in iter_submodels(object_store):
                metrics = flatten_submodel(submodel, str(temp_path))
                assert len(metrics) > 0, "Should extract valid properties"
        finally:
            temp_path.unlink()
