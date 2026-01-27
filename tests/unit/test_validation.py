"""Unit tests for semantic validation."""

import pytest

from aas_uns_bridge.config import ValidationConfig, ValueConstraint
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.validation import SemanticValidator
from aas_uns_bridge.validation.semantic_validator import ErrorType


@pytest.fixture
def basic_config() -> ValidationConfig:
    """Create a basic validation config."""
    return ValidationConfig(
        enabled=True,
        enforce_semantic_ids=True,
        required_for_types=["Property", "Range"],
        reject_invalid=False,
    )


@pytest.fixture
def config_with_constraints() -> ValidationConfig:
    """Create a config with value constraints."""
    return ValidationConfig(
        enabled=True,
        enforce_semantic_ids=True,
        required_for_types=["Property"],
        reject_invalid=True,
        value_constraints={
            "0173-1#02-AAO677#002": ValueConstraint(  # Temperature
                min=-40.0,
                max=120.0,
                unit="degC",
            ),
            "0173-1#02-AAH880#002": ValueConstraint(  # Serial number
                pattern=r"^[A-Z]{2}[0-9]{6}$",
            ),
        },
    )


class TestSemanticIdValidation:
    """Tests for semantic ID validation."""

    def test_missing_semantic_id_property(self, basic_config: ValidationConfig) -> None:
        """Test that missing semantic ID on Property is flagged."""
        validator = SemanticValidator(basic_config)
        metric = ContextMetric(
            path="TechnicalData.Name",
            value="Test",
            aas_type="Property",
            value_type="xs:string",
        )

        result = validator.validate(metric)

        assert not result.is_valid
        assert len(result.errors) == 1
        assert result.errors[0].error_type == ErrorType.MISSING_SEMANTIC_ID

    def test_missing_semantic_id_range(self, basic_config: ValidationConfig) -> None:
        """Test that missing semantic ID on Range is flagged."""
        validator = SemanticValidator(basic_config)
        metric = ContextMetric(
            path="TechnicalData.TempRange",
            value=50.0,
            aas_type="Range",
            value_type="xs:double",
        )

        result = validator.validate(metric)

        assert not result.is_valid
        assert result.errors[0].error_type == ErrorType.MISSING_SEMANTIC_ID

    def test_missing_semantic_id_not_required_type(
        self, basic_config: ValidationConfig
    ) -> None:
        """Test that missing semantic ID on non-required type is OK."""
        validator = SemanticValidator(basic_config)
        metric = ContextMetric(
            path="TechnicalData.Collection",
            value="data",
            aas_type="SubmodelElementCollection",
            value_type="xs:string",
        )

        result = validator.validate(metric)

        assert result.is_valid

    def test_valid_semantic_id(self, basic_config: ValidationConfig) -> None:
        """Test that valid semantic ID passes."""
        validator = SemanticValidator(basic_config)
        metric = ContextMetric(
            path="TechnicalData.Temperature",
            value=25.5,
            aas_type="Property",
            value_type="xs:double",
            semantic_id="0173-1#02-AAO677#002",
        )

        result = validator.validate(metric)

        assert result.is_valid

    def test_semantic_id_enforcement_disabled(self) -> None:
        """Test that semantic ID enforcement can be disabled."""
        config = ValidationConfig(
            enabled=True,
            enforce_semantic_ids=False,
            required_for_types=["Property"],
        )
        validator = SemanticValidator(config)
        metric = ContextMetric(
            path="TechnicalData.Name",
            value="Test",
            aas_type="Property",
            value_type="xs:string",
        )

        result = validator.validate(metric)

        assert result.is_valid


class TestValueConstraints:
    """Tests for value constraint validation."""

    def test_value_below_min(self, config_with_constraints: ValidationConfig) -> None:
        """Test that value below minimum is flagged."""
        validator = SemanticValidator(config_with_constraints)
        metric = ContextMetric(
            path="TechnicalData.Temperature",
            value=-50.0,  # Below min of -40
            aas_type="Property",
            value_type="xs:double",
            semantic_id="0173-1#02-AAO677#002",
            unit="degC",
        )

        result = validator.validate(metric)

        assert not result.is_valid
        errors = [e for e in result.errors if e.error_type == ErrorType.VALUE_OUT_OF_RANGE]
        assert len(errors) == 1
        assert "below minimum" in errors[0].message

    def test_value_above_max(self, config_with_constraints: ValidationConfig) -> None:
        """Test that value above maximum is flagged."""
        validator = SemanticValidator(config_with_constraints)
        metric = ContextMetric(
            path="TechnicalData.Temperature",
            value=150.0,  # Above max of 120
            aas_type="Property",
            value_type="xs:double",
            semantic_id="0173-1#02-AAO677#002",
            unit="degC",
        )

        result = validator.validate(metric)

        assert not result.is_valid
        errors = [e for e in result.errors if e.error_type == ErrorType.VALUE_OUT_OF_RANGE]
        assert len(errors) == 1
        assert "exceeds maximum" in errors[0].message

    def test_value_within_range(self, config_with_constraints: ValidationConfig) -> None:
        """Test that value within range passes."""
        validator = SemanticValidator(config_with_constraints)
        metric = ContextMetric(
            path="TechnicalData.Temperature",
            value=25.5,
            aas_type="Property",
            value_type="xs:double",
            semantic_id="0173-1#02-AAO677#002",
            unit="degC",
        )

        result = validator.validate(metric)

        assert result.is_valid

    def test_unit_mismatch(self, config_with_constraints: ValidationConfig) -> None:
        """Test that unit mismatch is flagged."""
        validator = SemanticValidator(config_with_constraints)
        metric = ContextMetric(
            path="TechnicalData.Temperature",
            value=25.5,
            aas_type="Property",
            value_type="xs:double",
            semantic_id="0173-1#02-AAO677#002",
            unit="degF",  # Wrong unit
        )

        result = validator.validate(metric)

        assert not result.is_valid
        errors = [e for e in result.errors if e.error_type == ErrorType.UNIT_MISMATCH]
        assert len(errors) == 1

    def test_pattern_match(self, config_with_constraints: ValidationConfig) -> None:
        """Test that valid pattern passes."""
        validator = SemanticValidator(config_with_constraints)
        metric = ContextMetric(
            path="TechnicalData.SerialNumber",
            value="AB123456",  # Matches ^[A-Z]{2}[0-9]{6}$
            aas_type="Property",
            value_type="xs:string",
            semantic_id="0173-1#02-AAH880#002",
        )

        result = validator.validate(metric)

        assert result.is_valid

    def test_pattern_mismatch(self, config_with_constraints: ValidationConfig) -> None:
        """Test that invalid pattern is flagged."""
        validator = SemanticValidator(config_with_constraints)
        metric = ContextMetric(
            path="TechnicalData.SerialNumber",
            value="12AB3456",  # Doesn't match pattern
            aas_type="Property",
            value_type="xs:string",
            semantic_id="0173-1#02-AAH880#002",
        )

        result = validator.validate(metric)

        assert not result.is_valid
        errors = [e for e in result.errors if e.error_type == ErrorType.PATTERN_MISMATCH]
        assert len(errors) == 1


class TestBatchValidation:
    """Tests for batch validation."""

    def test_batch_validation_mixed(self, basic_config: ValidationConfig) -> None:
        """Test batch validation with mixed valid/invalid metrics."""
        validator = SemanticValidator(basic_config)
        metrics = [
            ContextMetric(
                path="TechnicalData.Valid",
                value="test",
                aas_type="Property",
                value_type="xs:string",
                semantic_id="valid-id",
            ),
            ContextMetric(
                path="TechnicalData.Invalid",
                value="test",
                aas_type="Property",
                value_type="xs:string",
                # Missing semantic_id
            ),
        ]

        result = validator.validate_batch(metrics)

        assert len(result.valid_metrics) == 1
        assert result.invalid_count == 1
        assert result.total_errors == 1

    def test_filter_valid(self, basic_config: ValidationConfig) -> None:
        """Test filtering to only valid metrics."""
        validator = SemanticValidator(basic_config)
        metrics = [
            ContextMetric(
                path="TechnicalData.Valid1",
                value="test",
                aas_type="Property",
                value_type="xs:string",
                semantic_id="id1",
            ),
            ContextMetric(
                path="TechnicalData.Invalid",
                value="test",
                aas_type="Property",
                value_type="xs:string",
            ),
            ContextMetric(
                path="TechnicalData.Valid2",
                value="test",
                aas_type="Property",
                value_type="xs:string",
                semantic_id="id2",
            ),
        ]

        valid = validator.filter_valid(metrics)

        assert len(valid) == 2
        assert all(m.semantic_id is not None for m in valid)

    def test_all_errors_flattened(
        self, config_with_constraints: ValidationConfig
    ) -> None:
        """Test that all errors are properly flattened."""
        validator = SemanticValidator(config_with_constraints)
        metrics = [
            ContextMetric(
                path="TechnicalData.Temp1",
                value=200.0,  # Out of range
                aas_type="Property",
                value_type="xs:double",
                semantic_id="0173-1#02-AAO677#002",
                unit="degF",  # Wrong unit
            ),
            ContextMetric(
                path="TechnicalData.Temp2",
                value=-100.0,  # Out of range
                aas_type="Property",
                value_type="xs:double",
                semantic_id="0173-1#02-AAO677#002",
                unit="degC",
            ),
        ]

        result = validator.validate_batch(metrics)

        # First metric: 2 errors (range + unit)
        # Second metric: 1 error (range)
        assert result.total_errors == 3
        assert len(result.all_errors) == 3


class TestValidationErrorDetails:
    """Tests for validation error details."""

    def test_error_includes_path(self, basic_config: ValidationConfig) -> None:
        """Test that errors include the metric path."""
        validator = SemanticValidator(basic_config)
        metric = ContextMetric(
            path="Deep.Nested.Path.Element",
            value="test",
            aas_type="Property",
            value_type="xs:string",
        )

        result = validator.validate(metric)

        assert result.errors[0].path == "Deep.Nested.Path.Element"

    def test_error_includes_semantic_id(
        self, config_with_constraints: ValidationConfig
    ) -> None:
        """Test that constraint errors include semantic ID."""
        validator = SemanticValidator(config_with_constraints)
        metric = ContextMetric(
            path="TechnicalData.Temperature",
            value=200.0,
            aas_type="Property",
            value_type="xs:double",
            semantic_id="0173-1#02-AAO677#002",
            unit="degC",
        )

        result = validator.validate(metric)

        range_error = next(
            e for e in result.errors if e.error_type == ErrorType.VALUE_OUT_OF_RANGE
        )
        assert range_error.semantic_id == "0173-1#02-AAO677#002"
