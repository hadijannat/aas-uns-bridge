"""Semantic validation for AAS metrics."""

import logging
import re
from dataclasses import dataclass, field
from enum import Enum

from aas_uns_bridge.config import ValidationConfig, ValueConstraint
from aas_uns_bridge.domain.models import ContextMetric

logger = logging.getLogger(__name__)


class ErrorType(Enum):
    """Types of validation errors."""

    MISSING_SEMANTIC_ID = "missing_semantic_id"
    INVALID_VALUE_TYPE = "invalid_value_type"
    VALUE_OUT_OF_RANGE = "value_out_of_range"
    UNIT_MISMATCH = "unit_mismatch"
    PATTERN_MISMATCH = "pattern_mismatch"


@dataclass(frozen=True, slots=True)
class ValidationError:
    """A single validation error."""

    error_type: ErrorType
    """Type of validation error."""

    message: str
    """Human-readable error message."""

    path: str
    """Metric path where the error occurred."""

    semantic_id: str | None = None
    """Semantic ID if relevant to the error."""

    expected: str | None = None
    """Expected value (for constraint errors)."""

    actual: str | None = None
    """Actual value found."""


@dataclass
class ValidationResult:
    """Result of validating a single metric."""

    metric: ContextMetric
    """The validated metric."""

    errors: list[ValidationError] = field(default_factory=list)
    """List of validation errors (empty if valid)."""

    @property
    def is_valid(self) -> bool:
        """Check if the metric passed validation."""
        return len(self.errors) == 0


@dataclass
class BatchValidationResult:
    """Result of validating a batch of metrics."""

    results: list[ValidationResult] = field(default_factory=list)
    """Individual validation results."""

    @property
    def valid_metrics(self) -> list[ContextMetric]:
        """Get metrics that passed validation."""
        return [r.metric for r in self.results if r.is_valid]

    @property
    def invalid_count(self) -> int:
        """Count of metrics that failed validation."""
        return sum(1 for r in self.results if not r.is_valid)

    @property
    def total_errors(self) -> int:
        """Total number of validation errors across all metrics."""
        return sum(len(r.errors) for r in self.results)

    @property
    def all_errors(self) -> list[ValidationError]:
        """Flatten all errors from all results."""
        errors = []
        for result in self.results:
            errors.extend(result.errors)
        return errors


class SemanticValidator:
    """Validates AAS metrics against semantic rules.

    Performs pre-publish validation including:
    - Semantic ID presence (for configured element types)
    - Value type validation
    - Value constraint validation (min/max, unit, pattern)
    """

    def __init__(self, config: ValidationConfig):
        """Initialize the validator.

        Args:
            config: Validation configuration.
        """
        self.config = config
        self._required_types = set(config.required_for_types)
        self._compiled_patterns: dict[str, re.Pattern[str]] = {}

        # Pre-compile regex patterns from constraints
        for semantic_id, constraint in config.value_constraints.items():
            if constraint.pattern:
                try:
                    self._compiled_patterns[semantic_id] = re.compile(constraint.pattern)
                except re.error as e:
                    logger.warning("Invalid regex pattern for %s: %s", semantic_id, e)

    def validate(self, metric: ContextMetric) -> ValidationResult:
        """Validate a single metric.

        Args:
            metric: The metric to validate.

        Returns:
            ValidationResult with any errors found.
        """
        all_errors: list[ValidationError] = []

        # Check semantic ID presence
        all_errors.extend(self._validate_semantic_id(metric))

        # Check value constraints if semantic ID is present
        if metric.semantic_id:
            all_errors.extend(self._validate_constraints(metric))

        return ValidationResult(metric=metric, errors=all_errors)

    def validate_batch(self, metrics: list[ContextMetric]) -> BatchValidationResult:
        """Validate a batch of metrics.

        Args:
            metrics: List of metrics to validate.

        Returns:
            BatchValidationResult containing all individual results.
        """
        results = [self.validate(metric) for metric in metrics]
        return BatchValidationResult(results=results)

    def filter_valid(self, metrics: list[ContextMetric]) -> list[ContextMetric]:
        """Filter metrics to only those that pass validation.

        Args:
            metrics: List of metrics to filter.

        Returns:
            List of metrics that passed validation.
        """
        return self.validate_batch(metrics).valid_metrics

    def _validate_semantic_id(self, metric: ContextMetric) -> list[ValidationError]:
        """Validate semantic ID presence.

        Args:
            metric: The metric to check.

        Returns:
            List of errors (empty if valid).
        """
        result: list[ValidationError] = []

        if not self.config.enforce_semantic_ids:
            return result

        # Check if this element type requires a semantic ID
        if metric.aas_type in self._required_types and not metric.semantic_id:
            result.append(
                ValidationError(
                    error_type=ErrorType.MISSING_SEMANTIC_ID,
                    message=f"{metric.aas_type} element requires a semantic ID",
                    path=metric.path,
                    expected="non-empty semantic ID",
                    actual="None",
                )
            )

        return result

    def _validate_constraints(self, metric: ContextMetric) -> list[ValidationError]:
        """Validate value constraints for a metric.

        Args:
            metric: The metric with a semantic ID.

        Returns:
            List of errors (empty if valid).
        """
        result: list[ValidationError] = []

        if not metric.semantic_id:
            return result

        constraint = self.config.value_constraints.get(metric.semantic_id)
        if not constraint:
            return result

        # Check numeric range constraints
        result.extend(self._validate_range(metric, constraint))

        # Check unit constraint
        result.extend(self._validate_unit(metric, constraint))

        # Check pattern constraint
        result.extend(self._validate_pattern(metric, constraint))

        return result

    def _validate_range(
        self, metric: ContextMetric, constraint: ValueConstraint
    ) -> list[ValidationError]:
        """Validate numeric range constraints.

        Args:
            metric: The metric to check.
            constraint: The constraint to apply.

        Returns:
            List of errors (empty if valid).
        """
        result: list[ValidationError] = []

        # Only check range for numeric values
        if not isinstance(metric.value, (int, float)):
            return result

        value = float(metric.value)

        if constraint.min is not None and value < constraint.min:
            result.append(
                ValidationError(
                    error_type=ErrorType.VALUE_OUT_OF_RANGE,
                    message=f"Value {value} is below minimum {constraint.min}",
                    path=metric.path,
                    semantic_id=metric.semantic_id,
                    expected=f">= {constraint.min}",
                    actual=str(value),
                )
            )

        if constraint.max is not None and value > constraint.max:
            result.append(
                ValidationError(
                    error_type=ErrorType.VALUE_OUT_OF_RANGE,
                    message=f"Value {value} exceeds maximum {constraint.max}",
                    path=metric.path,
                    semantic_id=metric.semantic_id,
                    expected=f"<= {constraint.max}",
                    actual=str(value),
                )
            )

        return result

    def _validate_unit(
        self, metric: ContextMetric, constraint: ValueConstraint
    ) -> list[ValidationError]:
        """Validate unit constraint.

        Args:
            metric: The metric to check.
            constraint: The constraint to apply.

        Returns:
            List of errors (empty if valid).
        """
        errors = []

        if constraint.unit and metric.unit != constraint.unit:
            errors.append(
                ValidationError(
                    error_type=ErrorType.UNIT_MISMATCH,
                    message=f"Unit '{metric.unit}' does not match expected '{constraint.unit}'",
                    path=metric.path,
                    semantic_id=metric.semantic_id,
                    expected=constraint.unit,
                    actual=metric.unit or "None",
                )
            )

        return errors

    def _validate_pattern(
        self, metric: ContextMetric, constraint: ValueConstraint
    ) -> list[ValidationError]:
        """Validate pattern constraint for string values.

        Args:
            metric: The metric to check.
            constraint: The constraint to apply.

        Returns:
            List of errors (empty if valid).
        """
        result: list[ValidationError] = []

        if not constraint.pattern or not metric.semantic_id:
            return result

        # Only check pattern for string values
        if not isinstance(metric.value, str):
            return result

        pattern = self._compiled_patterns.get(metric.semantic_id)
        if pattern and not pattern.match(metric.value):
            result.append(
                ValidationError(
                    error_type=ErrorType.PATTERN_MISMATCH,
                    message=f"Value '{metric.value}' does not match pattern '{constraint.pattern}'",
                    path=metric.path,
                    semantic_id=metric.semantic_id,
                    expected=constraint.pattern,
                    actual=metric.value,
                )
            )

        return result
