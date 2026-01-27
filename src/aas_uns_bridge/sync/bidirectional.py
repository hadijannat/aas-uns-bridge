"""MQTT→AAS bidirectional synchronization handler.

This module enables write-back from MQTT command topics to AAS repositories,
supporting closed-loop digital twin operation.

Command topics follow the pattern:
    {enterprise}/{site}/{area}/{line}/{asset}/cmd/{submodel}/{property}

Response topics:
    .../cmd/{submodel}/{property}/ack  (success)
    .../cmd/{submodel}/{property}/nak  (failure)
"""

from __future__ import annotations

import fnmatch
import json
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from aas_uns_bridge.observability.metrics import METRICS

if TYPE_CHECKING:
    from aas_uns_bridge.aas.repository_client import AasRepositoryClient
    from aas_uns_bridge.mqtt.client import MqttClient

logger = logging.getLogger(__name__)


@dataclass
class WriteCommand:
    """Parsed write command from MQTT."""

    topic: str
    """Original MQTT topic."""

    submodel_id: str
    """Target submodel identifier."""

    property_path: str
    """Property path within submodel."""

    value: Any
    """Value to write."""

    correlation_id: str | None = None
    """Optional correlation ID for request tracking."""

    requestor: str | None = None
    """Optional requestor identifier."""

    timestamp_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    """When the command was received."""


@dataclass
class ValidationResult:
    """Result of command validation."""

    is_valid: bool
    errors: list[str] = field(default_factory=list)


class BidirectionalSync:
    """MQTT→AAS write-back synchronization handler.

    Subscribes to command topics and writes values back to the AAS
    repository after validation. Publishes confirmations/rejections
    to response topics.
    """

    def __init__(
        self,
        mqtt_client: MqttClient,
        aas_client: AasRepositoryClient,
        command_topic_suffix: str = "/cmd",
        allowed_patterns: list[str] | None = None,
        denied_patterns: list[str] | None = None,
        validate_before_write: bool = True,
        publish_confirmations: bool = True,
    ):
        """Initialize bidirectional sync.

        Args:
            mqtt_client: MQTT client for subscriptions and responses.
            aas_client: AAS repository client for writes.
            command_topic_suffix: Suffix identifying command topics.
            allowed_patterns: Glob patterns for allowed write paths.
            denied_patterns: Glob patterns for denied write paths.
            validate_before_write: Whether to validate before writing.
            publish_confirmations: Whether to publish ack/nak responses.
        """
        self._mqtt = mqtt_client
        self._aas = aas_client
        self._cmd_suffix = command_topic_suffix
        self._allowed = allowed_patterns or ["*"]
        self._denied = denied_patterns or []
        self._validate = validate_before_write
        self._publish_confirmations = publish_confirmations
        self._write_count = 0
        self._error_count = 0

    def subscribe_command_topics(self, base_patterns: list[str]) -> None:
        """Subscribe to command topics for write-back.

        Commands are at ISA-95 hierarchy level:
            {enterprise}/{site}/{area}/{line}/{asset}/cmd/{submodel}/{property}

        We subscribe to the full wildcard and filter for /cmd/ in the handler
        to match commands at any depth in the hierarchy.

        Args:
            base_patterns: Base topic patterns (e.g., ["Acme/Plant1/#"]).
        """
        for pattern in base_patterns:
            # Subscribe to full wildcard - filtering happens in _handle_message
            # This allows matching /cmd/ at any level in the ISA-95 hierarchy
            self._mqtt.subscribe(pattern, self._handle_message)
            logger.info(
                "Subscribed to topic pattern: %s (filtering for %s)", pattern, self._cmd_suffix
            )

    def _handle_message(self, topic: str, payload: bytes) -> None:
        """Handle incoming MQTT message on command topic.

        Filters for topics containing /cmd/ segment to identify command messages.
        Commands follow pattern: {enterprise}/.../asset/cmd/{submodel}/{property}

        Args:
            topic: The MQTT topic.
            payload: The message payload.
        """
        # Only process topics containing /cmd/ segment
        if f"{self._cmd_suffix}/" not in topic:
            return

        # Skip ack/nak response messages (our own responses to commands)
        if topic.endswith("/ack") or topic.endswith("/nak"):
            return

        try:
            cmd = self._parse_command(topic, payload)
            if cmd is None:
                return

            # Validate the command
            if self._validate:
                validation = self._validate_write(cmd)
                if not validation.is_valid:
                    self._publish_rejection(cmd, validation.errors)
                    return

            # Execute the write
            self._execute_write(cmd)

        except Exception as e:
            logger.error("Error handling command on %s: %s", topic, e)
            self._error_count += 1

    def _parse_command(self, topic: str, payload: bytes) -> WriteCommand | None:
        """Parse a write command from MQTT message.

        Expected topic format:
            .../cmd/{submodel}/{property_path}

        Expected payload format:
            {"value": <value>, "correlationId": "...", "requestor": "..."}

        Args:
            topic: The MQTT topic.
            payload: The message payload.

        Returns:
            WriteCommand if valid, None otherwise.
        """
        # Find /cmd/ in topic
        cmd_idx = topic.find(self._cmd_suffix + "/")
        if cmd_idx == -1:
            logger.debug("Topic %s doesn't contain %s/", topic, self._cmd_suffix)
            return None

        # Extract path after /cmd/
        cmd_path = topic[cmd_idx + len(self._cmd_suffix) + 1 :]

        # Split into submodel and property path
        parts = cmd_path.split("/", 1)
        if len(parts) < 2:
            logger.warning("Invalid command path: %s", cmd_path)
            return None

        submodel_id = parts[0]
        property_path = parts[1]

        # Parse payload
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as e:
            logger.warning("Invalid JSON payload on %s: %s", topic, e)
            return None

        if not isinstance(data, dict) or "value" not in data:
            logger.warning("Payload missing 'value' field on %s", topic)
            return None

        return WriteCommand(
            topic=topic,
            submodel_id=submodel_id,
            property_path=property_path,
            value=data["value"],
            correlation_id=data.get("correlationId"),
            requestor=data.get("requestor"),
        )

    def _validate_write(self, cmd: WriteCommand) -> ValidationResult:
        """Validate a write command before execution.

        Checks:
        - Property path matches allowed patterns
        - Property path doesn't match denied patterns
        - Value type compatibility (if possible)

        Args:
            cmd: The write command to validate.

        Returns:
            ValidationResult with is_valid and errors.
        """
        errors: list[str] = []
        full_path = f"{cmd.submodel_id}/{cmd.property_path}"

        # Check denied patterns first (explicit deny wins)
        for pattern in self._denied:
            if fnmatch.fnmatch(full_path, pattern):
                errors.append(f"Path '{full_path}' matches denied pattern '{pattern}'")
                METRICS.bidirectional_validations_total.labels(result="denied").inc()
                return ValidationResult(is_valid=False, errors=errors)

        # Check allowed patterns
        allowed = False
        for pattern in self._allowed:
            if fnmatch.fnmatch(full_path, pattern):
                allowed = True
                break

        if not allowed:
            errors.append(f"Path '{full_path}' not in allowed patterns")
            METRICS.bidirectional_validations_total.labels(result="denied").inc()
            return ValidationResult(is_valid=False, errors=errors)

        METRICS.bidirectional_validations_total.labels(result="allowed").inc()
        return ValidationResult(is_valid=True)

    def _convert_mqtt_path_to_api(self, mqtt_path: str) -> str:
        """Convert MQTT path to REST API path with proper array notation.

        MQTT paths use slashes for all separators including array indices.
        REST API paths use dots for object access and brackets for array indices.

        Examples:
            Limits/MaxTemp -> Limits.MaxTemp
            List/0/Value -> List[0].Value
            Settings/Items/2/Name -> Settings.Items[2].Name

        Args:
            mqtt_path: MQTT-style path with slash separators.

        Returns:
            REST API-style path with dot notation and bracket array indices.
        """
        parts = mqtt_path.split("/")
        result: list[str] = []

        for part in parts:
            if part.isdigit():
                # Array index - wrap in brackets, no dot before
                result.append(f"[{part}]")
            else:
                # Object property - add dot separator if result is non-empty
                if result:
                    result.append(".")
                result.append(part)

        return "".join(result)

    def _execute_write(self, cmd: WriteCommand) -> None:
        """Execute a validated write command.

        Args:
            cmd: The write command to execute.
        """
        from aas_uns_bridge.aas.repository_client import AasWriteError

        # Convert MQTT path to REST API path with proper array notation
        api_property_path = self._convert_mqtt_path_to_api(cmd.property_path)

        try:
            self._aas.update_property(
                submodel_id=cmd.submodel_id,
                property_path=api_property_path,
                value=cmd.value,
            )

            self._write_count += 1
            METRICS.bidirectional_writes_total.labels(result="success").inc()
            logger.info(
                "Write successful: %s/%s = %s",
                cmd.submodel_id,
                cmd.property_path,
                cmd.value,
            )

            if self._publish_confirmations:
                self._publish_confirmation(cmd)

        except AasWriteError as e:
            logger.error("Write failed: %s", e)
            self._error_count += 1
            METRICS.bidirectional_writes_total.labels(result="failure").inc()
            if self._publish_confirmations:
                self._publish_rejection(cmd, [str(e)])

    def _publish_confirmation(self, cmd: WriteCommand) -> None:
        """Publish success confirmation to ack topic.

        Args:
            cmd: The completed write command.
        """
        ack_topic = f"{cmd.topic}/ack"
        payload: dict[str, Any] = {
            "success": True,
            "timestamp": int(time.time() * 1000),
        }
        if cmd.correlation_id:
            payload["correlationId"] = cmd.correlation_id

        self._mqtt.publish(
            topic=ack_topic,
            payload=json.dumps(payload).encode(),
            qos=1,
            retain=False,
        )
        logger.debug("Published ack to %s", ack_topic)

    def _publish_rejection(self, cmd: WriteCommand, errors: list[str]) -> None:
        """Publish failure rejection to nak topic.

        Args:
            cmd: The rejected write command.
            errors: List of error messages.
        """
        nak_topic = f"{cmd.topic}/nak"
        payload: dict[str, Any] = {
            "success": False,
            "errors": errors,
            "timestamp": int(time.time() * 1000),
        }
        if cmd.correlation_id:
            payload["correlationId"] = cmd.correlation_id

        self._mqtt.publish(
            topic=nak_topic,
            payload=json.dumps(payload).encode(),
            qos=1,
            retain=False,
        )
        logger.debug("Published nak to %s", nak_topic)

    @property
    def write_count(self) -> int:
        """Total successful writes."""
        return self._write_count

    @property
    def error_count(self) -> int:
        """Total write errors."""
        return self._error_count
