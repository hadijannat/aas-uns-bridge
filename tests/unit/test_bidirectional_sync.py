"""Unit tests for bidirectional synchronization handler."""

import json
from unittest.mock import MagicMock, patch

import pytest

from aas_uns_bridge.sync.bidirectional import (
    BidirectionalSync,
    ValidationResult,
    WriteCommand,
)


@pytest.fixture
def mock_mqtt_client() -> MagicMock:
    """Create a mock MQTT client."""
    client = MagicMock()
    client.subscribe = MagicMock()
    client.publish = MagicMock()
    return client


@pytest.fixture
def mock_aas_client() -> MagicMock:
    """Create a mock AAS repository client."""
    client = MagicMock()
    client.update_property = MagicMock()
    return client


@pytest.fixture
def sync_handler(mock_mqtt_client: MagicMock, mock_aas_client: MagicMock) -> BidirectionalSync:
    """Create a bidirectional sync handler for testing."""
    return BidirectionalSync(
        mqtt_client=mock_mqtt_client,
        aas_client=mock_aas_client,
        command_topic_suffix="/cmd",
        allowed_patterns=["Setpoints/*", "Configuration/*"],
        denied_patterns=["readonly/*", "Identification/*"],
        validate_before_write=True,
        publish_confirmations=True,
    )


class TestWriteCommand:
    """Tests for WriteCommand dataclass."""

    def test_write_command_creation(self) -> None:
        """Test creating a WriteCommand."""
        cmd = WriteCommand(
            topic="Acme/Plant1/Line1/Machine1/cmd/TechnicalData/Temperature",
            submodel_id="TechnicalData",
            property_path="Temperature",
            value=25.5,
            correlation_id="corr-123",
            requestor="user@example.com",
        )

        assert cmd.submodel_id == "TechnicalData"
        assert cmd.property_path == "Temperature"
        assert cmd.value == 25.5
        assert cmd.correlation_id == "corr-123"

    def test_write_command_defaults(self) -> None:
        """Test WriteCommand default values."""
        cmd = WriteCommand(
            topic="test/cmd/sm/prop",
            submodel_id="sm",
            property_path="prop",
            value=42,
        )

        assert cmd.correlation_id is None
        assert cmd.requestor is None
        assert cmd.timestamp_ms > 0


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_valid_result(self) -> None:
        """Test creating a valid result."""
        result = ValidationResult(is_valid=True)

        assert result.is_valid
        assert result.errors == []

    def test_invalid_result_with_errors(self) -> None:
        """Test creating an invalid result with errors."""
        result = ValidationResult(
            is_valid=False,
            errors=["Path not allowed", "Value out of range"],
        )

        assert not result.is_valid
        assert len(result.errors) == 2


class TestBidirectionalSyncSubscription:
    """Tests for subscription behavior."""

    def test_subscribe_command_topics(
        self,
        sync_handler: BidirectionalSync,
        mock_mqtt_client: MagicMock,
    ) -> None:
        """Test subscribing to command topics.

        We now subscribe to the full wildcard pattern and filter for /cmd/
        in the message handler to support commands at any ISA-95 hierarchy level.
        """
        sync_handler.subscribe_command_topics(["Acme/Plant1/#"])

        mock_mqtt_client.subscribe.assert_called_once()
        call_args = mock_mqtt_client.subscribe.call_args
        # Subscribe to full pattern - filtering happens in handler
        assert call_args[0][0] == "Acme/Plant1/#"

    def test_subscribe_multiple_patterns(
        self,
        sync_handler: BidirectionalSync,
        mock_mqtt_client: MagicMock,
    ) -> None:
        """Test subscribing to multiple base patterns."""
        sync_handler.subscribe_command_topics(["Acme/#", "Contoso/#"])

        assert mock_mqtt_client.subscribe.call_count == 2


class TestBidirectionalSyncFiltering:
    """Tests for message filtering behavior."""

    def test_handler_ignores_non_cmd_topics(
        self,
        sync_handler: BidirectionalSync,
        mock_mqtt_client: MagicMock,
        mock_aas_client: MagicMock,
    ) -> None:
        """Test that handler ignores topics without /cmd/ segment."""
        # Regular data topic (not a command)
        topic = "Acme/Plant1/Line1/Machine1/context/TechnicalData/Temperature"
        payload = json.dumps({"value": 25.5}).encode()

        sync_handler._handle_message(topic, payload)

        # Should not call AAS client - not a command topic
        mock_aas_client.update_property.assert_not_called()
        mock_mqtt_client.publish.assert_not_called()

    def test_handler_processes_cmd_at_asset_level(
        self,
        mock_mqtt_client: MagicMock,
        mock_aas_client: MagicMock,
    ) -> None:
        """Test that handler processes commands at ISA-95 asset level."""
        sync = BidirectionalSync(
            mqtt_client=mock_mqtt_client,
            aas_client=mock_aas_client,
            allowed_patterns=["*/*"],
        )

        # Full ISA-95 hierarchy command at asset level
        topic = "Enterprise/Site/Area/Line/Asset/cmd/Setpoints/Temperature"
        payload = json.dumps({"value": 30.0}).encode()

        sync._handle_message(topic, payload)

        mock_aas_client.update_property.assert_called_once()

    def test_handler_skips_ack_nak_messages(
        self,
        sync_handler: BidirectionalSync,
        mock_aas_client: MagicMock,
    ) -> None:
        """Test that handler ignores ack/nak response messages."""
        ack_topic = "Acme/Plant1/cmd/Setpoints/Temperature/ack"
        nak_topic = "Acme/Plant1/cmd/Setpoints/Temperature/nak"
        payload = json.dumps({"success": True}).encode()

        sync_handler._handle_message(ack_topic, payload)
        sync_handler._handle_message(nak_topic, payload)

        # Should not process response messages
        mock_aas_client.update_property.assert_not_called()

    def test_handler_processes_rootless_cmd_topics(
        self,
        mock_mqtt_client: MagicMock,
        mock_aas_client: MagicMock,
    ) -> None:
        """Test that handler processes commands when root_topic is empty."""
        sync = BidirectionalSync(
            mqtt_client=mock_mqtt_client,
            aas_client=mock_aas_client,
            allowed_patterns=["*/*"],
        )

        # Rootless command topic (when uns.root_topic is empty)
        topic = "cmd/Setpoints/Temperature"
        payload = json.dumps({"value": 25.0}).encode()

        sync._handle_message(topic, payload)

        mock_aas_client.update_property.assert_called_once()


class TestBidirectionalSyncPathConversion:
    """Tests for MQTT to REST API path conversion."""

    def test_simple_path_conversion(self, sync_handler: BidirectionalSync) -> None:
        """Test converting simple slash-separated path."""
        result = sync_handler._convert_mqtt_path_to_api("Limits/MaxTemp")
        assert result == "Limits.MaxTemp"

    def test_array_index_conversion(self, sync_handler: BidirectionalSync) -> None:
        """Test converting path with array index marker (aNa format)."""
        result = sync_handler._convert_mqtt_path_to_api("List/a0a/Value")
        assert result == "List[0].Value"

    def test_multiple_array_indices(self, sync_handler: BidirectionalSync) -> None:
        """Test converting path with multiple array indices."""
        result = sync_handler._convert_mqtt_path_to_api("Settings/Items/a2a/Name")
        assert result == "Settings.Items[2].Name"

    def test_nested_arrays(self, sync_handler: BidirectionalSync) -> None:
        """Test converting path with nested arrays."""
        result = sync_handler._convert_mqtt_path_to_api("Matrix/a0a/a1a/Value")
        assert result == "Matrix[0][1].Value"

    def test_single_segment_path(self, sync_handler: BidirectionalSync) -> None:
        """Test converting single-segment path."""
        result = sync_handler._convert_mqtt_path_to_api("Temperature")
        assert result == "Temperature"

    def test_single_array_index(self, sync_handler: BidirectionalSync) -> None:
        """Test converting single array index marker."""
        result = sync_handler._convert_mqtt_path_to_api("a0a")
        assert result == "[0]"

    def test_numeric_idshort_not_treated_as_index(self, sync_handler: BidirectionalSync) -> None:
        """Test that numeric idShorts are NOT converted to array indices."""
        # A numeric idShort like "123" should remain as a property, not an index
        result = sync_handler._convert_mqtt_path_to_api("Config/123/Name")
        assert result == "Config.123.Name"

    def test_numeric_segment_without_marker(self, sync_handler: BidirectionalSync) -> None:
        """Test plain numeric segment is treated as property, not index."""
        result = sync_handler._convert_mqtt_path_to_api("0")
        assert result == "0"

    def test_idx_prefixed_idshort_not_treated_as_index(
        self, sync_handler: BidirectionalSync
    ) -> None:
        """Test that idx_ prefixed idShorts are NOT converted to array indices."""
        # An idShort named "idx_1" should remain as a property, not an index
        result = sync_handler._convert_mqtt_path_to_api("Config/idx_1/Name")
        assert result == "Config.idx_1.Name"


class TestBidirectionalSyncCommandParsing:
    """Tests for command parsing."""

    def test_parse_valid_command(self, sync_handler: BidirectionalSync) -> None:
        """Test parsing a valid command message."""
        topic = "Acme/Plant1/Line1/Machine1/cmd/Setpoints/TargetTemp"
        payload = json.dumps(
            {
                "value": 25.5,
                "correlationId": "corr-123",
                "requestor": "user@example.com",
            }
        ).encode()

        cmd = sync_handler._parse_command(topic, payload)

        assert cmd is not None
        assert cmd.submodel_id == "Setpoints"
        assert cmd.property_path == "TargetTemp"
        assert cmd.value == 25.5
        assert cmd.correlation_id == "corr-123"

    def test_parse_command_missing_cmd_suffix(self, sync_handler: BidirectionalSync) -> None:
        """Test parsing fails for topic without /cmd/."""
        topic = "Acme/Plant1/Line1/Machine1/Setpoints/TargetTemp"
        payload = json.dumps({"value": 25.5}).encode()

        cmd = sync_handler._parse_command(topic, payload)

        assert cmd is None

    def test_parse_command_invalid_json(self, sync_handler: BidirectionalSync) -> None:
        """Test parsing fails for invalid JSON payload."""
        topic = "Acme/Plant1/cmd/Setpoints/TargetTemp"
        payload = b"not valid json"

        cmd = sync_handler._parse_command(topic, payload)

        assert cmd is None

    def test_parse_command_missing_value_field(self, sync_handler: BidirectionalSync) -> None:
        """Test parsing fails when 'value' field is missing."""
        topic = "Acme/Plant1/cmd/Setpoints/TargetTemp"
        payload = json.dumps({"notValue": 25.5}).encode()

        cmd = sync_handler._parse_command(topic, payload)

        assert cmd is None

    def test_parse_command_nested_path(self, sync_handler: BidirectionalSync) -> None:
        """Test parsing command with nested property path."""
        topic = "Acme/Plant1/cmd/Configuration/Limits/MaxTemp"
        payload = json.dumps({"value": 100.0}).encode()

        cmd = sync_handler._parse_command(topic, payload)

        assert cmd is not None
        assert cmd.submodel_id == "Configuration"
        assert cmd.property_path == "Limits/MaxTemp"


class TestBidirectionalSyncValidation:
    """Tests for write validation."""

    def test_validate_allowed_path(self, sync_handler: BidirectionalSync) -> None:
        """Test validation passes for allowed paths."""
        cmd = WriteCommand(
            topic="test/cmd/Setpoints/Temperature",
            submodel_id="Setpoints",
            property_path="Temperature",
            value=25.5,
        )

        result = sync_handler._validate_write(cmd)

        assert result.is_valid

    def test_validate_denied_path(self, sync_handler: BidirectionalSync) -> None:
        """Test validation fails for denied paths."""
        cmd = WriteCommand(
            topic="test/cmd/readonly/Value",
            submodel_id="readonly",
            property_path="Value",
            value=25.5,
        )

        result = sync_handler._validate_write(cmd)

        assert not result.is_valid
        assert "denied" in result.errors[0].lower()

    def test_validate_not_in_allowed_patterns(self, sync_handler: BidirectionalSync) -> None:
        """Test validation fails for paths not in allowed patterns."""
        cmd = WriteCommand(
            topic="test/cmd/Unknown/Value",
            submodel_id="Unknown",
            property_path="Value",
            value=25.5,
        )

        result = sync_handler._validate_write(cmd)

        assert not result.is_valid
        assert "not in allowed" in result.errors[0].lower()

    def test_denied_patterns_take_precedence(
        self, mock_mqtt_client: MagicMock, mock_aas_client: MagicMock
    ) -> None:
        """Test that denied patterns override allowed patterns."""
        # Create handler with overlapping patterns
        sync = BidirectionalSync(
            mqtt_client=mock_mqtt_client,
            aas_client=mock_aas_client,
            allowed_patterns=["*/*"],  # Allow all
            denied_patterns=["Secret/*"],  # But deny Secret
        )

        cmd = WriteCommand(
            topic="test/cmd/Secret/Password",
            submodel_id="Secret",
            property_path="Password",
            value="hunter2",
        )

        result = sync._validate_write(cmd)

        assert not result.is_valid


class TestBidirectionalSyncExecution:
    """Tests for write execution."""

    def test_execute_write_success(
        self,
        sync_handler: BidirectionalSync,
        mock_aas_client: MagicMock,
        mock_mqtt_client: MagicMock,
    ) -> None:
        """Test successful write execution."""
        cmd = WriteCommand(
            topic="test/cmd/Setpoints/Temperature",
            submodel_id="Setpoints",
            property_path="Temperature",
            value=25.5,
            correlation_id="corr-123",
        )

        sync_handler._execute_write(cmd)

        # Verify AAS write was called
        mock_aas_client.update_property.assert_called_once_with(
            submodel_id="Setpoints",
            property_path="Temperature",
            value=25.5,
        )

        # Verify confirmation was published
        mock_mqtt_client.publish.assert_called_once()
        call_args = mock_mqtt_client.publish.call_args
        assert "/ack" in call_args[1]["topic"]

        # Verify write count incremented
        assert sync_handler.write_count == 1

    def test_execute_write_failure(
        self,
        sync_handler: BidirectionalSync,
        mock_aas_client: MagicMock,
        mock_mqtt_client: MagicMock,
    ) -> None:
        """Test write execution handles failures."""

        # Create an AasWriteError-like exception and patch the import
        class MockAasWriteError(Exception):
            pass

        # Patch the import inside _execute_write
        with patch(
            "aas_uns_bridge.aas.repository_client.AasWriteError",
            MockAasWriteError,
            create=True,
        ):
            mock_aas_client.update_property.side_effect = MockAasWriteError("Write failed")

            cmd = WriteCommand(
                topic="test/cmd/Setpoints/Temperature",
                submodel_id="Setpoints",
                property_path="Temperature",
                value=25.5,
            )

            sync_handler._execute_write(cmd)

            # Verify rejection was published
            mock_mqtt_client.publish.assert_called_once()
            call_args = mock_mqtt_client.publish.call_args
            assert "/nak" in call_args[1]["topic"]

            # Verify error count incremented
            assert sync_handler.error_count == 1


class TestBidirectionalSyncConfirmations:
    """Tests for ack/nak publishing."""

    def test_publish_confirmation(
        self,
        sync_handler: BidirectionalSync,
        mock_mqtt_client: MagicMock,
    ) -> None:
        """Test publishing success confirmation."""
        cmd = WriteCommand(
            topic="test/cmd/Setpoints/Temperature",
            submodel_id="Setpoints",
            property_path="Temperature",
            value=25.5,
            correlation_id="corr-123",
        )

        sync_handler._publish_confirmation(cmd)

        mock_mqtt_client.publish.assert_called_once()
        call_args = mock_mqtt_client.publish.call_args

        assert call_args[1]["topic"] == "test/cmd/Setpoints/Temperature/ack"
        assert call_args[1]["qos"] == 1
        assert call_args[1]["retain"] is False

        # Verify payload includes correlation ID
        payload = json.loads(call_args[1]["payload"])
        assert payload["success"] is True
        assert payload["correlationId"] == "corr-123"

    def test_publish_rejection(
        self,
        sync_handler: BidirectionalSync,
        mock_mqtt_client: MagicMock,
    ) -> None:
        """Test publishing failure rejection."""
        cmd = WriteCommand(
            topic="test/cmd/Setpoints/Temperature",
            submodel_id="Setpoints",
            property_path="Temperature",
            value=25.5,
            correlation_id="corr-123",
        )
        errors = ["Permission denied", "Value out of range"]

        sync_handler._publish_rejection(cmd, errors)

        mock_mqtt_client.publish.assert_called_once()
        call_args = mock_mqtt_client.publish.call_args

        assert call_args[1]["topic"] == "test/cmd/Setpoints/Temperature/nak"

        payload = json.loads(call_args[1]["payload"])
        assert payload["success"] is False
        assert payload["errors"] == errors
        assert payload["correlationId"] == "corr-123"


class TestBidirectionalSyncMessageHandling:
    """Tests for end-to-end message handling."""

    def test_handle_valid_message(
        self,
        mock_mqtt_client: MagicMock,
        mock_aas_client: MagicMock,
    ) -> None:
        """Test handling a valid command message."""
        # Create handler with permissive patterns
        sync = BidirectionalSync(
            mqtt_client=mock_mqtt_client,
            aas_client=mock_aas_client,
            allowed_patterns=["*/*"],  # Allow all
            validate_before_write=True,
            publish_confirmations=True,
        )

        topic = "Acme/Plant1/cmd/Setpoints/Temperature"
        payload = json.dumps({"value": 25.5}).encode()

        sync._handle_message(topic, payload)

        mock_aas_client.update_property.assert_called_once()
        assert sync.write_count == 1

    def test_handle_message_validation_failure(
        self,
        mock_mqtt_client: MagicMock,
        mock_aas_client: MagicMock,
    ) -> None:
        """Test handling a message that fails validation."""
        # Create handler with restrictive patterns
        sync = BidirectionalSync(
            mqtt_client=mock_mqtt_client,
            aas_client=mock_aas_client,
            allowed_patterns=["Allowed/*"],
            denied_patterns=["readonly/*"],
            validate_before_write=True,
            publish_confirmations=True,
        )

        topic = "Acme/Plant1/cmd/readonly/Secret"
        payload = json.dumps({"value": "secret"}).encode()

        sync._handle_message(topic, payload)

        # Should not call AAS client
        mock_aas_client.update_property.assert_not_called()

        # Should publish rejection
        mock_mqtt_client.publish.assert_called_once()
        call_args = mock_mqtt_client.publish.call_args
        assert "/nak" in call_args[1]["topic"]

    def test_handle_message_parse_failure(
        self,
        sync_handler: BidirectionalSync,
        mock_aas_client: MagicMock,
        mock_mqtt_client: MagicMock,
    ) -> None:
        """Test handling a message that cannot be parsed."""
        topic = "Acme/Plant1/cmd/Setpoints/Temperature"
        payload = b"invalid json"

        sync_handler._handle_message(topic, payload)

        # Should not call AAS client
        mock_aas_client.update_property.assert_not_called()

        # Should not publish anything (parse failure = ignore)
        mock_mqtt_client.publish.assert_not_called()


class TestBidirectionalSyncConfiguration:
    """Tests for sync handler configuration."""

    def test_validation_disabled(
        self,
        mock_mqtt_client: MagicMock,
        mock_aas_client: MagicMock,
    ) -> None:
        """Test that validation can be disabled."""
        sync = BidirectionalSync(
            mqtt_client=mock_mqtt_client,
            aas_client=mock_aas_client,
            validate_before_write=False,  # Disabled
            allowed_patterns=["*/Allowed/*"],
        )

        # Even a denied-looking path should work
        topic = "test/cmd/NotAllowed/Value"
        payload = json.dumps({"value": 123}).encode()

        sync._handle_message(topic, payload)

        mock_aas_client.update_property.assert_called_once()

    def test_confirmations_disabled(
        self,
        mock_mqtt_client: MagicMock,
        mock_aas_client: MagicMock,
    ) -> None:
        """Test that confirmations can be disabled."""
        sync = BidirectionalSync(
            mqtt_client=mock_mqtt_client,
            aas_client=mock_aas_client,
            publish_confirmations=False,  # Disabled
        )

        topic = "test/cmd/Any/Value"
        payload = json.dumps({"value": 123}).encode()

        sync._handle_message(topic, payload)

        # Should execute write
        mock_aas_client.update_property.assert_called_once()

        # Should NOT publish confirmation
        mock_mqtt_client.publish.assert_not_called()

    def test_counters_track_operations(
        self,
        mock_mqtt_client: MagicMock,
        mock_aas_client: MagicMock,
    ) -> None:
        """Test that counters track successful and failed operations."""
        sync = BidirectionalSync(
            mqtt_client=mock_mqtt_client,
            aas_client=mock_aas_client,
            allowed_patterns=["*/*"],  # Allow all
            validate_before_write=True,
            publish_confirmations=True,
        )

        assert sync.write_count == 0
        assert sync.error_count == 0

        # Successful write
        topic1 = "test/cmd/Setpoints/Value"
        sync._handle_message(topic1, json.dumps({"value": 1}).encode())

        assert sync.write_count == 1
        assert sync.error_count == 0
