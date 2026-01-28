"""Unit tests for bidirectional sync audit logging."""

import json
from unittest.mock import MagicMock, patch

import pytest

from aas_uns_bridge.sync.bidirectional import BidirectionalSync


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


class TestAuditLogOnWriteReceived:
    """Tests for audit logging when write commands are received."""

    def test_audit_log_on_write_received(
        self,
        mock_mqtt_client: MagicMock,
        mock_aas_client: MagicMock,
    ) -> None:
        """Verify command receipt is logged with all required fields."""
        sync = BidirectionalSync(
            mqtt_client=mock_mqtt_client,
            aas_client=mock_aas_client,
            allowed_patterns=["*/*"],
        )

        topic = "Acme/Plant1/cmd/Setpoints/Temperature"
        payload = json.dumps(
            {
                "value": 25.5,
                "correlationId": "corr-abc-123",
                "requestor": "operator@example.com",
            }
        ).encode()

        with patch("aas_uns_bridge.sync.bidirectional.audit_logger") as mock_audit:
            sync._handle_message(topic, payload)

            # Find the write_command_received call
            received_calls = [
                call
                for call in mock_audit.info.call_args_list
                if call[0][0] == "bidirectional_write_audit"
                and call[1].get("audit_event") == "write_command_received"
            ]

            assert len(received_calls) == 1
            call_kwargs = received_calls[0][1]

            # Verify all required audit fields
            assert call_kwargs["audit_event"] == "write_command_received"
            assert call_kwargs["topic"] == topic
            assert call_kwargs["submodel_id"] == "Setpoints"
            assert call_kwargs["property_path"] == "Temperature"
            assert call_kwargs["value"] == 25.5
            assert call_kwargs["correlation_id"] == "corr-abc-123"
            assert call_kwargs["requestor"] == "operator@example.com"
            assert "timestamp_ms" in call_kwargs
            assert isinstance(call_kwargs["timestamp_ms"], int)

    def test_audit_log_without_optional_fields(
        self,
        mock_mqtt_client: MagicMock,
        mock_aas_client: MagicMock,
    ) -> None:
        """Verify audit log works without optional correlationId and requestor."""
        sync = BidirectionalSync(
            mqtt_client=mock_mqtt_client,
            aas_client=mock_aas_client,
            allowed_patterns=["*/*"],
        )

        topic = "cmd/Setpoints/Pressure"
        payload = json.dumps({"value": 100}).encode()

        with patch("aas_uns_bridge.sync.bidirectional.audit_logger") as mock_audit:
            sync._handle_message(topic, payload)

            received_calls = [
                call
                for call in mock_audit.info.call_args_list
                if call[0][0] == "bidirectional_write_audit"
                and call[1].get("audit_event") == "write_command_received"
            ]

            assert len(received_calls) == 1
            call_kwargs = received_calls[0][1]

            # Optional fields should not be present
            assert "correlation_id" not in call_kwargs
            assert "requestor" not in call_kwargs


class TestAuditLogOnValidationDenied:
    """Tests for audit logging when validation denies a write."""

    def test_audit_log_on_validation_denied(
        self,
        sync_handler: BidirectionalSync,
    ) -> None:
        """Verify denied validation is logged with reason."""
        # Use a denied pattern
        topic = "Acme/Plant1/cmd/readonly/Secret"
        payload = json.dumps({"value": "secret-value"}).encode()

        with patch("aas_uns_bridge.sync.bidirectional.audit_logger") as mock_audit:
            sync_handler._handle_message(topic, payload)

            # Find the write_validated call with denied result
            validated_calls = [
                call
                for call in mock_audit.info.call_args_list
                if call[0][0] == "bidirectional_write_audit"
                and call[1].get("audit_event") == "write_validated"
            ]

            assert len(validated_calls) == 1
            call_kwargs = validated_calls[0][1]

            assert call_kwargs["audit_event"] == "write_validated"
            assert call_kwargs["result"] == "denied"
            assert call_kwargs["submodel_id"] == "readonly"
            assert call_kwargs["property_path"] == "Secret"
            assert "denied" in call_kwargs["reason"].lower()

    def test_audit_log_on_not_allowed_pattern(
        self,
        sync_handler: BidirectionalSync,
    ) -> None:
        """Verify validation denied when path not in allowed patterns."""
        topic = "test/cmd/Unknown/Property"
        payload = json.dumps({"value": 42}).encode()

        with patch("aas_uns_bridge.sync.bidirectional.audit_logger") as mock_audit:
            sync_handler._handle_message(topic, payload)

            validated_calls = [
                call
                for call in mock_audit.info.call_args_list
                if call[0][0] == "bidirectional_write_audit"
                and call[1].get("audit_event") == "write_validated"
            ]

            assert len(validated_calls) == 1
            call_kwargs = validated_calls[0][1]

            assert call_kwargs["result"] == "denied"
            assert "not in allowed" in call_kwargs["reason"].lower()


class TestAuditLogOnWriteSuccess:
    """Tests for audit logging on successful writes."""

    def test_audit_log_on_write_success(
        self,
        mock_mqtt_client: MagicMock,
        mock_aas_client: MagicMock,
    ) -> None:
        """Verify successful write is logged."""
        sync = BidirectionalSync(
            mqtt_client=mock_mqtt_client,
            aas_client=mock_aas_client,
            allowed_patterns=["*/*"],
            publish_confirmations=True,
        )

        topic = "test/cmd/Setpoints/Temperature"
        payload = json.dumps(
            {
                "value": 30.0,
                "correlationId": "write-001",
            }
        ).encode()

        with patch("aas_uns_bridge.sync.bidirectional.audit_logger") as mock_audit:
            sync._handle_message(topic, payload)

            # Find write_executed with success result
            executed_calls = [
                call
                for call in mock_audit.info.call_args_list
                if call[0][0] == "bidirectional_write_audit"
                and call[1].get("audit_event") == "write_executed"
            ]

            assert len(executed_calls) == 1
            call_kwargs = executed_calls[0][1]

            assert call_kwargs["audit_event"] == "write_executed"
            assert call_kwargs["result"] == "success"
            assert call_kwargs["submodel_id"] == "Setpoints"
            assert call_kwargs["property_path"] == "Temperature"
            assert call_kwargs["value"] == 30.0
            assert call_kwargs["correlation_id"] == "write-001"
            # Reason should not be present on success
            assert "reason" not in call_kwargs

    def test_audit_log_response_sent_ack(
        self,
        mock_mqtt_client: MagicMock,
        mock_aas_client: MagicMock,
    ) -> None:
        """Verify ack response publication is logged."""
        sync = BidirectionalSync(
            mqtt_client=mock_mqtt_client,
            aas_client=mock_aas_client,
            allowed_patterns=["*/*"],
            publish_confirmations=True,
        )

        topic = "test/cmd/Setpoints/Value"
        payload = json.dumps({"value": 50}).encode()

        with patch("aas_uns_bridge.sync.bidirectional.audit_logger") as mock_audit:
            sync._handle_message(topic, payload)

            # Find write_response_sent call
            response_calls = [
                call
                for call in mock_audit.info.call_args_list
                if call[0][0] == "bidirectional_write_audit"
                and call[1].get("audit_event") == "write_response_sent"
            ]

            assert len(response_calls) == 1
            call_kwargs = response_calls[0][1]

            assert call_kwargs["audit_event"] == "write_response_sent"
            assert call_kwargs["result"] == "success"
            assert "/ack" in call_kwargs["topic"]


class TestAuditLogOnWriteFailure:
    """Tests for audit logging on failed writes."""

    def test_audit_log_on_write_failure(
        self,
        mock_mqtt_client: MagicMock,
        mock_aas_client: MagicMock,
    ) -> None:
        """Verify failed write is logged with error reason."""

        # Create an AasWriteError-like exception
        class MockAasWriteError(Exception):
            pass

        sync = BidirectionalSync(
            mqtt_client=mock_mqtt_client,
            aas_client=mock_aas_client,
            allowed_patterns=["*/*"],
            publish_confirmations=True,
        )

        with patch(
            "aas_uns_bridge.aas.repository_client.AasWriteError",
            MockAasWriteError,
            create=True,
        ):
            mock_aas_client.update_property.side_effect = MockAasWriteError(
                "Property not found in submodel"
            )

            topic = "test/cmd/Setpoints/NonExistent"
            payload = json.dumps({"value": 999}).encode()

            with patch("aas_uns_bridge.sync.bidirectional.audit_logger") as mock_audit:
                sync._handle_message(topic, payload)

                # Find write_executed with failed result
                executed_calls = [
                    call
                    for call in mock_audit.info.call_args_list
                    if call[0][0] == "bidirectional_write_audit"
                    and call[1].get("audit_event") == "write_executed"
                ]

                assert len(executed_calls) == 1
                call_kwargs = executed_calls[0][1]

                assert call_kwargs["audit_event"] == "write_executed"
                assert call_kwargs["result"] == "failed"
                assert "Property not found" in call_kwargs["reason"]

    def test_audit_log_response_sent_nak(
        self,
        mock_mqtt_client: MagicMock,
        mock_aas_client: MagicMock,
    ) -> None:
        """Verify nak response publication is logged."""

        class MockAasWriteError(Exception):
            pass

        sync = BidirectionalSync(
            mqtt_client=mock_mqtt_client,
            aas_client=mock_aas_client,
            allowed_patterns=["*/*"],
            publish_confirmations=True,
        )

        with patch(
            "aas_uns_bridge.aas.repository_client.AasWriteError",
            MockAasWriteError,
            create=True,
        ):
            mock_aas_client.update_property.side_effect = MockAasWriteError("Write failed")

            topic = "test/cmd/Setpoints/Temperature"
            payload = json.dumps({"value": 100}).encode()

            with patch("aas_uns_bridge.sync.bidirectional.audit_logger") as mock_audit:
                sync._handle_message(topic, payload)

                # Find write_response_sent call
                response_calls = [
                    call
                    for call in mock_audit.info.call_args_list
                    if call[0][0] == "bidirectional_write_audit"
                    and call[1].get("audit_event") == "write_response_sent"
                ]

                assert len(response_calls) == 1
                call_kwargs = response_calls[0][1]

                assert call_kwargs["audit_event"] == "write_response_sent"
                assert call_kwargs["result"] == "denied"
                assert "/nak" in call_kwargs["topic"]
                assert "Write failed" in call_kwargs["reason"]


class TestAuditLogStructure:
    """Tests verifying audit log entries are properly structured."""

    def test_audit_log_entries_are_json_serializable(
        self,
        mock_mqtt_client: MagicMock,
        mock_aas_client: MagicMock,
    ) -> None:
        """Verify audit log entries can be serialized to JSON."""
        sync = BidirectionalSync(
            mqtt_client=mock_mqtt_client,
            aas_client=mock_aas_client,
            allowed_patterns=["*/*"],
        )

        topic = "test/cmd/Config/Setting"
        payload = json.dumps(
            {
                "value": {"nested": "object", "number": 42},
                "correlationId": "json-test",
            }
        ).encode()

        with patch("aas_uns_bridge.sync.bidirectional.audit_logger") as mock_audit:
            sync._handle_message(topic, payload)

            # All calls should produce JSON-serializable data
            for call in mock_audit.info.call_args_list:
                if call[0][0] == "bidirectional_write_audit":
                    kwargs = call[1]
                    # This should not raise
                    serialized = json.dumps(kwargs)
                    # And should be parseable back
                    parsed = json.loads(serialized)
                    assert parsed["audit_event"] in [
                        "write_command_received",
                        "write_validated",
                        "write_executed",
                        "write_response_sent",
                    ]

    def test_audit_log_timestamp_is_milliseconds(
        self,
        mock_mqtt_client: MagicMock,
        mock_aas_client: MagicMock,
    ) -> None:
        """Verify timestamp_ms field is in milliseconds since epoch."""
        import time

        sync = BidirectionalSync(
            mqtt_client=mock_mqtt_client,
            aas_client=mock_aas_client,
            allowed_patterns=["*/*"],
        )

        before_ms = int(time.time() * 1000)

        topic = "test/cmd/Data/Value"
        payload = json.dumps({"value": 1}).encode()

        with patch("aas_uns_bridge.sync.bidirectional.audit_logger") as mock_audit:
            sync._handle_message(topic, payload)

            after_ms = int(time.time() * 1000)

            for call in mock_audit.info.call_args_list:
                if call[0][0] == "bidirectional_write_audit":
                    ts = call[1].get("timestamp_ms")
                    if ts is not None:
                        assert before_ms <= ts <= after_ms

    def test_audit_log_method_redacts_sensitive_values(
        self,
        sync_handler: BidirectionalSync,
    ) -> None:
        """Verify _audit_log can redact sensitive values."""
        with patch("aas_uns_bridge.sync.bidirectional.audit_logger") as mock_audit:
            sync_handler._audit_log(
                "write_command_received",
                topic="test/cmd/Secrets/Password",
                submodel_id="Secrets",
                property_path="Password",
                value="super-secret-password",
                redact_value=True,
            )

            call_kwargs = mock_audit.info.call_args[1]
            assert call_kwargs["value"] == "[REDACTED]"


class TestAuditLogIntegration:
    """Integration tests for audit logging flow."""

    def test_full_successful_write_audit_trail(
        self,
        mock_mqtt_client: MagicMock,
        mock_aas_client: MagicMock,
    ) -> None:
        """Verify complete audit trail for successful write."""
        sync = BidirectionalSync(
            mqtt_client=mock_mqtt_client,
            aas_client=mock_aas_client,
            allowed_patterns=["*/*"],
            validate_before_write=True,
            publish_confirmations=True,
        )

        topic = "test/cmd/Setpoints/Target"
        payload = json.dumps(
            {
                "value": 75.0,
                "correlationId": "trace-123",
                "requestor": "system",
            }
        ).encode()

        with patch("aas_uns_bridge.sync.bidirectional.audit_logger") as mock_audit:
            sync._handle_message(topic, payload)

            # Extract all audit events
            events = [
                call[1]["audit_event"]
                for call in mock_audit.info.call_args_list
                if call[0][0] == "bidirectional_write_audit"
            ]

            # Should have received, executed, and response_sent
            assert "write_command_received" in events
            assert "write_executed" in events
            assert "write_response_sent" in events

    def test_full_denied_write_audit_trail(
        self,
        sync_handler: BidirectionalSync,
    ) -> None:
        """Verify complete audit trail for denied write."""
        topic = "test/cmd/Identification/SerialNumber"
        payload = json.dumps(
            {
                "value": "NEW-SERIAL",
                "correlationId": "denied-trace",
            }
        ).encode()

        with patch("aas_uns_bridge.sync.bidirectional.audit_logger") as mock_audit:
            sync_handler._handle_message(topic, payload)

            events = [
                call[1]["audit_event"]
                for call in mock_audit.info.call_args_list
                if call[0][0] == "bidirectional_write_audit"
            ]

            # Should have received, validated (denied), and response_sent
            assert "write_command_received" in events
            assert "write_validated" in events
            assert "write_response_sent" in events

            # Should NOT have write_executed (validation failed)
            assert "write_executed" not in events
