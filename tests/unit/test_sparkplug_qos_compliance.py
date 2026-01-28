"""Unit tests for Sparkplug B QoS compliance.

Sparkplug B specification requires:
- QoS = 0 for ALL message types (NBIRTH, DBIRTH, DDATA, DDEATH, NDEATH)
- retain = false for ALL message types

Reference: Eclipse Sparkplug Specification Version 3.0
"""

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock

import pytest

from aas_uns_bridge.config import SparkplugConfig
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.publishers.sparkplug import SparkplugPublisher
from aas_uns_bridge.state.alias_db import AliasDB


@pytest.fixture
def sparkplug_config() -> SparkplugConfig:
    """Create Sparkplug config for testing."""
    return SparkplugConfig(
        enabled=True,
        group_id="TestGroup",
        edge_node_id="TestNode",
        qos=1,  # Intentionally set to 1 to verify it's NOT used
    )


@pytest.fixture
def alias_db() -> AliasDB:
    """Create temporary alias database."""
    with TemporaryDirectory() as tmpdir:
        yield AliasDB(Path(tmpdir) / "aliases.db")


@pytest.fixture
def mock_client() -> MagicMock:
    """Create mock MQTT client."""
    client = MagicMock()
    client.subscribe = MagicMock()
    client.set_lwt = MagicMock()
    return client


@pytest.fixture
def sample_metrics() -> list[ContextMetric]:
    """Create sample metrics for testing."""
    return [
        ContextMetric(
            path="Data.Temperature",
            value=25.5,
            aas_type="Property",
            value_type="xs:double",
        ),
        ContextMetric(
            path="Data.Status",
            value="Running",
            aas_type="Property",
            value_type="xs:string",
        ),
    ]


class TestSparkplugQoSCompliance:
    """Tests for Sparkplug QoS=0 requirement compliance."""

    def test_nbirth_uses_qos_zero(
        self,
        mock_client: MagicMock,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
    ) -> None:
        """NBIRTH must use QoS=0 per Sparkplug 3.0 spec."""
        publisher = SparkplugPublisher(mock_client, sparkplug_config, alias_db)

        publisher.publish_nbirth()

        # Verify publish was called with qos=0
        mock_client.publish.assert_called_once()
        call_kwargs = mock_client.publish.call_args
        assert call_kwargs.kwargs.get("qos") == 0 or call_kwargs[1].get("qos") == 0, (
            f"NBIRTH must use QoS=0, got: {call_kwargs}"
        )

    def test_dbirth_uses_qos_zero(
        self,
        mock_client: MagicMock,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        sample_metrics: list[ContextMetric],
    ) -> None:
        """DBIRTH must use QoS=0 per Sparkplug 3.0 spec."""
        publisher = SparkplugPublisher(mock_client, sparkplug_config, alias_db)
        publisher._is_online = True  # Simulate NBIRTH already sent

        publisher.publish_dbirth("TestDevice", sample_metrics)

        # Verify publish was called with qos=0
        mock_client.publish.assert_called_once()
        call_kwargs = mock_client.publish.call_args
        assert call_kwargs.kwargs.get("qos") == 0 or call_kwargs[1].get("qos") == 0, (
            f"DBIRTH must use QoS=0, got: {call_kwargs}"
        )

    def test_ddata_uses_qos_zero(
        self,
        mock_client: MagicMock,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        sample_metrics: list[ContextMetric],
    ) -> None:
        """DDATA must use QoS=0 per Sparkplug 3.0 spec.

        This test verifies the bug fix where DDATA was incorrectly using
        config.qos instead of hardcoded qos=0.
        """
        publisher = SparkplugPublisher(mock_client, sparkplug_config, alias_db)
        publisher._is_online = True
        publisher._devices.add("TestDevice")

        # Pre-populate device metrics to avoid DBIRTH trigger
        publisher._device_metrics["TestDevice"] = {m.path: m for m in sample_metrics}

        # Publish DDATA
        publisher.publish_ddata("TestDevice", sample_metrics)

        # Verify publish was called with qos=0
        mock_client.publish.assert_called_once()
        call_kwargs = mock_client.publish.call_args
        assert call_kwargs.kwargs.get("qos") == 0 or call_kwargs[1].get("qos") == 0, (
            f"DDATA must use QoS=0 (not config.qos), got: {call_kwargs}"
        )

    def test_ddata_does_not_use_config_qos(
        self,
        mock_client: MagicMock,
        alias_db: AliasDB,
        sample_metrics: list[ContextMetric],
    ) -> None:
        """DDATA must NOT use config.qos - must always be QoS=0.

        Regression test for the bug where DDATA used self.config.qos.
        """
        # Config with QoS=2 to ensure it's NOT used
        config = SparkplugConfig(
            enabled=True,
            group_id="TestGroup",
            edge_node_id="TestNode",
            qos=2,  # This should NOT be used for DDATA
        )

        publisher = SparkplugPublisher(mock_client, config, alias_db)
        publisher._is_online = True
        publisher._devices.add("TestDevice")
        publisher._device_metrics["TestDevice"] = {m.path: m for m in sample_metrics}

        publisher.publish_ddata("TestDevice", sample_metrics)

        call_kwargs = mock_client.publish.call_args
        qos_used = (
            call_kwargs.kwargs.get("qos") if call_kwargs.kwargs else call_kwargs[1].get("qos")
        )
        assert qos_used == 0, f"DDATA must use QoS=0, not config.qos={config.qos}"

    def test_ddeath_uses_qos_zero(
        self,
        mock_client: MagicMock,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
    ) -> None:
        """DDEATH must use QoS=0 per Sparkplug 3.0 spec."""
        publisher = SparkplugPublisher(mock_client, sparkplug_config, alias_db)

        publisher.publish_ddeath("TestDevice")

        mock_client.publish.assert_called_once()
        call_kwargs = mock_client.publish.call_args
        assert call_kwargs.kwargs.get("qos") == 0 or call_kwargs[1].get("qos") == 0, (
            f"DDEATH must use QoS=0, got: {call_kwargs}"
        )

    def test_ndeath_lwt_uses_qos_zero(
        self,
        mock_client: MagicMock,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
    ) -> None:
        """NDEATH (LWT) must use QoS=0 per Sparkplug 3.0 spec."""
        # LWT is set during __init__
        SparkplugPublisher(mock_client, sparkplug_config, alias_db)

        mock_client.set_lwt.assert_called_once()
        call_kwargs = mock_client.set_lwt.call_args
        assert call_kwargs.kwargs.get("qos") == 0 or call_kwargs[1].get("qos") == 0, (
            f"NDEATH LWT must use QoS=0, got: {call_kwargs}"
        )


def _get_retain_from_call(call_kwargs) -> bool | None:
    """Extract retain flag from mock call arguments."""
    if call_kwargs.kwargs:
        return call_kwargs.kwargs.get("retain")
    return call_kwargs[1].get("retain") if len(call_kwargs) > 1 else None


class TestSparkplugRetainCompliance:
    """Tests for Sparkplug retain=false requirement compliance."""

    def test_nbirth_retain_false(
        self,
        mock_client: MagicMock,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
    ) -> None:
        """NBIRTH must use retain=false per Sparkplug 3.0 spec."""
        publisher = SparkplugPublisher(mock_client, sparkplug_config, alias_db)

        publisher.publish_nbirth()

        retain = _get_retain_from_call(mock_client.publish.call_args)
        assert retain is False, f"NBIRTH must use retain=false, got: {retain}"

    def test_dbirth_retain_false(
        self,
        mock_client: MagicMock,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        sample_metrics: list[ContextMetric],
    ) -> None:
        """DBIRTH must use retain=false per Sparkplug 3.0 spec."""
        publisher = SparkplugPublisher(mock_client, sparkplug_config, alias_db)
        publisher._is_online = True

        publisher.publish_dbirth("TestDevice", sample_metrics)

        retain = _get_retain_from_call(mock_client.publish.call_args)
        assert retain is False, f"DBIRTH must use retain=false, got: {retain}"

    def test_ddata_retain_false(
        self,
        mock_client: MagicMock,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
        sample_metrics: list[ContextMetric],
    ) -> None:
        """DDATA must use retain=false per Sparkplug 3.0 spec."""
        publisher = SparkplugPublisher(mock_client, sparkplug_config, alias_db)
        publisher._is_online = True
        publisher._devices.add("TestDevice")
        publisher._device_metrics["TestDevice"] = {m.path: m for m in sample_metrics}

        publisher.publish_ddata("TestDevice", sample_metrics)

        retain = _get_retain_from_call(mock_client.publish.call_args)
        assert retain is False, f"DDATA must use retain=false, got: {retain}"

    def test_ddeath_retain_false(
        self,
        mock_client: MagicMock,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
    ) -> None:
        """DDEATH must use retain=false per Sparkplug 3.0 spec."""
        publisher = SparkplugPublisher(mock_client, sparkplug_config, alias_db)

        publisher.publish_ddeath("TestDevice")

        retain = _get_retain_from_call(mock_client.publish.call_args)
        assert retain is False, f"DDEATH must use retain=false, got: {retain}"

    def test_ndeath_lwt_retain_false(
        self,
        mock_client: MagicMock,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
    ) -> None:
        """NDEATH (LWT) must use retain=false per Sparkplug 3.0 spec."""
        SparkplugPublisher(mock_client, sparkplug_config, alias_db)

        retain = _get_retain_from_call(mock_client.set_lwt.call_args)
        assert retain is False, f"NDEATH LWT must use retain=false, got: {retain}"


class TestSparkplugSequenceCompliance:
    """Tests for Sparkplug sequence number compliance."""

    def test_sequence_wraps_at_256(
        self,
        mock_client: MagicMock,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
    ) -> None:
        """Sequence numbers must wrap from 255 to 0."""
        publisher = SparkplugPublisher(mock_client, sparkplug_config, alias_db)

        # Set sequence to 255
        publisher._seq = 255

        # Get next sequence (should be 255)
        seq = publisher._next_seq()
        assert seq == 255

        # Next call should wrap to 0
        seq = publisher._next_seq()
        assert seq == 0

    def test_sequence_increments(
        self,
        mock_client: MagicMock,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
    ) -> None:
        """Sequence numbers should increment per message."""
        publisher = SparkplugPublisher(mock_client, sparkplug_config, alias_db)

        seq1 = publisher._next_seq()
        seq2 = publisher._next_seq()
        seq3 = publisher._next_seq()

        assert seq2 == seq1 + 1
        assert seq3 == seq2 + 1

    def test_sequence_resets_on_rebirth(
        self,
        mock_client: MagicMock,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
    ) -> None:
        """Sequence should reset to 0 on rebirth."""
        publisher = SparkplugPublisher(mock_client, sparkplug_config, alias_db)
        publisher._seq = 100
        publisher._is_online = True

        publisher.rebirth()

        # After rebirth, sequence should be reset
        # (rebirth calls publish_nbirth which uses _next_seq starting from 0)
        assert publisher._seq <= 1  # 0 or 1 depending on message sent


class TestSparkplugBdSeqCompliance:
    """Tests for Sparkplug bdSeq (birth/death sequence) compliance."""

    def test_bdseq_increments_on_rebirth(
        self,
        mock_client: MagicMock,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
    ) -> None:
        """bdSeq should increment on each rebirth."""
        publisher = SparkplugPublisher(mock_client, sparkplug_config, alias_db)
        publisher._is_online = True
        publisher._has_published_nbirth = True

        initial_bdseq = publisher._bd_seq

        # Trigger rebirth
        publisher.rebirth()

        # bdSeq should have incremented (set in publish_nbirth when reconnecting)
        assert publisher._bd_seq == initial_bdseq + 1

    def test_bdseq_increments_on_reconnect(
        self,
        mock_client: MagicMock,
        sparkplug_config: SparkplugConfig,
        alias_db: AliasDB,
    ) -> None:
        """bdSeq should increment when reconnecting after disconnect."""
        publisher = SparkplugPublisher(mock_client, sparkplug_config, alias_db)

        # First connection
        publisher.publish_nbirth()
        initial_bdseq = publisher._bd_seq

        # Simulate disconnect
        publisher.mark_offline()

        # Reconnect - should increment bdSeq
        publisher.publish_nbirth()

        assert publisher._bd_seq == initial_bdseq + 1
