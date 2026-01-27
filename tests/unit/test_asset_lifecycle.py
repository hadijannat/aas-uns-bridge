"""Unit tests for asset lifecycle tracking."""

import json
import tempfile
import time
from pathlib import Path

import pytest

from aas_uns_bridge.config import LifecycleConfig
from aas_uns_bridge.state.asset_lifecycle import (
    AssetLifecycleTracker,
    AssetState,
)


@pytest.fixture
def temp_db() -> Path:
    """Create a temporary database path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "lifecycle.db"


@pytest.fixture
def basic_config() -> LifecycleConfig:
    """Create a basic lifecycle config."""
    return LifecycleConfig(
        enabled=True,
        stale_threshold_seconds=60,
        clear_retained_on_offline=False,
        publish_lifecycle_events=True,
    )


@pytest.fixture
def short_threshold_config() -> LifecycleConfig:
    """Create config with short stale threshold for testing."""
    return LifecycleConfig(
        enabled=True,
        stale_threshold_seconds=1,  # 1 second for fast testing
        clear_retained_on_offline=False,
        publish_lifecycle_events=True,
    )


class TestAssetOnline:
    """Tests for marking assets online."""

    def test_mark_online_new_asset(self, temp_db: Path, basic_config: LifecycleConfig) -> None:
        """Test marking a new asset as online."""
        tracker = AssetLifecycleTracker(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        event = tracker.mark_online(asset_id)

        assert event is not None
        assert event.asset_id == asset_id
        assert event.previous_state is None
        assert event.new_state == AssetState.ONLINE
        assert event.reason == "first_seen"

    def test_mark_online_with_topic(self, temp_db: Path, basic_config: LifecycleConfig) -> None:
        """Test marking online with a topic."""
        tracker = AssetLifecycleTracker(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        tracker.mark_online(asset_id, topic="AcmeCorp/PlantA/Asset/context/data")

        status = tracker.get_asset_status(asset_id)
        assert "AcmeCorp/PlantA/Asset/context/data" in status.topics

    def test_mark_online_existing_online_asset(
        self, temp_db: Path, basic_config: LifecycleConfig
    ) -> None:
        """Test marking an already online asset (no event)."""
        tracker = AssetLifecycleTracker(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        tracker.mark_online(asset_id)
        event = tracker.mark_online(asset_id)  # Second call

        assert event is None  # No state change

    def test_mark_online_from_stale(
        self, temp_db: Path, short_threshold_config: LifecycleConfig
    ) -> None:
        """Test marking a stale asset as online."""
        tracker = AssetLifecycleTracker(temp_db, short_threshold_config)
        asset_id = "https://example.com/asset/001"

        tracker.mark_online(asset_id)
        time.sleep(1.5)  # Wait for stale threshold
        tracker.check_stale_assets()

        # Now mark online again
        event = tracker.mark_online(asset_id)

        assert event is not None
        assert event.previous_state == AssetState.STALE
        assert event.new_state == AssetState.ONLINE


class TestAssetOffline:
    """Tests for marking assets offline."""

    def test_mark_offline(self, temp_db: Path, basic_config: LifecycleConfig) -> None:
        """Test marking an asset as offline."""
        tracker = AssetLifecycleTracker(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        tracker.mark_online(asset_id)
        event = tracker.mark_offline(asset_id, reason="ddeath")

        assert event is not None
        assert event.previous_state == AssetState.ONLINE
        assert event.new_state == AssetState.OFFLINE
        assert event.reason == "ddeath"

    def test_mark_offline_already_offline(
        self, temp_db: Path, basic_config: LifecycleConfig
    ) -> None:
        """Test marking an already offline asset (no event)."""
        tracker = AssetLifecycleTracker(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        tracker.mark_online(asset_id)
        tracker.mark_offline(asset_id)
        event = tracker.mark_offline(asset_id)  # Second call

        assert event is None

    def test_mark_offline_unknown_asset(self, temp_db: Path, basic_config: LifecycleConfig) -> None:
        """Test marking an unknown asset as offline."""
        tracker = AssetLifecycleTracker(temp_db, basic_config)

        event = tracker.mark_offline("unknown-asset")

        assert event is None


class TestStaleDetection:
    """Tests for stale asset detection."""

    def test_check_stale_assets(
        self, temp_db: Path, short_threshold_config: LifecycleConfig
    ) -> None:
        """Test detecting stale assets."""
        tracker = AssetLifecycleTracker(temp_db, short_threshold_config)
        asset_id = "https://example.com/asset/001"

        tracker.mark_online(asset_id)
        time.sleep(1.5)  # Wait for stale threshold

        events = tracker.check_stale_assets()

        assert len(events) == 1
        assert events[0].previous_state == AssetState.ONLINE
        assert events[0].new_state == AssetState.STALE
        assert "no_data_for" in events[0].reason

    def test_online_asset_not_stale(self, temp_db: Path, basic_config: LifecycleConfig) -> None:
        """Test that recently active asset is not stale."""
        tracker = AssetLifecycleTracker(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        tracker.mark_online(asset_id)
        events = tracker.check_stale_assets()

        assert len(events) == 0

    def test_already_stale_not_detected_again(
        self, temp_db: Path, short_threshold_config: LifecycleConfig
    ) -> None:
        """Test that already stale asset is not detected again."""
        tracker = AssetLifecycleTracker(temp_db, short_threshold_config)
        asset_id = "https://example.com/asset/001"

        tracker.mark_online(asset_id)
        time.sleep(1.5)
        tracker.check_stale_assets()

        # Check again
        events = tracker.check_stale_assets()

        assert len(events) == 0


class TestAssetStatusQueries:
    """Tests for querying asset status."""

    def test_get_asset_status(self, temp_db: Path, basic_config: LifecycleConfig) -> None:
        """Test getting status of a single asset."""
        tracker = AssetLifecycleTracker(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        tracker.mark_online(asset_id, topic="topic1")
        status = tracker.get_asset_status(asset_id)

        assert status is not None
        assert status.asset_id == asset_id
        assert status.state == AssetState.ONLINE
        assert "topic1" in status.topics

    def test_get_all_assets(self, temp_db: Path, basic_config: LifecycleConfig) -> None:
        """Test getting all tracked assets."""
        tracker = AssetLifecycleTracker(temp_db, basic_config)

        tracker.mark_online("asset1")
        tracker.mark_online("asset2")

        all_assets = tracker.get_all_assets()

        assert len(all_assets) == 2
        assert "asset1" in all_assets
        assert "asset2" in all_assets

    def test_get_assets_by_state(self, temp_db: Path, basic_config: LifecycleConfig) -> None:
        """Test filtering assets by state."""
        tracker = AssetLifecycleTracker(temp_db, basic_config)

        tracker.mark_online("asset1")
        tracker.mark_online("asset2")
        tracker.mark_offline("asset1")

        online = tracker.get_assets_by_state(AssetState.ONLINE)
        offline = tracker.get_assets_by_state(AssetState.OFFLINE)

        assert len(online) == 1
        assert online[0].asset_id == "asset2"
        assert len(offline) == 1
        assert offline[0].asset_id == "asset1"

    def test_get_topics_for_asset(self, temp_db: Path, basic_config: LifecycleConfig) -> None:
        """Test getting topics for an asset."""
        tracker = AssetLifecycleTracker(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        tracker.mark_online(asset_id, topic="topic1")
        tracker.mark_online(asset_id, topic="topic2")

        topics = tracker.get_topics_for_asset(asset_id)

        assert topics == {"topic1", "topic2"}


class TestAssetCounts:
    """Tests for asset count properties."""

    def test_count_properties(self, temp_db: Path, short_threshold_config: LifecycleConfig) -> None:
        """Test online/stale/offline count properties."""
        tracker = AssetLifecycleTracker(temp_db, short_threshold_config)

        tracker.mark_online("asset1")
        tracker.mark_online("asset2")
        tracker.mark_online("asset3")
        tracker.mark_offline("asset1")

        assert tracker.online_count == 2
        assert tracker.stale_count == 0
        assert tracker.offline_count == 1
        assert tracker.total_count == 3


class TestLifecycleEventTopics:
    """Tests for lifecycle event topic and payload generation."""

    def test_build_lifecycle_topic(self, temp_db: Path, basic_config: LifecycleConfig) -> None:
        """Test building lifecycle topic from asset ID."""
        tracker = AssetLifecycleTracker(temp_db, basic_config)

        topic = tracker.build_lifecycle_topic("https://example.com/asset/001")

        assert topic == "UNS/Sys/Lifecycle/example.com_asset_001"

    def test_build_event_payload(self, temp_db: Path, basic_config: LifecycleConfig) -> None:
        """Test building event payload."""
        tracker = AssetLifecycleTracker(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        event = tracker.mark_online(asset_id)
        payload = tracker.build_event_payload(event)
        data = json.loads(payload)

        assert data["assetId"] == asset_id
        assert data["previousState"] is None
        assert data["newState"] == "online"
        assert data["reason"] == "first_seen"
        assert "timestamp" in data


class TestPersistence:
    """Tests for lifecycle state persistence."""

    def test_state_persists_across_instances(
        self, temp_db: Path, basic_config: LifecycleConfig
    ) -> None:
        """Test that state persists across tracker instances."""
        asset_id = "https://example.com/asset/001"

        # First instance marks asset online
        tracker1 = AssetLifecycleTracker(temp_db, basic_config)
        tracker1.mark_online(asset_id, topic="topic1")

        # Second instance should see the state
        tracker2 = AssetLifecycleTracker(temp_db, basic_config)
        status = tracker2.get_asset_status(asset_id)

        assert status is not None
        assert status.state == AssetState.ONLINE
        assert "topic1" in status.topics

    def test_remove_asset(self, temp_db: Path, basic_config: LifecycleConfig) -> None:
        """Test removing an asset from tracking."""
        tracker = AssetLifecycleTracker(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        tracker.mark_online(asset_id)
        result = tracker.remove_asset(asset_id)

        assert result is True
        assert tracker.get_asset_status(asset_id) is None

    def test_remove_unknown_asset(self, temp_db: Path, basic_config: LifecycleConfig) -> None:
        """Test removing an unknown asset."""
        tracker = AssetLifecycleTracker(temp_db, basic_config)

        result = tracker.remove_asset("unknown")

        assert result is False

    def test_clear_all(self, temp_db: Path, basic_config: LifecycleConfig) -> None:
        """Test clearing all tracked assets."""
        tracker = AssetLifecycleTracker(temp_db, basic_config)

        tracker.mark_online("asset1")
        tracker.mark_online("asset2")
        count = tracker.clear_all()

        assert count == 2
        assert tracker.total_count == 0


class TestAssetStatusProperties:
    """Tests for AssetStatus properties."""

    def test_age_seconds(self, temp_db: Path, basic_config: LifecycleConfig) -> None:
        """Test age_seconds property."""
        tracker = AssetLifecycleTracker(temp_db, basic_config)
        asset_id = "https://example.com/asset/001"

        tracker.mark_online(asset_id)
        time.sleep(0.1)

        status = tracker.get_asset_status(asset_id)
        assert status.age_seconds >= 0.1
