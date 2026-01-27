"""Main daemon orchestration for the AAS-UNS Bridge."""

import hashlib
import logging
import signal
import threading
import time
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from aas_uns_bridge.aas.loader import load_file
from aas_uns_bridge.aas.repo_client import AASRepoClient
from aas_uns_bridge.aas.traversal import flatten_submodel, iter_submodels
from aas_uns_bridge.config import BridgeConfig
from aas_uns_bridge.domain.models import ContextMetric
from aas_uns_bridge.mapping.isa95 import ISA95Mapper, MappingConfig
from aas_uns_bridge.mqtt.client import MqttClient
from aas_uns_bridge.observability.health import HealthServer, create_health_checker
from aas_uns_bridge.observability.metrics import METRICS, MetricsServer
from aas_uns_bridge.publishers.sparkplug import SparkplugPublisher
from aas_uns_bridge.publishers.uns_retained import UnsRetainedPublisher
from aas_uns_bridge.state.alias_db import AliasDB
from aas_uns_bridge.state.birth_cache import BirthCache
from aas_uns_bridge.state.last_published import LastPublishedHashes

logger = logging.getLogger(__name__)


class AASFileHandler(FileSystemEventHandler):
    """File system event handler for AASX files."""

    def __init__(
        self,
        callback: "Callable[[Path], None]",
        patterns: list[str],
        debounce_seconds: float,
    ):
        """Initialize the handler.

        Args:
            callback: Function to call when a file changes.
            patterns: File patterns to watch (e.g., ['*.aasx']).
            debounce_seconds: Debounce interval for rapid changes.
        """
        self.callback = callback
        self.patterns = [p.lower() for p in patterns]
        self.debounce = debounce_seconds
        self._pending: dict[str, float] = {}
        self._lock = threading.Lock()

    def _matches_pattern(self, path: Path) -> bool:
        """Check if a path matches any watched pattern."""
        name = path.name.lower()
        for pattern in self.patterns:
            if pattern.startswith("*"):
                if name.endswith(pattern[1:]):
                    return True
            elif name == pattern:
                return True
        return False

    def on_any_event(self, event: FileSystemEvent) -> None:
        """Handle file system events."""
        if event.is_directory:
            return

        path = Path(event.src_path)
        if not self._matches_pattern(path):
            return

        # Debounce rapid events
        with self._lock:
            now = time.time()
            last_event = self._pending.get(str(path), 0)
            if now - last_event < self.debounce:
                return
            self._pending[str(path)] = now

        # Delay callback to allow file writes to complete
        def delayed_callback() -> None:
            time.sleep(self.debounce)
            if path.exists():
                try:
                    self.callback(path)
                except Exception as e:
                    logger.error("Error processing %s: %s", path, e)

        threading.Thread(target=delayed_callback, daemon=True).start()


class BridgeDaemon:
    """Main daemon orchestrating AAS ingestion and publication."""

    def __init__(
        self,
        config: BridgeConfig,
        mappings_path: Path,
    ):
        """Initialize the bridge daemon.

        Args:
            config: Bridge configuration.
            mappings_path: Path to mappings.yaml file.
        """
        self.config = config
        self._shutdown = threading.Event()
        self._file_hashes: dict[str, str] = {}

        # Initialize components
        self._init_logging()

        # Load mappings
        self.mappings = MappingConfig.from_yaml(mappings_path)
        self.mapper = ISA95Mapper(self.mappings, config.uns.root_topic)

        # State management
        state_dir = config.state.db_path.parent
        state_dir.mkdir(parents=True, exist_ok=True)

        self.alias_db = AliasDB(state_dir / "aliases.db")
        self.birth_cache = (
            BirthCache(state_dir / "births.db") if config.state.cache_births else None
        )
        self.last_published = LastPublishedHashes(
            state_dir / "hashes.db" if config.state.deduplicate_publishes else None
        )

        # MQTT client
        self.mqtt_client = MqttClient(
            config.mqtt,
            on_connect=self._on_mqtt_connect,
            on_disconnect=self._on_mqtt_disconnect,
        )

        # Publishers
        self.uns_publisher = UnsRetainedPublisher(self.mqtt_client, config.uns)
        self.sparkplug_publisher = SparkplugPublisher(
            self.mqtt_client,
            config.sparkplug,
            self.alias_db,
            birth_cache=self.birth_cache,
        )

        # Observability servers
        self.metrics_server = MetricsServer(config.observability.metrics_port)
        self.health_server = HealthServer(
            config.observability.health_port,
            check_func=create_health_checker(
                self.mqtt_client,
                sparkplug_publisher=self.sparkplug_publisher,
                uns_publisher=self.uns_publisher,
            ),
        )

        # File watcher
        self._observer: Any | None = None
        if config.file_watcher.enabled:
            self._setup_file_watcher()

        # Repository client
        self._repo_client: AASRepoClient | None = None
        if config.repo_client.enabled:
            self._repo_client = AASRepoClient(config.repo_client)

        METRICS.mqtt_connected.set(0)

    def _init_logging(self) -> None:
        """Initialize logging configuration."""
        from aas_uns_bridge.observability.logging import setup_logging

        setup_logging(
            level=self.config.observability.log_level,
            format_type=self.config.observability.log_format,
        )

    def _setup_file_watcher(self) -> None:
        """Set up the file system watcher."""
        watch_dir = self.config.file_watcher.watch_dir
        watch_dir.mkdir(parents=True, exist_ok=True)

        handler = AASFileHandler(
            callback=self._process_aas_file,
            patterns=self.config.file_watcher.patterns,
            debounce_seconds=self.config.file_watcher.debounce_seconds,
        )

        self._observer = Observer()
        self._observer.schedule(  # type: ignore[no-untyped-call]
            handler,
            str(watch_dir),
            recursive=self.config.file_watcher.recursive,
        )

    def _on_mqtt_connect(self) -> None:
        """Handle MQTT connection."""
        logger.info("MQTT connected, publishing births")
        METRICS.mqtt_connected.set(1)

        # Publish Sparkplug NBIRTH
        if self.config.sparkplug.enabled:
            self.sparkplug_publisher.publish_nbirth()
            self.sparkplug_publisher.republish_dbirths()

    def _on_mqtt_disconnect(self) -> None:
        """Handle MQTT disconnection."""
        logger.warning("MQTT disconnected")
        METRICS.mqtt_connected.set(0)
        if self.config.sparkplug.enabled:
            self.sparkplug_publisher.mark_offline()

    def _compute_file_hash(self, path: Path) -> str:
        """Compute SHA256 hash of a file."""
        hasher = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _file_has_changed(self, path: Path) -> bool:
        """Check if a file has changed since last processing."""
        current_hash = self._compute_file_hash(path)
        previous_hash = self._file_hashes.get(str(path))
        self._file_hashes[str(path)] = current_hash
        return current_hash != previous_hash

    def _process_aas_file(self, path: Path) -> None:
        """Process an AAS file (AASX or JSON).

        Args:
            path: Path to the AAS file.
        """
        if not self._file_has_changed(path):
            logger.debug("File unchanged, skipping: %s", path)
            return

        logger.info("Processing AAS file: %s", path)

        try:
            object_store = load_file(path)
            METRICS.aas_loaded_total.labels(source_type="file").inc()
            self._process_object_store(object_store, str(path))
        except Exception as e:
            logger.error("Failed to process %s: %s", path, e)
            METRICS.errors_total.labels(error_type="aas_file").inc()

    def _process_object_store(self, object_store: Any, source: str) -> None:
        """Process an AAS object store.

        Args:
            object_store: BaSyx ObjectStore with AAS content.
            source: Source identifier (file path or URL).
        """
        device_metrics_all: dict[str, list[ContextMetric]] = defaultdict(list)
        device_metrics_changed: dict[str, list[ContextMetric]] = defaultdict(list)

        for submodel, global_asset_id in iter_submodels(object_store):
            if not submodel.id_short:
                continue

            # Flatten submodel to metrics
            metrics = flatten_submodel(
                submodel,
                aas_source=source,
                preferred_lang=self.config.preferred_language,
            )

            if not metrics:
                continue

            METRICS.metrics_flattened_total.inc(len(metrics))
            logger.debug(
                "Flattened %s: %d metrics",
                submodel.id_short,
                len(metrics),
            )

            # Build topics for UNS
            topic_metrics = self.mapper.build_topics_for_submodel(
                metrics,
                global_asset_id,
                submodel.id_short,
            )

            changed_topic_metrics = topic_metrics
            if self.config.state.deduplicate_publishes:
                changed_topic_metrics = self.last_published.filter_changed(topic_metrics)

            # Publish to UNS retained topics (changed only)
            if self.config.uns.enabled and changed_topic_metrics:
                self.uns_publisher.publish_batch(changed_topic_metrics, source)

            # Accumulate Sparkplug metrics
            if self.config.sparkplug.enabled and global_asset_id:
                identity = self.mapper.get_identity(global_asset_id)
                device_id = identity.asset or submodel.id_short

                device_metrics_all[device_id].extend(metrics)
                if self.config.state.deduplicate_publishes:
                    device_metrics_changed[device_id].extend(list(changed_topic_metrics.values()))
                else:
                    device_metrics_changed[device_id].extend(metrics)

            # Update hash cache for UNS deduplication
            if self.config.state.deduplicate_publishes and changed_topic_metrics:
                self.last_published.update_batch(changed_topic_metrics)
                METRICS.tracked_topics.set(self.last_published.count)

        # Publish Sparkplug after gathering per-device metrics
        if self.config.sparkplug.enabled:
            for device_id, metrics_all in device_metrics_all.items():
                metrics_changed = device_metrics_changed.get(device_id, [])
                self.sparkplug_publisher.publish_device_metrics(
                    device_id,
                    metrics_all,
                    metrics_changed,
                    aas_uri=source,
                )

    def _poll_repository(self) -> None:
        """Poll the AAS Repository for changes."""
        if not self._repo_client:
            return

        try:
            object_store, changed = self._repo_client.fetch_all()
            if changed:
                logger.info("Repository content changed, processing")
                METRICS.aas_loaded_total.labels(source_type="repository").inc()
                self._process_object_store(object_store, self.config.repo_client.base_url)
        except Exception as e:
            logger.error("Repository poll failed: %s", e)
            METRICS.errors_total.labels(error_type="repository").inc()

    def _scan_existing_files(self) -> None:
        """Scan and process existing files in the watch directory."""
        if not self.config.file_watcher.enabled:
            return

        watch_dir = self.config.file_watcher.watch_dir
        if not watch_dir.exists():
            return

        for pattern in self.config.file_watcher.patterns:
            glob_pattern = f"**/{pattern}" if self.config.file_watcher.recursive else pattern
            for path in watch_dir.glob(glob_pattern):
                try:
                    self._process_aas_file(path)
                except Exception as e:
                    logger.error("Error processing %s: %s", path, e)

    def start(self) -> None:
        """Start the bridge daemon."""
        logger.info("Starting AAS-UNS Bridge daemon")

        # Start observability endpoints
        self.metrics_server.start()
        self.health_server.start()

        # Connect to MQTT
        self.mqtt_client.connect()

        # Start file watcher
        if self._observer:
            self._observer.start()
            logger.info("File watcher started on %s", self.config.file_watcher.watch_dir)

        # Process existing files
        self._scan_existing_files()

        # Initial repository poll
        if self._repo_client:
            self._poll_repository()

    def run(self) -> None:
        """Run the main daemon loop."""
        self.start()

        poll_interval = (
            self.config.repo_client.poll_interval_seconds
            if self.config.repo_client.enabled
            else 60.0
        )

        try:
            while not self._shutdown.is_set():
                # Poll repository periodically
                if self._repo_client:
                    self._poll_repository()

                # Wait for shutdown or next poll
                self._shutdown.wait(poll_interval)

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")

        self.shutdown()

    def shutdown(self) -> None:
        """Gracefully shut down the daemon."""
        logger.info("Shutting down AAS-UNS Bridge daemon")
        self._shutdown.set()

        # Stop file watcher
        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=5)

        # Shutdown Sparkplug (sends DDEATHs)
        if self.config.sparkplug.enabled:
            self.sparkplug_publisher.shutdown()

        # Disconnect MQTT (triggers NDEATH via LWT)
        self.mqtt_client.disconnect()

        # Close repository client
        if self._repo_client:
            self._repo_client.close()

        # Stop observability servers
        self.health_server.stop()
        self.metrics_server.stop()

        logger.info("Daemon shutdown complete")


def run_daemon(config: BridgeConfig, mappings_path: Path) -> None:
    """Run the bridge daemon.

    Args:
        config: Bridge configuration.
        mappings_path: Path to mappings.yaml file.
    """
    daemon = BridgeDaemon(config, mappings_path)

    # Set up signal handlers
    def signal_handler(signum: int, frame: Any) -> None:
        logger.info("Received signal %d", signum)
        daemon.shutdown()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    daemon.run()
