"""Prometheus metrics for the AAS-UNS Bridge."""

import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from typing import Any

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)


# Metric definitions
class BridgeMetrics:
    """Collection of Prometheus metrics for the bridge."""

    def __init__(self) -> None:
        """Initialize metrics."""
        # Counters
        self.aas_loaded_total = Counter(
            "aas_bridge_aas_loaded_total",
            "Total number of AAS files loaded",
            ["source_type"],  # 'file' or 'repository'
        )

        self.metrics_flattened_total = Counter(
            "aas_bridge_metrics_flattened_total",
            "Total number of metrics flattened from AAS content",
        )

        self.uns_published_total = Counter(
            "aas_bridge_uns_published_total",
            "Total number of UNS retained messages published",
        )

        self.sparkplug_births_total = Counter(
            "aas_bridge_sparkplug_births_total",
            "Total number of Sparkplug birth messages published",
            ["birth_type"],  # 'nbirth' or 'dbirth'
        )

        self.sparkplug_data_total = Counter(
            "aas_bridge_sparkplug_data_total",
            "Total number of Sparkplug data messages published",
        )

        self.errors_total = Counter(
            "aas_bridge_errors_total",
            "Total number of errors",
            ["error_type"],
        )

        # Gauges
        self.mqtt_connected = Gauge(
            "aas_bridge_mqtt_connected",
            "MQTT connection status (1=connected, 0=disconnected)",
        )

        self.last_publish_timestamp = Gauge(
            "aas_bridge_last_publish_timestamp",
            "Unix timestamp of last successful publish",
        )

        self.active_devices = Gauge(
            "aas_bridge_active_devices",
            "Number of active Sparkplug devices",
        )

        self.tracked_topics = Gauge(
            "aas_bridge_tracked_topics",
            "Number of topics being tracked for deduplication",
        )

        self.alias_count = Gauge(
            "aas_bridge_alias_count",
            "Number of Sparkplug metric aliases",
        )

        # Semantic enforcement metrics
        self.validation_errors_total = Counter(
            "aas_bridge_validation_errors_total",
            "Total number of validation errors",
            ["error_type"],  # missing_semantic_id, value_out_of_range, etc.
        )

        self.validation_metrics_total = Counter(
            "aas_bridge_validation_metrics_total",
            "Total number of metrics validated",
            ["result"],  # 'valid' or 'invalid'
        )

        self.drift_events_total = Counter(
            "aas_bridge_drift_events_total",
            "Total number of schema drift events detected",
            ["event_type"],  # added, removed, type_changed, etc.
        )

        self.asset_lifecycle_events_total = Counter(
            "aas_bridge_asset_lifecycle_events_total",
            "Total number of asset lifecycle events",
            ["state"],  # online, stale, offline
        )

        self.assets_online = Gauge(
            "aas_bridge_assets_online",
            "Number of assets currently online",
        )

        self.assets_stale = Gauge(
            "aas_bridge_assets_stale",
            "Number of assets currently stale",
        )

        self.assets_offline = Gauge(
            "aas_bridge_assets_offline",
            "Number of assets currently offline",
        )

        # Fidelity metrics
        self.fidelity_overall = Gauge(
            "aas_bridge_fidelity_overall",
            "Overall transformation fidelity score (0.0-1.0)",
            ["asset_id"],
        )

        self.fidelity_structural = Gauge(
            "aas_bridge_fidelity_structural",
            "Structural fidelity score (0.0-1.0)",
            ["asset_id"],
        )

        self.fidelity_semantic = Gauge(
            "aas_bridge_fidelity_semantic",
            "Semantic fidelity score (0.0-1.0)",
            ["asset_id"],
        )

        self.fidelity_entropy_loss = Gauge(
            "aas_bridge_fidelity_entropy_loss",
            "Information entropy loss ratio (0.0-1.0)",
            ["asset_id"],
        )

        self.fidelity_evaluations_total = Counter(
            "aas_bridge_fidelity_evaluations_total",
            "Total number of fidelity evaluations",
        )

        # Streaming drift (hypervisor) metrics
        self.streaming_drift_detected_total = Counter(
            "aas_bridge_streaming_drift_detected_total",
            "Total streaming drift events detected",
            ["drift_type", "severity"],
        )

        self.streaming_drift_anomaly_score = Gauge(
            "aas_bridge_streaming_drift_anomaly_score",
            "Latest anomaly score from streaming detector",
            ["asset_id"],
        )

        self.streaming_drift_forest_observations = Gauge(
            "aas_bridge_streaming_drift_forest_observations",
            "Number of observations processed by Half-Space Forest",
            ["asset_id"],
        )

        # Semantic resolution cache metrics
        self.semantic_cache_hits_total = Counter(
            "aas_bridge_semantic_cache_hits_total",
            "Total semantic cache hits",
            ["cache_tier"],  # 'memory' or 'sqlite'
        )

        self.semantic_cache_misses_total = Counter(
            "aas_bridge_semantic_cache_misses_total",
            "Total semantic cache misses",
        )

        self.semantic_cache_size = Gauge(
            "aas_bridge_semantic_cache_size",
            "Number of entries in semantic cache",
            ["tier"],  # 'memory' or 'total'
        )

        self.semantic_pointers_registered_total = Counter(
            "aas_bridge_semantic_pointers_registered_total",
            "Total semantic pointers registered",
        )

        # Bidirectional sync metrics
        self.bidirectional_writes_total = Counter(
            "aas_bridge_bidirectional_writes_total",
            "Total write-back operations",
            ["result"],  # 'success' or 'failure'
        )

        self.bidirectional_validations_total = Counter(
            "aas_bridge_bidirectional_validations_total",
            "Total write validations",
            ["result"],  # 'allowed' or 'denied'
        )

        self.aas_write_retries_total = Counter(
            "aas_bridge_aas_write_retries_total",
            "Total AAS write retry attempts",
        )

        # State database metrics
        self.state_db_evictions_total = Counter(
            "aas_bridge_state_db_evictions_total",
            "Total state database evictions",
            ["db_type"],  # 'alias' or 'hash'
        )

        self.state_db_entries = Gauge(
            "aas_bridge_state_db_entries",
            "Current number of entries in state database",
            ["db_type"],  # 'alias' or 'hash'
        )

        self.state_db_max_entries = Gauge(
            "aas_bridge_state_db_max_entries",
            "Maximum entries allowed in state database",
            ["db_type"],  # 'alias' or 'hash'
        )

        # MQTT backpressure metrics
        self.mqtt_publish_queue_depth = Gauge(
            "aas_bridge_mqtt_publish_queue_depth",
            "Current depth of MQTT publish queue (pending messages awaiting acknowledgment)",
        )

        # Performance metrics for TRL 8 monitoring
        self.publish_latency_seconds = Histogram(
            "aas_bridge_publish_latency_seconds",
            "Time from publish call to acknowledgment",
            ["publisher_type"],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
        )

        self.state_db_size_bytes = Gauge(
            "aas_bridge_state_db_size_bytes",
            "Size of state database file in bytes",
            ["db_type"],
        )

        self.traversal_duration_seconds = Histogram(
            "aas_bridge_traversal_duration_seconds",
            "Duration of AAS submodel traversal",
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
        )

        self.aas_load_duration_seconds = Histogram(
            "aas_bridge_aas_load_duration_seconds",
            "Duration of AAS file loading",
            ["source_type"],
            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0],
        )


# Global metrics instance
METRICS = BridgeMetrics()


class MetricsHandler(SimpleHTTPRequestHandler):
    """HTTP handler for Prometheus metrics endpoint."""

    def do_GET(self) -> None:
        """Handle GET requests."""
        if self.path == "/metrics":
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(generate_latest())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress request logging."""
        pass


class MetricsServer:
    """HTTP server for Prometheus metrics."""

    def __init__(self, port: int = 9090):
        """Initialize the metrics server.

        Args:
            port: Port to listen on.
        """
        self.port = port
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the metrics server in a background thread."""
        self._server = HTTPServer(("0.0.0.0", self.port), MetricsHandler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the metrics server."""
        if self._server:
            self._server.shutdown()
