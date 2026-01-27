"""Prometheus metrics for the AAS-UNS Bridge."""

import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from typing import Any

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest


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
