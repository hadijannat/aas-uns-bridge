"""Health check endpoint for the AAS-UNS Bridge."""

import json
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any, Callable


class HealthHandler(BaseHTTPRequestHandler):
    """HTTP handler for health check endpoint."""

    check_func: Callable[[], dict[str, Any]] | None = None

    def do_GET(self) -> None:
        """Handle GET requests."""
        if self.path == "/health":
            self._handle_health()
        elif self.path == "/ready":
            self._handle_ready()
        elif self.path == "/live":
            self._handle_live()
        else:
            self.send_response(404)
            self.end_headers()

    def _handle_health(self) -> None:
        """Handle /health endpoint."""
        if self.check_func:
            health = self.check_func()
        else:
            health = {"status": "unknown"}

        status_code = 200 if health.get("status") == "healthy" else 503

        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(health).encode())

    def _handle_ready(self) -> None:
        """Handle /ready endpoint (Kubernetes readiness probe)."""
        if self.check_func:
            health = self.check_func()
            ready = health.get("mqtt_connected", False)
        else:
            ready = False

        self.send_response(200 if ready else 503)
        self.end_headers()

    def _handle_live(self) -> None:
        """Handle /live endpoint (Kubernetes liveness probe)."""
        # Always return 200 if the server is running
        self.send_response(200)
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress request logging."""
        pass


class HealthServer:
    """HTTP server for health checks."""

    def __init__(
        self,
        port: int = 8080,
        check_func: Callable[[], dict[str, Any]] | None = None,
    ):
        """Initialize the health server.

        Args:
            port: Port to listen on.
            check_func: Function that returns health status dict.
        """
        self.port = port
        self._check_func = check_func
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the health server in a background thread."""
        # Create a handler class with the check function
        class Handler(HealthHandler):
            check_func = self._check_func

        self._server = HTTPServer(("0.0.0.0", self.port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the health server."""
        if self._server:
            self._server.shutdown()


def create_health_checker(
    mqtt_client: Any,
    sparkplug_publisher: Any | None = None,
    uns_publisher: Any | None = None,
) -> Callable[[], dict[str, Any]]:
    """Create a health check function.

    Args:
        mqtt_client: MQTT client to check connection status.
        sparkplug_publisher: Optional Sparkplug publisher for metrics.
        uns_publisher: Optional UNS publisher for metrics.

    Returns:
        Function that returns health status dict.
    """

    def check() -> dict[str, Any]:
        mqtt_connected = mqtt_client.is_connected()

        health: dict[str, Any] = {
            "status": "healthy" if mqtt_connected else "degraded",
            "timestamp": int(time.time() * 1000),
            "mqtt_connected": mqtt_connected,
        }

        if sparkplug_publisher:
            health["sparkplug_online"] = sparkplug_publisher.is_online
            health["active_devices"] = len(sparkplug_publisher.active_devices)
            health["sparkplug_births"] = sparkplug_publisher.birth_count

        if uns_publisher:
            health["uns_published"] = uns_publisher.published_count

        return health

    return check
