"""MQTT client wrapper with TLS, auth, and reconnection support."""

import logging
import ssl
import threading
from collections.abc import Callable
from typing import Any

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion, MQTTErrorCode

from aas_uns_bridge.config import MqttConfig

logger = logging.getLogger(__name__)

# Type alias for message callbacks
MessageCallback = Callable[[str, bytes], None]


class MqttClientError(Exception):
    """Raised when MQTT operations fail."""

    pass


class MqttClient:
    """MQTT client wrapper with automatic reconnection and TLS support.

    This client wraps paho-mqtt v2.0+ and provides:
    - TLS/SSL support with certificate authentication
    - Username/password authentication
    - Automatic reconnection with exponential backoff
    - Last Will and Testament (LWT) configuration
    - Connection state tracking for health checks
    """

    def __init__(
        self,
        config: MqttConfig,
        on_connect: Callable[[], None] | None = None,
        on_disconnect: Callable[[], None] | None = None,
    ):
        """Initialize the MQTT client.

        Args:
            config: MQTT configuration.
            on_connect: Optional callback when connected.
            on_disconnect: Optional callback when disconnected.
        """
        self.config = config
        self._on_connect_callback = on_connect
        self._on_disconnect_callback = on_disconnect

        self._client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=config.client_id,
            protocol=mqtt.MQTTv5,
        )

        # Connection state
        self._connected = threading.Event()
        self._should_reconnect = True
        self._reconnect_delay = config.reconnect_delay_min
        self._subscriptions: dict[str, MessageCallback] = {}
        self._lock = threading.Lock()

        # Set up callbacks
        self._client.on_connect = self._handle_connect
        self._client.on_disconnect = self._handle_disconnect
        self._client.on_message = self._handle_message

        # Configure authentication
        if config.username:
            password = config.password.get_secret_value() if config.password else None
            self._client.username_pw_set(config.username, password)

        # Configure TLS
        if config.use_tls:
            self._setup_tls()

    def _setup_tls(self) -> None:
        """Configure TLS/SSL for the connection."""
        context = ssl.create_default_context()

        if self.config.ca_cert and self.config.ca_cert.exists():
            context.load_verify_locations(str(self.config.ca_cert))

        if (
            self.config.client_cert
            and self.config.client_key
            and self.config.client_cert.exists()
            and self.config.client_key.exists()
        ):
            context.load_cert_chain(
                certfile=str(self.config.client_cert),
                keyfile=str(self.config.client_key),
            )

        self._client.tls_set_context(context)

    def _handle_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: Any,
        properties: Any | None,
    ) -> None:
        """Handle connection callback."""
        # In paho-mqtt 2.0 with MQTTv5, reason_code is a ReasonCode object
        # Success is value 0, or the object has is_failure=False
        is_success = (
            reason_code == 0
            or (hasattr(reason_code, "value") and reason_code.value == 0)
            or (hasattr(reason_code, "is_failure") and not reason_code.is_failure)
        )
        if is_success:
            logger.info("Connected to MQTT broker %s:%d", self.config.host, self.config.port)
            self._connected.set()
            self._reconnect_delay = self.config.reconnect_delay_min

            # Resubscribe to topics
            with self._lock:
                for topic in self._subscriptions:
                    self._client.subscribe(topic)
                    logger.debug("Resubscribed to %s", topic)

            if self._on_connect_callback:
                self._on_connect_callback()
        else:
            logger.error("Connection failed: %s", reason_code)

    def _handle_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: mqtt.DisconnectFlags,
        reason_code: Any,
        properties: Any | None,
    ) -> None:
        """Handle disconnection callback."""
        self._connected.clear()
        logger.warning("Disconnected from MQTT broker: %s", reason_code)

        if self._on_disconnect_callback:
            self._on_disconnect_callback()

    def _handle_message(
        self,
        client: mqtt.Client,
        userdata: Any,
        message: mqtt.MQTTMessage,
    ) -> None:
        """Handle incoming message callback."""
        with self._lock:
            # Find matching subscription (including wildcards)
            for pattern, callback in self._subscriptions.items():
                if mqtt.topic_matches_sub(pattern, message.topic):
                    try:
                        callback(message.topic, message.payload)
                    except Exception as e:
                        logger.error("Error in message callback for %s: %s", message.topic, e)

    def set_lwt(self, topic: str, payload: bytes, qos: int = 0, retain: bool = False) -> None:
        """Set the Last Will and Testament message.

        Must be called before connect().

        Args:
            topic: LWT topic.
            payload: LWT payload.
            qos: Quality of Service level.
            retain: Whether to retain the LWT message.
        """
        self._client.will_set(topic, payload, qos, retain)
        logger.debug("Set LWT on topic %s", topic)

    def connect(self, timeout: float = 30.0) -> None:
        """Connect to the MQTT broker.

        Args:
            timeout: Connection timeout in seconds.

        Raises:
            MqttClientError: If connection fails within timeout.
        """
        try:
            self._client.connect(
                self.config.host,
                self.config.port,
                keepalive=self.config.keepalive,
            )
            self._client.loop_start()

            if not self._connected.wait(timeout):
                raise MqttClientError(
                    f"Connection timeout after {timeout}s to {self.config.host}:{self.config.port}"
                )

        except Exception as e:
            raise MqttClientError(f"Connection failed: {e}") from e

    def disconnect(self) -> None:
        """Disconnect from the MQTT broker."""
        self._should_reconnect = False
        self._client.loop_stop()
        self._client.disconnect()
        self._connected.clear()
        logger.info("Disconnected from MQTT broker")

    def is_connected(self) -> bool:
        """Check if the client is currently connected."""
        return self._connected.is_set()

    def publish(
        self,
        topic: str,
        payload: bytes | str,
        qos: int = 0,
        retain: bool = False,
    ) -> None:
        """Publish a message to a topic.

        Args:
            topic: MQTT topic to publish to.
            payload: Message payload.
            qos: Quality of Service level (0, 1, or 2).
            retain: Whether the broker should retain the message.

        Raises:
            MqttClientError: If not connected or publish fails.
        """
        if not self.is_connected():
            raise MqttClientError("Not connected to broker")

        if isinstance(payload, str):
            payload = payload.encode("utf-8")

        result = self._client.publish(topic, payload, qos=qos, retain=retain)

        if result.rc != MQTTErrorCode.MQTT_ERR_SUCCESS:
            raise MqttClientError(f"Publish failed: {result.rc}")

        logger.debug("Published to %s (qos=%d, retain=%s)", topic, qos, retain)

    def subscribe(self, topic: str, callback: MessageCallback) -> None:
        """Subscribe to a topic with a callback.

        Args:
            topic: MQTT topic pattern to subscribe to.
            callback: Function to call when a message is received.
        """
        with self._lock:
            self._subscriptions[topic] = callback

        if self.is_connected():
            result = self._client.subscribe(topic)
            if result[0] != MQTTErrorCode.MQTT_ERR_SUCCESS:
                logger.error("Subscribe failed for %s: %s", topic, result[0])
            else:
                logger.debug("Subscribed to %s", topic)

    def unsubscribe(self, topic: str) -> None:
        """Unsubscribe from a topic.

        Args:
            topic: MQTT topic pattern to unsubscribe from.
        """
        with self._lock:
            self._subscriptions.pop(topic, None)

        if self.is_connected():
            self._client.unsubscribe(topic)
            logger.debug("Unsubscribed from %s", topic)

    def wait_for_connection(self, timeout: float | None = None) -> bool:
        """Wait for the client to connect.

        Args:
            timeout: Maximum time to wait in seconds, or None to wait forever.

        Returns:
            True if connected, False if timeout occurred.
        """
        return self._connected.wait(timeout)
