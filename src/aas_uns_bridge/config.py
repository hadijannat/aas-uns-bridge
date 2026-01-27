"""Configuration models for the AAS-UNS Bridge."""

from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import BaseModel, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ValueConstraint(BaseModel):
    """Constraint definition for a specific semantic ID."""

    min: float | None = None
    """Minimum allowed value (inclusive)."""

    max: float | None = None
    """Maximum allowed value (inclusive)."""

    unit: str | None = None
    """Expected unit (UCUM code)."""

    pattern: str | None = None
    """Regex pattern for string values."""


class ValidationConfig(BaseModel):
    """Semantic validation configuration."""

    enabled: bool = False
    """Enable pre-publish validation."""

    enforce_semantic_ids: bool = True
    """Require semantic IDs on elements."""

    required_for_types: list[str] = Field(
        default_factory=lambda: ["Property", "Range", "Range.min", "Range.max"]
    )
    """AAS element types that require semantic IDs."""

    reject_invalid: bool = False
    """Reject invalid metrics (True) or warn only (False)."""

    value_constraints: dict[str, ValueConstraint] = Field(default_factory=dict)
    """Constraints keyed by semantic ID (IRDI/IRI)."""


class DriftConfig(BaseModel):
    """Schema drift detection configuration."""

    enabled: bool = False
    """Enable drift detection."""

    track_additions: bool = True
    """Detect new metrics."""

    track_removals: bool = True
    """Detect removed metrics."""

    track_type_changes: bool = True
    """Detect value_type/unit/semantic_id changes."""

    alert_topic_template: str = "UNS/Sys/DriftAlerts/{asset_id}"
    """Topic template for drift alerts. {asset_id} is replaced with sanitized asset ID."""


class LifecycleConfig(BaseModel):
    """Asset lifecycle tracking configuration."""

    enabled: bool = False
    """Enable lifecycle tracking."""

    stale_threshold_seconds: int = 300
    """Time after which an asset is considered stale."""

    clear_retained_on_offline: bool = False
    """Clear retained messages when asset goes offline."""

    publish_lifecycle_events: bool = True
    """Publish lifecycle events to UNS/Sys/Lifecycle/{asset_id}."""


class SemanticConfig(BaseModel):
    """Semantic enforcement configuration.

    Controls semantic QoS (sQoS) levels:
    - Level 0: Raw pass-through (no validation/enrichment)
    - Level 1: Validated (schema validation before publish)
    - Level 2: Enriched (validated + MQTT v5 User Properties)

    Note: Setting sqos_level automatically enables the appropriate features:
    - Level 1+ enables validation
    - Level 2 enables use_user_properties
    """

    sqos_level: Literal[0, 1, 2] = 0
    """Semantic QoS level (0=raw, 1=validated, 2=enriched)."""

    use_user_properties: bool = False
    """Include metadata in MQTT v5 User Properties (headers)."""

    payload_metadata_fallback: bool = True
    """Keep metadata in JSON payload for non-v5 subscribers."""

    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    """Validation settings."""

    drift: DriftConfig = Field(default_factory=DriftConfig)
    """Drift detection settings."""

    lifecycle: LifecycleConfig = Field(default_factory=LifecycleConfig)
    """Lifecycle tracking settings."""

    @model_validator(mode="after")
    def enforce_sqos_level(self) -> Self:
        """Enforce sQoS level settings.

        - Level 1+: Enable validation
        - Level 2: Enable User Properties
        """
        if self.sqos_level >= 1:
            # Enable validation at level 1+
            self.validation = ValidationConfig(
                enabled=True,
                enforce_semantic_ids=self.validation.enforce_semantic_ids,
                required_for_types=self.validation.required_for_types,
                reject_invalid=self.validation.reject_invalid,
                value_constraints=self.validation.value_constraints,
            )
        if self.sqos_level >= 2:
            # Enable User Properties at level 2
            self.use_user_properties = True
        return self


class MqttConfig(BaseModel):
    """MQTT broker connection configuration."""

    host: str = "localhost"
    port: int = 1883
    client_id: str = "aas-uns-bridge"
    username: str | None = None
    password: SecretStr | None = None
    use_tls: bool = False
    ca_cert: Path | None = None
    client_cert: Path | None = None
    client_key: Path | None = None
    keepalive: int = 60
    reconnect_delay_min: float = 1.0
    reconnect_delay_max: float = 120.0


class UnsConfig(BaseModel):
    """UNS retained topic publication configuration."""

    enabled: bool = True
    root_topic: str = ""
    qos: Literal[0, 1, 2] = 1
    retain: bool = True


class SparkplugConfig(BaseModel):
    """Sparkplug B publication configuration."""

    enabled: bool = True
    group_id: str = "AAS"
    edge_node_id: str = "Bridge"
    device_prefix: str = ""
    qos: Literal[0, 1, 2] = 0


class FileWatcherConfig(BaseModel):
    """AASX file watcher configuration."""

    enabled: bool = True
    watch_dir: Path = Path("./watch")
    patterns: list[str] = Field(default_factory=lambda: ["*.aasx", "*.json"])
    recursive: bool = True
    debounce_seconds: float = 2.0


class RepoClientConfig(BaseModel):
    """AAS Repository REST API client configuration."""

    enabled: bool = False
    base_url: str = "http://localhost:8080"
    poll_interval_seconds: float = 60.0
    timeout_seconds: float = 30.0
    auth_token: SecretStr | None = None


class StateConfig(BaseModel):
    """State persistence configuration."""

    db_path: Path = Path("./state/bridge.db")
    cache_births: bool = True
    deduplicate_publishes: bool = True


class ObservabilityConfig(BaseModel):
    """Observability configuration."""

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_format: Literal["json", "console"] = "console"
    metrics_port: int = 9090
    health_port: int = 8080


class BridgeConfig(BaseModel):
    """Root configuration for the AAS-UNS Bridge."""

    mqtt: MqttConfig = Field(default_factory=MqttConfig)
    uns: UnsConfig = Field(default_factory=UnsConfig)
    sparkplug: SparkplugConfig = Field(default_factory=SparkplugConfig)
    file_watcher: FileWatcherConfig = Field(default_factory=FileWatcherConfig)
    repo_client: RepoClientConfig = Field(default_factory=RepoClientConfig)
    state: StateConfig = Field(default_factory=StateConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
    semantic: SemanticConfig = Field(default_factory=SemanticConfig)
    preferred_language: str = "en"

    @classmethod
    def from_yaml(cls, path: Path) -> "BridgeConfig":
        """Load configuration from a YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data or {})


class BridgeSettings(BaseSettings):
    """Environment-based settings that override config file values."""

    model_config = SettingsConfigDict(
        env_prefix="AAS_BRIDGE_",
        env_nested_delimiter="__",
    )

    config_file: Path = Path("config/config.yaml")
    mappings_file: Path = Path("config/mappings.yaml")


def load_config(settings: BridgeSettings | None = None) -> BridgeConfig:
    """Load configuration from file, with environment overrides."""
    if settings is None:
        settings = BridgeSettings()

    if settings.config_file.exists():
        return BridgeConfig.from_yaml(settings.config_file)
    return BridgeConfig()
