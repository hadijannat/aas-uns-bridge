"""Configuration models for the AAS-UNS Bridge."""

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


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
