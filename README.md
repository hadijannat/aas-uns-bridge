# AAS-UNS Bridge

A daemonized integration service that ingests Asset Administration Shell (AAS) content and publishes it to both UNS retained topics and Sparkplug B for dual-plane discovery.

## Features

- **Dual Publication**: Publishes to both UNS retained topics and Sparkplug B simultaneously
- **Multiple AAS Sources**: AASX file watcher and AAS Repository REST API polling
- **ISA-95 Mapping**: Configurable hierarchy mapping for proper UNS topic structure
- **Change Detection**: Hash-based deduplication to avoid redundant publishes
- **Sparkplug Compliance**: Full NBIRTH/DBIRTH/NDEATH/DDEATH lifecycle support
- **Observability**: Prometheus metrics, structured logging, health endpoints

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        AAS-UNS Bridge                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────┐    ┌────────────────┐    ┌───────────────────┐   │
│  │ File Watcher │───▶│   AAS Loader   │───▶│    Traversal      │   │
│  │  (watchdog)  │    │ (BaSyx SDK)    │    │  (flatten SMEs)   │   │
│  └──────────────┘    └────────────────┘    └─────────┬─────────┘   │
│                                                      │             │
│  ┌──────────────┐                                    │             │
│  │  REST Poller │───────────────────────────────────▶│             │
│  │  (httpx)     │                                    │             │
│  └──────────────┘                                    ▼             │
│                                            ┌─────────────────┐     │
│                                            │  ISA-95 Mapper  │     │
│                                            │ (topic builder) │     │
│                                            └────────┬────────┘     │
│                                                     │              │
│                          ┌──────────────────────────┼──────────────┤
│                          │                          │              │
│                          ▼                          ▼              │
│               ┌────────────────────┐    ┌────────────────────┐     │
│               │  UNS Publisher     │    │ Sparkplug Publisher│     │
│               │  (retained JSON)   │    │ (protobuf births)  │     │
│               └─────────┬──────────┘    └─────────┬──────────┘     │
│                         │                         │                │
│                         └────────────┬────────────┘                │
│                                      │                             │
│                                      ▼                             │
│                              ┌──────────────┐                      │
│                              │ MQTT Client  │                      │
│                              │ (paho v2)    │                      │
│                              └──────┬───────┘                      │
└─────────────────────────────────────┼───────────────────────────────┘
                                      │
                                      ▼
                              ┌──────────────┐
                              │ MQTT Broker  │
                              └──────────────┘
```

## Quick Start

### Using Docker Compose

```bash
# Clone the repository
git clone https://github.com/example/aas-uns-bridge.git
cd aas-uns-bridge

# Copy and edit configuration
cp config/config.example.yaml docker/config/config.yaml
cp config/mappings.example.yaml docker/config/mappings.yaml

# Start the stack
cd docker
docker-compose up -d

# Copy AASX files to the watch directory
cp /path/to/your/*.aasx watch/

# View logs
docker-compose logs -f bridge
```

### Using pip

```bash
# Install the package
pip install aas-uns-bridge

# Copy configuration files
cp config/config.example.yaml config/config.yaml
cp config/mappings.example.yaml config/mappings.yaml

# Edit configuration for your environment
vi config/config.yaml

# Run the bridge
aas-uns-bridge run --config config/config.yaml --mappings config/mappings.yaml
```

## Configuration

### config.yaml

```yaml
mqtt:
  host: localhost
  port: 1883
  client_id: aas-uns-bridge
  # username: bridge_user
  # password: secret

uns:
  enabled: true
  root_topic: ""  # Optional prefix
  qos: 1
  retain: true

sparkplug:
  enabled: true
  group_id: AAS
  edge_node_id: Bridge

file_watcher:
  enabled: true
  watch_dir: ./watch
  patterns:
    - "*.aasx"
    - "*.json"

observability:
  log_level: INFO
  log_format: console  # or "json"
  metrics_port: 9090
  health_port: 8080
```

### mappings.yaml

Maps AAS globalAssetId to ISA-95 hierarchy:

```yaml
default:
  enterprise: DefaultCorp
  site: DefaultSite

assets:
  "https://example.com/aas/robot-001":
    enterprise: AcmeCorp
    site: PlantA
    area: Assembly
    line: Line1
    asset: Robot001

patterns:
  - pattern: "https://example.com/aas/sensor-*"
    enterprise: AcmeCorp
    site: PlantA
    area: Sensors
```

## Topic Structure

### UNS Retained Topics

```
{enterprise}/{site}/{area}/{line}/{asset}/context/{submodel}/{element_path}
```

Example:
```
AcmeCorp/PlantA/Assembly/Line1/Robot001/context/TechnicalData/GeneralInfo/ManufacturerName
```

Payload:
```json
{
  "value": "Acme Robotics",
  "timestamp": 1706369400000,
  "semanticId": "0173-1#02-AAO677#002",
  "unit": null,
  "source": "aas-uns-bridge",
  "aasUri": "/watch/robot-001.aasx"
}
```

### Sparkplug B Topics

```
spBv1.0/{group_id}/NBIRTH/{edge_node_id}
spBv1.0/{group_id}/DBIRTH/{edge_node_id}/{device_id}
spBv1.0/{group_id}/DDATA/{edge_node_id}/{device_id}
spBv1.0/{group_id}/NDEATH/{edge_node_id}
```

## CLI Commands

```bash
# Run the daemon
aas-uns-bridge run [--config PATH] [--mappings PATH]

# Validate configuration
aas-uns-bridge validate [--config PATH]

# Check status of running instance
aas-uns-bridge status

# Show version
aas-uns-bridge version
```

## Health Endpoints

- `GET /health` - Full health status (JSON)
- `GET /ready` - Kubernetes readiness probe
- `GET /live` - Kubernetes liveness probe

## Prometheus Metrics

Available at `http://localhost:9090/metrics`:

- `aas_bridge_aas_loaded_total` - AAS files loaded
- `aas_bridge_metrics_flattened_total` - Metrics extracted
- `aas_bridge_uns_published_total` - UNS messages published
- `aas_bridge_sparkplug_births_total` - Sparkplug births sent
- `aas_bridge_mqtt_connected` - Connection status gauge

## Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install with dev dependencies
pip install -e ".[dev]"

# Generate protobuf bindings
protoc --python_out=src/aas_uns_bridge/proto proto/sparkplug_b.proto

# Run tests
pytest

# Run linting
ruff check src tests
mypy src
```

## License

MIT License - see LICENSE file for details.
