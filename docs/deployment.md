# AAS-UNS Bridge Deployment Guide

This guide covers installation, configuration, and deployment of the AAS-UNS Bridge in production environments.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            AAS-UNS Bridge                                   │
│                                                                             │
│  ┌─────────────┐    ┌──────────────┐    ┌───────────────┐                   │
│  │ File Watcher│───▶│              │───▶│ UNS Publisher │──┐               │
│  │  (watchdog) │    │   Traversal  │    │ (retained)    │  │               │
│  └─────────────┘    │      &       │    └───────────────┘  │               │
│                     │   ISA-95     │                       │  ┌──────────┐ │
│  ┌─────────────┐    │   Mapping    │    ┌───────────────┐  ├─▶│   MQTT   │ │
│  │ Repo Client │───▶│              │───▶│  Sparkplug B  │──┘  │  Broker  │ │
│  │  (polling)  │    │              │    │  Publisher    │     └──────────┘ │
│  └─────────────┘    └──────────────┘    └───────────────┘                   │
│         │                  │                    │                           │
│         ▼                  ▼                    ▼                           │
│  ┌──────────────────────────────────────────────────────┐                   │
│  │                   SQLite State DB                    │                   │
│  │  • Sparkplug aliases  • Birth cache  • Last hashes   │                   │
│  └──────────────────────────────────────────────────────┘                   │
└─────────────────────────────────────────────────────────────────────────────┘

                           Data Flow
┌──────────┐    ┌──────────┐    ┌──────────────┐    ┌────────────┐
│   AAS    │    │  AASX/   │    │   Bridge     │    │    MQTT    │
│  Sources │───▶│   JSON   │───▶│  Processing  │───▶│   Topics   │
│          │    │  Files   │    │              │    │            │
└──────────┘    └──────────┘    └──────────────┘    └────────────┘
```

**Components:**

- **File Watcher**: Monitors directories for AASX/JSON files using watchdog
- **Repo Client**: Polls AAS repository APIs for shell/submodel updates
- **Traversal**: Recursively flattens submodel elements into metrics
- **ISA-95 Mapping**: Maps globalAssetId to equipment hierarchy
- **UNS Publisher**: Publishes JSON payloads with `retain=true`
- **Sparkplug B Publisher**: Publishes protobuf with NBIRTH/DBIRTH lifecycle
- **SQLite State DB**: Persists aliases, birth cache, and deduplication hashes

---

## Prerequisites

### System Requirements

| Component | Requirement |
|-----------|-------------|
| Python | 3.11 or later |
| OS | Linux (recommended), macOS, Windows |
| Memory | 512 MB minimum, 2 GB recommended |
| Disk | 100 MB for application, plus state DB growth |

### External Dependencies

- **MQTT Broker**: Mosquitto 2.0+, HiveMQ, EMQX, or compatible MQTTv5 broker
- **Network**: Outbound access to MQTT broker (ports 1883/8883)
- **Optional**: AAS Repository server for API polling mode

### Python Dependencies

Core dependencies (installed automatically):

- `paho-mqtt>=2.0` - MQTT client with v5 protocol support
- `basyx-python-sdk>=2.0` - AAS model handling
- `pydantic>=2.6` - Configuration validation
- `watchdog>=4.0` - File system monitoring
- `prometheus-client>=0.20` - Metrics exposition
- `structlog>=24.0` - Structured logging

---

## Installation

### From PyPI

```bash
# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate

# Install the package
pip install aas-uns-bridge

# Verify installation
aas-uns-bridge --version
```

### From Source

```bash
# Clone repository
git clone https://github.com/hadijannat/aas-uns-bridge.git
cd aas-uns-bridge

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install in development mode with dev dependencies
pip install -e ".[dev]"

# Optional: Generate Sparkplug protobuf bindings
make proto

# Verify installation
aas-uns-bridge --version
```

### Verifying Installation

```bash
# Check CLI is available
aas-uns-bridge --help

# Validate a configuration file
aas-uns-bridge validate --config config/config.example.yaml
```

---

## Configuration

The bridge uses two YAML configuration files:

### Main Configuration (config.yaml)

Copy the example and customize for your environment:

```bash
cp config/config.example.yaml config/config.yaml
```

#### MQTT Connection

```yaml
mqtt:
  host: mqtt.example.com
  port: 8883                    # 1883 for plain, 8883 for TLS
  client_id: aas-uns-bridge-prod
  username: bridge_user
  password: ${MQTT_PASSWORD}    # Environment variable substitution
  use_tls: true
  ca_cert: /etc/ssl/certs/ca.crt
  client_cert: /etc/ssl/certs/bridge.crt
  client_key: /etc/ssl/private/bridge.key
  keepalive: 60
  reconnect_delay_min: 1.0
  reconnect_delay_max: 120.0
```

#### UNS Publisher

```yaml
uns:
  enabled: true
  root_topic: ""                # Optional prefix (e.g., "uns")
  qos: 1                        # 0, 1, or 2
  retain: true                  # Enable retained messages
```

#### Sparkplug B Publisher

```yaml
sparkplug:
  enabled: true
  group_id: AAS                 # Sparkplug group identifier
  edge_node_id: Bridge          # Edge node identifier
  device_prefix: ""             # Optional device ID prefix
  qos: 0
```

#### File Watcher

```yaml
file_watcher:
  enabled: true
  watch_dir: /data/aas          # Directory to monitor
  patterns:
    - "*.aasx"
    - "*.json"
  recursive: true               # Watch subdirectories
  debounce_seconds: 2.0         # Delay before processing
```

#### Repository Client (Alternative to File Watcher)

```yaml
repo_client:
  enabled: false
  base_url: http://aas-server:8080
  poll_interval_seconds: 60.0
  timeout_seconds: 30.0
  auth_token: ${AAS_API_TOKEN}
```

#### State Persistence

```yaml
state:
  db_path: /var/lib/aas-uns-bridge/bridge.db
  cache_births: true            # Cache Sparkplug births
  deduplicate_publishes: true   # Skip unchanged values
```

#### Observability

```yaml
observability:
  log_level: INFO               # DEBUG, INFO, WARNING, ERROR
  log_format: json              # "console" or "json"
  metrics_port: 9090            # Prometheus metrics
  health_port: 8080             # Health check endpoint
```

### ISA-95 Mappings (mappings.yaml)

Maps AAS globalAssetId to ISA-95 equipment hierarchy for topic construction:

```bash
cp config/mappings.example.yaml config/mappings.yaml
```

```yaml
# Default mapping when no specific match is found
default:
  enterprise: DefaultEnterprise
  site: DefaultSite
  area: DefaultArea
  line: DefaultLine

# Specific asset mappings by globalAssetId
assets:
  "https://example.com/aas/robot-arm-001":
    enterprise: AcmeCorp
    site: PlantA
    area: Packaging
    line: Line1
    asset: RobotArm001

  "https://example.com/aas/temp-sensor-42":
    enterprise: AcmeCorp
    site: PlantA
    area: Assembly
    line: Line2
    asset: TempSensor42

# Pattern-based matching (glob-style, first match wins)
patterns:
  - pattern: "https://example.com/aas/robot-*"
    enterprise: AcmeCorp
    site: PlantA
    area: Robotics
    line: Assembly

  - pattern: "https://example.com/aas/sensor-*"
    enterprise: AcmeCorp
    site: PlantA
    area: Sensors
```

---

## TLS Setup

### Generate CA and Certificates

For production, use certificates from your organization's PKI. For testing:

```bash
# Create certificate directory
mkdir -p certs && cd certs

# Generate CA key and certificate
openssl genrsa -out ca.key 4096
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 \
    -out ca.crt -subj "/CN=AAS-UNS-Bridge-CA"

# Generate broker key and certificate
openssl genrsa -out broker.key 2048
openssl req -new -key broker.key -out broker.csr \
    -subj "/CN=mqtt.example.com"
openssl x509 -req -in broker.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out broker.crt -days 365 -sha256

# Generate bridge client key and certificate
openssl genrsa -out bridge.key 2048
openssl req -new -key bridge.key -out bridge.csr \
    -subj "/CN=aas-uns-bridge"
openssl x509 -req -in bridge.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out bridge.crt -days 365 -sha256

# Set secure permissions
chmod 600 *.key
chmod 644 *.crt
```

### Configure Mosquitto Broker

Add to `/etc/mosquitto/mosquitto.conf`:

```
listener 8883
cafile /etc/mosquitto/certs/ca.crt
certfile /etc/mosquitto/certs/broker.crt
keyfile /etc/mosquitto/certs/broker.key
require_certificate true
use_identity_as_username true
```

Restart Mosquitto:

```bash
sudo systemctl restart mosquitto
```

### Configure Bridge for TLS

Update `config.yaml`:

```yaml
mqtt:
  host: mqtt.example.com
  port: 8883
  use_tls: true
  ca_cert: /etc/ssl/certs/ca.crt
  client_cert: /etc/ssl/certs/bridge.crt
  client_key: /etc/ssl/private/bridge.key
```

---

## Running as Service

### Systemd Unit File

Create `/etc/systemd/system/aas-uns-bridge.service`:

```ini
[Unit]
Description=AAS-UNS Bridge Service
Documentation=https://github.com/hadijannat/aas-uns-bridge
After=network-online.target mosquitto.service
Wants=network-online.target

[Service]
Type=simple
User=aas-bridge
Group=aas-bridge
WorkingDirectory=/opt/aas-uns-bridge

# Environment
Environment=PYTHONUNBUFFERED=1
EnvironmentFile=-/etc/aas-uns-bridge/env

# Command
ExecStart=/opt/aas-uns-bridge/venv/bin/aas-uns-bridge run \
    --config /etc/aas-uns-bridge/config.yaml \
    --mappings /etc/aas-uns-bridge/mappings.yaml

# Restart policy
Restart=always
RestartSec=10
StartLimitIntervalSec=300
StartLimitBurst=5

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths=/var/lib/aas-uns-bridge /data/aas

# Resource limits
MemoryMax=2G
CPUQuota=200%

[Install]
WantedBy=multi-user.target
```

### Enable and Start

```bash
# Create service user
sudo useradd -r -s /bin/false aas-bridge

# Create directories
sudo mkdir -p /etc/aas-uns-bridge /var/lib/aas-uns-bridge /data/aas
sudo chown aas-bridge:aas-bridge /var/lib/aas-uns-bridge /data/aas

# Copy configuration
sudo cp config/config.yaml /etc/aas-uns-bridge/
sudo cp config/mappings.yaml /etc/aas-uns-bridge/
sudo chmod 640 /etc/aas-uns-bridge/*.yaml

# Reload systemd and enable service
sudo systemctl daemon-reload
sudo systemctl enable aas-uns-bridge
sudo systemctl start aas-uns-bridge

# Check status
sudo systemctl status aas-uns-bridge
```

---

## Kubernetes Deployment

### Deployment Manifest

Create `kubernetes/deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: aas-uns-bridge
  labels:
    app: aas-uns-bridge
spec:
  replicas: 1
  selector:
    matchLabels:
      app: aas-uns-bridge
  template:
    metadata:
      labels:
        app: aas-uns-bridge
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
    spec:
      containers:
        - name: bridge
          image: ghcr.io/hadijannat/aas-uns-bridge:latest
          ports:
            - name: metrics
              containerPort: 9090
              protocol: TCP
            - name: health
              containerPort: 8080
              protocol: TCP
          volumeMounts:
            - name: config
              mountPath: /etc/aas-uns-bridge
              readOnly: true
            - name: state
              mountPath: /var/lib/aas-uns-bridge
            - name: aas-files
              mountPath: /data/aas
            - name: tls-certs
              mountPath: /etc/ssl/bridge
              readOnly: true
          env:
            - name: MQTT_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: aas-uns-bridge-secrets
                  key: mqtt-password
          livenessProbe:
            httpGet:
              path: /health
              port: health
            initialDelaySeconds: 10
            periodSeconds: 30
            timeoutSeconds: 5
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /ready
              port: health
            initialDelaySeconds: 5
            periodSeconds: 10
            timeoutSeconds: 3
            failureThreshold: 3
          resources:
            requests:
              memory: "256Mi"
              cpu: "100m"
            limits:
              memory: "1Gi"
              cpu: "500m"
          securityContext:
            runAsNonRoot: true
            runAsUser: 1000
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
      volumes:
        - name: config
          configMap:
            name: aas-uns-bridge-config
        - name: state
          persistentVolumeClaim:
            claimName: aas-uns-bridge-state
        - name: aas-files
          persistentVolumeClaim:
            claimName: aas-files
        - name: tls-certs
          secret:
            secretName: aas-uns-bridge-tls
---
apiVersion: v1
kind: Service
metadata:
  name: aas-uns-bridge
spec:
  selector:
    app: aas-uns-bridge
  ports:
    - name: metrics
      port: 9090
      targetPort: metrics
    - name: health
      port: 8080
      targetPort: health
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: aas-uns-bridge-config
data:
  config.yaml: |
    mqtt:
      host: mosquitto.mqtt.svc.cluster.local
      port: 8883
      client_id: aas-uns-bridge
      use_tls: true
      ca_cert: /etc/ssl/bridge/ca.crt
      client_cert: /etc/ssl/bridge/tls.crt
      client_key: /etc/ssl/bridge/tls.key
    uns:
      enabled: true
      qos: 1
      retain: true
    sparkplug:
      enabled: true
      group_id: AAS
      edge_node_id: Bridge
    file_watcher:
      enabled: true
      watch_dir: /data/aas
      patterns: ["*.aasx", "*.json"]
    state:
      db_path: /var/lib/aas-uns-bridge/bridge.db
    observability:
      log_level: INFO
      log_format: json
      metrics_port: 9090
      health_port: 8080
  mappings.yaml: |
    default:
      enterprise: DefaultEnterprise
      site: DefaultSite
      area: DefaultArea
      line: DefaultLine
    assets: {}
    patterns: []
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: aas-uns-bridge-state
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
```

### Deploy to Kubernetes

```bash
# Create namespace
kubectl create namespace aas-bridge

# Create secrets
kubectl create secret generic aas-uns-bridge-secrets \
    --namespace aas-bridge \
    --from-literal=mqtt-password=your-password

kubectl create secret generic aas-uns-bridge-tls \
    --namespace aas-bridge \
    --from-file=ca.crt=certs/ca.crt \
    --from-file=tls.crt=certs/bridge.crt \
    --from-file=tls.key=certs/bridge.key

# Deploy
kubectl apply -f kubernetes/deployment.yaml -n aas-bridge

# Check status
kubectl get pods -n aas-bridge
kubectl logs -f deployment/aas-uns-bridge -n aas-bridge
```

---

## Capacity Planning

| Tier | AAS Files | Metrics | Publish Rate | Memory | CPU | State DB |
|------|-----------|---------|--------------|--------|-----|----------|
| **Small** | < 50 | < 1,000 | 10/sec | 256 MB | 0.25 cores | < 50 MB |
| **Medium** | 50-500 | 1,000-10,000 | 100/sec | 512 MB | 0.5 cores | 50-200 MB |
| **Large** | 500-5,000 | 10,000-100,000 | 1,000/sec | 2 GB | 2 cores | 200 MB-1 GB |
| **Enterprise** | 5,000+ | 100,000+ | 5,000+/sec | 4+ GB | 4+ cores | 1+ GB |

### Scaling Considerations

- **Memory**: Increases with number of active metrics and Sparkplug aliases
- **CPU**: Scales with publish rate and protobuf serialization overhead
- **State DB**: Grows with unique metric paths; consider periodic compaction
- **Network**: Estimate 200-500 bytes per UNS message, 50-150 bytes per Sparkplug metric

---

## Network Requirements

| Port | Protocol | Direction | Purpose |
|------|----------|-----------|---------|
| **1883** | TCP | Outbound | MQTT (plain) |
| **8883** | TCP | Outbound | MQTT (TLS) |
| **9090** | TCP | Inbound | Prometheus metrics |
| **8080** | TCP | Inbound | Health check endpoints |

### Firewall Rules

```bash
# Allow MQTT outbound (adjust destination IP)
sudo ufw allow out 8883/tcp comment "MQTT TLS"

# Allow metrics/health inbound from monitoring
sudo ufw allow from 10.0.0.0/8 to any port 9090 proto tcp comment "Prometheus"
sudo ufw allow from 10.0.0.0/8 to any port 8080 proto tcp comment "Health"
```

---

## Verification

### Check Service Status

```bash
# Systemd
sudo systemctl status aas-uns-bridge

# Kubernetes
kubectl get pods -l app=aas-uns-bridge -n aas-bridge
```

### View Logs

```bash
# Systemd
sudo journalctl -u aas-uns-bridge -f

# Kubernetes
kubectl logs -f deployment/aas-uns-bridge -n aas-bridge
```

### Query Metrics

```bash
# Prometheus metrics endpoint
curl -s http://localhost:9090/metrics | grep aas_

# Example metrics
# aas_uns_bridge_messages_published_total{publisher="uns"}
# aas_uns_bridge_messages_published_total{publisher="sparkplug"}
# aas_uns_bridge_files_processed_total
# aas_uns_bridge_mqtt_connection_status
```

### Health Check

```bash
# Liveness probe
curl -s http://localhost:8080/health
# {"status": "healthy", "timestamp": "2024-01-15T10:30:00Z"}

# Readiness probe (includes dependency checks)
curl -s http://localhost:8080/ready
# {"status": "ready", "mqtt": "connected", "state_db": "ok"}
```

### Validate MQTT Publishing

```bash
# Subscribe to UNS topics
mosquitto_sub -h localhost -t "AcmeCorp/#" -v

# Subscribe to Sparkplug topics
mosquitto_sub -h localhost -t "spBv1.0/AAS/#" -v
```

---

## Troubleshooting

### Common Issues

**Bridge fails to connect to MQTT**
- Verify broker hostname and port
- Check TLS certificate paths and permissions
- Confirm firewall allows outbound connections

**No messages published**
- Verify AAS files are in the watch directory
- Check file patterns match your files
- Review logs for parsing errors

**High memory usage**
- Enable `deduplicate_publishes` to reduce state
- Increase `debounce_seconds` to batch file changes
- Consider splitting large deployments

**State database locked**
- Ensure only one bridge instance accesses the database
- Check disk space and permissions

### Debug Mode

Enable debug logging for troubleshooting:

```yaml
observability:
  log_level: DEBUG
  log_format: console
```

Or via environment:

```bash
export LOG_LEVEL=DEBUG
aas-uns-bridge run --config config.yaml
```
