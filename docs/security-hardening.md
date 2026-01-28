# Security Hardening Guide

This guide provides security hardening recommendations for deploying the AAS-UNS Bridge in production environments.

## Pre-Deployment Checklist

Before deploying to production, verify the following security controls are in place:

- [ ] TLS enabled for all MQTT connections
- [ ] mTLS configured for broker authentication (if available)
- [ ] Passwords and secrets stored in environment variables, not config files
- [ ] File permissions restricted on configuration and state directories
- [ ] Dedicated service account created with minimal privileges
- [ ] Network segmentation configured
- [ ] Monitoring and alerting enabled
- [ ] Backup and recovery procedures documented

## Network Security

### Private Network Deployment

Deploy the bridge within a private network segment that is isolated from public internet access:

```
┌─────────────────────────────────────────────────────────┐
│                    Private Network                       │
│  ┌──────────────┐    TLS    ┌──────────────────────┐   │
│  │ AAS-UNS      │◄─────────►│ MQTT Broker          │   │
│  │ Bridge       │           │ (Mosquitto/HiveMQ)   │   │
│  └──────────────┘           └──────────────────────┘   │
│         │                            │                  │
│         ▼                            ▼                  │
│  ┌──────────────┐           ┌──────────────────────┐   │
│  │ AAS File     │           │ SCADA / UNS          │   │
│  │ Storage      │           │ Consumers            │   │
│  └──────────────┘           └──────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Firewall Rules

Configure firewall rules to restrict network access:

```bash
# Allow outbound MQTT to broker only
iptables -A OUTPUT -p tcp --dport 8883 -d <broker-ip> -j ACCEPT
iptables -A OUTPUT -p tcp --dport 8883 -j DROP

# Allow inbound only from management network
iptables -A INPUT -s <management-cidr> -p tcp --dport 22 -j ACCEPT
iptables -A INPUT -p tcp --dport 22 -j DROP
```

### VPN or mTLS for Broker Communication

For cross-network deployments, use VPN tunnels or mTLS:

```yaml
# config.yaml - mTLS configuration
mqtt:
  host: broker.example.com
  port: 8883
  tls:
    enabled: true
    ca_certs: /etc/aas-uns-bridge/certs/ca.crt
    certfile: /etc/aas-uns-bridge/certs/client.crt
    keyfile: /etc/aas-uns-bridge/certs/client.key
    cert_reqs: CERT_REQUIRED
```

## File System Security

### Read-Only AAS Mount

Mount AAS file directories as read-only to prevent tampering:

```bash
# /etc/fstab entry for read-only mount
/dev/sdb1 /var/lib/aas-uns-bridge/aas ext4 ro,noexec,nosuid 0 0
```

For container deployments:

```yaml
# docker-compose.yaml
volumes:
  - ./aas-files:/app/aas:ro
```

### Restricted State Directory

The state directory contains SQLite databases with Sparkplug aliases. Restrict access:

```bash
# Create dedicated directory with restricted permissions
mkdir -p /var/lib/aas-uns-bridge/state
chown aas-bridge:aas-bridge /var/lib/aas-uns-bridge/state
chmod 700 /var/lib/aas-uns-bridge/state
```

### Umask Configuration

Set restrictive umask for the service:

```bash
# In systemd service file
[Service]
UMask=0077
```

Or in the shell profile for the service account:

```bash
# ~/.bashrc for aas-bridge user
umask 0077
```

## Monitoring

### Audit Logging

Enable comprehensive audit logging to track all operations:

```yaml
# config.yaml
observability:
  logging:
    level: INFO
    format: json
    audit:
      enabled: true
      log_file: /var/log/aas-uns-bridge/audit.log
```

### Error Metrics

Configure Prometheus metrics export for error tracking:

```yaml
# config.yaml
observability:
  metrics:
    enabled: true
    port: 9090
    path: /metrics
```

Key metrics to monitor:

| Metric | Description | Alert Threshold |
| ------ | ----------- | --------------- |
| `aas_bridge_mqtt_errors_total` | MQTT connection/publish errors | > 0 in 5 min |
| `aas_bridge_validation_errors_total` | AAS validation failures | > 10 in 1 hour |
| `aas_bridge_file_watch_errors_total` | File system errors | > 0 in 5 min |

### Alerting

Configure alerts for security-relevant events:

```yaml
# alertmanager/rules.yaml (Prometheus Alertmanager)
groups:
  - name: aas-bridge-security
    rules:
      - alert: HighErrorRate
        expr: rate(aas_bridge_mqtt_errors_total[5m]) > 0.1
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "High MQTT error rate detected"

      - alert: ValidationFailures
        expr: increase(aas_bridge_validation_errors_total[1h]) > 10
        labels:
          severity: warning
        annotations:
          summary: "Multiple AAS validation failures"

      - alert: UnexpectedRestart
        expr: changes(process_start_time_seconds{job="aas-bridge"}[1h]) > 2
        labels:
          severity: critical
        annotations:
          summary: "Bridge restarting unexpectedly"
```

### Log Review

Implement regular log review procedures:

1. **Daily**: Review error logs for anomalies
2. **Weekly**: Analyze access patterns and failed operations
3. **Monthly**: Audit configuration changes and access permissions

Use log aggregation tools (ELK Stack, Loki, Splunk) for centralized analysis:

```bash
# Example: Search for authentication failures
grep -i "auth" /var/log/aas-uns-bridge/audit.log | grep -i "fail"
```

## Configuration Hardening

### Minimal Permissions

Disable unused features to reduce attack surface:

```yaml
# config.yaml - Disable unused publishers
publishers:
  uns:
    enabled: true
  sparkplug:
    enabled: false  # Disable if not needed

# Disable REST API polling if using file watcher only
ingestion:
  rest_api:
    enabled: false
  file_watcher:
    enabled: true
```

### Strict Validation

Enable strict validation mode to reject malformed input:

```yaml
# config.yaml
validation:
  enabled: true
  strict_mode: true
  reject_unknown_elements: true
  require_schema_compliance: true
```

### Write-Back Protection

If write-back functionality is enabled, restrict which paths can be written:

```yaml
# config.yaml
write_back:
  enabled: false  # Disable unless absolutely required

  # If enabled, restrict to specific paths
  allowed_patterns:
    - "/Enterprise/Site/Area/Line/*/setpoints/*"

  denied_patterns:
    - "/Enterprise/Site/Area/Line/*/config/*"
    - "/Enterprise/Site/Area/Line/*/security/*"
```

## Runtime Hardening

### Container Security

Run containers with security best practices:

```dockerfile
# Dockerfile
FROM python:3.11-slim

# Create non-root user
RUN groupadd -r aas-bridge && useradd -r -g aas-bridge aas-bridge

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY --chown=aas-bridge:aas-bridge . .

# Switch to non-root user
USER aas-bridge

# Run application
CMD ["python", "-m", "aas_uns_bridge"]
```

Docker Compose security configuration:

```yaml
# docker-compose.yaml
services:
  aas-bridge:
    image: aas-uns-bridge:latest
    user: "1000:1000"
    read_only: true
    security_opt:
      - no-new-privileges:true
    cap_drop:
      - ALL
    tmpfs:
      - /tmp
    volumes:
      - ./config:/app/config:ro
      - ./aas-files:/app/aas:ro
      - state-data:/app/state
```

### Resource Limits

Apply resource limits to prevent resource exhaustion attacks:

```yaml
# Kubernetes deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: aas-uns-bridge
spec:
  template:
    spec:
      containers:
        - name: aas-bridge
          image: aas-uns-bridge:latest
          resources:
            requests:
              memory: "128Mi"
              cpu: "100m"
            limits:
              memory: "512Mi"
              cpu: "500m"
          securityContext:
            runAsNonRoot: true
            runAsUser: 1000
            runAsGroup: 1000
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
            capabilities:
              drop:
                - ALL
```

Docker resource limits:

```yaml
# docker-compose.yaml
services:
  aas-bridge:
    deploy:
      resources:
        limits:
          cpus: '0.5'
          memory: 512M
        reservations:
          cpus: '0.1'
          memory: 128M
```

## Audit Logging

### Enable Comprehensive Logging

Configure audit logging to capture security-relevant events:

```yaml
# config.yaml
observability:
  logging:
    level: INFO
    format: json
    handlers:
      - type: file
        path: /var/log/aas-uns-bridge/app.log
        max_bytes: 10485760  # 10 MB
        backup_count: 5
      - type: file
        path: /var/log/aas-uns-bridge/audit.log
        max_bytes: 52428800  # 50 MB
        backup_count: 10
        level: INFO
        filter: audit
```

### Audit Log Events

The following events are logged for audit purposes:

| Event Type | Description |
| ---------- | ----------- |
| `STARTUP` | Bridge startup with configuration summary |
| `SHUTDOWN` | Bridge shutdown (graceful or forced) |
| `CONNECT` | MQTT broker connection established |
| `DISCONNECT` | MQTT broker disconnection |
| `PUBLISH` | Message published to topic |
| `FILE_LOAD` | AAS file loaded and processed |
| `FILE_ERROR` | Error processing AAS file |
| `CONFIG_CHANGE` | Configuration reload detected |
| `VALIDATION_ERROR` | Input validation failure |

### Log Rotation

Configure logrotate for audit logs:

```bash
# /etc/logrotate.d/aas-uns-bridge
/var/log/aas-uns-bridge/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 0640 aas-bridge aas-bridge
    sharedscripts
    postrotate
        systemctl reload aas-uns-bridge 2>/dev/null || true
    endscript
}
```

## Additional Recommendations

### Secrets Management

Use a secrets manager for production deployments:

```bash
# Using environment variables from secrets manager
export MQTT_PASSWORD=$(vault kv get -field=password secret/aas-bridge/mqtt)
export TLS_KEY_PASSWORD=$(vault kv get -field=key_password secret/aas-bridge/tls)

# Run bridge with injected secrets
aas-uns-bridge --config /etc/aas-uns-bridge/config.yaml
```

### Regular Updates

Keep the bridge and dependencies updated:

```bash
# Check for updates
pip list --outdated

# Update to latest patch version
pip install --upgrade aas-uns-bridge~=0.1.0
```

### Security Scanning

Regularly scan for vulnerabilities:

```bash
# Scan Python dependencies
pip-audit

# Scan container image
trivy image aas-uns-bridge:latest

# Scan configuration for secrets
gitleaks detect --source .
```
