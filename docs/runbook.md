# Operator's Runbook

This runbook provides operational guidance for maintaining and troubleshooting the AAS-UNS Bridge in production environments.

## Table of Contents

1. [Overview](#overview)
2. [Daily Operations](#daily-operations)
3. [Troubleshooting](#troubleshooting)
4. [Backup and Recovery](#backup-and-recovery)
5. [Metrics Reference](#metrics-reference)
6. [Escalation](#escalation)

---

## Overview

The AAS-UNS Bridge is a daemonized service that ingests Asset Administration Shell (AAS) content and publishes to two planes simultaneously:

- **UNS Retained Topics**: JSON payloads with `retain=true` for late-subscriber discovery
- **Sparkplug B**: Protobuf payloads with NBIRTH/DBIRTH lifecycle for SCADA integration

Key operational endpoints:
- **Health endpoint**: `http://localhost:8080/health` (configurable via `observability.health_port`)
- **Metrics endpoint**: `http://localhost:9090/metrics` (configurable via `observability.metrics_port`)
- **Kubernetes probes**: `/live` (liveness), `/ready` (readiness)

---

## Daily Operations

### Health Check Commands

**Check bridge status via CLI:**
```bash
aas-uns-bridge status
```

Expected output when healthy:
```
Status: healthy
MQTT connected: True
```

**Query health endpoint directly:**
```bash
curl -s http://localhost:8080/health | jq
```

Expected healthy response:
```json
{
  "status": "healthy",
  "timestamp": 1706443200000,
  "mqtt_connected": true,
  "sparkplug_online": true,
  "active_devices": 5,
  "sparkplug_births": 6,
  "uns_published": 150
}
```

**Check readiness (for load balancers/K8s):**
```bash
curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/ready
# Returns 200 when MQTT connected, 503 otherwise
```

**Check liveness:**
```bash
curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/live
# Returns 200 if process is running
```

**Query Prometheus metrics:**
```bash
curl -s http://localhost:9090/metrics | grep aas_bridge
```

### Log Analysis

The bridge uses structured logging. Key patterns to monitor:

**Check for errors:**
```bash
# Recent errors (JSON log format)
journalctl -u aas-uns-bridge --since "1 hour ago" | grep '"level":"ERROR"'

# Recent errors (console log format)
journalctl -u aas-uns-bridge --since "1 hour ago" | grep -E "ERROR|Error|error"
```

**Check MQTT connection issues:**
```bash
journalctl -u aas-uns-bridge | grep -E "Disconnected|Connection failed|Reconnect"
```

**Check AAS file processing:**
```bash
journalctl -u aas-uns-bridge | grep -E "Processing AAS|Failed to process|File unchanged"
```

**Check publish activity:**
```bash
journalctl -u aas-uns-bridge | grep -E "Published|publish_nbirth|publish_dbirth"
```

**Check validation errors (if semantic validation enabled):**
```bash
journalctl -u aas-uns-bridge | grep -E "Validation found|validation error"
```

---

## Troubleshooting

### Scenario 1: Bridge Not Starting

**Symptoms:**
- Service fails to start
- `aas-uns-bridge status` returns "Bridge is not running"
- systemd shows service in failed state

**Diagnostic steps:**

1. **Validate configuration:**
   ```bash
   aas-uns-bridge validate --config /path/to/config.yaml --mappings /path/to/mappings.yaml
   ```

   Expected output:
   ```
   Configuration valid: /path/to/config.yaml
     MQTT: localhost:1883
     UNS enabled: True
     Sparkplug enabled: True
     File watcher: True
     Repo client: False
   ```

2. **Check file permissions:**
   ```bash
   # Check config files are readable
   ls -la /path/to/config.yaml /path/to/mappings.yaml

   # Check state directory is writable
   ls -la ./state/

   # Check watch directory exists and is readable
   ls -la ./watch/
   ```

3. **Check systemd status (if using systemd):**
   ```bash
   systemctl status aas-uns-bridge
   journalctl -u aas-uns-bridge -n 50 --no-pager
   ```

4. **Test MQTT connectivity:**
   ```bash
   # Using mosquitto_pub (requires mosquitto-clients)
   mosquitto_pub -h localhost -p 1883 -t test/connection -m "test" -d

   # With TLS
   mosquitto_pub -h broker.example.com -p 8883 \
     --cafile /path/to/ca.crt \
     --cert /path/to/client.crt \
     --key /path/to/client.key \
     -t test/connection -m "test" -d
   ```

**Resolution:**
- Fix any configuration errors reported by `validate`
- Ensure state directory has write permissions: `mkdir -p ./state && chmod 755 ./state`
- Ensure watch directory exists: `mkdir -p ./watch`
- Verify MQTT broker is reachable

---

### Scenario 2: MQTT Connection Failing

**Symptoms:**
- `aas_bridge_mqtt_connected` metric is 0
- Health endpoint shows `"mqtt_connected": false`
- Logs show repeated "Disconnected" or "Reconnect attempt failed"

**Diagnostic steps:**

1. **Verify broker address and port:**
   ```bash
   # Check if broker is reachable
   nc -zv localhost 1883

   # For TLS connections
   nc -zv broker.example.com 8883
   ```

2. **Check TLS certificates (if using TLS):**
   ```bash
   # Verify CA certificate
   openssl x509 -in /path/to/ca.crt -text -noout | head -20

   # Verify client certificate validity
   openssl x509 -in /path/to/client.crt -text -noout | grep -A2 "Validity"

   # Verify key matches certificate
   openssl x509 -noout -modulus -in /path/to/client.crt | openssl md5
   openssl rsa -noout -modulus -in /path/to/client.key | openssl md5
   # Both should output the same MD5 hash
   ```

3. **Verify credentials:**
   ```bash
   # Test with mosquitto client
   mosquitto_pub -h localhost -p 1883 \
     -u bridge_user -P 'your_password' \
     -t test/auth -m "test" -d
   ```

4. **Check broker logs:**
   ```bash
   # For Mosquitto
   journalctl -u mosquitto -n 50

   # For EMQX
   tail -100 /var/log/emqx/emqx.log
   ```

**Resolution:**
- Update `mqtt.host` and `mqtt.port` in config.yaml
- For TLS: ensure `use_tls: true` and certificate paths are correct
- Renew expired certificates
- Verify username/password in config.yaml match broker ACLs
- Check broker firewall rules

---

### Scenario 3: Messages Not Publishing

**Symptoms:**
- `aas_bridge_uns_published_total` counter not incrementing
- `aas_bridge_sparkplug_data_total` counter not incrementing
- MQTT connected but no messages appearing on topics

**Diagnostic steps:**

1. **Check if AAS files are being loaded:**
   ```bash
   curl -s http://localhost:9090/metrics | grep aas_bridge_aas_loaded
   # aas_bridge_aas_loaded_total{source_type="file"} should increase
   ```

2. **Check file watcher status:**
   ```bash
   # Verify watch directory has files
   ls -la ./watch/

   # Check for supported file types
   ls ./watch/*.aasx ./watch/*.json 2>/dev/null

   # Verify file watcher is enabled in config
   grep -A5 "file_watcher:" config.yaml
   ```

3. **Check if metrics are being flattened:**
   ```bash
   curl -s http://localhost:9090/metrics | grep aas_bridge_metrics_flattened
   ```

4. **Check deduplication (hash-based skipping):**
   ```bash
   curl -s http://localhost:9090/metrics | grep aas_bridge_tracked_topics
   # High count indicates deduplication is active
   ```

   Files with unchanged content are skipped. To force reprocessing:
   ```bash
   # Touch file to update timestamp (does not trigger reprocessing)
   touch ./watch/asset.aasx

   # Force reprocessing by modifying file or restarting bridge
   systemctl restart aas-uns-bridge
   ```

5. **Check for processing errors:**
   ```bash
   curl -s http://localhost:9090/metrics | grep 'aas_bridge_errors_total'
   journalctl -u aas-uns-bridge | grep -E "Failed to process|Error processing"
   ```

6. **Check mappings configuration:**
   ```bash
   # Ensure globalAssetId is mapped to ISA-95 hierarchy
   cat config/mappings.yaml
   ```

**Resolution:**
- Add AAS files to the watch directory
- Verify mappings.yaml includes entries for your asset globalAssetIds
- If using deduplication, restart the bridge or modify the source AAS files
- Check for malformed AAS files in logs
- Enable `DEBUG` log level temporarily: `log_level: DEBUG`

---

### Scenario 4: High Memory Usage

**Symptoms:**
- Process memory (RSS) growing over time
- OOM kills reported by kernel
- Slow response on health endpoint

**Diagnostic steps:**

1. **Check alias count (Sparkplug metric aliases):**
   ```bash
   curl -s http://localhost:9090/metrics | grep aas_bridge_alias_count
   ```
   Each unique metric path gets a persistent alias. Very high counts (>100,000) may indicate topic explosion.

2. **Check tracked topics (deduplication cache):**
   ```bash
   curl -s http://localhost:9090/metrics | grep aas_bridge_tracked_topics
   ```
   Each published topic's hash is tracked for deduplication.

3. **Check semantic cache size (if hypervisor features enabled):**
   ```bash
   curl -s http://localhost:9090/metrics | grep aas_bridge_semantic_cache_size
   ```

4. **Check active devices:**
   ```bash
   curl -s http://localhost:9090/metrics | grep aas_bridge_active_devices
   ```

5. **Monitor process memory:**
   ```bash
   # Get bridge PID
   pgrep -f aas-uns-bridge

   # Check memory usage
   ps -o pid,rss,vsz,comm -p $(pgrep -f aas-uns-bridge)

   # Continuous monitoring
   watch -n 5 'ps -o pid,rss,vsz,comm -p $(pgrep -f aas-uns-bridge)'
   ```

**Resolution:**
- Reduce number of tracked assets/submodels
- Limit semantic cache size: `hypervisor.resolution_cache.max_memory_entries`
- Clear state databases and restart (see [Full Reset](#full-reset))
- Review AAS content for unnecessary metric proliferation
- Consider disabling deduplication if memory is critical: `state.deduplicate_publishes: false`

---

### Scenario 5: Sparkplug Birth Not Sent

**Symptoms:**
- SCADA/Ignition not discovering devices
- `aas_bridge_sparkplug_births_total` counter is 0
- No NBIRTH/DBIRTH messages on `spBv1.0/+/+BIRTH/#` topics

**Diagnostic steps:**

1. **Verify Sparkplug is enabled:**
   ```bash
   grep -A5 "sparkplug:" config.yaml
   # Should show: enabled: true
   ```

2. **Check MQTT connection (births require connection):**
   ```bash
   curl -s http://localhost:8080/health | jq '.mqtt_connected'
   ```

3. **Check birth cache state:**
   ```bash
   # List cached births
   sqlite3 ./state/births.db "SELECT key, topic, datetime(timestamp, 'unixepoch') FROM birth_cache;"
   ```

4. **Check for birth publishing in logs:**
   ```bash
   journalctl -u aas-uns-bridge | grep -E "NBIRTH|DBIRTH|publish_nbirth|publish_dbirth"
   ```

5. **Verify group_id and edge_node_id configuration:**
   ```bash
   grep -A3 "sparkplug:" config.yaml
   ```

6. **Subscribe to birth topics manually:**
   ```bash
   mosquitto_sub -h localhost -p 1883 -t 'spBv1.0/+/+BIRTH/#' -v
   # Then restart the bridge to trigger births
   ```

**Resolution:**
- Ensure `sparkplug.enabled: true` in config.yaml
- Verify MQTT connection is established before births can be sent
- Clear birth cache to force regeneration: `rm ./state/births.db`
- Check group_id matches SCADA expectations
- Restart bridge to trigger NBIRTH: `systemctl restart aas-uns-bridge`

---

## Backup and Recovery

### State Database Backup

The bridge maintains state in SQLite databases under the configured `state.db_path` directory:

| Database | Purpose |
|----------|---------|
| `aliases.db` | Sparkplug metric alias mappings |
| `births.db` | Cached NBIRTH/DBIRTH payloads |
| `hashes.db` | Last-published value hashes for deduplication |
| `drift.db` | Schema drift detection fingerprints |
| `lifecycle.db` | Asset lifecycle state tracking |
| `semantic_cache.db` | Semantic resolution cache |
| `fidelity.db` | Fidelity calculation history |
| `streaming_drift.db` | Streaming drift detector state |

**Backup procedure:**

```bash
# Stop the bridge to ensure consistent backup
systemctl stop aas-uns-bridge

# Create backup directory
BACKUP_DIR="/backup/aas-uns-bridge/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

# Copy all state databases
cp ./state/*.db "$BACKUP_DIR/"

# Optionally backup configuration
cp config.yaml mappings.yaml "$BACKUP_DIR/"

# Start the bridge
systemctl start aas-uns-bridge

# Verify backup
ls -la "$BACKUP_DIR"
```

### State Database Recovery

```bash
# Stop the bridge
systemctl stop aas-uns-bridge

# Remove current state
rm -f ./state/*.db

# Restore from backup
cp /backup/aas-uns-bridge/20240128_120000/*.db ./state/

# Start the bridge
systemctl start aas-uns-bridge

# Verify recovery
aas-uns-bridge status
curl -s http://localhost:8080/health | jq
```

### Full Reset

To completely reset the bridge state (useful for clean deployments or resolving corruption):

```bash
# Stop the bridge
systemctl stop aas-uns-bridge

# Remove all state databases
rm -f ./state/*.db

# Start the bridge (will recreate databases)
systemctl start aas-uns-bridge
```

**Note:** A full reset will:
- Lose all Sparkplug alias mappings (aliases will be reassigned)
- Clear birth cache (births will be regenerated on next publish)
- Clear deduplication hashes (all values will be republished)
- Reset drift detection baselines
- Clear lifecycle tracking state

---

## Metrics Reference

### Core Operational Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `aas_bridge_mqtt_connected` | Gauge | MQTT connection status (1=connected, 0=disconnected) |
| `aas_bridge_aas_loaded_total{source_type}` | Counter | Total AAS files loaded (file/repository) |
| `aas_bridge_metrics_flattened_total` | Counter | Total metrics extracted from AAS content |
| `aas_bridge_uns_published_total` | Counter | Total UNS retained messages published |
| `aas_bridge_sparkplug_births_total{birth_type}` | Counter | Sparkplug birth messages (nbirth/dbirth) |
| `aas_bridge_sparkplug_data_total` | Counter | Sparkplug data messages published |
| `aas_bridge_errors_total{error_type}` | Counter | Errors by type (aas_file, repository, etc.) |
| `aas_bridge_last_publish_timestamp` | Gauge | Unix timestamp of last successful publish |

### State Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `aas_bridge_active_devices` | Gauge | Number of active Sparkplug devices |
| `aas_bridge_tracked_topics` | Gauge | Topics tracked for deduplication |
| `aas_bridge_alias_count` | Gauge | Sparkplug metric aliases assigned |

### Semantic Enforcement Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `aas_bridge_validation_metrics_total{result}` | Counter | Validated metrics (valid/invalid) |
| `aas_bridge_validation_errors_total{error_type}` | Counter | Validation errors by type |
| `aas_bridge_drift_events_total{event_type}` | Counter | Schema drift events (added/removed/changed) |
| `aas_bridge_asset_lifecycle_events_total{state}` | Counter | Lifecycle events (online/stale/offline) |
| `aas_bridge_assets_online` | Gauge | Assets currently online |
| `aas_bridge_assets_stale` | Gauge | Assets currently stale |
| `aas_bridge_assets_offline` | Gauge | Assets currently offline |

### Hypervisor Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `aas_bridge_fidelity_overall{asset_id}` | Gauge | Overall transformation fidelity (0.0-1.0) |
| `aas_bridge_fidelity_structural{asset_id}` | Gauge | Structural fidelity score |
| `aas_bridge_fidelity_semantic{asset_id}` | Gauge | Semantic fidelity score |
| `aas_bridge_semantic_cache_hits_total{cache_tier}` | Counter | Cache hits (memory/sqlite) |
| `aas_bridge_semantic_cache_size{tier}` | Gauge | Cache entries count |
| `aas_bridge_streaming_drift_anomaly_score{asset_id}` | Gauge | Anomaly score from streaming detector |
| `aas_bridge_bidirectional_writes_total{result}` | Counter | Write-back operations (success/failure) |

---

## Escalation

When standard troubleshooting does not resolve an issue, collect the following information before escalating:

### Information to Collect

1. **Bridge version:**
   ```bash
   aas-uns-bridge version
   ```

2. **Configuration (redact secrets):**
   ```bash
   # Copy and redact passwords/tokens
   cat config.yaml | sed 's/password:.*/password: [REDACTED]/' | sed 's/auth_token:.*/auth_token: [REDACTED]/'
   ```

3. **Recent logs (last 1000 lines):**
   ```bash
   journalctl -u aas-uns-bridge -n 1000 --no-pager > bridge_logs.txt
   ```

4. **Current metrics snapshot:**
   ```bash
   curl -s http://localhost:9090/metrics > metrics_snapshot.txt
   ```

5. **Health endpoint response:**
   ```bash
   curl -s http://localhost:8080/health > health_status.json
   ```

6. **State database sizes:**
   ```bash
   ls -la ./state/*.db
   ```

7. **System resources:**
   ```bash
   # Memory and CPU
   top -b -n 1 -p $(pgrep -f aas-uns-bridge) > process_stats.txt

   # Disk space
   df -h ./state/ >> process_stats.txt
   ```

8. **Network connectivity:**
   ```bash
   # MQTT broker reachability
   nc -zv localhost 1883
   ```

### Creating a Support Ticket

Include the following in your support ticket:

1. **Summary**: Brief description of the issue
2. **Impact**: Business impact and urgency
3. **Timeline**: When did the issue start?
4. **Steps to reproduce**: If applicable
5. **What has been tried**: Troubleshooting steps already attempted
6. **Attachments**:
   - bridge_logs.txt
   - metrics_snapshot.txt
   - health_status.json
   - process_stats.txt
   - Redacted config.yaml

### Emergency Contacts

For critical production issues:
- Check your organization's on-call rotation
- Refer to your internal escalation procedures
- For open-source issues, file a GitHub issue with collected diagnostic information
