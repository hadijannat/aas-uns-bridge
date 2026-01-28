# Grafana Dashboard

This directory contains the Grafana dashboard for monitoring the AAS-UNS Bridge service.

## Installation

### Option 1: Import via Grafana UI

1. Open Grafana in your browser
2. Navigate to **Dashboards** > **Import**
3. Click **Upload JSON file**
4. Select `aas-bridge-dashboard.json`
5. Select your Prometheus data source
6. Click **Import**

### Option 2: Provisioning

For automated deployments, copy the dashboard to your Grafana provisioning directory:

```bash
cp aas-bridge-dashboard.json /etc/grafana/provisioning/dashboards/
```

Ensure you have a dashboard provider configured in `/etc/grafana/provisioning/dashboards/provider.yaml`:

```yaml
apiVersion: 1
providers:
  - name: 'default'
    orgId: 1
    folder: 'AAS-UNS Bridge'
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    options:
      path: /etc/grafana/provisioning/dashboards
```

### Option 3: Docker Compose

If using Docker Compose, mount the dashboard:

```yaml
services:
  grafana:
    image: grafana/grafana:latest
    volumes:
      - ./monitoring/grafana:/etc/grafana/provisioning/dashboards
      - ./monitoring/grafana/provider.yaml:/etc/grafana/provisioning/dashboards/provider.yaml
```

## Dashboard Overview

The dashboard is organized into six rows:

### Row 1: Connection Status
- **MQTT Connected**: Real-time connection status (green/red indicator)
- **Active Devices**: Count of registered Sparkplug devices
- **Assets Online/Stale/Offline**: Asset lifecycle state breakdown

### Row 2: Throughput
- **Messages Published**: Rate of UNS and Sparkplug DATA messages
- **Sparkplug Births**: NBIRTH and DBIRTH message counts
- **AAS Files Loaded**: Total files ingested by source type

### Row 3: Bidirectional Sync
- **Write Operations**: Success/failure rate of write-back operations
- **Validation Results**: Allowed vs. denied write validations
- **Write Retries**: Count of retry attempts

### Row 4: Fidelity & Drift
- **Overall Fidelity Score**: Aggregate transformation quality (0-1 gauge)
- **Fidelity Components**: Structural, semantic, and entropy loss breakdown
- **Streaming Drift Events**: Real-time drift detection by type and severity

### Row 5: State Databases
- **State DB Entries**: Current entry counts for alias and hash databases
- **State DB Evictions**: LRU eviction rate over time
- **Semantic Cache Stats**: Hit/miss ratio for semantic resolution cache

### Row 6: Errors
- **Error Rate**: Overall error rate by error type
- **Validation Errors**: Schema and value validation failures
- **Drift Events by Severity**: Bar chart showing drift event distribution

## Variables

The dashboard includes an `instance` variable for filtering by bridge instance. This is useful for multi-instance deployments.

## Customization

### Changing Thresholds

The dashboard uses default thresholds that may need adjustment for your environment:

| Metric | Default Threshold | Adjustment Location |
|--------|-------------------|---------------------|
| Fidelity Score | Warning < 0.8, Critical < 0.5 | Row 4: Overall Fidelity panel |
| Error Rate | Any errors show as warning | Row 6: Error Rate panel |
| Assets Offline | Any offline shows as warning | Row 1: Assets Offline panel |

### Adding Panels

To add custom panels:

1. Edit the dashboard in Grafana
2. Add your panel with the appropriate `aas_bridge_*` metric
3. Export the updated JSON
4. Replace `aas-bridge-dashboard.json`

## Troubleshooting

### No Data Displayed

1. Verify Prometheus is scraping the bridge:
   ```bash
   curl http://localhost:9090/api/v1/targets
   ```

2. Check the bridge metrics endpoint:
   ```bash
   curl http://localhost:9090/metrics | grep aas_bridge
   ```

3. Ensure the Prometheus data source is correctly configured in Grafana

### Metrics Missing Labels

Some metrics require specific operations to populate labels:
- `fidelity_*` metrics require fidelity evaluation to run
- `bidirectional_*` metrics require write-back operations
- `streaming_drift_*` metrics require drift detection to be enabled

## Version Compatibility

- Grafana: 9.0+ recommended (tested with 10.x)
- Prometheus: 2.0+
- AAS-UNS Bridge: 1.0+
