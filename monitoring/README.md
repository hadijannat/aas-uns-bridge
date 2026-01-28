# Monitoring

This directory contains monitoring configurations for the AAS-UNS Bridge service.

## Overview

The AAS-UNS Bridge exposes Prometheus metrics on a configurable port (default: 9090) at the `/metrics` endpoint. These metrics can be scraped by Prometheus and visualized using the provided Grafana dashboard.

## Components

### Prometheus Alerting Rules

Production alerting rules are available in `prometheus/alerting-rules.yaml`. See [prometheus/README.md](prometheus/README.md) for configuration and customization.

Alert categories include:
- **Connection**: MQTT disconnection, high reconnect rate
- **Performance**: Publish latency, queue depth, throughput
- **Errors**: Error rate, write failures, validation denials
- **Resources**: State database limits, eviction rate
- **Drift/Fidelity**: Schema drift detection, fidelity drops
- **Assets**: Offline/stale asset detection

### Prometheus Metrics

The bridge exports metrics with the prefix `aas_bridge_`. Key metric categories include:

| Category | Metrics | Description |
|----------|---------|-------------|
| **Connection** | `mqtt_connected`, `active_devices` | MQTT connection status and device count |
| **Assets** | `assets_online`, `assets_stale`, `assets_offline` | Asset lifecycle states |
| **Throughput** | `uns_published_total`, `sparkplug_data_total`, `sparkplug_births_total` | Message publication rates |
| **Fidelity** | `fidelity_overall`, `fidelity_structural`, `fidelity_semantic`, `fidelity_entropy_loss` | Transformation quality scores |
| **Bidirectional** | `bidirectional_writes_total`, `bidirectional_validations_total` | Write-back operation status |
| **State DB** | `state_db_entries`, `state_db_evictions_total` | Persistence layer health |
| **Errors** | `errors_total`, `validation_errors_total`, `drift_events_total` | Error tracking |

### Grafana Dashboard

See [grafana/README.md](grafana/README.md) for dashboard installation instructions.

## Quick Start

### 1. Configure Prometheus

Add the following scrape configuration to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'aas-uns-bridge'
    static_configs:
      - targets: ['localhost:9090']
    scrape_interval: 15s
```

For multiple bridge instances:

```yaml
scrape_configs:
  - job_name: 'aas-uns-bridge'
    static_configs:
      - targets:
          - 'bridge-1:9090'
          - 'bridge-2:9090'
        labels:
          environment: 'production'
```

### 2. Import Grafana Dashboard

Import `grafana/aas-bridge-dashboard.json` into your Grafana instance.

### 3. Configure Alerting Rules

Add the alerting rules to your Prometheus configuration:

```yaml
rule_files:
  - /path/to/monitoring/prometheus/alerting-rules.yaml
```

See [prometheus/README.md](prometheus/README.md) for the complete set of production alerting rules covering connection, performance, errors, resources, drift, and fidelity monitoring.

## Architecture

```
+------------------+     scrape      +------------+     query     +---------+
| AAS-UNS Bridge   | --------------> | Prometheus | <------------ | Grafana |
| :9090/metrics    |                 |            |               |         |
+------------------+                 +------------+               +---------+
```

## Metric Reference

For the complete list of available metrics, see the source code at:
`src/aas_uns_bridge/observability/metrics.py`
