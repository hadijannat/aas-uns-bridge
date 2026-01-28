# Prometheus Alerting Rules for AAS-UNS Bridge

This directory contains Prometheus alerting rules for monitoring the AAS-UNS Bridge in production environments.

## Quick Start

### 1. Configure Prometheus to Load Alerting Rules

Add the alerting rules file to your `prometheus.yml`:

```yaml
rule_files:
  - /path/to/monitoring/prometheus/alerting-rules.yaml
```

### 2. Verify Rules Are Loaded

After restarting Prometheus, check the Rules page in the Prometheus UI:
- Navigate to `http://<prometheus-host>:9090/rules`
- You should see alert groups: `aas-bridge-connection`, `aas-bridge-performance`, etc.

### 3. Configure Alertmanager (Optional)

For alert notifications, configure Alertmanager in `prometheus.yml`:

```yaml
alerting:
  alertmanagers:
    - static_configs:
        - targets:
            - 'alertmanager:9093'
```

## Alert Categories

### Connection Alerts

| Alert | Severity | Trigger Condition |
|-------|----------|-------------------|
| `MQTTDisconnected` | critical | MQTT not connected for > 1 minute |
| `HighReconnectRate` | warning | > 5 reconnects in 15 minutes |
| `NoActiveDevices` | warning | Connected but no Sparkplug devices for 5 minutes |

### Performance Alerts

| Alert | Severity | Trigger Condition |
|-------|----------|-------------------|
| `HighPublishLatency` | warning | 95th percentile > 500ms for 5 minutes |
| `HighPublishQueueDepth` | warning | Queue depth > 1000 for 5 minutes |
| `CriticalPublishQueueDepth` | critical | Queue depth > 5000 for 1 minute |
| `LowThroughput` | warning | < 1 message/minute for 10 minutes when connected |
| `HighTraversalDuration` | warning | 95th percentile > 1 second for 5 minutes |
| `SlowAASLoading` | warning | 95th percentile > 10 seconds for 5 minutes |

### Error Alerts

| Alert | Severity | Trigger Condition |
|-------|----------|-------------------|
| `HighErrorRate` | critical | > 10 errors/minute for 5 minutes |
| `BidirectionalWriteFailures` | warning | > 5 write failures in 5 minutes |
| `HighWriteRetryRate` | warning | Elevated retry rate for 5 minutes |
| `ValidationDenialSpike` | warning | > 20 denied validations in 5 minutes |
| `HighValidationErrorRate` | warning | Elevated validation errors for 5 minutes |
| `SparkplugBirthFailure` | warning | Any birth message failure |

### Resource Alerts

| Alert | Severity | Trigger Condition |
|-------|----------|-------------------|
| `StateDBNearLimit` | warning | > 90% of max_entries for 5 minutes |
| `StateDBAtLimit` | critical | At maximum capacity for 1 minute |
| `HighEvictionRate` | warning | > 100 evictions/minute for 5 minutes |
| `LargeStateDatabaseSize` | warning | Database file > 1GB |
| `LowSemanticCacheHitRate` | info | Cache hit rate < 50% for 15 minutes |

### Drift and Fidelity Alerts

| Alert | Severity | Trigger Condition |
|-------|----------|-------------------|
| `DriftDetectedHigh` | warning | Any high severity drift event |
| `DriftDetectedCritical` | critical | Any critical severity drift event |
| `HighDriftAnomalyScore` | warning | Anomaly score > 0.8 for 5 minutes |
| `FidelityDrop` | warning | Fidelity score < 0.8 for 10 minutes |
| `CriticalFidelityDrop` | critical | Fidelity score < 0.5 for 5 minutes |
| `HighEntropyLoss` | info | Entropy loss > 0.3 for 10 minutes |
| `HighDriftEventRate` | warning | > 0.1 drift events/second for 5 minutes |

### Asset Lifecycle Alerts

| Alert | Severity | Trigger Condition |
|-------|----------|-------------------|
| `AssetsGoingOffline` | warning | > 5 assets go offline in 5 minutes |
| `HighStaleAssetCount` | warning | > 10 stale assets for 15 minutes |
| `AllAssetsOffline` | critical | No online assets for 5 minutes |

### Service Health Alerts

| Alert | Severity | Trigger Condition |
|-------|----------|-------------------|
| `BridgeNotPublishing` | critical | No messages published for 30 minutes |
| `NoBirthMessagesPublished` | warning | No birth messages for 1 hour |
| `MetricsEndpointDown` | critical | Prometheus cannot scrape metrics for 2 minutes |

## Severity Levels

| Level | Response Time | Description |
|-------|---------------|-------------|
| **critical** | Immediate | Service is down or data loss is occurring |
| **warning** | Within 1 hour | Investigate and resolve before escalation |
| **info** | During business hours | Informational, monitor for trends |

## Customization

### Adjusting Thresholds

To customize alert thresholds, edit the `expr` field in `alerting-rules.yaml`. Common adjustments:

```yaml
# Increase queue depth threshold for high-throughput deployments
- alert: HighPublishQueueDepth
  expr: aas_bridge_mqtt_publish_queue_depth > 2000  # Changed from 1000
  for: 5m

# Lower fidelity threshold for less critical deployments
- alert: FidelityDrop
  expr: aas_bridge_fidelity_overall < 0.7  # Changed from 0.8
  for: 10m
```

### Adding Labels for Routing

Add custom labels to route alerts to specific teams:

```yaml
- alert: MQTTDisconnected
  expr: aas_bridge_mqtt_connected == 0
  for: 1m
  labels:
    severity: critical
    team: platform  # Added for Alertmanager routing
    on_call: true   # Added for PagerDuty integration
```

### Environment-Specific Rules

Create separate rule files for different environments:

```
monitoring/prometheus/
  alerting-rules.yaml           # Base rules
  alerting-rules.production.yaml  # Production overrides
  alerting-rules.staging.yaml     # Staging overrides (higher thresholds)
```

## Alertmanager Configuration Example

Route alerts by severity:

```yaml
route:
  group_by: ['alertname', 'instance']
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  receiver: 'default'
  routes:
    - match:
        severity: critical
      receiver: 'pagerduty-critical'
      repeat_interval: 15m
    - match:
        severity: warning
      receiver: 'slack-warnings'
      repeat_interval: 1h
    - match:
        severity: info
      receiver: 'slack-info'
      repeat_interval: 24h

receivers:
  - name: 'default'
    email_configs:
      - to: 'ops@example.com'

  - name: 'pagerduty-critical'
    pagerduty_configs:
      - service_key: '<your-pagerduty-key>'
        severity: critical

  - name: 'slack-warnings'
    slack_configs:
      - api_url: '<slack-webhook-url>'
        channel: '#aas-bridge-alerts'
        send_resolved: true

  - name: 'slack-info'
    slack_configs:
      - api_url: '<slack-webhook-url>'
        channel: '#aas-bridge-info'
```

## Testing Alerts

### Using Prometheus Unit Tests

Create a test file `alerting-rules_test.yaml`:

```yaml
rule_files:
  - alerting-rules.yaml

tests:
  - interval: 1m
    input_series:
      - series: 'aas_bridge_mqtt_connected{instance="bridge-1:9090"}'
        values: '0 0 0 0 0'  # Disconnected for 5 minutes
    alert_rule_test:
      - eval_time: 2m
        alertname: MQTTDisconnected
        exp_alerts:
          - exp_labels:
              severity: critical
              component: mqtt
              instance: bridge-1:9090
```

Run tests:
```bash
promtool test rules alerting-rules_test.yaml
```

### Validating Rule Syntax

```bash
promtool check rules alerting-rules.yaml
```

## Troubleshooting

### Alert Not Firing

1. Check if metric exists: `http://<prometheus>:9090/graph?g0.expr=aas_bridge_mqtt_connected`
2. Verify scrape target is healthy: `http://<prometheus>:9090/targets`
3. Check rule evaluation: `http://<prometheus>:9090/rules`

### Too Many Alerts

1. Increase the `for` duration to reduce flapping
2. Add inhibition rules in Alertmanager for dependent alerts
3. Use aggregation to reduce cardinality

### Alert Labels Not Matching

Ensure instance labels match between your scrape config and alert queries. Use `{instance=~".*"}` for broad matching.

## Related Documentation

- [Operator's Runbook](../../docs/operations/runbook.md) - Response procedures for each alert
- [Grafana Dashboard](../grafana/README.md) - Visualization of metrics
- [Metrics Reference](../../src/aas_uns_bridge/observability/metrics.py) - Complete metric definitions
