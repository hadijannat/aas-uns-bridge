# Performance Baseline

**Project:** aas-uns-bridge
**Date:** 2026-01-28
**Target TRL:** 5-6

## Overview

This document establishes performance baselines for the AAS-UNS Bridge. These metrics serve as reference points for regression testing and TRL evidence.

## Test Environment

### Hardware Reference
| Component | Specification |
|-----------|---------------|
| CPU | [To be filled during baseline test] |
| RAM | [To be filled] |
| Storage | [To be filled] |
| Network | [To be filled] |

### Software Reference
| Component | Version |
|-----------|---------|
| Python | 3.11+ |
| OS | [To be filled] |
| MQTT Broker | Mosquitto 2.0.18 |

## Throughput Baselines

### Message Publishing Rate

| Scenario | Target | Baseline | Status |
|----------|--------|----------|--------|
| UNS only (small payload) | 1000 msg/s | TBD | ⏳ |
| UNS only (medium payload) | 500 msg/s | TBD | ⏳ |
| Sparkplug only (protobuf) | 1000 msg/s | TBD | ⏳ |
| Dual-plane (UNS + Sparkplug) | 500 msg/s | TBD | ⏳ |

### Payload Sizes
| Type | Size |
|------|------|
| Small | < 100 bytes |
| Medium | 100-500 bytes |
| Large | 500+ bytes |

## Latency Baselines

### End-to-End Latency

| Scenario | P50 | P95 | P99 | Max |
|----------|-----|-----|-----|-----|
| UNS publish (local broker) | TBD | TBD | TBD | TBD |
| Sparkplug DDATA (local) | TBD | TBD | TBD | TBD |
| Dual-plane (local) | TBD | TBD | TBD | TBD |

### Component Latency

| Component | Typical | Max |
|-----------|---------|-----|
| AAS file parsing | TBD | TBD |
| Submodel traversal | TBD | TBD |
| ISA-95 mapping lookup | TBD | TBD |
| JSON serialization | TBD | TBD |
| Protobuf serialization | TBD | TBD |
| MQTT publish | TBD | TBD |

## Resource Baselines

### Memory Usage

| Scenario | Initial | Steady-State | Peak |
|----------|---------|--------------|------|
| Idle (connected, no traffic) | TBD | TBD | TBD |
| Low load (10 msg/s) | TBD | TBD | TBD |
| Medium load (100 msg/s) | TBD | TBD | TBD |
| High load (1000 msg/s) | TBD | TBD | TBD |

### CPU Usage

| Scenario | Average | Peak |
|----------|---------|------|
| Idle | TBD | TBD |
| Low load | TBD | TBD |
| Medium load | TBD | TBD |
| High load | TBD | TBD |

### SQLite Database Size

| Metric | Size |
|--------|------|
| Per 1000 aliases | TBD |
| Per 1000 birth cache entries | TBD |
| Max recommended size | TBD |

## Scalability

### Device Scaling

| Devices | Metrics/Device | Total Metrics | Throughput | Memory |
|---------|----------------|---------------|------------|--------|
| 10 | 10 | 100 | TBD | TBD |
| 50 | 10 | 500 | TBD | TBD |
| 100 | 10 | 1000 | TBD | TBD |
| 500 | 10 | 5000 | TBD | TBD |

### Metric Scaling

| Devices | Metrics/Device | Total Metrics | Throughput | Memory |
|---------|----------------|---------------|------------|--------|
| 10 | 100 | 1000 | TBD | TBD |
| 10 | 500 | 5000 | TBD | TBD |
| 10 | 1000 | 10000 | TBD | TBD |

## Benchmark Commands

```bash
# Run throughput benchmark
pytest tests/load/test_publish_throughput.py -v

# Run with profiling
python -m cProfile -o profile.out -m pytest tests/load/test_publish_throughput.py
python -c "import pstats; p = pstats.Stats('profile.out'); p.sort_stats('cumtime').print_stats(20)"

# Memory profiling
pytest tests/stability/test_memory_stability.py -v

# Full performance suite
pytest tests/load tests/stability -v --timeout=600
```

## Baseline Establishment Process

1. **Environment Setup**
   - Clean VM or container
   - No other workloads running
   - Broker running locally

2. **Warmup**
   - Run 5 minutes warmup before measurements
   - Discard warmup data

3. **Measurement**
   - Run each test 5 times
   - Record mean and standard deviation
   - Report P50, P95, P99 for latency

4. **Documentation**
   - Record exact environment specifications
   - Note any anomalies
   - Store raw data alongside summary

## Regression Thresholds

Performance regression is flagged if:
- Throughput decreases by > 20%
- Latency increases by > 50%
- Memory usage increases by > 30%

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-28 | TRL Evidence Pack | Initial template |
