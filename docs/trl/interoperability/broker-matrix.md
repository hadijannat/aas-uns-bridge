# MQTT Broker Interoperability Matrix

**Project:** aas-uns-bridge
**Date:** 2026-01-28

## Overview

This document tracks MQTT broker interoperability testing for the aas-uns-bridge. The bridge must work correctly with multiple broker implementations to ensure deployment flexibility.

## Broker Matrix

| Broker | Version | Protocol | Tested | Result | Notes |
|--------|---------|----------|--------|--------|-------|
| **Eclipse Mosquitto** | 2.0.18 | MQTTv5 | ✅ Yes | PASS | Primary CI broker |
| **EMQX** | 5.5+ | MQTTv5 | ⏳ Pending | - | Docker compose available |
| **HiveMQ CE** | 4.x | MQTTv5 | ⏳ Pending | - | Docker compose available |
| **NanoMQ** | 0.21+ | MQTTv5 | ⏳ Pending | - | Lightweight alternative |
| **VerneMQ** | 1.x | MQTTv5 | ⏳ Pending | - | Clusterable |

## Test Categories

### Basic Connectivity
- [ ] Connect with clean session
- [ ] Reconnect after disconnect
- [ ] LWT (Last Will Testament) delivery

### Sparkplug B
- [ ] NBIRTH publish (QoS=0, retain=false)
- [ ] DBIRTH publish (QoS=0, retain=false)
- [ ] DDATA publish (QoS=0, retain=false)
- [ ] NDEATH via LWT
- [ ] NCMD subscription and handling

### UNS Retained
- [ ] Retained message publication
- [ ] Late subscriber receives retained
- [ ] Retained message survives broker restart

### Performance
- [ ] 100 msg/sec throughput
- [ ] 1000 msg/sec throughput
- [ ] Memory stability over time

## Mosquitto (Primary)

**Status:** ✅ Fully Tested

### Configuration
```yaml
# docker-compose.mosquitto.yml
services:
  mosquitto:
    image: eclipse-mosquitto:2
    ports:
      - "1883:1883"
    volumes:
      - ./mosquitto.conf:/mosquitto/config/mosquitto.conf
```

### Test Results
- Basic Connectivity: PASS
- Sparkplug B: PASS
- UNS Retained: PASS
- Performance: PASS (>1000 msg/sec)

### Notes
- MQTTv5 protocol version confirmed
- All integration and E2E tests pass
- Retained messages persist across restart (with persistence config)

## EMQX

**Status:** ⏳ Pending

### Configuration
See `docker/docker-compose.emqx.yml`

### Test Procedure
```bash
# Start EMQX broker
docker compose -f docker/docker-compose.emqx.yml up -d

# Run integration tests
TEST_MQTT_HOST=localhost TEST_MQTT_PORT=1883 pytest tests/integration -v

# Run Sparkplug compliance
TEST_MQTT_HOST=localhost TEST_MQTT_PORT=1883 pytest tests/e2e/test_sparkplug_e2e.py -v
```

### Expected Behavior
- EMQX supports Sparkplug via plugin
- Dashboard available at http://localhost:18083 (admin/public)
- Verify retain behavior matches specification

## HiveMQ CE

**Status:** ⏳ Pending

### Configuration
See `docker/docker-compose.hivemq.yml`

### Test Procedure
```bash
# Start HiveMQ broker
docker compose -f docker/docker-compose.hivemq.yml up -d

# Run integration tests
TEST_MQTT_HOST=localhost TEST_MQTT_PORT=1883 pytest tests/integration -v
```

### Expected Behavior
- HiveMQ has native Sparkplug support
- Verify QoS=0 message delivery
- Check retained message behavior

## Test Execution Template

For each broker, record results:

```markdown
### [Broker Name] - [Version]
**Test Date:** YYYY-MM-DD
**Tester:** [Name]

#### Basic Connectivity
- [ ] Connect: PASS/FAIL
- [ ] Reconnect: PASS/FAIL
- [ ] LWT: PASS/FAIL

#### Sparkplug B
- [ ] NBIRTH: PASS/FAIL
- [ ] DBIRTH: PASS/FAIL
- [ ] DDATA: PASS/FAIL
- [ ] NCMD: PASS/FAIL

#### UNS Retained
- [ ] Publish retained: PASS/FAIL
- [ ] Late subscriber: PASS/FAIL

#### Notes
[Any issues, workarounds, or observations]
```

## Known Broker-Specific Issues

### Mosquitto
- None known

### EMQX
- TBD

### HiveMQ
- TBD

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-28 | TRL Evidence Pack | Initial matrix |
