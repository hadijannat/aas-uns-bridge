# Consumer Interoperability Matrix

**Project:** aas-uns-bridge
**Date:** 2026-01-28

## Overview

This document tracks consumer application interoperability testing. The bridge publishes to two planes (UNS and Sparkplug), and various consumer applications should be able to receive and process messages correctly.

## Consumer Matrix

### Sparkplug Consumers

| Consumer | Version | Type | Tested | Result | Notes |
|----------|---------|------|--------|--------|-------|
| **Ignition** | 8.1+ | SCADA | ⏳ Pending | - | Industry standard |
| **United Manufacturing Hub** | 0.9+ | Industrial | ⏳ Pending | - | Open source |
| **Cirrus Link Modules** | Latest | Ignition Module | ⏳ Pending | - | Official Sparkplug |
| **mqtt-spy** | 1.0 | Debug Tool | ✅ Yes | PASS | Payload inspection |
| **MQTT Explorer** | 0.4+ | Debug Tool | ✅ Yes | PASS | Topic browsing |

### UNS Consumers

| Consumer | Version | Type | Tested | Result | Notes |
|----------|---------|------|--------|--------|-------|
| **Node-RED** | 3.x | Integration | ⏳ Pending | - | Workflow automation |
| **InfluxDB Telegraf** | 1.x | Time-series | ⏳ Pending | - | MQTT input plugin |
| **Grafana** | 10.x | Dashboard | ⏳ Pending | - | Via data source |
| **Custom Python** | - | Client | ✅ Yes | PASS | paho-mqtt |

## Test Categories

### Sparkplug Consumers
- [ ] Receives NBIRTH and tracks edge node
- [ ] Receives DBIRTH and registers device
- [ ] Receives DDATA and updates metric values
- [ ] Handles NDEATH (edge node offline)
- [ ] Decodes protobuf payload correctly
- [ ] Reads semantic properties (aas:semanticId, etc.)

### UNS Consumers
- [ ] Receives retained messages on late subscribe
- [ ] Parses JSON payload correctly
- [ ] Extracts semantic metadata
- [ ] Handles topic hierarchy correctly

## Ignition

**Status:** ⏳ Pending

### Prerequisites
- Ignition Gateway 8.1+
- Cirrus Link MQTT Engine module
- Cirrus Link MQTT Distributor module (optional)

### Test Procedure
1. Configure MQTT Engine to connect to broker
2. Set namespace to match bridge group_id
3. Verify edge node appears in tag tree
4. Verify devices appear under edge node
5. Confirm metric values update

### Expected Behavior
- Edge node shows as "online" after NBIRTH
- All device metrics visible after DBIRTH
- Real-time value updates via DDATA
- Edge node shows "offline" after NDEATH

## United Manufacturing Hub (UMH)

**Status:** ⏳ Pending

### Prerequisites
- UMH deployment (Kubernetes or Docker)
- Kafka broker configured

### Test Procedure
1. Deploy UMH stack
2. Configure mqtt-to-kafka bridge
3. Verify Sparkplug messages flow through
4. Check Kafka topics for converted data

### Expected Behavior
- Messages converted to internal format
- Historical data stored in TimescaleDB
- Grafana dashboards populate

## Node-RED

**Status:** ⏳ Pending

### Prerequisites
- Node-RED 3.x
- node-red-contrib-mqtt-broker node

### Test Procedure
1. Add MQTT-in node subscribing to UNS topics
2. Connect JSON parse node
3. Add debug node to inspect payloads
4. Verify ISA-95 hierarchy in topics

### Expected Behavior
- UNS messages received with retain flag
- JSON payload parses correctly
- Semantic metadata accessible

## Test Execution Template

```markdown
### [Consumer Name] - [Version]
**Test Date:** YYYY-MM-DD
**Tester:** [Name]

#### Configuration
[Key settings used]

#### Sparkplug Integration (if applicable)
- [ ] NBIRTH recognition: PASS/FAIL
- [ ] DBIRTH device registration: PASS/FAIL
- [ ] DDATA value updates: PASS/FAIL
- [ ] Semantic properties visible: PASS/FAIL

#### UNS Integration (if applicable)
- [ ] Retained message delivery: PASS/FAIL
- [ ] JSON parse: PASS/FAIL
- [ ] Topic hierarchy: PASS/FAIL

#### Screenshots
[Attach relevant screenshots]

#### Notes
[Any issues, workarounds, or observations]
```

## Known Consumer-Specific Issues

### Ignition
- Cirrus Link modules required for Sparkplug
- Tag browser may need refresh after rebirth

### UMH
- TBD

### Node-RED
- Standard MQTT nodes work for UNS plane
- Sparkplug requires additional parsing

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-28 | TRL Evidence Pack | Initial matrix |
