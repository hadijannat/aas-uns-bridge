# System Requirements Specification

**Project:** aas-uns-bridge
**Version:** 1.0
**Date:** 2026-01-28

## 1. Purpose

This document defines the functional and reliability requirements for the AAS-UNS Bridge system. Requirements are traceable to verification tests in the [Verification Matrix](./verification-matrix.md).

## 2. Scope

The AAS-UNS Bridge ingests Asset Administration Shell (AAS) content and publishes to dual MQTT planes:
- **UNS Plane**: JSON payloads with retained messages for late-subscriber discovery
- **Sparkplug B Plane**: Protobuf payloads with lifecycle management for SCADA integration

## 3. Functional Requirements

### 3.1 AAS Ingestion

| ID | Requirement | Priority | Rationale |
|----|-------------|----------|-----------|
| REQ-FUNC-001 | The system SHALL ingest AASX and JSON AAS files | Must | Core functionality - support both package and raw formats |
| REQ-FUNC-002 | The system SHALL watch configured directories for new/modified AAS files | Must | Enable continuous synchronization without manual intervention |
| REQ-FUNC-003 | The system SHALL extract submodel elements recursively | Must | AAS submodels contain nested structures that must be flattened |

### 3.2 UNS Publishing

| ID | Requirement | Priority | Rationale |
|----|-------------|----------|-----------|
| REQ-FUNC-010 | The system SHALL publish UNS messages with `retain=true` | Must | Late subscribers must receive current state on connect |
| REQ-FUNC-011 | The system SHALL use ISA-95 hierarchy in UNS topics | Must | Industry-standard topic structure for manufacturing |
| REQ-FUNC-012 | The system SHALL publish JSON payloads to UNS plane | Must | Human-readable, widely supported format |

### 3.3 Sparkplug B Publishing

| ID | Requirement | Priority | Rationale |
|----|-------------|----------|-----------|
| REQ-FUNC-020 | The system SHALL publish Sparkplug messages with `retain=false` | Must | Sparkplug spec requirement - state is managed by births |
| REQ-FUNC-021 | The system SHALL use `qos=0` for ALL Sparkplug messages | Must | Sparkplug spec requirement - reliability via seq/rebirth |
| REQ-FUNC-022 | The system SHALL publish NBIRTH on connection | Must | Sparkplug lifecycle - announce edge node presence |
| REQ-FUNC-023 | The system SHALL publish DBIRTH before any DDATA | Must | Sparkplug lifecycle - devices must birth before data |
| REQ-FUNC-024 | The system SHALL configure NDEATH as Last Will & Testament (LWT) | Must | Sparkplug lifecycle - broker announces node death |
| REQ-FUNC-025 | The system SHALL increment bdSeq on each rebirth | Must | Sparkplug spec - correlate deaths with births |
| REQ-FUNC-026 | The system SHALL respond to NCMD Rebirth requests | Must | Sparkplug spec - support primary host rebirth commands |
| REQ-FUNC-027 | The system SHALL use sequence numbers 0-255 with wrapping | Must | Sparkplug spec - message ordering within session |

### 3.4 Topic Structure

| ID | Requirement | Priority | Rationale |
|----|-------------|----------|-----------|
| REQ-FUNC-030 | UNS topics SHALL follow `{enterprise}/{site}/{area}/{line}/{asset}/context/{submodel}/{path}` | Must | ISA-95 hierarchy standard |
| REQ-FUNC-031 | Sparkplug topics SHALL follow `spBv1.0/{group}/{msg_type}/{edge_node}[/{device}]` | Must | Sparkplug namespace specification |

## 4. Reliability Requirements

| ID | Requirement | Priority | Rationale |
|----|-------------|----------|-----------|
| REQ-REL-001 | The system SHALL reconnect on broker disconnect | Must | Operational resilience - handle network interruptions |
| REQ-REL-002 | Metric aliases SHALL persist across restarts | Must | Sparkplug alias reuse - avoid alias exhaustion |
| REQ-REL-003 | The system SHALL deduplicate unchanged metric values | Should | Reduce network traffic and broker load |
| REQ-REL-004 | The system SHALL support graceful shutdown with device deaths | Must | Clean lifecycle management |

## 5. Performance Requirements

| ID | Requirement | Priority | Rationale |
|----|-------------|----------|-----------|
| REQ-PERF-001 | The system SHALL support at least 1000 metrics per second throughput | Should | Target industrial scale |
| REQ-PERF-002 | Memory usage SHALL remain stable over 14-day operation | Must | No memory leaks in daemon mode |

## 6. Security Requirements

| ID | Requirement | Priority | Rationale |
|----|-------------|----------|-----------|
| REQ-SEC-001 | The system SHALL support TLS connections to MQTT broker | Should | Encrypted transport |
| REQ-SEC-002 | The system SHALL support MQTT username/password authentication | Should | Basic broker authentication |

## 7. Requirement Traceability

See [Verification Matrix](./verification-matrix.md) for mapping of requirements to test cases.

## 8. Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-28 | TRL Evidence Pack | Initial specification |
