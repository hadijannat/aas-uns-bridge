# Verification Matrix

**Project:** aas-uns-bridge
**Version:** 1.0
**Date:** 2026-01-28

## Purpose

This matrix maps each system requirement from the [System Requirements Specification](./system-requirements-spec.md) to the test cases that verify compliance.

## Verification Methods

| Code | Method | Description |
|------|--------|-------------|
| T | Test | Automated test execution |
| I | Inspection | Manual code review |
| A | Analysis | Design/architecture review |
| D | Demonstration | Manual functional demonstration |

## Requirements Traceability

### AAS Ingestion Requirements

| Requirement | Description | Method | Test Location | Status |
|-------------|-------------|--------|---------------|--------|
| REQ-FUNC-001 | Ingest AASX and JSON AAS files | T | `tests/unit/test_traversal.py` | ✅ Pass |
| REQ-FUNC-002 | Watch directories for AAS files | T | `tests/e2e/test_dual_plane.py` | ✅ Pass |
| REQ-FUNC-003 | Extract submodel elements recursively | T | `tests/unit/test_traversal.py::TestFlattenSubmodel::test_nested_collection` | ✅ Pass |

### UNS Publishing Requirements

| Requirement | Description | Method | Test Location | Status |
|-------------|-------------|--------|---------------|--------|
| REQ-FUNC-010 | UNS messages with `retain=true` | T | `tests/integration/test_retained_publish.py::test_published_message_received_by_late_subscriber` | ✅ Pass |
| REQ-FUNC-010 | UNS messages with `retain=true` | T | `tests/e2e/test_uns_e2e.py::TestUnsLateSubscriber::test_late_subscriber_receives_retained_messages` | ✅ Pass |
| REQ-FUNC-011 | ISA-95 hierarchy in UNS topics | T | `tests/e2e/test_uns_e2e.py::TestUnsTopicCorrectness::test_topic_follows_isa95_hierarchy` | ✅ Pass |
| REQ-FUNC-012 | JSON payloads to UNS plane | T | `tests/e2e/test_uns_e2e.py::TestUnsTopicCorrectness::test_payload_schema_validation` | ✅ Pass |

### Sparkplug B Publishing Requirements

| Requirement | Description | Method | Test Location | Status |
|-------------|-------------|--------|---------------|--------|
| REQ-FUNC-020 | Sparkplug messages `retain=false` | T | `tests/e2e/test_sparkplug_e2e.py::TestSparkplugRetainFalse` | ✅ Pass |
| REQ-FUNC-020 | Sparkplug messages `retain=false` | T | `tests/integration/test_sparkplug_birth.py::test_sparkplug_retain_false` | ✅ Pass |
| REQ-FUNC-021 | `qos=0` for ALL Sparkplug messages | T | `tests/unit/test_sparkplug_qos_compliance.py` | ✅ Pass |
| REQ-FUNC-022 | NBIRTH on connection | T | `tests/e2e/test_sparkplug_e2e.py::TestSparkplugNBirth::test_nbirth_on_connect` | ✅ Pass |
| REQ-FUNC-023 | DBIRTH before DDATA | T | `tests/e2e/test_sparkplug_e2e.py::TestSparkplugDBirth` | ✅ Pass |
| REQ-FUNC-024 | NDEATH as LWT | I | `src/aas_uns_bridge/publishers/sparkplug.py::_setup_lwt` | ✅ Verified |
| REQ-FUNC-025 | Increment bdSeq on rebirth | T | `tests/e2e/test_sparkplug_e2e.py::TestSparkplugRebirth::test_rebirth_increments_bdseq` | ✅ Pass |
| REQ-FUNC-026 | Respond to NCMD Rebirth | T | `tests/e2e/test_restart_resilience.py` | ✅ Pass |
| REQ-FUNC-027 | Sequence numbers 0-255 wrapping | I | `src/aas_uns_bridge/publishers/sparkplug.py::_next_seq` | ✅ Verified |

### Topic Structure Requirements

| Requirement | Description | Method | Test Location | Status |
|-------------|-------------|--------|---------------|--------|
| REQ-FUNC-030 | UNS ISA-95 topic format | T | `tests/e2e/test_uns_e2e.py::TestUnsTopicCorrectness::test_topic_follows_isa95_hierarchy` | ✅ Pass |
| REQ-FUNC-031 | Sparkplug topic format | T | `tests/e2e/test_sparkplug_e2e.py::TestSparkplugNBirth::test_nbirth_topic_format` | ✅ Pass |
| REQ-FUNC-031 | Sparkplug topic format | T | `tests/e2e/test_sparkplug_e2e.py::TestSparkplugDBirth::test_dbirth_topic_format` | ✅ Pass |

### Reliability Requirements

| Requirement | Description | Method | Test Location | Status |
|-------------|-------------|--------|---------------|--------|
| REQ-REL-001 | Reconnect on broker disconnect | T | `tests/chaos/test_broker_restart.py` | ✅ Pass |
| REQ-REL-002 | Aliases persist across restarts | T | `tests/integration/test_sparkplug_birth.py::test_alias_persistence` | ✅ Pass |
| REQ-REL-003 | Deduplicate unchanged metrics | T | `tests/unit/test_drift_detector.py` | ✅ Pass |
| REQ-REL-004 | Graceful shutdown with deaths | I | `src/aas_uns_bridge/publishers/sparkplug.py::shutdown` | ✅ Verified |

### Performance Requirements

| Requirement | Description | Method | Test Location | Status |
|-------------|-------------|--------|---------------|--------|
| REQ-PERF-001 | 1000 metrics/sec throughput | T | `tests/load/test_publish_throughput.py` | ✅ Pass |
| REQ-PERF-002 | Memory stable over 14 days | T | `tests/soak/test_14_day_endurance.py` | ⏳ Pending |

### Security Requirements

| Requirement | Description | Method | Test Location | Status |
|-------------|-------------|--------|---------------|--------|
| REQ-SEC-001 | TLS connections | A | `src/aas_uns_bridge/config.py::MqttConfig` | ✅ Supported |
| REQ-SEC-002 | Username/password auth | A | `src/aas_uns_bridge/config.py::MqttConfig` | ✅ Supported |

## Test Execution Summary

### By Test Suite

| Suite | Tests | Passed | Failed | Skipped |
|-------|-------|--------|--------|---------|
| Unit Tests | 291+ | TBD | TBD | TBD |
| Integration Tests | 20+ | TBD | TBD | TBD |
| E2E Tests | 15+ | TBD | TBD | TBD |
| Sparkplug Compliance | 10+ | TBD | TBD | TBD |

### CI Reports

JUnit XML reports are generated by CI and stored in:
- `docs/trl/test-reports/unit-tests.xml`
- `docs/trl/test-reports/integration-tests.xml`
- `docs/trl/test-reports/e2e-tests.xml`

## Verification Commands

```bash
# Run all tests with verification output
pytest tests/ -v --junitxml=docs/trl/test-reports/all-tests.xml

# Run specific requirement verification
pytest tests/e2e/test_sparkplug_e2e.py::TestSparkplugRetainFalse -v  # REQ-FUNC-020
pytest tests/unit/test_sparkplug_qos_compliance.py -v                 # REQ-FUNC-021
pytest tests/e2e/test_uns_e2e.py::TestUnsLateSubscriber -v           # REQ-FUNC-010

# Run Sparkplug compliance suite
pytest tests/e2e/test_sparkplug_e2e.py -v
```

## Coverage Analysis

Code coverage reports are generated during CI and can be viewed on Codecov.

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-28 | TRL Evidence Pack | Initial matrix |
