# TRL Evidence Pack

Technology Readiness Level (TRL) evidence for **aas-uns-bridge**.

## Overview

This folder contains formal verification and validation (V&V) evidence supporting TRL claims for the AAS-UNS Bridge project. The bridge ingests Asset Administration Shell (AAS) content and publishes to dual planes:

1. **UNS (Unified Namespace)** - JSON payloads with `retain=true` for late-subscriber discovery
2. **Sparkplug B** - Protobuf payloads with NBIRTH/DBIRTH lifecycle for SCADA integration

## Folder Structure

```
docs/trl/
├── README.md                     # This file
├── requirements/
│   ├── system-requirements-spec.md   # Functional requirements (REQ-*)
│   └── verification-matrix.md        # Requirement → Test mapping
├── test-reports/
│   └── *.xml                         # JUnit XML from CI (auto-generated)
├── compliance/
│   └── sparkplug-checklist.md        # Sparkplug 3.0 spec compliance
├── interoperability/
│   ├── broker-matrix.md              # Tested MQTT brokers
│   └── consumer-matrix.md            # Tested consumers (Ignition, etc.)
├── operational/
│   ├── soak-test-template.md         # 14-day endurance log template
│   └── performance-baseline.md       # Throughput/latency benchmarks
└── release/
    └── release-checklist.md          # Release validation steps
```

## TRL Definitions

| TRL | Definition | Evidence Required |
|-----|------------|-------------------|
| **TRL 4** | Component validated in lab | Unit tests pass, requirements documented |
| **TRL 5** | Component validated in relevant environment | Integration tests pass, interoperability demonstrated |
| **TRL 6** | System demonstrated in relevant environment | 14-day soak test, performance baselines |

## Current Status

### TRL 4 Evidence
- [x] System requirements specification
- [x] Verification matrix (requirements → tests)
- [x] Unit test suite (291+ tests)
- [x] CI generates JUnit XML reports

### TRL 5 Evidence
- [x] Sparkplug 3.0 compliance checklist
- [x] QoS enforcement tests (qos=0, retain=false)
- [ ] Multi-broker interoperability (EMQX, HiveMQ)
- [ ] Consumer validation (Ignition, UMH)

### TRL 6 Evidence
- [ ] 14-day soak test execution
- [ ] Performance baselines documented
- [ ] Memory/resource stability verified

## Running Tests

```bash
# Unit tests with JUnit output
pytest tests/unit -v --junitxml=docs/trl/test-reports/unit-tests.xml

# Integration tests
pytest tests/integration -v --junitxml=docs/trl/test-reports/integration-tests.xml

# E2E tests
pytest tests/e2e -v --junitxml=docs/trl/test-reports/e2e-tests.xml

# Sparkplug QoS compliance
pytest tests/unit/test_sparkplug_qos_compliance.py -v
```

## Related Documentation

- [CLAUDE.md](../../CLAUDE.md) - Development guide
- [Architecture](../architecture.md) - System design (if exists)
- [Sparkplug B Specification](https://sparkplug.eclipse.org/specification/version/3.0/)
