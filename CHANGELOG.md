# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **TRL 8 Advancement Complete** - Production readiness validation
- Chaos engineering test suite for resilience validation
  - Network partition simulation tests
  - Broker restart recovery tests
  - Disk full scenario handling
  - Resource exhaustion tests (CPU, memory)
- Stability testing framework
  - Memory stability tests with leak detection
  - 8-hour endurance test framework
- Security test suite
  - MQTT topic fuzzing tests
  - Configuration injection tests
- Production documentation
  - Operations runbook with troubleshooting procedures
  - Deployment guide for production environments
  - Security hardening guide
- SECURITY.md vulnerability disclosure policy
- Audit logging for bidirectional sync operations
- State database size limits with LRU eviction
- Grafana dashboard with operational metrics panels
- Prometheus alerting rules for production monitoring
- Performance metrics for backpressure and queue monitoring

## [0.1.0] - 2025-01-28

### Added

- AAS ingestion capabilities
  - File watcher for AASX/JSON files using watchdog
  - AAS repository REST API polling
  - BaSyx Python SDK v2.0 integration
- Dual publication system
  - UNS retained topics with JSON payloads for late-subscriber discovery
  - Sparkplug B protocol support with NBIRTH/DBIRTH lifecycle for SCADA integration
- Bidirectional synchronization
  - MQTT to AAS write-back for command/control flows
- ISA-95 hierarchy mapping
  - globalAssetId to ISA-95 structure mapping (enterprise/site/area/line/asset)
  - MQTT topic sanitization
- State persistence
  - SQLite-based storage for Sparkplug aliases and birth cache
  - Hash-based deduplication to skip unchanged metric republishing
- Observability
  - Prometheus metrics endpoint
  - Fidelity scoring for data quality assessment
- Recursive submodel traversal
  - Flattening of submodel elements into ContextMetric dataclasses
  - Support for ReferenceElement, Entity, and RelationshipElement types
- Configuration system
  - YAML-based configuration (config.yaml, mappings.yaml)
  - Configuration validation via CLI
- Daemonized operation
  - Main loop with file watching and change detection
  - Automatic reconnection handling
  - Paho MQTT v2.0 with MQTTv5 protocol support
