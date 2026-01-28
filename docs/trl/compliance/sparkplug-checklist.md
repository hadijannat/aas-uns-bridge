# Sparkplug B 3.0 Compliance Checklist

**Project:** aas-uns-bridge
**Specification:** Sparkplug B Version 3.0
**Date:** 2026-01-28

## Overview

This checklist documents compliance with the Eclipse Sparkplug B Specification v3.0 for the aas-uns-bridge edge node implementation.

Reference: [Sparkplug Specification 3.0](https://sparkplug.eclipse.org/specification/version/3.0/)

## Compliance Summary

| Section | Status | Notes |
|---------|--------|-------|
| Topic Namespace | ✅ Compliant | `spBv1.0/{group}/{msg}/{node}[/{device}]` |
| NBIRTH | ✅ Compliant | QoS=0, retain=false, bdSeq present |
| DBIRTH | ✅ Compliant | QoS=0, retain=false, all metrics |
| NDEATH | ✅ Compliant | LWT, QoS=0, retain=false, bdSeq |
| DDEATH | ✅ Compliant | QoS=0, retain=false |
| DDATA | ✅ Compliant | QoS=0, retain=false |
| Rebirth | ✅ Compliant | NCMD handling, bdSeq increment |
| Sequence | ✅ Compliant | 0-255 wrapping |
| Payload | ✅ Compliant | Protobuf encoding |

## Detailed Checklist

### 1. Topic Namespace (Section 6.1)

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| [x] Namespace prefix `spBv1.0` | ✅ | `SPARKPLUG_NAMESPACE = "spBv1.0"` |
| [x] Topic format: `spBv1.0/{group_id}/{message_type}/{edge_node_id}` | ✅ | `_build_topic()` method |
| [x] Device topics: `spBv1.0/{group_id}/{message_type}/{edge_node_id}/{device_id}` | ✅ | `_build_topic(device_id=...)` |
| [x] Valid message types: NBIRTH, NDEATH, DBIRTH, DDEATH, NDATA, DDATA, NCMD, DCMD | ✅ | Implemented |

**Code Location:** `src/aas_uns_bridge/publishers/sparkplug.py:71-91`

### 2. NBIRTH (Section 6.4.1)

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| [x] Publish on initial connection | ✅ | `publish_nbirth()` called after connect |
| [x] QoS level 0 | ✅ | `qos=0` in publish call |
| [x] Retain flag = false | ✅ | `retain=False` in publish call |
| [x] Contains `bdSeq` metric | ✅ | bdSeq metric in payload |
| [x] Contains `Node Control/Rebirth` metric | ✅ | Rebirth control metric present |
| [x] All node metrics included | ✅ | Payload includes node-level metrics |

**Code Location:** `src/aas_uns_bridge/publishers/sparkplug.py:264-322`

**Test Evidence:**
- `tests/e2e/test_sparkplug_e2e.py::TestSparkplugNBirth::test_nbirth_on_connect`
- `tests/e2e/test_sparkplug_e2e.py::TestSparkplugRetainFalse::test_nbirth_not_retained`

### 3. DBIRTH (Section 6.4.2)

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| [x] Publish after NBIRTH | ✅ | Enforced by `_is_online` check |
| [x] QoS level 0 | ✅ | `qos=0` in publish call |
| [x] Retain flag = false | ✅ | `retain=False` in publish call |
| [x] Contains ALL device metrics | ✅ | Full metric list in DBIRTH |
| [x] Metrics have aliases | ✅ | AliasDB assigns unique aliases |
| [x] Publish before any DDATA | ✅ | `publish_ddata()` triggers DBIRTH if needed |

**Code Location:** `src/aas_uns_bridge/publishers/sparkplug.py:323-383`

**Test Evidence:**
- `tests/e2e/test_sparkplug_e2e.py::TestSparkplugDBirth::test_dbirth_topic_format`
- `tests/e2e/test_sparkplug_e2e.py::TestSparkplugRetainFalse::test_dbirth_not_retained`

### 4. NDEATH (Section 6.4.3)

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| [x] Configured as Last Will & Testament (LWT) | ✅ | `_setup_lwt()` on init |
| [x] QoS level 0 | ✅ | `qos=0` in LWT config |
| [x] Retain flag = false | ✅ | `retain=False` in LWT config |
| [x] Contains `bdSeq` metric | ✅ | `_build_ndeath_payload()` |
| [x] bdSeq matches last NBIRTH | ✅ | Uses `self._bd_seq` |

**Code Location:** `src/aas_uns_bridge/publishers/sparkplug.py:60-65`

### 5. DDEATH (Section 6.4.4)

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| [x] Publish when device goes offline | ✅ | `publish_ddeath()` method |
| [x] QoS level 0 | ✅ | `qos=0` in publish call |
| [x] Retain flag = false | ✅ | `retain=False` in publish call |

**Code Location:** `src/aas_uns_bridge/publishers/sparkplug.py:436-456`

### 6. DDATA (Section 6.4.6)

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| [x] Publish only after DBIRTH | ✅ | Device must be in `_devices` set |
| [x] QoS level 0 | ✅ | `qos=0` in publish call (FIXED) |
| [x] Retain flag = false | ✅ | `retain=False` in publish call |
| [x] Can use aliases instead of names | ✅ | Alias-only after DBIRTH |
| [x] Metrics with changed values only | ✅ | Caller provides changed metrics |

**Code Location:** `src/aas_uns_bridge/publishers/sparkplug.py:385-434`

**Bug Fix:** Line 429 changed from `qos=self.config.qos` to `qos=0`

### 7. Rebirth Handling (Section 6.6)

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| [x] Subscribe to NCMD topic | ✅ | After NBIRTH in `publish_nbirth()` |
| [x] Handle Rebirth command | ✅ | `_handle_ncmd()` method |
| [x] Increment bdSeq on rebirth | ✅ | `self._bd_seq += 1` in `rebirth()` |
| [x] Republish NBIRTH on rebirth | ✅ | `rebirth()` calls `publish_nbirth()` |
| [x] Republish all DBIRTHs on rebirth | ✅ | `rebirth()` calls `republish_dbirths()` |
| [x] Reset sequence number | ✅ | `self._seq = 0` on rebirth |

**Code Location:** `src/aas_uns_bridge/publishers/sparkplug.py:458-513`

**Test Evidence:**
- `tests/e2e/test_sparkplug_e2e.py::TestSparkplugRebirth::test_rebirth_increments_bdseq`

### 8. Sequence Numbers (Section 6.2)

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| [x] 8-bit unsigned integer (0-255) | ✅ | `% 256` wrapping |
| [x] Reset to 0 on BIRTH | ✅ | Reset in `rebirth()` |
| [x] Increment per message | ✅ | `_next_seq()` method |
| [x] Wrap from 255 to 0 | ✅ | `(self._seq + 1) % 256` |

**Code Location:** `src/aas_uns_bridge/publishers/sparkplug.py:93-97`

### 9. Payload Encoding (Section 7)

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| [x] Protobuf serialization | ✅ | `sparkplug_payload.py` |
| [x] Timestamp in milliseconds | ✅ | `int(time.time() * 1000)` |
| [x] Metrics array | ✅ | PayloadBuilder |
| [x] Metric properties supported | ✅ | Semantic properties |
| [x] JSON fallback for testing | ✅ | Import error handling |

**Code Location:** `src/aas_uns_bridge/publishers/sparkplug_payload.py`

## QoS Enforcement Verification

All Sparkplug message types use `qos=0` as required:

| Message Type | Code Location | QoS Value |
|--------------|---------------|-----------|
| NBIRTH | Line 301 | `qos=0` |
| DBIRTH | Line 369 | `qos=0` |
| DDATA | Line 429 | `qos=0` |
| DDEATH | Line 449 | `qos=0` |
| NDEATH (LWT) | Line 64 | `qos=0` |

## Retain=False Verification

All Sparkplug message types use `retain=false` as required:

| Message Type | Code Location | Retain Value |
|--------------|---------------|--------------|
| NBIRTH | Line 301 | `retain=False` |
| DBIRTH | Line 369 | `retain=False` |
| DDATA | Line 429 | `retain=False` |
| DDEATH | Line 449 | `retain=False` |
| NDEATH (LWT) | Line 64 | `retain=False` |

## Test Verification Commands

```bash
# Run Sparkplug compliance test suite
pytest tests/e2e/test_sparkplug_e2e.py -v

# Run QoS enforcement tests
pytest tests/unit/test_sparkplug_qos_compliance.py -v

# Run retain=false verification
pytest tests/e2e/test_sparkplug_e2e.py::TestSparkplugRetainFalse -v

# Run rebirth tests
pytest tests/e2e/test_sparkplug_e2e.py::TestSparkplugRebirth -v
```

## Known Limitations

1. **No TCK Integration**: Eclipse Sparkplug TCK not integrated (manual compliance verification only)
2. **Primary Host Awareness**: Not implemented (out of scope for edge node)
3. **STATE messages**: Not implemented (for primary host applications)

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-28 | TRL Evidence Pack | Initial checklist |
| 1.1 | 2026-01-28 | TRL Evidence Pack | DDATA QoS bug fixed |
