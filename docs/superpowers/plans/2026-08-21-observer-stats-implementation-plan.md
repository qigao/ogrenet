# Observer and Stats Runtime Implementation Plan

Status: approved
Tracking: #54
Branch: feat/observer-stats

## Phase 1 — Public contract

1. Add root public types:
   - ResourceKind
   - EventKind
   - Event
   - Observer
   - EngineStats
   - SessionStats
   - PacketConnStats
   - ListenerStats

2. Extend application interfaces:
   - Engine.Stats()
   - Session.Stats()
   - PacketConn.Stats()
   - Listener.Stats()

3. Add compile-time contract tests.

## Phase 2 — Internal accounting

1. Add atomic counters owned by existing lifecycle owners.
2. Reuse admissionController accounting instead of duplicating resource state.
3. Add per-resource counters:
   - RX/TX bytes
   - message/packet counts
   - queue gauges
   - backpressure
   - decode/protocol errors
4. Add immutable snapshot builders.

## Phase 3 — Observer dispatcher

1. Add WithObserver option.
2. Add bounded event dispatcher.
3. Ensure:
   - no blocking on I/O paths
   - no channel close races
   - panic isolation
   - dropped event accounting

## Phase 4 — Event integration

Add emission only at existing ownership points:

- accept
- connect
- handshake
- read
- write
- backpressure
- drop
- close

Do not alter lifecycle ownership or typed error selection.

## Phase 5 — Verification

Tests:

- stats snapshot correctness
- concurrent Stats during Send/Close
- observer queue saturation
- observer panic recovery
- dispatcher shutdown races
- TCP/TLS/WS/WSS/UDP accounting

Benchmarks:

- observer disabled overhead
- observer enabled overhead
- saturated observer latency
- Stats snapshot allocations
- existing Send/TrySend gates

## Non-goals

- OpenTelemetry adapter
- Prometheus adapter
- native backend changes
- lifecycle redesign
- error taxonomy changes
