# Observer and Stats Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add stable snapshot Stats and bounded asynchronous Observer hooks to Engine, Listener, Session, and PacketConn without making telemetry part of transport correctness or adding core telemetry dependencies.

**Architecture:** Public value types live in the root `ogrenet` package and become mandatory methods on the root transport contracts. The portable `transport` backend owns atomic per-resource counters plus one bounded observer dispatcher per Engine; Stats remain authoritative, events are best-effort, and all event construction is guarded behind the disabled-observer fast path. Existing P0-1 admission accounting, P0-3 lifecycle ownership, and P0-4 typed errors remain the ownership sources rather than being duplicated or re-arbitrated.

**Tech Stack:** Go 1.25/1.26, standard library atomics/channels, existing `transport` runtime, `coder/websocket`, GitHub Actions race/benchmark gates.

**Spec:** `docs/superpowers/specs/2026-08-21-observer-stats-design.md`

## Global Constraints

- Core must not depend on OpenTelemetry, Prometheus, or a logging framework.
- `Observer.Observe(Event)` is the only observer callback entry point.
- Observer queue default capacity is exactly 1024 events; explicit capacity must be positive.
- Observer callbacks never execute on I/O/admission/lifecycle goroutines.
- Queue saturation drops events and increments `ObserverDroppedEvents`; Stats remain authoritative.
- Dispatcher shutdown must not close a channel that concurrent producers can send to.
- `Engine.Done()` must not wait for an externally blocked observer callback.
- Callback panic is recovered, counted in `ObserverPanics`, and never becomes a transport error.
- `Event.Bytes` and resource `BytesRX/BytesTX` count application payload bytes only.
- Session/Packet `QueuedBytes` reports bytes currently held by the existing send byte quota (encoded/encrypted retained bytes where applicable), because that is the runtime resource-pressure gauge.
- Existing P0-3 first-owner lifecycle behavior and P0-4 typed error identity must not change.
- Disabled observer event paths add zero per-event allocations and must not take observer-only timestamps or address snapshots.
- Existing graceful running `Send`/`TrySend` allocation gates remain unchanged.

---

## File map

Public contract:

- Create `observer.go`: `ResourceKind`, `EventKind`, `Event`, `Observer`.
- Create `stats.go`: immutable `EngineStats`, `ListenerStats`, `SessionStats`, `PacketConnStats` values.
- Modify `transport.go`: add mandatory `Stats()` to all four root resource contracts.

Portable implementation:

- Create `transport/stats.go`: atomic counters, stable age helper, snapshot builders, Engine Stats adapter.
- Create `transport/observer.go`: bounded dispatcher, non-blocking emission, panic isolation, stop semantics.
- Modify `transport/options.go`: observer configuration and default capacity.
- Modify `transport/errors.go`: direct `ErrInvalidObserverBuffer` configuration sentinel.
- Modify `transport/engine.go` / `engine_shutdown.go`: construct and stop dispatcher; expose Engine Stats.
- Modify `transport/limits.go`: preserve admission ownership and make per-listener current accounting available even when the listener limit is unlimited.
- Modify `transport/listener.go`, `websocket_server.go`: IDs, listener Stats, accept/reject/current accounting, close event.
- Modify `transport/conn.go`, `stream_graceful.go`: stream Stats/event hooks.
- Modify `transport/websocket.go`, `websocket_graceful.go`, `websocket_client.go`: WS/WSS Stats/event hooks without touching write-owner arbitration.
- Modify `transport/packet.go`, `packet_graceful.go`: UDP Stats/event hooks.
- Modify TLS/WS connection setup files only at existing connect/handshake ownership points.

Tests/benchmarks/docs:

- Create `transport/observability_contract_test.go`.
- Create `transport/observer_test.go`.
- Create `transport/stats_stream_test.go`.
- Create `transport/stats_websocket_test.go`.
- Create `transport/stats_packet_test.go`.
- Create `transport/stats_listener_test.go`.
- Create `transport/observability_race_test.go`.
- Create `transport/observability_benchmark_test.go`.
- Create `docs/observability.md`.
- Modify `.github/workflows/netpoll-v2.yml` with observer benchmark smoke/race repetition while retaining existing allocation gates.

---

### Task 1: Public observability contract and snapshot skeleton

**Files:**
- Create: `observer.go`
- Create: `stats.go`
- Modify: `transport.go`
- Create: `transport/observability_contract_test.go`
- Create: `transport/stats.go`
- Modify: `transport/engine.go`
- Modify: `transport/listener.go`
- Modify: `transport/websocket_server.go`
- Modify: `transport/conn.go`
- Modify: `transport/websocket.go`
- Modify: `transport/packet.go`

**Interfaces:**
- Produces: `Observer.Observe(Event)`, `Event`, `EventKind`, `ResourceKind`, `EngineStats`, `ListenerStats`, `SessionStats`, `PacketConnStats`, and mandatory `Stats()` methods.
- Later tasks may add counter updates, but no later task may change these public names or field meanings without changing the approved spec.

- [ ] **Step 1: Write the failing public contract test**

```go
func TestObservabilityPublicContracts(t *testing.T) {
    var _ ogrenet.Engine = (*Engine)(nil)
    var _ ogrenet.Listener = (*listener)(nil)
    var _ ogrenet.Session = (*conn)(nil)
    var _ ogrenet.Session = (*wsSession)(nil)
    var _ ogrenet.PacketConn = (*packetConn)(nil)

    var _ ogrenet.Observer = ogrenet.ObserverFunc(func(ogrenet.Event) {})
    _ = ogrenet.Event{Kind: ogrenet.EventRead, Resource: ogrenet.ResourceSession}
    _ = ogrenet.EngineStats{}
    _ = ogrenet.ListenerStats{}
    _ = ogrenet.SessionStats{}
    _ = ogrenet.PacketConnStats{}
}
```

Define `ObserverFunc` as the convenience adapter:

```go
type ObserverFunc func(Event)
func (f ObserverFunc) Observe(e Event) { if f != nil { f(e) } }
```

- [ ] **Step 2: Run the focused test and verify RED**

Run: `go test ./transport -run '^TestObservabilityPublicContracts$' -count=1`

Expected: compile failure because public observability types and `Stats()` methods do not exist.

- [ ] **Step 3: Add the public value types exactly as specified**

`observer.go` must define the four resource kinds, eight initial event kinds, `Event`, `Observer`, and `ObserverFunc`. `stats.go` must define the four immutable snapshot structs using the approved fields.

- [ ] **Step 4: Add `Stats()` to root contracts and minimal race-safe snapshot owners**

Create internal owners in `transport/stats.go`:

```go
type resourceAge struct {
    started time.Time
    finalNS atomic.Int64
}

type sessionCounters struct {
    bytesRX, bytesTX       atomic.Uint64
    messagesRX, messagesTX atomic.Uint64
    queuedFrames           atomic.Uint64
    queuedBytes            atomic.Uint64
    backpressure           atomic.Uint64
    decodeErrors           atomic.Uint64
    age                     resourceAge
}

type packetCounters struct {
    bytesRX, bytesTX       atomic.Uint64
    packetsRX, packetsTX   atomic.Uint64
    queuedPackets          atomic.Uint64
    queuedBytes            atomic.Uint64
    backpressure           atomic.Uint64
    droppedDatagrams       atomic.Uint64
    age                     resourceAge
}

type listenerCounters struct {
    accepted atomic.Uint64
    age      resourceAge
}
```

Attach one owner to each concrete resource, initialize age at construction, and implement `Stats()` snapshots. At this task, dynamic counters may remain zero; addresses/protocol/ID/age must already be correct. `Engine.Stats()` must map the existing admission snapshot into `ogrenet.EngineStats` without creating a second engine accounting system.

- [ ] **Step 5: Run focused and existing compile tests**

Run: `go test ./transport -run 'TestObservabilityPublicContracts|TestLimits' -count=1`

Expected: PASS.

- [ ] **Step 6: Commit**

Commit message: `runtime: add observability contracts`

---

### Task 2: Bounded observer dispatcher and configuration

**Files:**
- Create: `transport/observer.go`
- Create: `transport/observer_test.go`
- Modify: `transport/options.go`
- Modify: `transport/errors.go`
- Modify: `transport/engine.go`
- Modify: `transport/engine_shutdown.go`
- Modify: `transport/stats.go`

**Interfaces:**
- Consumes: root `ogrenet.Observer` / `ogrenet.Event` and `EngineStats` from Task 1.
- Produces: `WithObserver(ogrenet.Observer) Option`, `WithObserverBuffer(int) Option`, internal `observerDispatcher.emit(Event) bool`, `stop()`, `dropped()`, `panics()`.

- [ ] **Step 1: Write dispatcher tests before implementation**

Required test cases:

```go
func TestObserverDisabledCreatesNoDispatcher(t *testing.T) { /* New() => e.observer == nil */ }
func TestObserverBufferRejectsNonPositiveSize(t *testing.T) { /* errors.Is(err, ErrInvalidObserverBuffer) */ }
func TestObserverQueueOverflowDoesNotBlockProducer(t *testing.T) { /* blocked observer + tiny buffer => emit returns promptly and drop count rises */ }
func TestObserverPanicIsRecoveredAndCounted(t *testing.T) { /* panic first callback, later callback still runs */ }
func TestObserverStopDoesNotCloseProducerQueue(t *testing.T) { /* emit/stop race repeated; no panic */ }
func TestBlockedObserverDoesNotDelayEngineDone(t *testing.T) { /* callback blocks; Close/Done still completes */ }
```

- [ ] **Step 2: Run tests and verify RED**

Run: `go test ./transport -run '^TestObserver' -count=1`

Expected: FAIL because configuration/dispatcher do not exist.

- [ ] **Step 3: Add configuration**

In `config`:

```go
observer       ogrenet.Observer
observerBuffer int
```

Default `observerBuffer` to `1024`. Add:

```go
func WithObserver(observer ogrenet.Observer) Option
func WithObserverBuffer(size int) Option
```

`WithObserverBuffer(size <= 0)` returns direct `ErrInvalidObserverBuffer`.

- [ ] **Step 4: Implement one bounded dispatcher per Engine**

Use a never-closed event queue and separate stop signal:

```go
type observerDispatcher struct {
    observer ogrenet.Observer
    queue    chan ogrenet.Event
    stopCh   chan struct{}
    stopped  atomic.Bool
    stopOnce sync.Once
    dropped  atomic.Uint64
    panics   atomic.Uint64
}

func (d *observerDispatcher) emit(event ogrenet.Event) bool {
    if d == nil || d.stopped.Load() { return false }
    select {
    case d.queue <- event:
        return true
    default:
        d.dropped.Add(1)
        return false
    }
}
```

The worker serially invokes `Observe`, wraps each call in a `recover`, increments `panics`, and continues. `stop()` first marks `stopped`, then closes only `stopCh`; it never closes `queue` and never waits for the worker.

- [ ] **Step 5: Wire Engine lifetime**

`New` creates a dispatcher only when `cfg.observer != nil`. The engine final transition to done calls `observer.stop()` but does not join the observer worker. `Engine.Stats()` reads dropped/panic counters from the dispatcher in addition to admission counters.

- [ ] **Step 6: Run dispatcher tests under race**

Run: `go test -race ./transport -run '^TestObserver' -count=20`

Expected: PASS with no race/panic/deadlock.

- [ ] **Step 7: Commit**

Commit message: `transport: add bounded observer dispatcher`

---

### Task 3: Listener IDs and authoritative accept/reject/current stats

**Files:**
- Modify: `transport/limits.go`
- Modify: `transport/listener.go`
- Modify: `transport/websocket_server.go`
- Modify: `transport/engine.go`
- Create: `transport/stats_listener_test.go`

**Interfaces:**
- Consumes: `ListenerStats`, observer dispatcher.
- Produces: stable Engine-local Listener `ResourceID`, `EventAccept`, `EventClose`, listener accepted/rejected/current snapshots.

- [ ] **Step 1: Write failing listener accounting tests**

Cover both unlimited and limited listeners:

```go
func TestListenerStatsTrackAcceptedAndCurrentWithoutLimit(t *testing.T)
func TestListenerStatsTrackPerListenerRejection(t *testing.T)
func TestListenerStatsFreezeAgeAndCurrentAtClose(t *testing.T)
func TestAcceptEventCorrelatesListenerAndSessionIDs(t *testing.T)
```

Assert that unlimited listeners still report current children; do not derive `CurrentConnections` only from a capacity object that is nil when limit=0.

- [ ] **Step 2: Run and verify RED**

Run: `go test ./transport -run 'TestListenerStats|TestAcceptEvent' -count=1`

- [ ] **Step 3: Make listener capacity accounting exist for unlimited listeners**

Change `newListenerCapacity(0)` to return an accounting object with `limit == 0`. `acquire()` must increment `used` for all accepted leases and reject only when `limit > 0 && used >= limit`; `release()` decrements exactly once. Preserve the existing per-listener rejection counter.

- [ ] **Step 4: Assign Listener IDs and count adoption**

Use the Engine `nextID` allocator for Listener IDs as well as Session IDs. Increment `AcceptedConnections` only after child adoption succeeds. `CurrentConnections` comes from the same listener lease accounting that owns release. `RejectedConnections` comes from the listener capacity rejection count.

- [ ] **Step 5: Emit accept/close events after committed state**

For accepted sessions:

```go
Event{Kind: EventAccept, Resource: ResourceSession, ResourceID: session.ID(), ParentID: listener.id, ...}
```

Emit listener close exactly once after final age/error state is stable. WS/WSS listener paths must use the same ID/accounting rules.

- [ ] **Step 6: Run listener tests and existing limit/race tests**

Run: `go test -race ./transport -run 'TestListenerStats|TestAcceptEvent|Test.*Limit' -count=1`

Expected: PASS.

- [ ] **Step 7: Commit**

Commit message: `transport: expose listener observability`

---

### Task 4: TCP/TLS Session stats, payload accounting, backpressure, and events

**Files:**
- Modify: `transport/conn.go`
- Modify: `transport/stream_graceful.go`
- Modify: stream/TLS dial/listen setup files at existing connection/handshake ownership points
- Create: `transport/stats_stream_test.go`

**Interfaces:**
- Consumes: `sessionCounters`, dispatcher, typed `*transport.Error` ownership.
- Produces: TCP/TLS read/write/backpressure/close stats/events and connect/handshake events.

- [ ] **Step 1: Write failing stream tests**

Required cases:

```go
func TestTCPStatsCountApplicationPayloadNotWireBytes(t *testing.T)
func TestTLSStatsCountApplicationPayloadNotCiphertext(t *testing.T)
func TestStreamStatsTrackQueuedFramesAndQuotaBytes(t *testing.T)
func TestStreamTrySendBackpressureCountsOnce(t *testing.T)
func TestStreamCloseEventSeesFinalStatsAndTerminalError(t *testing.T)
func TestConnectEventUsesTypedFailureWithoutCreatingSession(t *testing.T)
func TestTLSHandshakeEventReportsDurationAndTypedFailure(t *testing.T)
```

The payload test must use a non-empty message whose encoded frame is larger than `len(msg.Data)` and assert `BytesTX == uint64(len(msg.Data))`.

- [ ] **Step 2: Run and verify RED**

Run: `go test ./transport -run 'Test(TCP|TLS|Stream|Connect)' -count=1`

- [ ] **Step 3: Preserve application payload length through outbound queue**

Extend stream outbound metadata:

```go
type outbound struct {
    frame        []byte
    ack          chan error
    bytes        int // existing quota-accounted encoded bytes
    payloadBytes int // application payload bytes
}
```

Set `payloadBytes = len(msg.Data)` when the send is admitted. Increment queued frame/quota-byte gauges exactly when quota+frame ownership transfers to the queue; decrement at every release/failure path exactly once.

- [ ] **Step 4: Count successful RX/TX at ownership points**

RX: after decode+validation, before observer event and `Handler.OnMessage`, increment `MessagesRX` and `BytesRX += len(msg.Data)`.

TX: after physical protocol write succeeds, before event/ack, increment `MessagesTX` and `BytesTX += payloadBytes`.

`TrySend` local admission failures that return `ErrWouldBlock` increment `Backpressure` once and optionally emit one backpressure event. Blocking `Send` capacity waits do not increment repeatedly.

- [ ] **Step 5: Freeze final age and emit close after terminal ownership**

Finalize age before close event and before `Done()` closes. Close event reuses the already-owned `Err()` object; do not normalize/classify again.

- [ ] **Step 6: Add connect/TLS handshake events only at existing ownership points**

Take observer-only start timestamps only when dispatcher is enabled. Failed pre-Session attempts use `ResourceID=0`; successful adopted Session events use its stable ID. Preserve caller context cancellation semantics: an event may carry the direct caller context error, but this must not create/populate a Session terminal error.

- [ ] **Step 7: Run stream tests, typed-error tests, and graceful tests under race**

Run: `go test -race ./transport -run 'Test(TCP|TLS|Stream|Connect|TransportError|Graceful)' -count=1`

Expected: PASS with existing terminal ownership unchanged.

- [ ] **Step 8: Commit**

Commit message: `transport: instrument stream sessions`

---

### Task 5: WebSocket/WSS Session stats and events without changing writer arbitration

**Files:**
- Modify: `transport/websocket.go`
- Modify: `transport/websocket_graceful.go`
- Modify: `transport/websocket_client.go`
- Modify: `transport/websocket_server.go`
- Create: `transport/stats_websocket_test.go`

**Interfaces:**
- Consumes: public SessionStats/event contract and existing `wsWriteState` terminal ownership.
- Produces: WS/WSS payload stats, queue gauges, backpressure, connect/handshake/read/write/close events.

- [ ] **Step 1: Write failing WS/WSS tests**

```go
func TestWebSocketStatsCountPlainApplicationPayload(t *testing.T)
func TestWebSocketStatsTrackQueuedQuotaBytes(t *testing.T)
func TestWebSocketTrySendBackpressureCountsOnce(t *testing.T)
func TestWebSocketNormalCloseEventHasNilError(t *testing.T)
func TestWebSocketFailureCloseEventPreservesOwnedTypedError(t *testing.T)
func TestWebSocketHandshakeEventCorrelation(t *testing.T)
```

For encrypted WS message-security coverage, assert `BytesTX == len(msg.Data)` while queue quota bytes may reflect retained encrypted payload length.

- [ ] **Step 2: Run and verify RED**

Run: `go test ./transport -run 'TestWebSocket.*(Stats|Event|Backpressure)' -count=1`

- [ ] **Step 3: Preserve plaintext payload length in `wsOutbound`**

Add `payloadBytes int` separately from existing `bytes` quota accounting. Count TX only on successful `ws.Write`; count RX after decode succeeds and immediately before callback.

- [ ] **Step 4: Instrument without modifying `wsWriteState` ownership**

No new observer code may participate in `begin/end/deferRead/timeoutCause` arbitration. Event emission occurs only after existing write/read/abort decisions are complete.

- [ ] **Step 5: Add WS/WSS handshake/connect events**

Measure only with observer enabled. Failed server upgrades retain Listener `ParentID`; failed client setup uses `ResourceID=0`; successful sessions use the adopted Session ID.

- [ ] **Step 6: Run WS typed-error and graceful ownership suites under race**

Run: `go test -race ./transport -run 'TestWebSocket|TestTransportErrorWebSocket|TestGraceful.*WebSocket' -count=1`

Expected: PASS with no change to terminal error winner.

- [ ] **Step 7: Commit**

Commit message: `transport: instrument websocket sessions`

---

### Task 6: UDP PacketConn stats, drop semantics, and events

**Files:**
- Modify: `transport/packet.go`
- Modify: `transport/packet_graceful.go`
- Create: `transport/stats_packet_test.go`

**Interfaces:**
- Consumes: `packetCounters`, dispatcher.
- Produces: UDP packet/payload counters, queue gauges, backpressure/drop/read/write/close events.

- [ ] **Step 1: Write failing UDP tests**

```go
func TestPacketStatsCountPayloadAndPackets(t *testing.T)
func TestPacketStatsTrackQueuedPacketsAndQuotaBytes(t *testing.T)
func TestPacketTrySendBackpressureCountsOnce(t *testing.T)
func TestPacketOversizeReceiveCountsDropNotRX(t *testing.T)
func TestPacketDropEventUsesReceivedPayloadSize(t *testing.T)
func TestPacketCloseEventSeesFinalStatsAndError(t *testing.T)
```

- [ ] **Step 2: Run and verify RED**

Run: `go test ./transport -run 'TestPacket.*(Stats|Drop|Backpressure|Event)' -count=1`

- [ ] **Step 3: Count queue ownership and successful I/O**

Queue gauges increment when a packet becomes owned by the outbound queue and decrement on every success/failure/drain path. `BytesTX/PacketsTX` increment only after successful UDP write; `BytesRX/PacketsRX` increment only after packet-size policy passes and before callback.

- [ ] **Step 4: Count oversize inbound datagrams as drops**

When `n > maxPacket`, increment `DroppedDatagrams` and emit `EventDrop{Bytes:uint64(n)}` if enabled. Do not increment RX counters and do not deliver to handler.

- [ ] **Step 5: Final age/close event**

Freeze age and gauges before exactly-once close event; preserve existing PacketConn terminal error ownership.

- [ ] **Step 6: Run UDP and resource-limit tests under race**

Run: `go test -race ./transport -run 'Test(Packet|UDP|TransportErrorUDP|.*QueuedBytes)' -count=1`

Expected: PASS.

- [ ] **Step 7: Commit**

Commit message: `transport: instrument packet connections`

---

### Task 7: Cross-resource race/stress invariants

**Files:**
- Create: `transport/observability_race_test.go`

**Interfaces:**
- Consumes all prior implementation.
- Produces race regression suite for Stats/observer shutdown invariants.

- [ ] **Step 1: Add deterministic races**

Required tests:

```go
func TestObservabilityRaceStatsVsSendClose(t *testing.T)
func TestObservabilityRaceObserverSaturationVsShutdown(t *testing.T)
func TestObservabilityRaceListenerAcceptRejectVsClose(t *testing.T)
func TestObservabilityRaceTerminalFailureVsStats(t *testing.T)
func TestObservabilityRaceStopVsEmit(t *testing.T)
```

Each test must have a deterministic synchronization point; do not use timing sleeps as the correctness oracle.

- [ ] **Step 2: Run repeated race loop**

Run: `go test -race ./transport -run '^TestObservabilityRace' -count=20`

Expected: PASS with no race, deadlock, negative/underflow gauge, or send-on-closed panic.

- [ ] **Step 3: Run full transport race suite**

Run: `go test -race ./transport -count=1`

Expected: PASS.

- [ ] **Step 4: Commit**

Commit message: `test: harden observability races`

---

### Task 8: Benchmarks, CI gates, and public documentation

**Files:**
- Create: `transport/observability_benchmark_test.go`
- Create: `docs/observability.md`
- Modify: `.github/workflows/netpoll-v2.yml`

**Interfaces:**
- Consumes completed P0-5 implementation.
- Produces regression evidence and user-facing ownership/cardinality guidance.

- [ ] **Step 1: Add benchmarks**

Required benchmark names:

```go
BenchmarkObserverDisabledEmitPath
BenchmarkObserverEnabledNoop
BenchmarkObserverSaturatedProducer
BenchmarkSessionStatsSnapshot
BenchmarkPacketStatsSnapshot
BenchmarkEngineStatsSnapshot
```

`BenchmarkObserverDisabledEmitPath` must report `0 allocs/op`. Stats snapshot benchmarks must report `0 allocs/op` for stable in-memory test resources; enabled observer overhead is reported, not assigned an arbitrary percentage gate.

- [ ] **Step 2: Run benchmark comparison locally/CI**

Run:

```bash
go test ./transport -run '^$' -bench 'BenchmarkObserver|Benchmark.*StatsSnapshot' -benchmem -benchtime=100x -count=3
go test ./transport -run '^$' -bench 'BenchmarkGraceful(SendRunning|TrySendRunning)' -benchmem -benchtime=1x -count=5
```

Expected: observer disabled and Stats snapshots at 0 allocs/op; existing graceful allocation thresholds remain within their current Go 1.25/1.26 limits.

- [ ] **Step 3: Document semantics**

`docs/observability.md` must cover:

- Stats authoritative vs events best-effort;
- exact payload-vs-quota byte semantics;
- event loss and `ObserverDroppedEvents`;
- blocked callback and `Engine.Done()` separation;
- panic isolation;
- Event IDs/ParentID lifetime;
- high-cardinality fields and adapter guidance;
- no event ordering guarantee across concurrent producers;
- error identity reuse from P0-4;
- example `ObserverFunc` and periodic Stats polling.

- [ ] **Step 4: Add CI verification**

Keep all existing gates and add:

```yaml
- name: Observability benchmark smoke
  run: >-
    go test ./transport -run '^$'
    -bench 'BenchmarkObserver|Benchmark.*StatsSnapshot'
    -benchmem -benchtime=100x

- name: Observability race loop
  if: matrix.go == '1.26.x'
  run: go test -race ./transport -run '^TestObservabilityRace' -count=20
```

Add an explicit awk allocation check only for benchmark samples whose semantics are deterministic: disabled observer emit and Stats snapshot must be 0 allocs/op. Do not weaken the existing graceful Send/TrySend gates.

- [ ] **Step 5: Run full verification**

Run:

```bash
gofmt -w .
go vet ./...
go test -race -count=1 ./...
go test ./transport -run '^$' -bench 'BenchmarkObserver|Benchmark.*StatsSnapshot|BenchmarkGraceful(SendRunning|TrySendRunning)' -benchmem -benchtime=1x
```

Expected: PASS.

- [ ] **Step 6: Commit**

Commit message: `runtime: document and gate observability`

---

## Final verification and PR readiness

- [ ] Fetch exact feature-branch head SHA.
- [ ] Open/update a draft PR referencing `#54` and `#38`.
- [ ] Require Linux Go 1.25/1.26 race, Windows, macOS, FreeBSD runtime classifier coverage, GmSSL, and cross-compile jobs to complete on the exact head.
- [ ] Verify the new observer race loop and allocation gates from workflow logs rather than only checking overall job status.
- [ ] Verify no OpenTelemetry/Prometheus dependency entered `go.mod`/`go.sum`.
- [ ] Verify no review thread reports lifecycle/error ownership regression.
- [ ] Update #54 with exact-head verification evidence and only then mark the PR Ready for review.
