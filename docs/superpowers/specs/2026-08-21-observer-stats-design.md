# Observer and Stats Runtime Design

Status: approved architecture, implementation not started
Tracking issue: #54
Parent roadmap: #38 (P0-5)

## Goal

Add a stable, dependency-light observability contract to `ogrenet` that is shared by the portable runtime and future native backends. The contract must provide structured best-effort events and authoritative pull-based statistics without making OpenTelemetry, Prometheus, logging, or user callback latency part of core transport correctness.

The design preserves the P0-3 lifecycle ownership rules and the P0-4 typed error model. Observability consumes committed runtime state; it never arbitrates lifecycle or error ownership.

## Design principles

1. **Stats are authoritative; events are best effort.** Event loss must never make resource accounting incorrect.
2. **Observer callbacks never run on I/O runtime paths.** A slow observer may stall observation delivery, but not socket read/write, admission, close, or Engine shutdown.
3. **Disabled observer work starts after a nil guard.** No `Event`, address lookup, duration measurement, or error metadata is constructed when no observer is configured.
4. **One stable observer method.** Adding a new event kind must not require extending an interface implemented by adapters.
5. **Stats are part of the root application contract.** Portable and future native backends expose the same `Stats()` methods rather than optional capability interfaces.
6. **No unbounded cardinality or retained telemetry state in core.** External adapters own label/cardinality policy.
7. **No core telemetry dependency.** OpenTelemetry and Prometheus integration are follow-up adapters.

## Public API

### Resource kind

Structured events must identify what emitted them. Protocol plus ID is insufficient because a listener close and a session close can share the same protocol.

```go
type ResourceKind uint8

const (
    ResourceEngine ResourceKind = iota + 1
    ResourceListener
    ResourceSession
    ResourcePacketConn
)
```

### Event kind

```go
type EventKind uint8

const (
    EventAccept EventKind = iota + 1
    EventConnect
    EventHandshake
    EventRead
    EventWrite
    EventBackpressure
    EventDrop
    EventClose
)
```

The initial set is deliberately small. Resource-limit rejection totals are exposed through Stats; a separate rejection event kind is not required in P0-5.

### Observer

```go
type Observer interface {
    Observe(Event)
}
```

A single method is preferred over `OnAccept`, `OnConnect`, `OnRead`, and similar methods because a multi-method interface becomes source-incompatible whenever the event vocabulary expands.

### Event

```go
type Event struct {
    Kind       EventKind
    Resource   ResourceKind
    ResourceID uint64
    ParentID   uint64
    Protocol   Scheme
    Local      net.Addr
    Remote     net.Addr
    Bytes      uint64
    Duration   time.Duration
    Err        error
}
```

Semantics:

- `ResourceID` identifies the resource that owns the event. IDs are Engine-local and stable only for the resource lifetime.
- `ParentID` is zero unless a stable parent exists. Accepted Sessions use the Listener ID as `ParentID`.
- Failed pre-resource dial/handshake attempts may use `ResourceID == 0` because no durable Session exists. Failed inbound handshakes retain the Listener ID in `ParentID`.
- `Bytes` is **application payload bytes** for read/write/drop events. Framing bytes, WebSocket framing, TLS ciphertext overhead, IP headers, and link-layer bytes are excluded.
- `Duration` is populated only for operations that explicitly measure timing for observer delivery, initially connect and handshake. It may be zero for other event kinds.
- `Err` is the already-classified/owned operational error. Observer code never changes which error wins.
- Events never contain application payload data.

There is no total ordering guarantee across concurrent goroutines. The dispatcher preserves successful enqueue order, but adapters must not infer lifecycle correctness from event order. Close events are emitted only after terminal error and final Stats state have been committed.

## Stats API

Stats are immutable value snapshots. Calling `Stats()` does not subscribe the caller and never returns a live mutable counter object.

The root interfaces gain:

```go
type Engine interface {
    // existing methods ...
    Stats() EngineStats
}

type Session interface {
    // existing methods ...
    Stats() SessionStats
}

type PacketConn interface {
    // existing methods ...
    Stats() PacketConnStats
}

type Listener interface {
    // existing methods ...
    Stats() ListenerStats
}
```

This is an intentional application-contract change. Optional `StatsProvider` capability interfaces are rejected because they would force every caller and future native backend to branch on observability support.

### EngineStats

```go
type EngineStats struct {
    OpeningConnections  uint64
    ActiveConnections   uint64
    DrainingConnections uint64
    ActiveHandshakes    uint64
    PendingUpgrades     uint64
    GlobalQueuedBytes   uint64

    RejectedConnections uint64
    RejectedPeers       uint64
    RejectedListeners   uint64
    RejectedHandshakes  uint64
    RejectedUpgrades    uint64
    RejectedQueuedBytes uint64

    ObserverDroppedEvents uint64
    ObserverPanics        uint64
}
```

The existing admission controller remains the ownership source for opening/active/draining/handshake/upgrade/queued-byte accounting. P0-5 exposes that state rather than creating a second accounting system.

### SessionStats

```go
type SessionStats struct {
    ResourceID uint64
    Protocol   Scheme
    Local      net.Addr
    Remote     net.Addr
    Age        time.Duration

    BytesRX    uint64
    BytesTX    uint64
    MessagesRX uint64
    MessagesTX uint64

    QueuedFrames uint64
    QueuedBytes  uint64

    Backpressure uint64
    DecodeErrors uint64
}
```

`BytesRX/TX` count application payload bytes from successfully delivered/written messages. TCP/TLS framing bytes, TLS record overhead, and WebSocket framing are excluded. One application message increments `MessagesRX/TX` once regardless of partial syscalls or internal framing.

`Age` stops increasing when the resource terminates. After `Done()` closes, repeated Stats calls return the same final age.

### PacketConnStats

```go
type PacketConnStats struct {
    ResourceID uint64
    Protocol   Scheme
    Local      net.Addr
    Remote     net.Addr
    Age        time.Duration

    BytesRX   uint64
    BytesTX   uint64
    PacketsRX uint64
    PacketsTX uint64

    QueuedPackets uint64
    QueuedBytes   uint64

    Backpressure     uint64
    DroppedDatagrams uint64
}
```

`BytesRX/TX` count UDP application datagram payload bytes. Inbound datagrams discarded because they exceed the configured application maximum increment `DroppedDatagrams` by one and contribute their received payload length to the `EventDrop.Bytes` field, but they do not increment `PacketsRX` or `BytesRX`. Observer queue overflow does not increment `DroppedDatagrams`.

### ListenerStats

```go
type ListenerStats struct {
    ResourceID uint64
    Protocol   Scheme
    Local      net.Addr
    Age        time.Duration

    AcceptedConnections uint64
    RejectedConnections uint64
    CurrentConnections  uint64
}
```

`AcceptedConnections` increments only after a child Session is successfully adopted by the Engine, not merely after an OS-level `Accept`. `RejectedConnections` counts per-listener admission rejection. Handshake failures are not classified as listener admission rejections. `CurrentConnections` covers opening, active, and draining children whose listener capacity lease has not yet been released.

## Counter ownership and counting points

Counters attach to the component that already owns the relevant transition:

- Engine admission counters: `admissionController` / global byte quota.
- Listener accepted/rejected/current counters: listener capacity lease ownership.
- Session queue depth: send admission/release points that already own frame and byte quota.
- Packet queue depth: packet send admission/release points.
- Session RX: after a message has decoded and validated, immediately before `Handler.OnMessage`; increment bytes/messages first, enqueue the optional read event second, then invoke the application callback.
- Packet RX: after packet-size policy passes, immediately before `PacketHandler.OnPacket`; increment bytes/packets first, enqueue the optional read event second, then invoke the application callback.
- Session TX: after one outbound application message has completed its protocol write successfully; increment payload bytes/messages first, enqueue the optional write event second, then acknowledge a blocking `Send` if applicable.
- Packet TX: after one outbound datagram write succeeds; increment payload bytes/packets first, enqueue the optional write event second, then acknowledge a blocking send if applicable.
- Backpressure: once per `TrySend`/`TrySendTo` attempt that fails due to local queue/byte-budget admission pressure (`ErrWouldBlock`, including typed queued-byte limit composition).
- Decode/protocol errors: when an already-classified read/decode failure is committed as the terminal operational error.
- Final age: committed during resource finalization before the close event and before `Done()` becomes observable.

Stats must not infer counts by periodically inspecting queue lengths when a race-free ownership transition already exists.

## Observer configuration

The portable runtime adds:

```go
func WithObserver(observer ogrenet.Observer) Option
func WithObserverBuffer(size int) Option
```

Rules:

- nil observer means event delivery is disabled;
- the default bounded observer queue capacity is **1024 events**;
- an explicitly configured buffer size must be positive;
- configuring a buffer without an observer is allowed and has no runtime effect until an observer is present;
- Stats remain available regardless of observer configuration.

Configuration validation remains a direct configuration/programmer error and is not wrapped in the P0-4 operational error envelope.

## Dispatcher architecture

Each Engine with a non-nil observer owns exactly one bounded dispatcher queue and at most one observer worker goroutine.

Producer code must guard before event construction:

```go
if e.observer != nil {
    e.observer.emit(eventFields...)
}
```

The nil check occurs before constructing `Event`, calling `LocalAddr`/`RemoteAddr` solely for telemetry, or taking observer-only timestamps.

The dispatcher uses non-blocking enqueue semantics:

```go
select {
case queue <- event:
default:
    dropped.Add(1)
}
```

Consequences:

- network I/O never waits for observer queue capacity;
- event delivery is best effort;
- Stats remain correct if every event is dropped;
- observer queue retained memory is strictly bounded by 1024 events by default, or by the explicit configured size.

The worker invokes `Observer.Observe` serially. Serial delivery avoids concurrent callback requirements and bounds callback concurrency at one per Engine.

### Dispatcher shutdown

The event queue is **not closed by shutdown**. Closing a channel concurrently reachable by producers would create a send-on-closed race.

Instead the dispatcher has a one-way stopped state plus a stop signal:

- Engine termination marks the dispatcher stopped so future `emit` calls become no-ops;
- the worker observes the stop signal and exits when it is not inside user callback code;
- queued best-effort events may be discarded on stop;
- producers never send to a channel that can be closed underneath them.

The exact implementation may use atomics and a separate `done/stop` channel, but it must preserve those semantics.

### Slow observers

A callback that blocks forever may stall that Engine's observer worker, but it cannot stall I/O runtime goroutines. Once the queue fills, further events are dropped and counted.

`Engine.Done()` does not wait for an externally blocked observer callback. Observer execution is telemetry work, not part of the protocol shutdown barrier. If an in-progress user callback eventually returns after Engine termination, the worker exits.

This deliberately prevents user telemetry from redefining the meaning of `Done()`.

### Panics

The worker recovers around each `Observe` call. A panic:

- increments `ObserverPanics`;
- does not close or fail transport resources;
- does not replace `Session.Err`, `PacketConn.Err`, `Listener.Err`, or Engine shutdown errors;
- does not recursively emit another observer event.

After recovery the worker continues with subsequent queued events.

## Event emission points

### Accept

Emit after an inbound Session has been successfully adopted by the Engine and assigned its stable Session ID. `ParentID` is the Listener ID. Rejected accepts are represented by authoritative Listener/Engine rejection counters rather than a required event in P0-5.

### Connect

Emit after successful outbound transport connection establishment. A failed outbound connect may emit an event with `ResourceID == 0` and the typed P0-4 error when an observer is enabled. Connect duration is measured only when observer delivery is enabled.

### Handshake

Emit after TLS or WebSocket/WSS handshake completion, successful or failed. Handshake error fields reuse the existing typed error model. Failed pre-Session handshakes use `ResourceID == 0`; inbound failures retain the Listener ID in `ParentID`. Handshake duration is measured only when observer delivery is enabled.

### Read

Emit one event per successfully delivered application message/packet after Stats counters are updated and immediately before the corresponding application callback. `Bytes` is application payload size.

### Write

Emit one event per successfully completed outbound application message/packet after Stats counters are updated and before acknowledging a blocking send. `Bytes` is application payload size. Failed writes are represented by the terminal close/error path; P0-5 does not require a duplicate write-error event.

### Backpressure

Emit when a non-blocking send attempt returns local admission backpressure. A blocking `Send` that merely waits for capacity does not emit repeated backpressure events.

### Drop

Initially used for inbound datagrams intentionally discarded by runtime policy, such as exceeding configured packet size. Observer queue overflow is counted internally and does not recursively produce `EventDrop`.

### Close

Emit exactly once per Listener, Session, and PacketConn finalization after terminal error ownership, final queue gauges, and final age are committed. Explicit clean close uses `Err == nil`.

## Interaction with P0-4 typed errors

Observer events reuse existing operational error objects. The pipeline remains one-way:

```text
raw/library error
    -> normalize lifecycle artifact
    -> classify/envelope (*transport.Error)
    -> terminal owner/store
    -> stats/event observation
```

Observability never reclassifies an existing `*transport.Error` and never races to choose terminal ownership.

## Cardinality guidance

The core exposes resource IDs and addresses because they are useful for structured events and diagnostics, but it does not create metrics label maps.

Adapters must treat these as high-cardinality by default:

- ResourceID
- peer IP/address
- local ephemeral port
- raw error strings

Recommended low-cardinality external metric labels are protocol, event/error kind, limit kind, and coarse operation kind. `ResourceID` and addresses belong in traces/log-style events unless an adapter user explicitly opts into high-cardinality metrics.

## Performance model

Stats add fixed atomic/accounting work at existing ownership transitions. Observer delivery adds no allocation-producing work beyond the nil branch when disabled.

Required benchmarks:

1. existing graceful `Send`/`TrySend` allocation gates remain unchanged;
2. observer-disabled send/read benchmark verifies zero additional allocations on the event path;
3. observer-enabled no-op observer benchmark measures CPU/alloc overhead;
4. saturated observer benchmark proves producer latency remains bounded while `ObserverDroppedEvents` increases;
5. Stats snapshot benchmark verifies zero allocations per snapshot with the chosen value representation.

The acceptance target is semantic first: no per-event allocation when disabled and no regression of existing running Send/TrySend allocation limits. Enabled observer CPU overhead is reported and tracked rather than hidden behind an arbitrary initial percentage threshold.

## Concurrency and lifecycle invariants

- Stats counters are race-safe under concurrent read/write/close/shutdown.
- No counter transition may acquire locks in an order that can invert existing lifecycle/admission locks.
- Event enqueue never invokes user code and never waits for capacity.
- User observer code runs only on the dispatcher worker.
- Close event emission cannot occur before terminal error is stable.
- Queue/frame byte gauges return to zero after resource finalization.
- Listener current-child gauges return to zero after all child leases release.
- Engine resource gauges remain driven by the admission owner and reach zero at shutdown completion.
- Observer queue state and callback completion are excluded from the Engine protocol shutdown barrier.
- Observer shutdown cannot race producers into a send-on-closed panic.

## Testing strategy

### Contract tests

- root interface compile-time coverage for all new `Stats()` methods;
- snapshot field semantics and stable final age;
- Session TCP/TLS/WS/WSS RX/TX payload/message accounting;
- UDP payload/packet/drop accounting;
- Listener accept/reject/current accounting;
- Engine admission/rejection snapshot parity with existing internal accounting.

### Observer tests

- event kind/resource/ID/parent correlation;
- successful accept/connect/handshake/read/write/close coverage;
- typed error preservation in failed connect/handshake/close events;
- backpressure and UDP drop events;
- bounded queue overflow increments drop count;
- blocked observer does not stall runtime progress or Engine `Done()`;
- callback panic is recovered and counted;
- clean explicit close does not invent an error;
- dispatcher stop races cannot panic producers.

### Race/stress tests

- Stats during concurrent Send/TrySend/Close;
- observer queue saturation during shutdown;
- listener accept/reject races;
- terminal failure vs close with concurrent Stats snapshots;
- repeated blocked/panicking observer cases under `-race`.

### Benchmarks / CI

- observer disabled vs no-op enabled;
- saturated observer producer path;
- Stats snapshot;
- keep the P0-3 graceful allocation gate as a hard regression check.

## File/module direction

Public contract:

```text
stats.go                # root ogrenet Stats value types
observer.go             # root ogrenet Event/Observer/ResourceKind/EventKind
transport.go            # add Stats() to root interfaces
```

Portable implementation:

```text
transport/stats.go      # atomic counters and snapshot builders
transport/observer.go   # bounded dispatcher and panic isolation
transport/options.go    # WithObserver / WithObserverBuffer
```

Existing transport files receive only local counting/emission hooks at ownership points. No unrelated lifecycle refactor is part of P0-5.

## Non-goals

- OpenTelemetry SDK/exporter dependency in core
- Prometheus client dependency in core
- logging framework integration
- runtime retry or business policy
- native epoll/kqueue/IOCP high-level backend implementation
- lifecycle ownership redesign from P0-3
- error taxonomy redesign from P0-4
- payload capture, packet tracing, or arbitrary user metadata
- durable/reliable event delivery
- physical wire-byte/ciphertext accounting

## Follow-up work

After this contract is stable, adapters such as `ogrenet/otel` can translate Stats/events into traces and metrics with explicit cardinality policy. Those adapters are separate PRs and must not expand the P0-5 core surface unless a concrete missing primitive is demonstrated.

## Acceptance summary

P0-5 is complete when:

- all root resources expose stable snapshot Stats;
- structured observer delivery is bounded and asynchronous;
- slow/panicking observers cannot alter transport correctness or shutdown barriers;
- dropped observation events are visible through authoritative counters;
- Stats remain correct when events are dropped;
- disabled observer paths add no per-event allocation;
- existing Send/TrySend allocation gates remain green;
- cross-protocol and race coverage verifies counters and event semantics;
- observability/cardinality semantics are documented;
- no OTel/Prometheus dependency enters core.
