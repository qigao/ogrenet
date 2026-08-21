# Runtime Observability

`ogrenet` exposes two complementary observability surfaces:

- pull-based immutable `Stats()` snapshots on `Engine`, `Listener`, `Session`, and `PacketConn`;
- best-effort structured events delivered through `Observer.Observe(Event)`.

The two surfaces intentionally have different reliability semantics. Stats are authoritative runtime accounting. Observer events are diagnostic telemetry and may be dropped under load.

## Enabling an observer

The portable `transport` runtime accepts an observer through `WithObserver`:

```go
observer := ogrenet.ObserverFunc(func(event ogrenet.Event) {
    // Forward the structured event to an application-owned telemetry system.
    // Do not retain unbounded per-resource state here.
})

engine, err := transport.New(
    transport.WithObserver(observer),
)
```

`ObserverFunc` is only a convenience adapter for the single public callback:

```go
type Observer interface {
    Observe(Event)
}
```

The core runtime does not depend on OpenTelemetry, Prometheus, or a logging framework. Adapters can be built above this contract without changing transport correctness.

## Stats are authoritative

Every public runtime resource exposes a value snapshot:

```go
engineStats := engine.Stats()
sessionStats := session.Stats()
listenerStats := listener.Stats()
packetStats := packetConn.Stats()
```

Snapshots are safe to read concurrently with normal I/O and lifecycle operations. They are values, not live mutable counter objects.

When a resource terminates, its age is frozen before `Done()` becomes observable. Repeated snapshots after termination therefore report a stable final age.

Engine admission/accounting remains owned by the existing resource-governance subsystem. P0-5 exposes that accounting; it does not maintain a second independent connection or byte-budget model.

## Byte semantics

`SessionStats.BytesRX` and `SessionStats.BytesTX` count successfully delivered application message payload bytes.

They do **not** count:

- ogrenet framing bytes;
- WebSocket framing;
- TLS record/ciphertext overhead;
- TCP/IP headers;
- link-layer bytes.

`PacketConnStats.BytesRX` and `PacketConnStats.BytesTX` similarly count UDP application datagram payload bytes.

`QueuedBytes` has a different purpose. It is a resource-pressure gauge and reports bytes currently held by the existing send quota. For stream or message-security paths this can include encoded or encrypted retained bytes and therefore does not have to equal application payload bytes.

## Messages, packets, and drops

A successfully delivered application message increments the corresponding message counter once, regardless of partial socket reads/writes or internal framing.

A successfully delivered UDP datagram increments the corresponding packet counter once.

Inbound UDP datagrams rejected by runtime packet-size policy increment `DroppedDatagrams`, but do not increment `PacketsRX` or `BytesRX`. When observation is enabled, the associated `EventDrop.Bytes` reports the received datagram payload length.

Observer queue overflow is separate from protocol/datagram drops. It increments `EngineStats.ObserverDroppedEvents`; it never increments `DroppedDatagrams`.

## Event delivery

An enabled `Engine` owns one bounded observer dispatcher. The default queue capacity is 1024 events and can be changed with `WithObserverBuffer`.

I/O, admission, and lifecycle producers use non-blocking enqueue. If the queue is full, the event is discarded and `ObserverDroppedEvents` increases. Network progress does not wait for telemetry capacity.

The observer worker calls `Observe` serially, so one Engine never invokes its Observer concurrently from multiple worker goroutines. A callback that blocks can stop delivery from that Engine and eventually fill the bounded queue, but it cannot block socket I/O or admission processing.

`Engine.Done()` is a protocol/runtime shutdown barrier, not a telemetry flush barrier. Engine termination stops future observer delivery but does not wait for an externally blocked observer callback. Queued best-effort events may be discarded at shutdown.

## Panic isolation

A panic from `Observer.Observe` is recovered by the dispatcher. It increments `EngineStats.ObserverPanics`, does not fail a transport resource, does not replace a terminal transport error, and does not recursively emit another event. The dispatcher continues with later events.

Applications should still treat panicking telemetry code as a bug; recovery exists to prevent instrumentation from corrupting networking semantics.

## Event identity and correlation

Each event carries:

- `Kind`: accept, connect, handshake, read, write, backpressure, drop, or close;
- `Resource`: Engine, Listener, Session, or PacketConn category;
- `ResourceID`: Engine-local resource identity;
- `ParentID`: parent identity when a stable parent exists;
- protocol and local/remote addresses where available;
- application payload byte count where relevant;
- duration for explicitly timed setup operations;
- the already-owned error where relevant.

Accepted Sessions use the accepting Listener ID as `ParentID`. Failed setup attempts can have `ResourceID == 0` because no durable Session was created. Inbound failed handshakes retain the Listener ID in `ParentID` when available.

Resource IDs are stable only for the resource lifetime and only within the Engine that created them.

## Ordering

There is no total event order across concurrent runtime producers. The single dispatcher preserves the order in which events successfully enter its queue, but concurrent producers can race before that point.

Applications must not infer lifecycle correctness from event order. The runtime's resource state, `Done()`, `Err()`, and Stats snapshots remain the correctness surfaces.

Close events are emitted only after terminal error ownership and final Stats state have been committed, but the close event itself is still best effort and can be dropped.

## Typed errors

Observability is downstream of the P0-4 typed error pipeline:

```text
raw/library error
    -> lifecycle normalization
    -> typed operational classification
    -> terminal owner/store
    -> Stats / event observation
```

Observer code does not normalize or classify errors again and cannot participate in first-terminal-failure ownership. When an event carries a transport error, callers can use the same `errors.Is` / `errors.As` behavior documented in `runtime-errors.md`.

Explicit clean local close does not invent an operational error merely for telemetry.

## Backpressure

`Backpressure` counts non-blocking send attempts that fail local queue or byte-budget admission with `ErrWouldBlock` semantics. Blocking `Send` calls that wait for capacity do not repeatedly increment the counter while waiting.

When enabled, one `EventBackpressure` can be emitted for the failed non-blocking attempt. Event loss does not affect the authoritative counter.

## Cardinality guidance

The core exposes resource IDs and addresses because they are useful for structured diagnostics and tracing. They should normally be treated as high-cardinality data by external metric adapters.

Avoid using these as default metrics labels:

- `ResourceID`;
- peer IP/address;
- local ephemeral port;
- raw error text.

Prefer low-cardinality labels such as protocol, event kind, typed error kind, operation kind, and resource-limit kind. Resource IDs and addresses are generally more appropriate for traces or structured logs unless an adapter user explicitly opts into their cardinality cost.

## Polling example

A service can combine best-effort events with periodic authoritative snapshots:

```go
stats := engine.Stats()

fmt.Printf(
    "active=%d draining=%d queued_bytes=%d observer_dropped=%d\n",
    stats.ActiveConnections,
    stats.DrainingConnections,
    stats.GlobalQueuedBytes,
    stats.ObserverDroppedEvents,
)
```

This separation is intentional: events provide timely detail when capacity permits, while Stats provide the stable accounting source needed for health checks and metrics collection.
