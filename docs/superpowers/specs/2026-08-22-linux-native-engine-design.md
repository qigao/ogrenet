# Linux Native Engine Architecture

Status: design approved in chat; written-spec review pending

Parent roadmap: #38
Native Engine umbrella: #56
Linux implementation tracking: #57

## 1. Goal

Add the first production native high-level Engine for Linux using the existing low-level `epoll` package while preserving the stable root `ogrenet.Engine`, `Session`, `HalfCloseSession`, `Listener`, and `PacketConn` contracts.

The portable `transport.Engine` remains the correctness/reference backend. Native operation is explicit, never automatic, and never silently falls back to the portable backend.

The first native phase supports TCP and UDP. TLS, WS, and WSS remain unsupported by the native epoll Engine until their socket ownership can be implemented without splitting one connection between epoll and Go's blocking `net.Conn` runtime.

## 2. Non-negotiable compatibility boundary

P0 established semantics that the native backend must preserve for supported protocols:

- Engine-wide admission and global byte quota;
- timeout/deadline behavior;
- graceful Session shutdown and TCP half-close;
- first-terminal-error ownership and typed transport errors;
- snapshot Stats ownership;
- best-effort Observer events;
- `Done()` as a lifecycle barrier;
- serialized per-Session handler callbacks.

The native backend changes how socket progress is driven. It does not define a second application contract.

## 3. Rejected architectures

### 3.1 Replace `transport.Engine` internals with a backend interface

Rejected. This would put backend conditionals through the already-stable portable implementation and reopen the P0-1 through P0-5 regression surface. Portable transport should remain simple and independently understandable.

### 3.2 Add high-level runtime behavior directly to `epoll`, `kqueue`, and `iocp`

Rejected. Those packages intentionally expose their native readiness/completion primitives. They must remain useful as low-level building blocks and must not accumulate Session framing, admission, callbacks, Stats, or protocol policy.

### 3.3 Pretend TLS/WS/WSS are native while delegating their I/O to Go netpoll

Rejected. A connection cannot have two independent I/O owners without making shutdown, readiness, timeout, and terminal-error arbitration ambiguous. Unsupported capability is preferable to a misleading backend label.

## 4. Public API and package placement

The native Engine is a separate concrete implementation in the existing `transport` package. It is not a mode field inside the portable `Engine`.

This placement is deliberate:

- the existing `transport.Option` configuration can be reused without duplicating config APIs;
- native and portable implementations return the exact same public `transport.Error`, `TimeoutError`, `LimitError`, and sentinel identities;
- shared package-private protocol/configuration helpers remain available without exporting implementation details;
- the low-level top-level `epoll` package stays independent.

### 4.1 Constructor

Add:

```go
type EpollConfig struct {
    Pollers         int
    EventBatch      int
    CallbackWorkers int
    CallbackQueue   int
    IOBudgetBytes   int
    IOBudgetOps     int
}

func NewEpoll(cfg EpollConfig, opts ...Option) (ogrenet.Engine, error)
```

`transport.New(opts...)` keeps its current meaning and always returns the portable backend.

`NewEpoll` is present on every supported build target so cross-platform applications can compile shared construction code. On non-Linux builds it returns `ErrBackendUnsupported` without applying options or creating resources.

On Linux it applies the same transport `Option` set as `New` and then constructs an independent epoll Engine.

### 4.2 EpollConfig defaults

Zero means use the production default. Negative values are invalid.

Resolved defaults:

```text
Pollers         = max(1, GOMAXPROCS)
EventBatch      = 256 events per epoll_wait buffer
CallbackWorkers = max(1, GOMAXPROCS)
CallbackQueue   = min(1024, max(64, 4 * CallbackWorkers))
IOBudgetBytes   = 256 KiB per resource turn
IOBudgetOps     = 64 syscalls/messages/accepts per resource turn
```

The resolved values are immutable after Engine construction.

Invalid values return an error wrapping `ErrInvalidEpollConfig`. Validation must also reject integer-overflow cases when deriving queue/buffer sizes.

### 4.3 Capability errors

Add direct configuration/capability sentinels:

```go
var ErrBackendUnsupported = errors.New("transport: backend unsupported on this platform")
var ErrProtocolUnsupported = errors.New("transport: protocol unsupported by backend")
var ErrInvalidEpollConfig = errors.New("transport: invalid epoll configuration")
```

For the Linux epoll Engine:

- `Listen` / `Dial` accept only `tcp`;
- `ListenPacket` / `DialPacket` accept only `udp`;
- `tls`, `ws`, and `wss` fail with `ErrProtocolUnsupported` before DNS, admission, fd creation, or Observer setup events;
- protocol mismatch between a stream method and `udp`, or a packet method and `tcp`, continues to use the existing `ErrProtocolMismatch` semantics.

Unsupported capability is a configuration/programmer error and is not wrapped in `*transport.Error`.

TLS/WS-specific transport options may be supplied to `NewEpoll`; they remain inert unless a future native implementation supports those schemes. Their presence never enables fallback.

## 5. Shared semantic core

P1-6A creates `internal/runtimecore` for ownership primitives that can be shared without knowing whether I/O is portable, readiness-driven, or completion-driven.

The extraction is incremental, not a big-bang rewrite.

Initial candidates:

```text
internal/runtimecore/
    quota.go        bounded byte accounting
    send_gate.go    send admission close/drain gate
    lifecycle.go    protocol-independent Session lifecycle state
    observer.go     bounded Observer dispatcher
    stats.go        atomic counters/resource age
```

Rules:

1. `runtimecore` contains no socket syscalls, `net.Conn`, epoll/kqueue/IOCP types, DNS, TLS, or WebSocket code.
2. `runtimecore` may depend on root `ogrenet` contracts but must not import `transport`; this prevents an import cycle.
3. Public error envelope construction/classification and public option resolution remain in `transport`, where both portable and epoll implementations can reuse them directly.
4. Admission code that currently depends tightly on `transport.Limits` / `LimitError` may remain in `transport` until it has a clean scalar/internal boundary. Shared code does not need to be moved merely to satisfy a directory diagram.
5. Each primitive is migrated independently with the existing portable tests green before the next migration.

The intent is one semantic owner, not maximum file movement.

## 6. Linux reactor topology

```text
                          epoll Engine
                              |
              +---------------+---------------+
              |               |               |
          reactor 0       reactor 1       reactor N-1
              |               |               |
        epoll.Wait(1)    epoll.Wait(1)    epoll.Wait(1)
              |               |               |
        fixed fd owner    fixed fd owner    fixed fd owner
              +---------------+---------------+
                              |
                    bounded callback executor
                              |
                 user Handler / PacketHandler
```

One goroutine owns each reactor and is the only goroutine that performs physical I/O on fds assigned to that reactor.

No native Session or PacketConn gets a reader goroutine, writer goroutine, or timeout watchdog goroutine.

## 7. Resource identity and epoll Data

The Engine's monotonic resource ID allocator remains the single ID namespace for listeners, sessions, and packet sockets.

The epoll `Event.Data` field stores that resource ID, never a Go pointer.

Each reactor maintains a reactor-owned map:

```go
map[uint64]reactorResource
```

A stale epoll event after `Del`/close is harmless: its resource ID no longer exists in the map and is ignored. IDs are not reused during an Engine lifetime.

The allocator must fail safely rather than emit the low-level epoll package's reserved wake value if uint64 exhaustion is ever reached.

## 8. Reactor inbox and wake coalescing

Do not put every Send/Close/callback completion into a generic channel that can become a second hidden queue.

Each reactor owns a small mutex-protected intrusive inbox of resources needing reactor attention. A resource embeds only its queue linkage and an `inboxQueued` flag.

Producers update resource-owned state first and then signal the owning reactor:

```text
Send/TrySend       -> enqueue outbound -> signal resource
Close/Shutdown     -> set lifecycle flag -> signal resource
callback complete  -> set callback result -> signal resource
cross-reactor handoff -> initialize resource -> signal target reactor
```

A resource can appear in the inbox at most once at a time. Inbox retained memory is therefore O(number of owned resources) with no per-signal allocation.

The reactor also owns a `wakePending` state. The first producer transitioning an empty/asleep inbox calls `Poller.Wake()`. Additional producers only append/deduplicate work. Before entering `epoll_wait`, the reactor drains the inbox and performs the wake-state handshake under the inbox mutex so a producer can never create a lost wake race.

This is intentionally a short critical section, not a custom lock-free MPSC queue. A lock-free replacement is out of scope unless profiling shows this mutex is a material bottleneck.

## 9. Reactor event loop

Conceptually:

```text
for engine/reactor is alive:
    drain resource inbox
    run explicitly requeued fair-work items
    run expired deadlines
    timeout = min(next deadline, 0 if runnable work remains)
    events = epoll_wait(timeout)
    dispatch each readiness event with per-resource fairness budgets
```

All sockets use non-blocking mode. Readiness paths drain until one of:

- `EAGAIN`;
- the configured operation budget;
- the configured byte budget;
- application callback capacity is unavailable;
- codec ownership is temporarily unavailable;
- resource lifecycle says no more I/O is legal.

If a fairness budget is reached before `EAGAIN`, the resource is placed on the reactor-local runnable list. The implementation must not rely on a second edge being generated for work it deliberately left ready.

## 10. Fairness

One busy connection must not monopolize a shard.

Per reactor turn, one resource may perform at most:

- `IOBudgetOps` accept/read/write/datagram operations; and
- `IOBudgetBytes` of stream payload/socket progress where a byte budget applies.

The first limit reached yields the resource and requeues it locally if progress can continue.

Accept loops use the operation budget. UDP packet receive/send loops use both operation and byte budgets. TCP read/write loops use both.

Fairness limits are scheduler mechanics only; they do not alter Send completion semantics, quotas, or Stats.

## 11. Listener and accept ownership

A TCP listener fd is owned by exactly one reactor. v1 does not use `SO_REUSEPORT` fan-out.

The listener reactor performs `accept4` with non-blocking + close-on-exec flags and drains accepts according to the fairness budget.

For every accepted socket:

1. create the opening/admission lease using the existing Engine admission semantics;
2. configure TCP socket options;
3. select a target reactor round-robin;
4. create the native Session resource and transfer it to the target reactor inbox;
5. ownership transfers exactly once when the target reactor registers the fd;
6. any failure before registration closes the fd and releases the lease exactly once.

The listener reactor is the handoff owner until target registration succeeds. Engine shutdown tracks this opening/handoff state, so `Done()` cannot close while a socket is between accept and registration.

No target-reactor command queue can reject the handoff merely because of queue saturation; the intrusive resource inbox retains one node already owned by the resource.

## 12. Dial ownership and DNS

`Dial` remains a caller-blocking operation and keeps context cancellation semantics.

DNS is not part of the native data-plane ownership problem. In v1:

- IP literals bypass DNS;
- hostnames use Go's `net.Resolver` under the caller/Connect timeout;
- resolved addresses are attempted sequentially in resolver order;
- no Happy Eyeballs racing is introduced in P1-6;
- DNS failures continue to use the P0 typed error taxonomy.

After address preparation, the chosen reactor owns socket creation and non-blocking connect:

```text
socket -> connect
       -> success immediately, or
       -> EINPROGRESS + EPOLLOUT
       -> SO_ERROR completion check
```

The reactor reports the final result to the waiting Dial caller. Caller context cancellation is posted as operation state and wakes the reactor; cancellation cannot close or mutate the fd from the caller goroutine.

P1-11 may later replace the resolver/address-racing policy without changing reactor fd ownership.

## 13. TCP socket setup

Native TCP supports IPv4 and IPv6 sockets. It applies the existing `TCPConfig` semantics using native socket options before the Session becomes active:

- TCP_NODELAY;
- SO_KEEPALIVE and keepalive period where supported;
- receive buffer size;
- send buffer size.

Setup failures follow the existing typed operational/configuration error rules and release admission/fd ownership exactly once.

## 14. Framing, message cipher, and codec serialization

Native TCP reuses the existing framer/cipher configuration, including custom `FramerFactory` and per-session cipher factories.

Stateful codecs create a concurrency problem because Send encoding occurs on application goroutines while decode occurs on the reactor. The reactor must never block on a codec mutex.

Each native Session therefore owns one codec admission token:

- `Send(ctx)` waits for the codec token subject to `ctx` and lifecycle state;
- `TrySend` tries the token and returns the existing backpressure semantics if it is unavailable;
- encoding executes synchronously on the caller after successful non-blocking admission, preserving current TrySend behavior;
- reactor decode tries the same token but never waits;
- if decode cannot acquire it, read processing for that resource is paused and the next encoder release signals the reactor;
- decode releases the token between complete application messages to preserve fairness for Send encoders.

This preserves one serialization domain for mutable custom framers/ciphers without allowing application work to block the epoll loop.

The default wire format and message-security semantics remain unchanged.

## 15. Outbound Send/TrySend path

Application goroutines perform only admission, validation, synchronous encoding, queue ownership transfer, and waiting for completion. They never call `write(2)` on the socket.

Conceptual flow:

```text
Send/TrySend
  -> lifecycle send gate
  -> frame-count admission
  -> codec admission
  -> encode/cipher
  -> local + global byte quota
  -> bounded per-session outbound queue
  -> signal owning reactor
  -> reactor writes/continues partial write on EPOLLOUT
```

`Send(ctx)` waits for its physical frame write result after admission. Caller cancellation after queue ownership transfer keeps the existing semantic that the frame may still be written even if `Send` returns the caller context cause.

`TrySend` never waits for reactor progress or network I/O. Queue/codec/quota unavailability maps to the same P0 backpressure behavior.

The reactor keeps offset state for partial writes. Queue/global byte ownership is released only when the frame is completely written or terminally failed/aborted.

EPOLLOUT interest is enabled only while a resource has incomplete physical output and disabled after the queue drains.

## 16. Inbound TCP path

The reactor owns socket reads and the incomplete encoded read buffer.

Before reading work that may produce an application callback, the reactor reserves callback-executor capacity. If no capacity is available, the resource is read-paused without consuming additional socket bytes.

With capacity reserved, the reactor reads/decodes until one complete application message is ready, then submits exactly one `OnMessage` task for that Session and marks its callback state busy.

While the callback is busy, that Session does not produce another application callback. Its socket read path stays paused. Kernel receive buffering provides natural upstream backpressure.

When the callback finishes, the executor signals the owning reactor. The reactor immediately retries read/decode before relying on a future edge, so edge-triggered readiness cannot be lost while reads were intentionally paused.

Incomplete framed bytes remain bounded by the existing `MaxBufferedRead`; application message size remains bounded by `MaxMessageBytes`.

## 17. Callback executor

User callbacks never run on reactor goroutines.

The Engine owns one bounded callback executor with:

- `CallbackWorkers` worker goroutines;
- a `CallbackQueue` bounded task queue;
- a reservation count covering running + queued tasks;
- one in-flight callback at a time per Session/PacketConn.

Lifecycle order remains:

```text
Session:     OnOpen -> OnMessage* -> OnClose
PacketConn:  OnPacket* -> OnClose
```

`OnOpen` must complete before enabling Session reads. `OnClose` is queued only after no later message callback can be created.

The executor never drops application callbacks.

If callback capacity is exhausted, reactors continue processing writes, closes, deadlines, and control work but pause new application reads. Complete callback saturation can therefore apply global inbound backpressure without blocking a reactor goroutine or creating unbounded tasks.

Retained callback payload memory is hard-bounded by callback task count multiplied by the configured maximum application message/datagram size, plus already-bounded per-resource incomplete read buffers. The implementation must document the resolved worst-case bound in benchmark/test diagnostics.

A user callback that never returns can still keep that resource and ultimately `Engine.Done()` open. This matches the existing portable Handler barrier semantics. `Shutdown(ctx)` may return its context cause; `Close()` cannot forcibly unwind user Go code.

Observer callbacks remain outside this barrier, as defined by P0-5.

## 18. TCP graceful lifecycle

The native state machine preserves the P0-3 ownership model.

### Local graceful Shutdown

1. stop new Send/TrySend admission;
2. drain already-admitted outbound frames through the reactor;
3. issue `shutdown(fd, SHUT_WR)` on the owning reactor;
4. expose write-half completion;
5. continue reading until peer FIN / terminal failure / caller-owned graceful timeout policy;
6. finalize only after callback lifecycle completion.

### CloseWrite

`HalfCloseSession.CloseWrite(ctx)` performs steps 1-4 without requiring the read half to close.

### Peer FIN

A clean peer FIN marks `ReadClosed()` and prevents future inbound messages. It is not by itself a terminal `Session.Err()`. The local write half may remain usable until explicitly closed/shutdown/failed.

### Abort

`Close()` sets abort state and signals the reactor. The owning reactor closes the fd. Derived post-close readiness/errors cannot replace an already-owned terminal error.

## 19. Native deadline scheduler

Each reactor owns a deadline min-heap. No deadline entry owns a goroutine or `time.Timer`.

Deadline records include a resource ID, deadline kind, deadline timestamp, and generation/version. Updating activity increments the generation; stale heap entries are ignored when popped.

The nearest live deadline determines the `epoll_wait` timeout.

Supported P0 domains for native TCP/connected UDP:

- Connect;
- Write;
- ReadIdle;
- ConnectionIdle;
- MaxLifetime;
- graceful close/drain deadline state where the existing public API requires it.

Successful network progress updates the same business/network activity semantics as the portable backend. Handler execution time does not count as read idle time.

If the runnable/inbox list is non-empty, the reactor polls with zero timeout and services work before sleeping.

## 20. UDP ownership

UDP fds are reactor-owned and non-blocking.

Native UDP preserves both root modes:

- connected `DialPacket`;
- unconnected `ListenPacket`.

Application sends follow the same local/global quota and bounded queue ownership as portable UDP. Physical `send`/`sendto` occurs only on the owning reactor.

Receive readiness drains datagrams under fairness and callback-capacity limits. One PacketConn callback is active at a time, preserving current serialized reader behavior.

Oversized datagrams are never delivered as complete packets and increment the same drop Stats/Observer semantics. P1-9 metadata/batching (`recvmmsg`, `sendmmsg`, ECN/DSCP, interface metadata) remains out of scope.

Connected UDP gets the existing read-idle/connection-idle/max-lifetime behavior. Unconnected ListenPacket remains alive until explicit close/Engine shutdown.

## 21. Stats and Observer parity

Native resources use the same public snapshot structs and event types.

Stats remain authoritative. Observer remains best-effort.

Required parity includes:

- payload bytes RX/TX;
- messages/packets RX/TX;
- queued frames/packets and encoded queued bytes;
- backpressure;
- dropped datagrams;
- decode/protocol errors;
- resource age/endpoints/protocol;
- Engine admission/rejection/global queued bytes;
- Observer dropped events and panics.

Native scheduler/inbox internals are not automatically exported as new Stats fields in P1-6. Add new public metrics only if a concrete operational need appears and the cardinality/ownership contract is clear.

## 22. Error parity

Because the implementation lives in `transport`, native code reuses the existing public error envelope and classifier directly.

Rules remain:

- caller context cancellation/deadline is returned unchanged;
- configuration/capability errors remain direct sentinels;
- operational socket failures are `*transport.Error`;
- raw errno remains reachable through `errors.Is/As`;
- clean FIN is lifecycle, not failure;
- first real terminal failure wins over derived close fallout;
- no string parsing of errno text.

Linux-specific errno mappings are covered by parity + epoll-specific tests.

## 23. Engine shutdown and reactor termination

### Graceful Engine Shutdown

1. transition Engine out of running state;
2. reject new Listen/Dial/adoption;
3. signal all listener resources to stop accepting;
4. request graceful drain on existing TCP Sessions and packet drain behavior matching portable semantics;
5. wake every reactor;
6. wait on the existing global lifecycle barrier until complete or caller context ends;
7. if the owning graceful caller times out/cancels, abort remaining native resources using existing precedence semantics.

### Immediate Engine Close

`Close()` marks abort state and wakes every reactor. Fd close is performed by the owning reactor, not the caller.

Reactors terminate after they own no live/opening/handoff resources and have processed shutdown state. Callback workers remain alive until every final `OnClose` callback has run. Only then are callback workers stopped, Observer delivery is stopped according to P0-5 semantics, and `Engine.Done()` closes.

A blocked Observer never holds `Done()`. A blocked application Handler may hold `Done()`, matching portable semantics.

## 24. Portable/native parity harness

Create backend-neutral tests around an Engine factory rather than copying assertions.

Common test helpers live in external test code (`package transport_test`) so they can exercise only public contracts and cannot accidentally depend on private portable/native implementation.

Conceptual factory:

```go
type engineFactory struct {
    name string
    new  func(t *testing.T, profile contractProfile) ogrenet.Engine
}
```

The shared suite covers TCP/UDP behavior common to both factories:

- lifecycle callback order and Done barriers;
- Send/TrySend ownership and backpressure;
- global/local quota release;
- connection/listener limits;
- timeout domains;
- TCP graceful Shutdown / CloseWrite / peer FIN;
- typed terminal errors;
- Stats snapshots;
- Observer events and saturation independence;
- Engine Close/Shutdown races;
- connected and unconnected UDP.

Portable tests remain independently present. The parity suite is an additional cross-backend contract, not a replacement for backend-specific tests.

Linux-only tests cover:

- fixed reactor affinity;
- non-blocking connect completion;
- accept handoff ownership;
- wake coalescing/no lost wake;
- edge-trigger drain-to-EAGAIN behavior;
- fairness requeue before EAGAIN;
- callback saturation and read pause/retry;
- codec-token pause/retry;
- deadline heap stale-entry handling;
- shutdown while fd handoff is in flight.

## 25. Race and stress requirements

Before Linux native support is considered ready:

```text
go test -race ./transport -run Native -count=20
go test -race ./...
```

Deterministic race/stress scenarios must include:

- Send/TrySend vs Close;
- Send vs codec-decode contention;
- callback completion vs abort;
- accept handoff vs Engine Shutdown;
- non-blocking Dial completion vs caller cancellation;
- write timeout vs peer reset;
- peer FIN vs CloseWrite;
- reactor Wake vs Close;
- admission/global quota saturation across reactors;
- listener close during accept flood;
- thousands of short-lived connections distributed across reactors.

Post-shutdown invariants:

```text
open fds owned by native Engine      == 0
opening/admission leases             == 0
active/draining resources            == 0
global queued bytes                  == 0
callback tasks/reservations          == 0
reactor inbox/runnable resources      == 0
native reactor goroutines             == 0
```

Tests must use deterministic synchronization points rather than sleeps as correctness oracles.

## 26. Benchmark requirements

Performance claims compare identical protocol/framing/options on portable and epoll factories.

Required benchmark dimensions:

- TCP throughput at 1 KiB, 4 KiB, and 64 KiB;
- TCP request/echo latency distribution harness;
- UDP packets/sec for small and medium datagrams;
- connection setup rate;
- Send/TrySend allocations and bytes/op;
- callback enabled/disabled overhead where meaningful;
- Engine graceful shutdown fan-out;
- CPU profile/report outside hard pass/fail thresholds where CI runner stability is insufficient.

Do not add hard ns/op or percentage-improvement CI thresholds from noisy hosted-runner samples.

Hard gates are appropriate for deterministic invariants such as:

- no new allocation on a disabled fast path where the benchmark is stable;
- no quota/lease leak;
- bounded task counts;
- no goroutine-per-direction/per-timeout regression.

The native backend does not have to win every payload/workload. Documentation must state where the portable backend remains competitive or preferable.

## 27. CI policy

Existing Linux Go 1.25/1.26, Windows, macOS, FreeBSD runtime, GmSSL, and cross-compile checks remain intact.

Linux native runtime tests execute only on Linux. Non-Linux builds compile the `NewEpoll` stub and capability errors.

The cross-platform matrix must prove that adding epoll high-level code does not create imports/build-tag leakage into Windows/macOS/FreeBSD portable builds.

No P1-6 PR is marked Ready until its exact head has fresh race/parity evidence.

## 28. Implementation phases

### 6A — Semantic sharing + contract harness

- introduce `EpollConfig`, constructor stubs, and capability sentinels;
- create parity test harness;
- migrate clearly reusable ownership primitives into `internal/runtimecore` one at a time;
- prove portable behavior/allocations unchanged.

No native socket I/O claim is made in 6A.

### 6B — Linux TCP

- reactor/inbox/deadline scheduler;
- native listener/accept/handoff;
- native Dial/connect completion;
- TCP read/decode and write/partial-write paths;
- callback executor;
- graceful/half-close/error/Stats/Observer parity;
- TCP race/stress/benchmark evidence.

### 6C — Linux UDP

- connected/unconnected UDP sockets;
- queue/quota/write readiness;
- datagram receive/drop/callback path;
- timeout/error/Stats/Observer parity;
- UDP race/stress/benchmark evidence.

### 6D — Productionization

- full parity matrix;
- exact-head CI gates;
- public native backend documentation;
- portable-vs-native benchmark report;
- only after these gates is Linux native support described as production-ready.

## 29. Deferred work

Explicitly deferred:

- native TLS;
- native WS/WSS;
- kqueue high-level Engine;
- IOCP high-level Engine;
- connection migration/work stealing;
- lock-free MPSC queues;
- buffer pool redesign;
- writev/sendmsg/sendfile optimization;
- UDP recvmmsg/sendmmsg and metadata;
- writable notification API;
- Happy Eyeballs/custom resolver;
- proxy/PROXY protocol;
- QUIC/HTTP changes.

These may build on the reactor/semantic model later but are not allowed to expand #57.

## 30. Definition of done for #57

#57 is complete only when:

- `transport.New()` remains unchanged and portable tests remain green;
- `transport.NewEpoll(EpollConfig, ...Option)` is explicit and stable;
- Linux TCP and UDP satisfy the shared public contract suite;
- TLS/WS/WSS fail explicitly with no portable fallback;
- native socket progress is reactor-owned;
- no native connection has reader/writer/watchdog goroutines;
- callback execution cannot block reactor progress and is bounded;
- graceful lifecycle, error, Stats, and Observer parity are demonstrated;
- race/stress shutdown invariants are clean;
- benchmark evidence compares portable and native honestly;
- exact-head CI is green.
