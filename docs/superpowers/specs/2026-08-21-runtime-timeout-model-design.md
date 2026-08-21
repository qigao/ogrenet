# Runtime Timeout and Deadline Model Design

Issue: #47  
Parent roadmap: #38  
Branch: `feat/runtime-timeout-model`

## 1. Scope and intent

This design completes the P0-2 timeout/deadline item from the production runtime roadmap for the portable `transport.Engine`.

The goal is to ensure that operations with a real risk of blocking indefinitely are bounded by production defaults, while preserving long-lived connection semantics unless the caller explicitly opts into idle or lifetime policies.

This change applies only to the `transport` runtime for TCP, TLS, WS, WSS, and UDP. It does not change the HTTP/QUIC client stack, graceful drain/half-close semantics, the full typed error taxonomy, observability APIs, or native epoll/kqueue/IOCP Engine implementations.

The public `Session` and `PacketConn` interfaces remain unchanged. Timeouts are Engine policy, not caller-controlled raw socket deadlines.

## 2. Public API

Add:

```go
type Timeouts struct {
    Connect        time.Duration
    Handshake      time.Duration
    Write          time.Duration
    ReadIdle       time.Duration
    ConnectionIdle time.Duration
    MaxLifetime    time.Duration
}

func WithTimeouts(Timeouts) Option
```

### 2.1 Default values

The effective defaults are:

```text
Connect         10s
Handshake       10s
Write           10s
ReadIdle         0   // disabled
ConnectionIdle   0   // disabled
MaxLifetime      0   // disabled
```

Rationale:

- Connect, handshake, and write are bounded operations and must not block indefinitely in the default production configuration.
- Read idle, connection idle, and maximum lifetime alter the semantics of long-lived or low-frequency connections, so they remain disabled until explicitly configured.
- `Shutdown(ctx)` already has a caller-supplied deadline model and therefore does not gain a second Engine-level shutdown timeout.

### 2.2 Zero and negative values

Normalization is field-specific:

- `Connect == 0` means the 10s production default.
- `Handshake == 0` means the 10s production default.
- `Write == 0` means the 10s production default.
- `ReadIdle == 0` means disabled.
- `ConnectionIdle == 0` means disabled.
- `MaxLifetime == 0` means disabled.
- any negative duration is invalid and returns `ErrInvalidTimeout` before Engine creation succeeds.

The timeout policy therefore intentionally differs from `Limits`, where zero means unlimited.

## 3. Compatibility with existing configuration

Existing API remains supported:

- `WithTLSHandshakeTimeout`
- `WebSocketConfig.HandshakeTimeout`
- `WebSocketConfig.WriteTimeout`

The effective precedence is fixed and independent of option call order:

```text
protocol-specific explicit override > Timeouts base > production default
```

The implementation must track whether a protocol-specific value was explicitly configured so that applying `WithTimeouts` before or after a protocol-specific option produces the same result.

Examples:

```go
New(
    WithTLSHandshakeTimeout(5*time.Second),
    WithTimeouts(Timeouts{Handshake: 20*time.Second}),
)
```

and:

```go
New(
    WithTimeouts(Timeouts{Handshake: 20*time.Second}),
    WithTLSHandshakeTimeout(5*time.Second),
)
```

must both produce a 5s TLS handshake timeout.

`WithWebSocketConfig` keeps its existing validation contract. Supplying it remains an explicit WS/WSS handshake/write override.

## 4. Timeout error taxonomy

This issue introduces only the minimal timeout taxonomy needed for deterministic behavior. It does not implement the broader P0-4 transport error model.

Add:

```go
var ErrTimeout = errors.New("transport: operation timed out")

type TimeoutKind uint8

const (
    TimeoutConnect TimeoutKind = iota + 1
    TimeoutHandshake
    TimeoutWrite
    TimeoutReadIdle
    TimeoutConnectionIdle
    TimeoutMaxLifetime
)

type TimeoutError struct {
    Kind  TimeoutKind
    Cause error
}
```

`TimeoutError` behavior:

```go
func (e *TimeoutError) Error() string
func (e *TimeoutError) Unwrap() error
func (e *TimeoutError) Is(target error) bool
func (e *TimeoutError) Timeout() bool
func (e *TimeoutError) Temporary() bool
```

Required semantics:

- `errors.Is(err, ErrTimeout)` is true.
- `errors.As(err, *TimeoutError)` exposes the domain.
- `Timeout() == true`.
- `Temporary() == false`.
- `Unwrap()` returns `Cause`, preserving the underlying OS/TLS/WS/context error where one exists.
- `Is` recognizes `ErrTimeout`; ordinary root-cause traversal continues through `Unwrap`.
- policy-generated idle/lifetime timeouts may have a nil cause.

Engine-triggered timeouts must not masquerade as `context.DeadlineExceeded`.

## 5. Caller context precedence

Caller cancellation/deadline and Engine timeout policy describe different ownership domains.

For Dial and handshake stages, the runtime derives a child context from the caller context:

```text
callerCtx
  -> operationCtx = bounded by Engine timeout
```

Return precedence is deterministic:

1. if the caller context has a cause, return the caller cause;
2. otherwise, if the internal operation timeout expired, return `*TimeoutError` for the corresponding domain;
3. otherwise, return the underlying failure.

Examples:

```text
caller deadline 5s + Connect 10s  -> context.DeadlineExceeded
caller deadline 30s + Connect 10s -> TimeoutConnect
caller cancel                      -> context.Canceled
```

This precedence must not depend on which channel wins a `select` race.

## 6. `Send(ctx)` ownership semantics

The existing contract is preserved: after a frame/message/datagram has been admitted to the writer queue, caller cancellation may stop waiting even though the writer may still transmit it.

The layers are:

```text
Send(ctx)
  admission / queue wait -> caller context
  actual writer I/O      -> Engine Write timeout
```

If caller context ends first, `Send` returns the caller cause. If the writer subsequently times out, the Session/PacketConn still closes with `TimeoutWrite`.

If the writer timeout is acknowledged while the caller context is still valid, `Send` returns `TimeoutWrite`.

This intentionally permits the return value of one `Send` call to differ from the final `Session.Err()` because they describe different ownership domains.

## 7. Timeout domain semantics

### 7.1 Connect

`Connect` starts when a Dial operation begins and ends when the underlying connected socket exists.

For TCP this includes resolver and TCP connect work performed by `net.Dialer.DialContext`.

For connected UDP it includes resolver/address setup and connected UDP socket establishment.

Inbound accepted sockets do not have a Connect timeout.

### 7.2 Handshake

Handshake applies after the underlying TCP connection exists.

TLS:

```text
TCP connected -> TLS handshake
```

WS:

```text
TCP connected -> HTTP/WebSocket upgrade
```

WSS:

```text
TCP connected -> TLS handshake -> HTTP/WebSocket upgrade
```

For WSS, the TLS and HTTP upgrade stages each receive their own effective protocol-specific handshake budget. They do not share one decrementing combined timer. This preserves the existing independent TLS and WebSocket configuration model.

Both TLS and WebSocket upgrade timeout failures map to `TimeoutHandshake`. A later P0-4 error model may add an explicit operation/stage dimension.

### 7.3 Write

`Write` starts when the writer goroutine begins one actual frame/message/datagram write.

The deadline is a hard upper bound for the whole write operation. Partial progress must not extend the Write deadline.

For TCP/TLS, a multi-call `writeAll` sequence uses one deadline established before the first write call.

Successful partial writes may refresh ConnectionIdle activity but never refresh WriteTimeout.

### 7.4 ReadIdle

ReadIdle detects inbound network silence.

TCP/TLS refresh ReadIdle whenever a raw `Read` returns `n > 0`, even when no complete application frame has been decoded yet.

This is intentionally not a slow-frame/slowloris timeout. A peer making real inbound progress remains active; protection against pathological partial-frame construction belongs to the existing bounded buffered-read limit or a future dedicated policy.

Handler execution time is not counted as ReadIdle. The implementation must clear the read deadline immediately after the blocking read returns and only establish a new one immediately before the next blocking read.

WS/WSS refresh ReadIdle only when a business message read completes successfully because the websocket library does not expose raw partial read progress at this layer.

### 7.5 ConnectionIdle

ConnectionIdle detects absence of successful business/network activity in either direction.

TCP/TLS activity:

- any successful read progress (`n > 0`);
- any successful write progress (`n > 0`).

WS/WSS activity:

- successful business message read;
- successful business message write.

Automatic WebSocket ping/pong does not refresh ConnectionIdle. Heartbeat liveness must not cause a connection with no business traffic to live forever.

Connected UDP activity:

- successful inbound datagram read;
- successful outbound datagram write.

### 7.6 MaxLifetime

MaxLifetime is measured from successful Session or connected PacketConn establishment.

It is never refreshed by traffic.

The clock starts only after protocol setup succeeds. Time spent connecting or handshaking does not consume Session lifetime.

If MaxLifetime and ConnectionIdle become due at the exact same instant, MaxLifetime wins deterministically.

## 8. Deadline implementation rules

### 8.1 Never use `SetDeadline`

TCP/TLS and UDP must use independent read and write deadlines:

- `SetReadDeadline`
- `SetWriteDeadline`

The implementation must never use `SetDeadline`, because a writer changing the deadline must not clobber the reader deadline or vice versa.

### 8.2 TCP/TLS read loop

Immediately before a blocking read:

```go
SetReadDeadline(now + ReadIdle)
```

if ReadIdle is enabled.

Immediately after the read returns, clear the read deadline before decoding frames or calling user handlers:

```go
SetReadDeadline(time.Time{})
```

If setting or clearing the deadline fails, the failure is terminal transport failure and the connection closes through the normal ownership path.

If the read returns a timeout and the connection was not already closing, normalize it to `TimeoutReadIdle`.

### 8.3 TCP/TLS write loop

Immediately before writing one frame:

```go
SetWriteDeadline(now + Write)
```

The same deadline remains in force across all partial writes of that frame.

After the frame write completes, clear the write deadline.

If a write returns a timeout and the connection was not already closing, normalize it to `TimeoutWrite`.

Any partial write with `n > 0` may refresh ConnectionIdle activity.

### 8.4 UDP

Connected UDP reader follows the same read deadline rule as TCP/TLS.

Both connected and listening UDP writers use WriteTimeout for individual datagrams.

ListenPacket never receives ReadIdle, ConnectionIdle, or MaxLifetime policy. It remains alive until explicit Close, its parent context, or Engine shutdown.

### 8.5 WebSocket

WS/WSS continue to use bounded contexts for websocket library I/O.

Writer:

```text
one ws.Write call -> effective WS WriteTimeout
```

Reader:

```text
one ws.Read call -> ReadIdle context when enabled
```

The read timeout context is active only during the blocking read. User handler execution is outside that context.

Existing ping/pong liveness remains separate and does not map to ReadIdle.

## 9. Activity clock and watchdog

ConnectionIdle and MaxLifetime share one internal watchdog component.

When both are disabled:

- no activity clock is allocated;
- no watchdog goroutine is created;
- no activity atomic is updated in read/write hot paths.

When either is enabled, each Session or connected PacketConn gets at most one activity clock, one watchdog goroutine, and one timer.

Suggested internal shape:

```go
type activityClock struct {
    born           time.Time
    lastActivityNS atomic.Int64
    wake           chan struct{}
}
```

`born` retains Go monotonic time. `lastActivityNS` stores elapsed monotonic duration from `born`, not wall-clock Unix time.

### 9.1 Activity updates

Activity touch is lock-free:

```go
lastActivityNS.Store(time.Since(born).Nanoseconds())
```

Wake notifications use a capacity-one buffered channel with non-blocking send so repeated activity coalesces into at most one pending wakeup.

I/O goroutines must never block waiting for the watchdog.

### 9.2 Timer loop

The watchdog calculates the earliest applicable deadline from:

```text
born + MaxLifetime
lastActivity + ConnectionIdle
```

It owns one timer and resets it to that earliest deadline.

When a timer fires, it must re-read current activity and recompute the deadline before closing. This prevents a race in which a read/write activity update occurs immediately before the old timer tick is selected.

The timer fire path only closes if the recomputed deadline is still due.

### 9.3 Timer reset discipline

Timer reset must follow a stop/drain/reset helper. The implementation must not blindly call `Reset` in a way that can leave a stale tick in the timer channel.

### 9.4 Shutdown barrier

The watchdog is part of the existing resource WaitGroup.

The resource lifecycle remains:

```text
reader
writer
optional websocket ping
optional activity watchdog
   -> loops.Wait()
   -> finalize()
   -> OnClose
   -> Done()
```

Therefore `Done()` guarantees that all timeout timer/watchdog work for the resource has stopped.

## 10. First terminal cause wins

The existing exact-once close ownership model remains authoritative.

The first terminal event that enters `initiateClose` determines the stable final cause.

Examples:

```text
explicit Close first      -> Err() == nil
timeout first             -> Err() == TimeoutError
peer/protocol error first -> Err() == that error
```

Later socket errors caused by closing the underlying connection must not overwrite the first cause.

Timeout watchdogs therefore call:

```go
initiateClose(&TimeoutError{Kind: ...})
```

before closing the raw socket.

## 11. Protocol-specific mapping

### 11.1 TCP

Outbound:

- Connect timeout around `net.Dialer.DialContext`.
- after success, adopt the stream and start optional lifetime/idle watchdog.

Inbound:

- no Connect timeout;
- accepted socket becomes subject to Session read/write/idle/lifetime policy only after successful adoption.

Read/write rules follow sections 8 and 9.

### 11.2 TLS

Outbound:

```text
Connect -> acquire connection admission -> acquire handshake admission -> TLS handshake -> adopt Session
```

Connect and Handshake are separate timeout domains.

Inbound:

```text
accept -> connection admission -> handshake admission -> TLS handshake -> adopt Session
```

A slow or timed-out server-side handshake closes only that accepted socket and releases handshake/connection admission. The Listener remains healthy.

### 11.3 WS

Outbound:

```text
Connect -> connection admission -> upgrade admission -> HTTP/WebSocket upgrade -> adopt wsSession
```

The raw TCP dial uses Connect timeout. The upgrade uses the effective WS Handshake timeout.

Inbound server upgrade remains bounded by the effective WS Handshake timeout via the existing HTTP server timeout configuration.

### 11.4 WSS

Outbound:

```text
Connect -> connection admission -> handshake admission -> TLS handshake
        -> upgrade admission -> HTTP/WebSocket upgrade -> adopt wsSession
```

TLS handshake and WebSocket upgrade each have an independent stage budget.

Inbound WSS follows the same separation through the gated TLS listener and HTTP upgrade path.

### 11.5 UDP

DialPacket:

- Connect timeout applies.
- successful connected socket may use ReadIdle, ConnectionIdle, and MaxLifetime.
- WriteTimeout applies per datagram.

ListenPacket:

- no Connect timeout after creation;
- no ReadIdle, ConnectionIdle, or MaxLifetime;
- WriteTimeout applies per outbound datagram.

## 12. Resource accounting invariants

Every timeout path must reuse existing ownership/cleanup paths and preserve #39/#40 guarantees.

After timeout completion:

```text
queue slots == released
local queued-byte quota == released
Engine global queued-byte quota == released
connection lease == released
handshake/upgrade lease == released
socket == closed
watchdog/timer goroutine == stopped
Done barrier == eventually closed
```

No timeout-specific cleanup path may bypass the existing gate, queue, quota, lease, or finalization machinery.

## 13. Testing strategy

### 13.1 Policy and error tests

Cover:

- production defaults;
- negative validation;
- zero semantics;
- protocol override precedence;
- option order independence;
- `errors.Is(err, ErrTimeout)`;
- `errors.As` for all TimeoutKind values;
- root-cause preservation;
- `Timeout() == true`, `Temporary() == false`.

### 13.2 Activity clock tests

Deterministic tests must cover:

- touch immediately before timer fire does not cause false timeout;
- repeated touch wakeups coalesce;
- ConnectionIdle and MaxLifetime choose the earliest deadline;
- exact tie selects MaxLifetime;
- close vs timer fire preserves first terminal cause;
- watchdog exits before resource Done;
- no watchdog is created when idle/lifetime policy is disabled.

### 13.3 TCP/TLS integration tests

Use deterministic local sockets and `net.Pipe` where appropriate.

Cover:

- blocked write -> TimeoutWrite;
- silent peer -> TimeoutReadIdle;
- partial inbound reads refresh ReadIdle;
- handler execution time does not count as ReadIdle;
- inbound/outbound activity refreshes ConnectionIdle;
- continuous traffic cannot extend MaxLifetime;
- caller deadline shorter than Connect timeout returns caller cause;
- TLS slow outbound handshake -> TimeoutHandshake;
- TLS slow accepted handshake -> TimeoutHandshake and no listener failure;
- timeout paths release connection/handshake admission and byte quotas.

### 13.4 WS/WSS integration tests

Use local deterministic servers.

Cover:

- slow WS HTTP upgrade -> TimeoutHandshake;
- slow WSS TLS handshake -> TimeoutHandshake;
- WS/WSS business read idle;
- WS/WSS write timeout;
- ConnectionIdle;
- MaxLifetime;
- traffic refresh behavior;
- ping/pong does not refresh business ConnectionIdle;
- timeout paths release connection/upgrade/handshake admission.

### 13.5 UDP integration tests

Cover:

- DialPacket connect timeout precedence;
- connected UDP read idle;
- connected UDP connection idle;
- connected UDP max lifetime;
- successful reads/writes refresh connection activity;
- ListenPacket remains alive during inactivity;
- datagram writer timeout helper/normalization;
- quota and admission cleanup after timeout.

### 13.6 Race and chaos tests

Run under `go test -race`:

- timeout vs Send;
- timeout vs TrySend;
- timeout vs Close;
- timeout vs Engine.Close;
- timeout vs Engine.Shutdown;
- activity touch vs idle expiry;
- write timeout vs caller cancellation;
- TLS handshake timeout vs listener close;
- hundreds of short-timeout Sessions/PacketConns concurrently created and destroyed.

Final assertions must include zero outstanding admission and queued-byte accounting.

## 14. Performance and benchmark requirements

This change is correctness hardening, not a performance optimization, but it must not add avoidable hot-path cost.

Benchmarks should record:

- stream send baseline versus timeout-enabled;
- activity `touch` ns/op and allocations;
- timeout policy normalization;
- watchdog-disabled construction;
- watchdog-enabled construction and lifecycle.

Hard requirements:

```text
activity touch                0 allocs/op
timeout-disabled idle path    no per-message allocation regression
ConnectionIdle/MaxLifetime=0  no watchdog goroutine
```

No hard latency threshold is introduced in this PR. The benchmark results establish a baseline for the later performance-regression roadmap work.

## 15. File boundaries

New files:

```text
transport/timeouts.go
transport/timeouts_test.go
transport/activity_clock.go
transport/activity_clock_test.go
transport/timeout_integration_test.go
```

Existing files updated only where timeout policy naturally enters the current ownership path:

```text
transport/errors.go
transport/options.go
transport/tcp.go
transport/tls.go
transport/conn.go
transport/packet.go
transport/websocket.go
transport/websocket_client.go
transport/websocket_server.go
```

`conn.go` and `packet.go` must not become owners of policy normalization or timer arbitration; those belong in the new focused helpers.

## 16. Implementation phases

### Phase A — policy and errors

Implement `Timeouts`, normalization, override tracking, `TimeoutKind`, `TimeoutError`, and unit tests. No I/O behavior changes yet.

### Phase B — TCP/TLS

Implement Connect/Handshake/Write/ReadIdle and activity/lifetime behavior for stream Sessions. Establish the reference semantics for all other transports.

### Phase C — WS/WSS

Unify existing WebSocket timeout behavior under the new policy and add read idle/activity/lifetime support without changing websocket message semantics.

### Phase D — UDP

Apply connect/write timeout behavior and connected-UDP idle/lifetime behavior while keeping ListenPacket inactivity-neutral.

### Phase E — stress, race, docs, benchmarks

Add cross-protocol concurrency/cleanup validation, benchmark scaffolding, and timeout documentation.

## 17. PR structure

Use one feature branch and one Draft PR:

```text
branch: feat/runtime-timeout-model
PR:     runtime: add complete timeout and deadline model
```

Suggested reviewable commits:

```text
1. transport: add timeout policy and typed timeout errors
2. transport: enforce stream connect and I/O deadlines
3. transport: apply timeout policy to WebSocket runtime
4. transport: apply timeout policy to UDP runtime
5. test: harden timeout races stress and benchmarks
```

The PR references #47 and #38 and remains unmerged until explicitly authorized.

## 18. CI acceptance

The final PR head must pass:

- gofmt cleanliness;
- module hygiene;
- `go vet ./...`;
- `go test -race -count=1 ./...` on supported Linux Go versions;
- Windows tests;
- macOS tests;
- GmSSL job;
- existing cross-compile matrix;
- focused timeout benchmark smoke if added to CI.

No public-network-dependent tests are allowed.

## 19. Non-goals

This work does not include:

- graceful drain, half-close, or abort APIs;
- a unified cross-protocol `transport.Error` taxonomy;
- Observer/OpenTelemetry integration;
- HTTP/QUIC client changes;
- native epoll/kqueue/IOCP Engine wiring;
- new protocols;
- WebSocket ping/pong semantic redesign;
- a public raw-deadline API on Session or PacketConn.

## 20. Definition of done

The timeout model is complete when:

- all blocking transport operations have deterministic bounded behavior where required;
- long-lived idle/lifetime semantics remain opt-in;
- caller context and Engine timeout ownership are distinguishable;
- timeout failures are machine-detectable without string parsing;
- TCP/TLS/WS/WSS/connected UDP obey the documented read/write/idle/lifetime rules;
- ListenPacket remains inactivity-neutral;
- no timeout path leaks quota, admission, sockets, timers, or goroutines;
- `Done()` remains a true shutdown barrier;
- default idle/lifetime-disabled hot paths retain negligible overhead;
- the full existing CI matrix remains green.
