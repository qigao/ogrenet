# Graceful Session Lifecycle Design

Issue: #50  
Parent roadmap: #38  
Branch: `feat/graceful-session-lifecycle`

## 1. Scope

This design adds deterministic graceful shutdown, stream half-close, and abort semantics to the portable `transport.Engine`.

It covers graceful full close for TCP/TLS/WS/WSS Sessions, TCP/TLS read-half and write-half behavior, immediate abort from every lifecycle phase, graceful Engine shutdown that stops admission before draining owned resources, bounded WebSocket close handshakes, UDP drain during Engine shutdown, admission accounting for draining connections, and the required race/stress/benchmark gates.

It does not implement the P0-4 full typed error taxonomy, P0-5 observer APIs, native high-level epoll/kqueue/IOCP Engine work, a generic MPMC/MPSC queue, or HTTP/QUIC lifecycle changes.

## 2. Public API

`Session` gains graceful full shutdown while preserving `Close` as immediate abort:

```go
type Session interface {
    ID() uint64
    Protocol() Scheme
    Endpoint() Endpoint
    Send(context.Context, Message) error
    TrySend(Message) error
    LocalAddr() net.Addr
    RemoteAddr() net.Addr
    Done() <-chan struct{}
    Err() error
    Shutdown(context.Context) error
    Close() error
}
```

TCP and TLS additionally implement:

```go
type HalfCloseSession interface {
    Session
    CloseWrite(context.Context) error
    ReadClosed() <-chan struct{}
}
```

`ReadClosed()` closes when no additional inbound application message can ever arrive. Peer FIN / clean TLS EOF may close it before full Session completion. Abort and final close also close it exactly once so waiters cannot hang. `Done()` remains the stronger final lifecycle barrier.

WS/WSS do not implement `HalfCloseSession`. `PacketConn` remains unchanged.

No public `Drain`, `Abort`, or lifecycle-state accessor is added. The stable primitives are:

```text
CloseWrite(ctx) = graceful protocol write-side close
Shutdown(ctx)   = graceful full protocol close
Close()         = immediate abort
ReadClosed()    = TCP/TLS inbound side can no longer produce messages
Done()          = final lifecycle barrier
```

## 3. Compatibility

This is an intentional pre-v1 API evolution. Adding `Session.Shutdown(context.Context)` is source-breaking for external `Session` implementations, which is acceptable because #38 prioritizes a correct lifecycle contract over compatibility with the current incomplete close model.

`Engine.Shutdown(ctx)` changes from abort-and-wait to true graceful shutdown. Callers requiring the previous behavior use:

```go
_ = engine.Close()
<-engine.Done()
```

`Session.Close()` and `Engine.Close()` retain immediate-termination semantics.

## 4. Protocol semantics

### 4.1 TCP

`CloseWrite(ctx)`:

```text
stop new send admission
-> allow already-admitted sends to complete producer ownership
-> drain remaining writer queue
-> ensure writer has no active frame
-> net.TCPConn.CloseWrite()
-> local FIN / write side closed
```

After successful `CloseWrite`, inbound data continues through `OnMessage` until peer FIN/EOF.

Peer FIN/EOF is read-half close only:

```text
peer FIN/EOF
-> ReadClosed() closes
-> reader exits cleanly
-> Session.Err() remains nil
-> Send/TrySend remain legal while write side is open
-> Done() remains open
```

When both halves are closed, the physical connection closes, `OnClose` runs, then `Done()` closes.

`Shutdown(ctx)` from a fully open TCP Session is:

```text
drain -> local FIN -> wait peer FIN -> final close -> Done
```

If `ReadClosed()` is already closed, Shutdown only needs to complete the local write side before finalization.

`Close()` skips drain and peer wait and closes the physical transport immediately. The API promises prompt abortive local termination, not a specific kernel FIN/RST packet pattern.

### 4.2 TLS

TLS half-close is protocol-level, not raw TCP FIN.

`CloseWrite(ctx)`:

```text
stop new send admission
-> drain application records
-> tls.Conn.CloseWrite()
-> local TLS close_notify
-> write side closed
```

The runtime does not additionally call underlying TCP `CloseWrite` after `tls.Conn.CloseWrite`.

Peer clean TLS EOF / close-notify closes `ReadClosed()` while the local write side may continue. Malformed/truncated TLS termination remains an error.

`Shutdown(ctx)` drains, sends local close-notify, waits for clean peer TLS EOF if the read side is still open, then closes the physical transport and finalizes.

Abort must bypass graceful TLS close behavior. The Session retains a physical transport abort closer so `Close()` or abort escalation does not wait for protocol shutdown.

### 4.3 WS / WSS

WebSocket has no application half-close. `Shutdown(ctx)` is full protocol close.

Local graceful shutdown:

```text
stop new business-send admission
-> drain already-admitted business messages
-> start Close(StatusNormalClosure)
-> wait peer Close
-> close physical transport
-> finalize
```

Peer-initiated Close is different:

```text
peer Close
-> stop new business-send admission immediately
-> fail queued-but-not-started business messages
-> allow an already-active write to finish or fail under WriteTimeout
-> complete/reply to close handshake
-> finalize
```

After peer Close begins protocol closing, queued data frames are not transmitted merely to satisfy a local drain guarantee.

Normal-closure and going-away statuses are clean. Other close/protocol failures remain errors.

WSS follows WebSocket lifecycle ownership; P0-3 does not separately run TLS graceful close underneath an active WS close handshake.

### 4.4 UDP

`PacketConn` gains no public graceful API.

During Engine graceful shutdown:

```text
stop new Send/TrySend/SendTo/TrySendTo admission
-> let already-admitted producers finish enqueue ownership
-> single writer drains accepted datagrams
-> close UDP socket
-> finalize
```

There is no peer wait. `PacketConn.Close()` stays immediate.

## 5. Session lifecycle model

TCP/TLS use independent read/write sides:

```text
ReadOpen ----------------------------> ReadClosed
                    peer FIN / clean EOF

WriteOpen -> Draining -> WriteClosed
             graceful request

ReadClosed + WriteClosed -> Finalize -> Done
Any nonterminal state -> Aborting -> Done
```

Abort closes `ReadClosed()` if it has not already closed, before terminal callback/finalization completes.

WS/WSS use:

```text
Open -> LocalDraining -> CloseHandshake -> Done
Open -> PeerClosing   -> CloseHandshake -> Done
Any  -> Aborting      -> Done
```

The internal goal is monotonic:

```text
Running < WriteClosed < FullyClosed < Aborted
```

`Shutdown` may upgrade a prior `CloseWrite`; abort permanently dominates graceful goals.

## 6. Send admission and drain linearization

The existing bounded Go channel remains the outbound queue. No custom MPSC/MPMC queue, sentinel, or per-message write tracker is introduced.

The existing `sendGate` is the send/shutdown linearization primitive.

A send that successfully enters the gate before graceful shutdown is admitted. A send attempting entry after the gate is closed receives `ErrClosed`.

Graceful write shutdown linearizes under the lifecycle lock:

```text
write state Open -> Draining
-> sendGate.close()
```

`Send` keeps its gate lease through writer acknowledgement / return. `TrySend` keeps its lease through successful or failed enqueue. Therefore when `sendGate.done()` closes, no producer can ever enqueue again.

The single writer then drains the remaining queue to a stable empty state:

```text
wait sendGate.done()
-> no future enqueue is possible
-> finish any active item
-> drain queued items until nonblocking receive finds none
-> protocol write-side close
```

An empty nonblocking receive is safe only after gate quiescence and only because there is one writer consumer.

This adds no new per-message lifecycle mutex, atomic, allocation, generic queue, or sentinel to the Running-state send hot path.

## 7. Graceful and abort signals

The current single `closing` concept must be separated semantically:

```text
send admission closed  // sendGate
writer graceful done   // graceful writer completion
physical abort         // hard termination
```

Graceful shutdown does not use the hard-abort signal merely to stop sends. Reader loops remain alive when peer wait or half-close semantics require it.

Abort closes the physical transport and wakes I/O loops, quota waiters, and lifecycle waiters.

## 8. Concurrent Session lifecycle calls and context ownership

Graceful phases are shared, caller contexts are not merged.

The first caller that starts a phase is its owner. Later callers are waiters.

- owner context bounds that phase;
- owner context expiry escalates the Session to abort;
- waiter context expiry only returns that waiter’s `context.Cause(ctx)`;
- `Close()` aborts any phase immediately;
- completed phases are reused;
- `Shutdown` can upgrade a write-close goal without reopening the write side.

Examples:

```text
A CloseWrite(5s) owns drain/write-close
B Shutdown(30s) waits A, then may own peer-wait
```

```text
A Shutdown(30s) owns full graceful shutdown
B Shutdown(100ms) returns deadline after 100ms
A continues unaffected
```

After an owner context expires, the method initiates abort and returns the caller cause rather than waiting unboundedly for `Done()`. `Done()` still closes eventually only after all internal work and `OnClose` finish.

## 9. Session error and return precedence

P0-3 does not introduce P0-4’s full error taxonomy.

Stable precedence:

```text
existing transport/protocol failure
    > runtime-owned WebSocket close timeout (TimeoutClose)
    > explicit abort / ErrClosed
```

Caller context cause is returned by the lifecycle API but is not stored in `Session.Err()` merely because it forced abort.

Examples:

```text
Shutdown + TimeoutWrite:
    Shutdown -> TimeoutWrite
    Err      -> TimeoutWrite

Shutdown + owner caller deadline first:
    Shutdown -> context.DeadlineExceeded
    Err      -> nil unless an earlier failure exists

Shutdown + concurrent Close:
    Shutdown -> ErrClosed unless an earlier failure exists
    Err      -> nil unless an earlier failure exists

WS close handshake timeout:
    Shutdown -> TimeoutClose
    Err      -> TimeoutClose
```

Explicit `Close()` is not itself a transport failure.

Repeated calls are stable: completed `CloseWrite`/`Shutdown` return nil, post-abort lifecycle calls return `ErrClosed` or the already-recorded stronger failure, and `Close()` remains idempotent.

## 10. WebSocket close timeout

`WebSocketConfig` gains:

```go
type WebSocketConfig struct {
    OriginPatterns   []string
    Subprotocols     []string
    HandshakeTimeout time.Duration
    WriteTimeout     time.Duration
    CloseTimeout     time.Duration
    PingInterval     time.Duration
    PongTimeout      time.Duration
}
```

`CloseTimeout` semantics:

```text
0  -> default 10s
<0 -> ErrInvalidWebSocketConfig
>0 -> explicit bound
```

This zero behavior preserves existing composite literals that omit the new field. Existing handshake/write/pong validation semantics remain unchanged.

`TimeoutKind` appends without renumbering existing values:

```go
TimeoutClose
```

`CloseTimeout` bounds only WS/WSS protocol close handshake, not application queue drain.

For local `Shutdown(ctx)`, whichever occurs first between the owner caller context and `CloseTimeout` controls termination. Peer-initiated close has no caller owner, so `CloseTimeout` is the runtime-owned hard bound.

If `CloseTimeout` fires first, the runtime stores `TimeoutClose`, physically aborts, unblocks the library close path, and finalizes with the typed error. If caller context fires first, physical abort occurs but no synthetic timeout is stored in `Session.Err()`.

## 11. Physical WebSocket abort ownership

`coder/websocket` owns graceful WS close, but its close implementation has internal waits and cannot guarantee a later `CloseNow()` immediately interrupts an already-running Close handshake.

Each `wsSession` therefore retains a physical transport abort closer in addition to `*websocket.Conn`.

Client path: the custom HTTP/WebSocket dial state retains the underlying admitted TCP/TLS physical connection and transfers its abort closer into the final `wsSession` with the admission lease and addresses.

Server path: the HTTP connection tracker/admission holder retains the accepted physical connection and transfers its abort closer after successful upgrade/admission transfer.

Graceful WS shutdown uses `websocket.Conn.Close`. Hard abort closes the retained physical transport directly, then library goroutines converge. The physical closer is internal lifecycle infrastructure only.

## 12. Engine lifecycle

Engine state becomes explicit:

```text
Running -> Draining -> Done
Running/Draining -> Aborting -> Done
```

The first `Engine.Shutdown(ctx)` owner linearizes under the Engine lock:

```text
state Running -> Draining
-> snapshot tracked resources for graceful request
```

Immediately after Draining begins:

- listeners are closed first;
- new Dial/Listen/DialPacket/ListenPacket and new admission/adoption work are rejected;
- new `beginOp` calls fail;
- already-started opens/accepts may clean up but cannot adopt after state is non-Running;
- already-tracked resources form the drain set.

Adoption enforces the no-late-arrival invariant, so no repeated snapshot loop is required.

Engine sends nonblocking internal graceful requests to tracked Sessions and PacketConns and does not call blocking `Session.Shutdown(ctx)` sequentially. Existing resource I/O/lifecycle loops advance their own drain.

Engine then waits on its existing `Done()` barrier.

Owner context expiry transitions Engine to Aborting, physically aborts remaining resources, and returns `context.Cause(ctx)` without requiring an unbounded wait for final `Done()`.

`Engine.Close()` always means immediate Aborting and remains idempotent.

## 13. Concurrent Engine Shutdown calls

Engine uses the same owner/waiter rule as Session lifecycle phases.

The first caller that transitions Running -> Draining owns the Engine graceful phase. Later `Shutdown(ctx)` calls are waiters.

- the owner context can force global abort on expiry;
- waiter context expiry returns only to that waiter and cannot shorten the owner’s drain;
- concurrent `Engine.Close()` aborts the shared graceful phase immediately;
- after successful Engine completion, later `Shutdown` calls return nil;
- after explicit abort, graceful waiters return `ErrClosed` unless a stronger Engine-level control failure exists.

This prevents a later short deadline from destroying an already-running longer graceful shutdown.

## 14. Engine shutdown error policy

Engine shutdown is control-plane behavior and does not aggregate arbitrary child Session failures.

A child reset/timeout/protocol error stays in that child’s `Session.Err()` / `OnClose`; Engine continues draining others.

`Engine.Shutdown(ctx)` returns:

- nil when all owned resources terminate in time;
- owner `context.Cause(ctx)` when owner expiry forces abort;
- `ErrClosed` when concurrent explicit Engine Close aborts the graceful phase, unless a stronger Engine-level control error exists;
- direct listener/control close failures owned by Engine itself.

It does not create an unbounded `errors.Join` of child errors.

## 15. Admission accounting during drain

Connection lease state expands internally:

```text
Opening -> Active -> Draining -> Released
```

Graceful shutdown moves Active -> Draining. Final removal moves Draining -> Released.

Draining connections remain owned resources and continue counting against `MaxConnections`, `MaxConnectionsPerPeer`, and `MaxConnectionsPerListener` until final release.

The effective global connection count is:

```text
opening + active + draining
```

`admissionSnapshot` gains internal `DrainingConnections`; public `Limits` fields do not change.

## 16. Interaction with P0-2 timeout policy

Graceful lifecycle does not suspend timeout safety:

- every application write remains bounded by `WriteTimeout`;
- `ReadIdle` remains active while read side is open;
- `ConnectionIdle` continues using actual activity;
- `MaxLifetime` is never extended by drain;
- WS/WSS protocol close uses the new `CloseTimeout`.

A transport timeout may therefore end graceful shutdown before its caller context and becomes the Session terminal failure returned by `Session.Shutdown`.

After TCP/TLS `ReadClosed()` closes, the reader/read-idle path stops; other applicable timeout policy continues until final completion.

## 17. Callback and barrier semantics

Callback order remains:

```text
OnOpen -> zero or more OnMessage -> OnClose
```

For TCP/TLS, `ReadClosed()` may close before `OnClose`. No `OnMessage` may occur after `ReadClosed()` closes.

On abort/final close, `ReadClosed()` is closed before `OnClose` if it was still open, so a waiter never remains blocked after terminal shutdown.

`Done()` closes only after I/O/lifecycle/watchdog/close-handshake work has stopped, queue/accounting cleanup is complete, `OnClose` has returned, and final tracking/admission ownership can be released safely. It remains the exact final barrier.

## 18. File boundaries

Preferred boundaries:

```text
transport.go
transport/
├── lifecycle.go
├── lifecycle_test.go
├── gate.go
├── conn.go
├── stream_graceful.go
├── stream_graceful_test.go
├── websocket.go
├── websocket_graceful.go
├── websocket_graceful_test.go
├── websocket_client.go
├── websocket_dial_admission.go
├── websocket_server.go
├── packet.go
├── packet_graceful.go
├── packet_graceful_test.go
├── engine.go
├── engine_shutdown.go
├── engine_graceful_test.go
├── limits.go
├── options.go
├── timeouts.go
└── graceful_race_test.go
```

Steady-state I/O stays in `conn.go`, `websocket.go`, and `packet.go`; lifecycle coordination lives in focused files.

## 19. Test matrix

TCP/TLS tests must prove peer read-half close leaves sending usable, `ReadClosed()` differs from `Done()`, local CloseWrite drains before FIN/close-notify, inbound delivery continues after local write close, either half-close order converges, Shutdown waits correctly, Close aborts promptly, repeated calls are stable, and abort closes `ReadClosed()`.

WS/WSS tests must prove local Shutdown drains before Close frame, peer Close rejects new/queued business traffic, active write follows WriteTimeout ownership, normal/going-away are clean, CloseTimeout produces `TimeoutClose`, caller deadline physically aborts without synthetic Session timeout, explicit Close interrupts a running handshake promptly, and WSS does not introduce competing TLS graceful ownership.

UDP tests must prove Engine shutdown drains accepted connected/unconnected datagrams, rejects new admission after drain starts, and direct PacketConn Close remains immediate.

Concurrency/race tests cover Send vs Shutdown, TrySend vs Shutdown, send context cancellation after enqueue, CloseWrite vs Shutdown upgrade, Session owner vs waiter deadlines, Engine owner vs waiter deadlines, Shutdown vs Close, peer FIN vs CloseWrite, P0-2 timeout vs drain, WS peer Close vs queued writes, WS close handshake vs WriteTimeout, and close-timeout vs explicit abort.

Engine tests cover listener-first shutdown, no late adoption, graceful request fan-out without blocking per child, child error isolation, owner deadline global abort, exact Done barrier, and no O(N) extra waiter goroutine fan-out.

Accounting tests cover Opening/Active/Draining/Released races, global/peer/listener limit retention, queued-byte zero convergence, and opening/handshake/upgrade cleanup.

## 20. Performance and benchmark gates

No per-message lifecycle accounting is added to the Running hot path.

Benchmarks cover Send and TrySend baseline before/after P0-3, Session construction allocations, graceful drain with 1 and 256 queued items, and Engine graceful drain for representative 100 and 1,000 Session sets.

Hard requirements:

- no additional normal-state goroutine per Session solely for graceful lifecycle;
- no per-message lifecycle allocation added to Send/TrySend;
- no O(N) extra shutdown waiter goroutines;
- WS close timeout machinery exists only during close handshake.

No absolute latency/throughput threshold is introduced; this phase establishes regression visibility.

## 21. Implementation phases

One reviewable PR with five logical commit groups:

1. `transport: add session lifecycle and graceful API`
   - Session/HalfCloseSession contract;
   - internal lifecycle coordinator;
   - send-gate graceful drain behavior;
   - ownership/repeated-call tests.

2. `transport: add TCP and TLS half-close semantics`
   - `ReadClosed`;
   - TCP FIN / TLS close-notify;
   - physical abort ownership;
   - stream integration tests.

3. `transport: add bounded WebSocket close handshake`
   - `CloseTimeout` / `TimeoutClose`;
   - physical transport abort transfer;
   - local/peer close states;
   - WS/WSS race/timeout tests.

4. `transport: make Engine shutdown drain resources`
   - Engine lifecycle state and owner/waiter coordination;
   - listener/adoption stop first;
   - nonblocking child graceful requests;
   - UDP drain;
   - admission Active/Draining accounting.

5. `test: harden graceful shutdown races and benchmarks`
   - high-contention lifecycle races;
   - cleanup/accounting stress;
   - graceful benchmarks;
   - documentation and CI smoke.

## 22. PR and verification scope

PR title:

```text
runtime: add graceful shutdown and half-close semantics
```

Branch: `feat/graceful-session-lifecycle`  
References: #50 and #38.

Merge gates:

- Linux Go 1.25/1.26 format, module hygiene, vet, and race;
- Windows full tests;
- macOS full tests;
- GmSSL security/wire/transport tests;
- existing Linux/Windows/Darwin/FreeBSD cross-compile matrix;
- graceful lifecycle benchmark smoke;
- no admission, queued-byte, socket, timer, or goroutine leaks in new stress cases.

## 23. Definition of done

P0-3 is complete when `Session.Shutdown(ctx)` performs graceful full close for TCP/TLS/WS/WSS; `Close()` remains prompt abort; TCP/TLS provide usable `HalfCloseSession` and non-hanging `ReadClosed()` semantics; peer TCP/TLS read-half close does not disable local sends; admitted local sends drain before local graceful write close; WS close handshakes are bounded and physically abortable; peer WS Close prevents further queued business traffic; Engine shutdown stops listeners/adoption before draining owned resources; UDP accepted datagrams drain; draining resources remain counted until final release; Session and Engine owner/waiter deadline semantics are deterministic; P0-2 timeout policy stays active; `Done()` remains exact; race/stress/platform CI gates are green; and steady-state send hot paths avoid unnecessary lifecycle overhead.
