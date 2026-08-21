# Graceful Session Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deterministic graceful shutdown, TCP/TLS half-close, bounded WebSocket close handshakes, Engine-wide draining, and immediate abort semantics without adding steady-state send-path lifecycle overhead.

**Architecture:** Keep the existing bounded Go channels and single-writer ownership model. Add a monotonic lifecycle object per Session/resource, use `sendGate` as the Send/Shutdown linearization primitive, let the existing reader/writer goroutines drive drain and half-close completion, and retain a physical transport closer wherever TLS or WebSocket protocol wrappers would otherwise make `Close()` graceful. Engine shutdown becomes an explicit Running→Draining→Done / Aborting state machine and requests child drains non-blockingly while preserving the current tracking barrier.

**Tech Stack:** Go 1.25/1.26, `net`, `crypto/tls`, `net/http`, `github.com/coder/websocket` v1.8.15, existing `transport` admission/quota/gate/runtime code, GitHub Actions cross-platform matrix.

**Spec:** `docs/superpowers/specs/2026-08-21-graceful-session-lifecycle-design.md`

## Global Constraints

- `Session.Close()` and `Engine.Close()` remain immediate abort operations.
- `Session.Shutdown(context.Context)` is graceful full close; an owner caller context can force abort, while a waiter context cannot shorten another owner’s phase.
- TCP/TLS implement `CloseWrite(context.Context)` and `ReadClosed() <-chan struct{}`; WS/WSS do not expose half-close.
- TCP peer FIN / clean TLS EOF is read-half close only; sending remains legal while the local write side is open.
- WS/WSS peer Close starts full protocol closing and stops new business traffic.
- `PacketConn` public API remains unchanged; UDP only drains internally during Engine graceful shutdown.
- Keep the existing bounded Go channel queue. Do not add generic MPSC/MPMC queues, queue sentinels, or per-message lifecycle trackers.
- `sendGate.enter()` is the Send/Shutdown linearization point. After `sendGate.done()` closes, no producer can enqueue again.
- Do not add a lifecycle allocation, mutex, atomic, or goroutine per message on Running-state `Send` / `TrySend`.
- Existing P0-2 `WriteTimeout`, `ReadIdle`, `ConnectionIdle`, and `MaxLifetime` remain active during graceful phases.
- Append `TimeoutClose`; never renumber an existing `TimeoutKind`.
- `WebSocketConfig.CloseTimeout`: zero selects 10s default, negative is invalid, positive is explicit.
- Draining connections keep consuming global/per-peer/per-listener admission budgets until final release.
- `Done()` remains the exact final barrier and closes only after I/O/lifecycle/watchdog work and `OnClose` complete.
- No P0-4 full typed error taxonomy, P0-5 observer API, native high-level Engine, HTTP/QUIC lifecycle changes, or unrelated refactors.
- Merge gate: Linux Go 1.25/1.26 race tests, Windows/macOS full tests, GmSSL tests, and the existing cross-compile matrix must pass.

---

## File Structure

### New files

- `transport/lifecycle.go` — monotonic close goal, owner/waiter arbitration, abort reason, and lifecycle channels.
- `transport/lifecycle_test.go` — deterministic lifecycle ownership and channel tests.
- `transport/graceful_test_helpers_test.go` — shared TCP/TLS/WS/WSS loopback fixtures and bounded channel assertions; test-only.
- `transport/stream_graceful.go` — TCP/TLS `Shutdown`, `CloseWrite`, `ReadClosed`, physical abort, and protocol write close.
- `transport/stream_graceful_test.go` — stream half-close/drain tests.
- `transport/websocket_graceful.go` — WS/WSS local drain, peer close, close timeout, and physical abort.
- `transport/websocket_graceful_test.go` — WS/WSS close-handshake tests.
- `transport/packet_graceful.go` — internal UDP drain request.
- `transport/packet_graceful_test.go` — UDP drain/abort tests.
- `transport/engine_graceful_test.go` — Engine listener-first/no-late-adoption/fan-out/accounting tests.
- `transport/graceful_race_test.go` — lifecycle race matrix.
- `transport/graceful_benchmark_test.go` — Running send and drain benchmarks.
- `docs/runtime-graceful-shutdown.md` — public lifecycle contract.

### Existing files modified

- `transport.go`
- `transport/conn.go`
- `transport/engine_stream_dial.go`
- `transport/websocket.go`
- `transport/websocket_client.go`
- `transport/websocket_dial_admission.go`
- `transport/websocket_server.go`
- `transport/websocket_server_admission.go`
- `transport/packet.go`
- `transport/engine.go`
- `transport/engine_shutdown.go`
- `transport/engine_tracking_add.go`
- `transport/engine_tracking_remove.go`
- `transport/limits.go`
- `transport/options.go`
- `transport/timeouts.go`
- `transport/timeouts_test.go`
- `.github/workflows/netpoll-v2.yml`

---

### Task 1: Lifecycle State Machine and Close Policy Foundation

**Files:**
- Create: `transport/lifecycle.go`
- Create: `transport/lifecycle_test.go`
- Modify: `transport/options.go`
- Modify: `transport/timeouts.go`
- Modify: `transport/timeouts_test.go`

**Interfaces:**
- Produces `closeGoalRunning`, `closeGoalWrite`, `closeGoalFull`, `closeGoalAbort`.
- Produces `abortNone`, `abortExplicit`, `abortCaller`, `abortFailure`.
- Produces `newSessionLifecycle`, `request`, `abort`, `reason`, `writeRequested`, `fullRequested`, `aborted`, `readDone`, `writeDone`, `terminalDone`, `markReadClosed`, `markWriteClosed`, `markTerminal`.
- Produces `defaultWSCloseTimeout = 10 * time.Second`, `WebSocketConfig.CloseTimeout`, and appended `TimeoutClose`.

- [ ] **Step 1: Write failing lifecycle tests**

Create `transport/lifecycle_test.go` with these exact ownership assertions:

```go
func TestSessionLifecycleRequestOwnership(t *testing.T) {
    l := newSessionLifecycle()
    if !l.request(closeGoalWrite) {
        t.Fatal("first write request did not own transition")
    }
    if l.request(closeGoalWrite) {
        t.Fatal("duplicate write request incorrectly became owner")
    }
    if !l.request(closeGoalFull) {
        t.Fatal("full request did not own goal upgrade")
    }
    if l.request(closeGoalFull) {
        t.Fatal("duplicate full request incorrectly became owner")
    }
}

func TestSessionLifecycleAbortFirstWins(t *testing.T) {
    l := newSessionLifecycle()
    if !l.abort(abortCaller) {
        t.Fatal("first abort did not win")
    }
    if l.abort(abortExplicit) {
        t.Fatal("second abort replaced winner")
    }
    if got := l.reason(); got != abortCaller {
        t.Fatalf("abort reason = %v, want %v", got, abortCaller)
    }
    select {
    case <-l.aborted():
    default:
        t.Fatal("abort channel is open")
    }
}

func TestSessionLifecycleChannelsCloseExactlyOnce(t *testing.T) {
    l := newSessionLifecycle()
    l.markReadClosed()
    l.markReadClosed()
    l.markWriteClosed()
    l.markWriteClosed()
    l.markTerminal()
    l.markTerminal()
    for name, ch := range map[string]<-chan struct{}{
        "read": l.readDone(), "write": l.writeDone(), "terminal": l.terminalDone(),
    } {
        select {
        case <-ch:
        default:
            t.Fatalf("%s channel is open", name)
        }
    }
}
```

Add `TestSessionLifecycleConcurrentFullRequestHasOneOwner`: start 100 goroutines behind a start channel, each calls `request(closeGoalFull)`, count owners with `atomic.Int32`, and assert the final count is exactly 1 after `sync.WaitGroup.Wait()`.

- [ ] **Step 2: Verify RED**

Run:

```bash
go test ./transport -run '^TestSessionLifecycle' -count=1
```

Expected: compile failure for undefined lifecycle symbols.

- [ ] **Step 3: Implement `sessionLifecycle`**

Use one mutex for goal/reason and `sync.Once` per close-only channel:

```go
type closeGoal uint8

const (
    closeGoalRunning closeGoal = iota
    closeGoalWrite
    closeGoalFull
    closeGoalAbort
)

type abortReason uint8

const (
    abortNone abortReason = iota
    abortExplicit
    abortCaller
    abortFailure
)

type sessionLifecycle struct {
    mu   sync.Mutex
    goal closeGoal
    why  abortReason

    writeReq chan struct{}
    fullReq  chan struct{}
    abortCh  chan struct{}
    readCh   chan struct{}
    writeCh  chan struct{}
    termCh   chan struct{}

    writeReqOnce sync.Once
    fullReqOnce  sync.Once
    abortOnce    sync.Once
    readOnce     sync.Once
    writeOnce    sync.Once
    termOnce     sync.Once
}
```

`request(closeGoalWrite)` closes `writeReq`; `request(closeGoalFull)` closes `writeReq` and `fullReq`; only a strict goal increase returns true. `abort` stores the first reason, raises goal to abort, closes request channels plus `abortCh`, and returns whether that caller won.

- [ ] **Step 4: Verify lifecycle GREEN including race**

```bash
go test ./transport -run '^TestSessionLifecycle' -count=1
go test -race ./transport -run '^TestSessionLifecycle' -count=1
```

Expected: PASS.

- [ ] **Step 5: Write failing close-policy tests**

Add to `transport/timeouts_test.go`:

```go
func TestWebSocketCloseTimeoutDefaultAndValidation(t *testing.T) {
    cfg := defaultConfig()
    if cfg.ws.CloseTimeout != defaultWSCloseTimeout {
        t.Fatalf("CloseTimeout = %v, want %v", cfg.ws.CloseTimeout, defaultWSCloseTimeout)
    }
    ws := cfg.ws
    ws.CloseTimeout = -time.Second
    if err := WithWebSocketConfig(ws)(&cfg); !errors.Is(err, ErrInvalidWebSocketConfig) {
        t.Fatalf("negative CloseTimeout error = %v", err)
    }
    ws.CloseTimeout = 0
    if err := WithWebSocketConfig(ws)(&cfg); err != nil {
        t.Fatalf("zero CloseTimeout error = %v", err)
    }
    if cfg.ws.CloseTimeout != defaultWSCloseTimeout {
        t.Fatalf("normalized CloseTimeout = %v", cfg.ws.CloseTimeout)
    }
}

func TestTimeoutCloseKind(t *testing.T) {
    err := &TimeoutError{Kind: TimeoutClose}
    if !errors.Is(err, ErrTimeout) {
        t.Fatalf("TimeoutClose does not match ErrTimeout: %v", err)
    }
    if got := err.Kind.String(); got != "close" {
        t.Fatalf("TimeoutClose string = %q", got)
    }
}
```

- [ ] **Step 6: Verify close-policy RED**

```bash
go test ./transport -run 'Test(WebSocketCloseTimeout|TimeoutClose)' -count=1
```

Expected: compile failure for `CloseTimeout`, `defaultWSCloseTimeout`, and `TimeoutClose`.

- [ ] **Step 7: Implement close policy**

Set `defaultWSCloseTimeout = 10 * time.Second`; default config assigns it. `WithWebSocketConfig` preserves existing handshake/write/pong/ping validation, rejects `CloseTimeout < 0`, and normalizes only `CloseTimeout == 0` to the default. Append `TimeoutClose` after `TimeoutMaxLifetime` and map it to `"close"`.

- [ ] **Step 8: Verify Task 1**

```bash
go test ./transport -run 'Test(SessionLifecycle|WebSocketCloseTimeout|TimeoutClose)' -count=1
go test -race ./transport -run '^TestSessionLifecycle' -count=1
go test ./transport -count=1
```

Expected: PASS.

- [ ] **Step 9: Commit Task 1**

```bash
git add transport/lifecycle.go transport/lifecycle_test.go transport/options.go transport/timeouts.go transport/timeouts_test.go
git commit -m "transport: add graceful lifecycle foundation"
```

---

### Task 2: TCP/TLS Half-Close and Graceful Stream Shutdown

**Files:**
- Create: `transport/graceful_test_helpers_test.go`
- Create: `transport/stream_graceful.go`
- Create: `transport/stream_graceful_test.go`
- Modify: `transport/conn.go`
- Modify: `transport/engine_stream_dial.go`

**Interfaces:**
- Consumes Task 1 `sessionLifecycle`.
- Produces on `*conn`: `Shutdown`, `CloseWrite`, `ReadClosed`, `requestWriteClose`, `requestShutdown`, `abort`, `closeProtocolWrite`.
- New `conn` fields: `life *sessionLifecycle`, `physical io.Closer`, `writerDrained chan struct{}` and a once guard.
- Tests use local interfaces until the root public API changes in Task 3:

```go
type gracefulShutdowner interface {
    Shutdown(context.Context) error
}

type halfCloseProbe interface {
    CloseWrite(context.Context) error
    ReadClosed() <-chan struct{}
}
```

- [ ] **Step 1: Add reusable loopback test fixture**

Create `transport/graceful_test_helpers_test.go`. Reuse `testTLSConfigs(t)` from `protocol_test.go`. Implement `dialSessionPair` by creating a server Engine, listening on `127.0.0.1:0`, capturing the accepted Session through `HandlerFuncs.Open`, then dialing with a client Engine.

Use this exact shape:

```go
type sessionPair struct {
    clientEngine *Engine
    serverEngine *Engine
    client       ogrenet.Session
    server       ogrenet.Session
}

func (p *sessionPair) close() {
    _ = p.clientEngine.Close()
    _ = p.serverEngine.Close()
}

func dialSessionPair(t *testing.T, scheme ogrenet.Scheme) *sessionPair {
    t.Helper()
    serverTLS, clientTLS := testTLSConfigs(t)
    var serverOpts, clientOpts []Option
    if scheme == ogrenet.SchemeTLS || scheme == ogrenet.SchemeWSS {
        serverOpts = []Option{WithTLSServerConfig(serverTLS)}
        clientOpts = []Option{WithTLSClientConfig(clientTLS)}
    }
    server, err := New(serverOpts...)
    if err != nil { t.Fatal(err) }
    path := ""
    if scheme == ogrenet.SchemeWS || scheme == ogrenet.SchemeWSS { path = "/graceful" }
    accepted := make(chan ogrenet.Session, 1)
    ln, err := server.Listen(context.Background(), ogrenet.Endpoint{Scheme: scheme, Host: "127.0.0.1", Port: 0, Path: path}, ogrenet.HandlerFuncs{Open: func(s ogrenet.Session) { accepted <- s }})
    if err != nil { _ = server.Close(); t.Fatal(err) }
    client, err := New(clientOpts...)
    if err != nil { _ = server.Close(); t.Fatal(err) }
    cs, err := client.Dial(context.Background(), ln.Endpoint(), ogrenet.HandlerFuncs{})
    if err != nil { _ = client.Close(); _ = server.Close(); t.Fatal(err) }
    var ss ogrenet.Session
    select {
    case ss = <-accepted:
    case <-time.After(2 * time.Second):
        _ = client.Close(); _ = server.Close(); t.Fatal("accepted session timeout")
    }
    return &sessionPair{clientEngine: client, serverEngine: server, client: cs, server: ss}
}
```

Add helper `waitClosed(t, ch, name)` with a 2s timer and helper `assertStillOpen(t, ch, name)` using a nonblocking select.

- [ ] **Step 2: Write failing TCP half-close tests**

In `transport/stream_graceful_test.go`, type assert `halfCloseProbe` and `gracefulShutdowner` instead of the not-yet-public `HalfCloseSession`.

For peer FIN/read-half behavior:

```go
p := dialSessionPair(t, ogrenet.SchemeTCP)
defer p.close()
serverHalf := p.server.(halfCloseProbe)
clientHalf := p.client.(halfCloseProbe)
if err := serverHalf.CloseWrite(context.Background()); err != nil { t.Fatal(err) }
waitClosed(t, clientHalf.ReadClosed(), "client read half")
assertStillOpen(t, p.client.Done(), "client session")
if err := p.client.Send(context.Background(), ogrenet.Text("response-after-fin")); err != nil { t.Fatal(err) }
```

Attach a server `HandlerFuncs.Message` channel in the fixture variant used by this test and assert it receives `"response-after-fin"` after the client `ReadClosed` channel closes.

For local drain ordering, send 64 distinct `TrySend` messages, record each successful acceptance, call `CloseWrite`, and assert the peer’s handler receives exactly the accepted sequence before its `ReadClosed` closes. After graceful request, assert both `Send` and `TrySend` return `ErrClosed`.

For full Shutdown, call client `Shutdown` in a goroutine, prove it is still pending before the server write side closes, then call server `CloseWrite` and assert client Shutdown returns nil and client Done closes.

- [ ] **Step 3: Verify TCP tests RED**

```bash
go test ./transport -run '^TestTCP(PeerFIN|CloseWrite|Shutdown)' -count=1
```

Expected: type assertion or compile failure because `*conn` lacks graceful methods.

- [ ] **Step 4: Initialize stream lifecycle and physical abort closer**

In `adoptStreamWithLease`, initialize lifecycle fields. Use:

```go
func physicalStreamCloser(raw net.Conn) io.Closer {
    if tc, ok := raw.(*tls.Conn); ok {
        return tc.NetConn()
    }
    return raw
}
```

Never read or write through `tls.Conn.NetConn()`; use it only for hard close.

- [ ] **Step 5: Rework stream writer/reader termination**

Writer behavior:

```text
abort -> fail pending, close writerDrained, exit
writeRequested -> wait sendGate.done
               -> drain queue with nonblocking receives until stable empty
               -> closeProtocolWrite exactly once
               -> mark write closed
               -> close writerDrained
               -> exit
```

Running write behavior and P0-2 write deadlines remain unchanged. Do not close the outbound channel and do not use `len(queue)`.

Reader behavior on clean EOF: mark read closed and return without closing the write side or recording an error. Reader behavior on non-clean error: preserve first failure and hard abort. When both read and write halves are closed, mark lifecycle terminal so watchdog/activity loops can exit before `loops.Wait()` finalizes.

- [ ] **Step 6: Implement stream lifecycle API**

`requestWriteClose` raises goal to write and closes `sendGate` only on the first transition that stops send admission. `requestShutdown` raises full goal and also guarantees send admission is closed.

`CloseWrite(ctx)` waits `life.writeDone`, `life.aborted`, or caller ctx. If caller owns the write goal and its ctx expires first, it wins `abortCaller`, closes `physical`, and returns `context.Cause(ctx)`. A waiter whose ctx expires returns its own cause without aborting the owner.

`Shutdown(ctx)` uses the same owner/waiter rule for the full goal and waits `Done()` on success. `ReadClosed()` returns `life.readDone()`.

Hard `Close()` must use `abortExplicit`, close `physical`, leave `Session.Err()` nil unless a stronger failure already exists, and remain idempotent.

- [ ] **Step 7: Implement TCP/TLS protocol write close**

Type switch TLS first, then a generic close-writer interface so tests can inject deterministic wrappers:

```go
type writeHalfCloser interface { CloseWrite() error }

func (c *conn) closeProtocolWrite() error {
    if tc, ok := c.raw.(*tls.Conn); ok {
        return c.closeTLSWrite(tc)
    }
    cw, ok := c.raw.(writeHalfCloser)
    if !ok {
        return fmt.Errorf("transport: %s stream does not support write half-close", c.protocol)
    }
    return cw.CloseWrite()
}
```

For TLS, set a write deadline of `time.Now().Add(c.timeouts.Write)`, call `tls.Conn.CloseWrite()`, then clear the deadline. Map a timeout to `TimeoutWrite`; preserve other causes.

- [ ] **Step 8: Write TLS tests with concrete assertions**

Use `dialSessionPair(t, ogrenet.SchemeTLS)`.

1. Server `CloseWrite`; assert client `ReadClosed` closes while client `Done` remains open; send `ogrenet.Text("tls-response")` from client and assert server handler receives it.
2. Client `CloseWrite`; assert server `ReadClosed` closes and client can still receive a final server message before server closes its own write side.
3. Start client `Shutdown` with a 50ms owner context while server deliberately leaves write side open; assert `errors.Is(err, context.DeadlineExceeded)` and `client.Err() == nil` after eventual Done.
4. Start client `Shutdown` with a long context, then call client `Close`; assert Shutdown returns `ErrClosed` promptly and `client.Err() == nil`.

- [ ] **Step 9: Verify Task 2**

```bash
go test ./transport -run 'Test(TCP|TLS).*?(CloseWrite|PeerFIN|ReadHalf|Shutdown)' -count=1
go test ./transport -run '^TestTCPWriteTimeout|^TestTCPReadIdle|^TestTLS' -count=1
go test -race ./transport -run 'Test(TCP|TLS).*?(CloseWrite|Shutdown)' -count=1
```

Expected: PASS.

- [ ] **Step 10: Commit Task 2**

```bash
git add transport/graceful_test_helpers_test.go transport/stream_graceful.go transport/stream_graceful_test.go transport/conn.go transport/engine_stream_dial.go
git commit -m "transport: add TCP and TLS half-close semantics"
```

---

### Task 3: WebSocket/WSS Graceful Close, Physical Abort, and Public Session API

**Files:**
- Create: `transport/websocket_graceful.go`
- Create: `transport/websocket_graceful_test.go`
- Modify: `transport/websocket.go`
- Modify: `transport/websocket_client.go`
- Modify: `transport/websocket_dial_admission.go`
- Modify: `transport/websocket_server.go`
- Modify: `transport/websocket_server_admission.go`
- Modify: `transport.go`

**Interfaces:**
- Produces `wsSession.Shutdown`, `requestShutdown`, `beginPeerClose`, `runCloseHandshake`, `abort`.
- New `wsSession` fields: `life *sessionLifecycle`, `physical io.Closer`, `closeTO time.Duration`, `peerClosing chan struct{}` with once guard, `writerDrained chan struct{}` with once guard, `closeHandshakeOnce sync.Once`, and `closeHandshakeDone chan struct{}`.
- At the end of this task, root `Session` gains `Shutdown` and root `HalfCloseSession` is added.

- [ ] **Step 1: Write failing local WS drain test**

Use `dialSessionPair(t, ogrenet.SchemeWS)` with a server handler that records message data. Send 32 numbered messages with `TrySend`; store only the indexes for which `TrySend` returns nil. Start client `Shutdown(context.Background())`. Assert the server records exactly those accepted indexes in order before server `OnClose` runs. Assert any new client `Send` after shutdown request returns `ErrClosed`.

Core order assertion:

```go
if diff := cmp.Diff(wantAccepted, gotMessages); diff != "" {
    t.Fatalf("drain order mismatch (-want +got):\n%s", diff)
}
```

Do not add a new comparison dependency if `cmp` is not already in `go.mod`; in that case use `reflect.DeepEqual` and print both slices.

- [ ] **Step 2: Write failing peer-close and waiter tests**

For peer-close behavior, use the server-side concrete `*wsSession` in same-package tests to invoke the protocol close path after one active client write is blocked by a test transport wrapper. Queue additional client writes. Assert the active write may complete or fail, every queued-not-started ack receives `ErrClosed`, and the server records no data frame after peer close starts.

For waiter behavior, start owner `Shutdown` with a 2s context and a second waiter `Shutdown` with a 50ms context while the peer withholds its close response. Assert the waiter returns `context.DeadlineExceeded`; then allow peer close completion and assert owner returns nil.

- [ ] **Step 3: Verify WS graceful tests RED**

```bash
go test ./transport -run '^TestWebSocket(Shutdown|PeerClose)' -count=1
```

Expected: missing `wsSession.Shutdown`/lifecycle behavior.

- [ ] **Step 4: Retain physical client connection**

Extend `wsDialAdmission` with `physical net.Conn`. For WS set `physical=raw`. For WSS keep the pre-TLS raw TCP connection as `physical` while returning `*tls.Conn` to `http.Transport`. Change transfer signature to:

```go
func (s *wsDialAdmission) take() (*connectionLease, net.Addr, net.Addr, net.Conn)
```

Pass the returned physical closer into `newWSSession`.

- [ ] **Step 5: Retain physical server connection**

Extend `httpConnLease` with `physical net.Conn`, assigned when the accepted TCP connection is first registered. `rebind` changes HTTP/TLS lookup keys but never replaces `physical`. Change holder transfer to:

```go
func (l *httpConnLease) take() (*connectionLease, net.Conn)
```

Transfer both lease and raw physical closer after WebSocket upgrade succeeds.

- [ ] **Step 6: Implement WS local and peer-close writer semantics**

Local shutdown: request full goal, close gate, let existing writes finish, wait gate quiescence, drain stable queue, mark `writerDrained`, then start one close handshake.

Peer close: close gate and signal `peerClosing` immediately. The writer must finish/fail only its current active write, then fail every queued-not-started request with `ErrClosed` before waiting for gate quiescence. This ordering releases `Send` callers that hold gate leases while waiting for acks.

Keep the P0-2 `writeState` ownership logic intact for `TimeoutWrite`.

- [ ] **Step 7: Implement bounded close handshake and hard physical abort**

Exactly one caller executes `websocket.Conn.Close`. Wrap it with configured `closeTO`:

```go
func (s *wsSession) runCloseHandshake(code websocket.StatusCode) {
    s.closeHandshakeOnce.Do(func() {
        timer := time.AfterFunc(s.closeTO, s.failCloseTimeout)
        err := s.ws.Close(code, "")
        timer.Stop()
        s.finishCloseHandshake(err)
        close(s.closeHandshakeDone)
    })
    <-s.closeHandshakeDone
}
```

`failCloseTimeout` only stores `TimeoutClose` if `abortFailure` wins lifecycle abort ownership. It then closes the raw physical connection to interrupt the blocking library close.

Caller-owned shutdown deadline: if the owner ctx wins first and `abortCaller` wins, close physical and return caller cause; do not store the library’s derived close error in `Session.Err()`.

Explicit `Close`: if `abortExplicit` wins, close physical immediately; do not rely on `CloseNow` to interrupt an already-running `Close`.

- [ ] **Step 8: Write close-timeout, caller-abort, explicit-abort, and WSS tests**

Use a server that completes upgrade but does not complete the close handshake.

```go
ws := defaultConfig().ws
ws.CloseTimeout = 50 * time.Millisecond
```

Assert `Shutdown` returns a `*TimeoutError` with `Kind == TimeoutClose` and `errors.Is(err, ErrTimeout)`, and that `Session.Err()` has the same kind.

With `CloseTimeout=2s` and owner context 50ms, assert Shutdown returns caller deadline, Done eventually closes, and `Session.Err()==nil`.

With long owner context, start Shutdown then call `Close`; assert Shutdown returns `ErrClosed` before 500ms and `Session.Err()==nil`.

Repeat caller/explicit physical-abort cases for WSS to prove raw TCP ownership survives TLS wrapping.

- [ ] **Step 9: Add final public Session interfaces**

Only now modify `transport.go` because both concrete Session families implement Shutdown:

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

type HalfCloseSession interface {
    Session
    CloseWrite(context.Context) error
    ReadClosed() <-chan struct{}
}
```

Compile assertions:

```go
var _ ogrenet.Session = (*conn)(nil)
var _ ogrenet.HalfCloseSession = (*conn)(nil)
var _ ogrenet.Session = (*wsSession)(nil)
```

There must be no `ogrenet.HalfCloseSession` assertion for `wsSession`.

- [ ] **Step 10: Verify Task 3**

```bash
go test ./transport -run 'Test(WebSocket|WSS).*?(Shutdown|Close|Peer)' -count=1
go test ./transport -run '^TestWebSocketWriteTimeoutIsTyped$' -count=1
go test -race ./transport -run 'Test(WebSocket|WSS).*?(Shutdown|Close)' -count=1
go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 11: Commit Task 3**

```bash
git add transport.go transport/websocket_graceful.go transport/websocket_graceful_test.go transport/websocket.go transport/websocket_client.go transport/websocket_dial_admission.go transport/websocket_server.go transport/websocket_server_admission.go
git commit -m "transport: add bounded WebSocket graceful shutdown"
```

---

### Task 4: Engine Graceful Fan-Out, UDP Drain, and Admission Draining State

**Files:**
- Create: `transport/packet_graceful.go`
- Create: `transport/packet_graceful_test.go`
- Create: `transport/engine_graceful_test.go`
- Modify: `transport/packet.go`
- Modify: `transport/engine.go`
- Modify: `transport/engine_shutdown.go`
- Modify: `transport/engine_tracking_add.go`
- Modify: `transport/engine_tracking_remove.go`
- Modify: `transport/limits.go`
- Modify: `transport/limits_test.go`

**Interfaces:**
- Produces `engineRunning`, `engineDraining`, `engineAborting`, `engineDone`.
- Produces `packetConn.requestDrain`, `Engine.requestGracefulShutdown`, `Engine.abortRemaining`, `connectionLease.beginDrain`, and `admissionSnapshot.DrainingConnections`.

- [ ] **Step 1: Write failing admission Draining tests**

Add:

```go
func TestConnectionLeaseDrainingCountsTowardLimit(t *testing.T) {
    a := newAdmissionController(Limits{MaxConnections: 1})
    lease, err := a.acquireConnection("127.0.0.1")
    if err != nil { t.Fatal(err) }
    if !lease.beginDrain() { t.Fatal("beginDrain did not transition") }
    snap := a.snapshot()
    if snap.ActiveConnections != 0 || snap.DrainingConnections != 1 {
        t.Fatalf("snapshot = %+v", snap)
    }
    if _, err := a.acquireConnection("127.0.0.2"); !errors.Is(err, ErrResourceExhausted) {
        t.Fatalf("new connection while draining = %v", err)
    }
    lease.release()
    snap = a.snapshot()
    if snap.DrainingConnections != 0 { t.Fatalf("draining after release = %d", snap.DrainingConnections) }
}
```

Add a per-peer case with `MaxConnectionsPerPeer=1` and a per-listener case using `newListenerCapacity(1)`: the second acquisition remains rejected until the draining lease releases.

- [ ] **Step 2: Verify admission RED**

```bash
go test ./transport -run '^TestConnectionLeaseDraining' -count=1
```

Expected: undefined `beginDrain`/`DrainingConnections`.

- [ ] **Step 3: Implement Draining accounting**

Add `connectionDraining` and controller field `draining int`. `beginDrain` transitions Active→Draining, moves one count from active to draining, and leaves peer/listener ownership intact. Global MaxConnections checks use `opening + active + draining`. `release` decrements the matching state; `idle()` requires draining==0; snapshot exports internal `DrainingConnections`.

- [ ] **Step 4: Write failing UDP drain tests**

Use `ListenPacket` plus connected `DialPacket`. Queue numbered datagrams with `TrySend`, store every accepted payload, call internal `requestDrain`, and assert the listener handler receives every accepted payload before client packet Done closes. After drain request, assert new `Send` and `TrySend` return `ErrClosed`.

For immediate Close, queue work, call `Close`, assert Done closes promptly; do not assert queued datagrams arrive.

- [ ] **Step 5: Implement internal UDP drain**

Use shared lifecycle state internally without changing public `PacketConn`. On first drain request, mark its admission lease Draining and close gate. Writer waits producer quiescence, drains stable queue, closes UDP socket, marks terminal, and exits. Abort fails pending and closes socket immediately.

- [ ] **Step 6: Write Engine listener-first and no-late-adoption tests**

Use coordination channels, not arbitrary sleeps.

1. Establish a TCP Session; start Engine Shutdown; assert listener Done closes while the established Session is still waiting for peer close.
2. Hold an accepted TLS handshake/prepare path, start Shutdown, release the hold, and assert adoption returns `ErrClosed` and no new tracked stream remains.
3. Hold a WS upgrade before final adoption, start Shutdown, release it, and assert the connection lease is released rather than becoming a tracked WS Session.
4. After Engine enters Draining, call `Dial`, `Listen`, `DialPacket`, and `ListenPacket`; assert `errors.Is(err, ErrClosed)` for each.

- [ ] **Step 7: Replace Engine `closed bool` with explicit state**

```go
type engineState uint8

const (
    engineRunning engineState = iota
    engineDraining
    engineAborting
    engineDone
)
```

`beginOp` and every add/adopt helper require `state == engineRunning`. `maybeDoneLocked` can complete from Draining or Aborting only after activeOps==0, all tracked maps are empty, and admission is idle; set `engineDone` before closing Engine Done.

- [ ] **Step 8: Implement Engine owner/waiter graceful shutdown**

The first Running→Draining transition is the owner. Under Engine lock snapshot listeners and tracked resources; after unlock close listeners first, then call nonblocking `requestShutdown`/`requestDrain` on existing resources. Do not block per child and do not start a waiter goroutine per child.

Owner `Shutdown(ctx)` waits Engine Done or owner ctx. If owner ctx expires and Draining→Aborting succeeds, abort remaining resources and return `context.Cause(ctx)`. A waiter ctx expiry only returns its own cause. Explicit Engine `Close()` transitions to Aborting immediately; graceful waiters return `ErrClosed` unless an Engine-level control failure already exists.

- [ ] **Step 9: Mark child leases Draining exactly once**

When stream/WS/packet graceful request first closes send admission, call an Engine helper that finds the resource lease and invokes `beginDrain()`. Keep the lease attached to the resource; only existing remove/finalize paths release it.

- [ ] **Step 10: Write Engine owner/waiter and child-error tests**

Owner/waiter: owner uses 2s ctx, waiter 50ms; peer holds close open. Assert waiter deadline, then peer closes and owner returns nil.

Owner deadline: peer never closes, owner 50ms; assert owner deadline, all remaining resources eventually Done, admission snapshot returns all zero.

Explicit Close: start graceful owner, call `Engine.Close`, assert graceful call returns `ErrClosed` promptly.

Child failure isolation: establish two Sessions; reset/abort one peer during Engine drain and close the other cleanly; assert Engine Shutdown returns nil once all resources terminate, while the failed child retains its own non-nil `Session.Err()`.

- [ ] **Step 11: Verify Task 4**

```bash
go test ./transport -run 'Test(ConnectionLeaseDraining|PacketDrain|PacketClose|EngineShutdown|EngineClose)' -count=1
go test -race ./transport -run 'Test(EngineShutdown|EngineClose|ConnectionLeaseDraining)' -count=1
go test ./transport -count=1
```

Expected: PASS and no opening/active/draining/queued-byte accounting remains after Done.

- [ ] **Step 12: Commit Task 4**

```bash
git add transport/packet_graceful.go transport/packet_graceful_test.go transport/packet.go transport/engine_graceful_test.go transport/engine.go transport/engine_shutdown.go transport/engine_tracking_add.go transport/engine_tracking_remove.go transport/limits.go transport/limits_test.go
git commit -m "transport: make Engine shutdown drain resources"
```

---

### Task 5: Race Hardening, Benchmarks, Documentation, and CI

**Files:**
- Create: `transport/graceful_race_test.go`
- Create: `transport/graceful_benchmark_test.go`
- Create: `docs/runtime-graceful-shutdown.md`
- Modify: `.github/workflows/netpoll-v2.yml`
- Modify existing tests only where they assert the old aborting `Engine.Shutdown` semantics.

- [ ] **Step 1: Add deterministic race tests**

Create one test per race. Each test uses start/release channels so a transition occurs at a known boundary:

```text
TestGracefulRaceSendVsShutdown
TestGracefulRaceTrySendVsShutdown
TestGracefulRaceSendContextCancelAfterEnqueue
TestGracefulRaceCloseWriteVsShutdown
TestGracefulRaceShutdownVsClose
TestGracefulRacePeerFINVsCloseWrite
TestGracefulRaceTimeoutVsDrain
TestGracefulRaceWebSocketCloseVsWriteTimeout
```

Core assertions for every test:

```go
select {
case <-session.Done():
case <-time.After(2 * time.Second):
    t.Fatal("session did not converge")
}
snap := engine.admissionSnapshot()
if snap.OpeningConnections != 0 || snap.ActiveConnections != 0 || snap.DrainingConnections != 0 || snap.GlobalQueuedBytes != 0 {
    t.Fatalf("leaked accounting: %+v", snap)
}
```

For Send/TrySend races, keep an explicit slice of accepted message IDs and assert every accepted-before-gate-close ID is delivered before FIN/close, while every after-close call returns `ErrClosed`.

- [ ] **Step 2: Run focused race loop**

```bash
go test -race ./transport -run '^TestGracefulRace' -count=20
```

Any failure blocks progress. Use the systematic-debugging skill before changing runtime code; do not relax synchronization to make the failure disappear.

- [ ] **Step 3: Add benchmarks with concrete loops**

`BenchmarkGracefulSendRunning`: establish one local TCP pair before `b.ResetTimer()`, call `b.ReportAllocs()`, and loop `session.Send(context.Background(), ogrenet.Bin(payload))`, with the peer draining messages continuously.

`BenchmarkGracefulTrySendRunning`: same fixture; for each iteration retry only on `ErrWouldBlock` after waiting for peer progress, and fail on every other error.

`BenchmarkGracefulDrainOneFrame` and `BenchmarkGracefulDrain256Frames`: each benchmark iteration creates a fresh local pair, accepts exactly N `TrySend` messages, calls `CloseWrite`, closes peer write side, waits Done, then closes Engines outside the timed region where practical.

`BenchmarkEngineGracefulDrain100` and `BenchmarkEngineGracefulDrain1000`: pre-establish N local Sessions, reset timer, start peer-side graceful completion concurrently using one goroutine that iterates all peer sessions, call Engine Shutdown, stop timer, verify Done. Do not create one benchmark waiter goroutine per Session.

- [ ] **Step 4: Run benchmark smoke**

```bash
go test ./transport -run '^$' -bench 'BenchmarkGraceful(SendRunning|TrySendRunning|DrainOneFrame|Drain256Frames)' -benchmem -benchtime=1x
go test ./transport -run '^$' -bench '^BenchmarkEngineGracefulDrain100$' -benchtime=1x
```

Expected: PASS. Running-state Send/TrySend must not acquire a new lifecycle allocation attributable to P0-3.

- [ ] **Step 5: Write public documentation**

Create `docs/runtime-graceful-shutdown.md` with these sections:

```text
1. Close vs Shutdown
2. TCP/TLS Half-Close and ReadClosed
3. WebSocket Full Close and CloseTimeout
4. Engine Graceful Shutdown
5. UDP Behavior
6. Context Ownership and Concurrent Shutdown Calls
7. Error Precedence
8. Interaction with Runtime Timeouts
9. Admission Accounting During Drain
10. Migration from Pre-P0-3 Engine.Shutdown
```

Include this compiling usage pattern:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if hs, ok := session.(ogrenet.HalfCloseSession); ok {
    if err := hs.CloseWrite(ctx); err != nil {
        log.Printf("close write: %v", err)
    }
    select {
    case <-hs.ReadClosed():
    case <-ctx.Done():
    }
}

if err := session.Shutdown(ctx); err != nil {
    log.Printf("shutdown: %v", err)
}
```

Document that `Close()` promises prompt local abort with no graceful guarantee; it does not promise a specific TCP RST packet.

- [ ] **Step 6: Add graceful benchmark smoke to CI**

In `.github/workflows/netpoll-v2.yml`, after the current runtime-timeout benchmark smoke step in Linux jobs, add:

```yaml
- name: Graceful lifecycle benchmark smoke
  run: >-
    go test ./transport -run '^$'
    -bench 'BenchmarkGraceful(SendRunning|TrySendRunning|DrainOneFrame|Drain256Frames)'
    -benchmem -benchtime=1x
```

Keep 100/1000-Session Engine benchmarks out of the normal PR matrix; run them manually when comparing performance.

- [ ] **Step 7: Run complete verification where the toolchain is available**

```bash
gofmt -w transport.go transport/*.go
git diff --exit-code
go mod tidy
git diff --exit-code -- go.mod go.sum
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
go test ./transport -run '^$' -bench 'BenchmarkGraceful(SendRunning|TrySendRunning|DrainOneFrame|Drain256Frames)' -benchmem -benchtime=1x
```

If local module/toolchain access is unavailable, do not claim these commands passed; use GitHub Actions as integration authority.

- [ ] **Step 8: Commit Task 5**

Stage only files touched by this task:

```bash
git add transport/graceful_race_test.go transport/graceful_benchmark_test.go docs/runtime-graceful-shutdown.md .github/workflows/netpoll-v2.yml
git commit -m "test: harden graceful shutdown races and benchmarks"
```

If an old-semantics test file required adjustment, add that exact path explicitly before the commit; never use `git add .` or `git add -A`.

- [ ] **Step 9: Open one Draft PR**

```text
Title: runtime: add graceful shutdown and half-close semantics
Body references: #50 and #38
Base: master
Head: feat/graceful-session-lifecycle
Draft: true
```

- [ ] **Step 10: Require exact-head full CI**

Final evidence must include:

```text
Linux Go 1.25.x: format, module hygiene, vet, HTTP benchmark smoke, runtime timeout benchmark smoke, graceful benchmark smoke, race
Linux Go 1.26.x: same
Windows: vet + full tests
macOS: vet + full tests
GmSSL 3.2.0: build + security/wire/transport tests
cross-compile: Linux arm64/386/riscv64, Windows arm64, Darwin amd64/arm64, FreeBSD amd64/arm64/riscv64
```

- [ ] **Step 11: Final requirements review before Ready for Review**

Use verification-before-completion and requesting-code-review. Require direct exact-head evidence for every item below:

```text
TCP peer FIN keeps write side usable
TLS close-notify half-close
ReadClosed closes before Done when only read half ends
ReadClosed also closes on abort/final terminal close
local Send/TrySend drain ordering before FIN/Close
explicit Close interrupts graceful phases promptly
WS local drain differs from peer-close queued-message failure
TimeoutClose and caller-deadline precedence
physical WS and WSS abort interrupts active coder/websocket Close
Engine listeners stop before child drain
late accept/dial/upgrade cannot adopt after Draining
Engine owner/waiter context semantics
UDP accepted datagram drain
Active -> Draining -> Released accounting
zero queued-byte/admission leakage
P0-2 timeout behavior remains active during drain
full race/cross-platform matrix green
```

Only then mark the PR Ready for Review. Do not merge without explicit user authorization.
