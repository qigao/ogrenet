# Linux Native Engine 6A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish the Linux native Engine construction/capability contract, a reusable public-contract parity harness, and a small shared semantic core without implementing native socket I/O or changing portable transport behavior.

**Architecture:** `transport.New()` remains the portable reference backend. `transport.NewEpoll(EpollConfig, ...Option)` is explicit and never selected automatically. P1-6A introduces only construction/capability scaffolding plus `internal/runtimecore` ownership primitives (`SendGate`, `Lifecycle`, `ObserverDispatcher`) behind compatibility wrappers; native accept/connect/read/write starts only in 6B.

**Tech Stack:** Go 1.25+, standard library, existing `golang.org/x/sys`, root `ogrenet` contracts, existing `transport` package, race detector and benchmark gates.

**Spec:** `docs/superpowers/specs/2026-08-22-linux-native-engine-design.md`

## Global Constraints

- `transport.New(opts...)` always means the portable backend.
- 6A performs no native socket accept/connect/read/write and makes no native TCP/UDP support claim.
- The eventual Linux backend supports TCP + UDP first; TLS/WS/WSS never fall back to portable I/O.
- `internal/runtimecore` must not import `transport`, `epoll`, `kqueue`, `iocp`, TLS, WebSocket, DNS, or own socket syscalls.
- Existing P0 limits, timeout, graceful lifecycle, typed error, Stats, Observer, allocation, and `Done()` semantics remain unchanged for the portable backend.
- No fake cross-platform poller abstraction.
- No lock-free queue, buffer pool, scatter/gather, Happy Eyeballs, proxy, QUIC, or HTTP work in 6A.
- Every production-code task follows RED -> GREEN -> focused regression/race verification -> commit.
- Existing Linux Go 1.25/1.26, Windows, macOS, FreeBSD runtime, GmSSL, and cross-compile gates stay intact.

## File Structure

```text
internal/runtimecore/
    gate.go
    gate_test.go
    lifecycle.go
    lifecycle_test.go
    observer.go
    observer_test.go
    dependency_test.go

transport/
    epoll_config.go
    epoll_config_test.go
    epoll_constructor_linux.go
    epoll_constructor_stub.go
    epoll_engine_phase6a_linux.go
    epoll_capability_linux_test.go
    epoll_stub_test.go
    contract_harness_test.go       // package transport_test
    contract_tcp_test.go           // package transport_test
    contract_udp_test.go           // package transport_test
    contract_native_linux_test.go  // package transport_test
    gate.go
    lifecycle.go
    lifecycle_test.go
    observer.go
    stats.go
    errors.go
```

`quota.go`, `limits.go`, admission leases, and Stats counters intentionally remain in `transport` during 6A. They directly encode `transport` error identities/snapshots, and the future epoll implementation also lives in `transport`, so moving them now would create artificial factories/interfaces without a clean dependency benefit.

---

### Task 1: Freeze Epoll config, constructor, and capability semantics

**Files:**
- Create: `transport/epoll_config.go`
- Create: `transport/epoll_config_test.go`
- Create: `transport/epoll_constructor_linux.go`
- Create: `transport/epoll_constructor_stub.go`
- Create: `transport/epoll_engine_phase6a_linux.go`
- Create: `transport/epoll_capability_linux_test.go`
- Create: `transport/epoll_stub_test.go`
- Modify: `transport/errors.go`

**Interfaces:**

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

var ErrBackendUnsupported error
var ErrProtocolUnsupported error
var ErrInvalidEpollConfig error
```

Package-private resolved form:

```go
type resolvedEpollConfig struct {
    pollers         int
    eventBatch      int
    callbackWorkers int
    callbackQueue   int
    ioBudgetBytes   int
    ioBudgetOps     int
}

func resolveEpollConfig(cfg EpollConfig, gomaxprocs int) (resolvedEpollConfig, error)
```

- [ ] **Step 1: Write RED config tests**

`transport/epoll_config_test.go`:

```go
func TestResolveEpollConfigDefaults(t *testing.T) {
    got, err := resolveEpollConfig(EpollConfig{}, 8)
    if err != nil { t.Fatal(err) }
    want := resolvedEpollConfig{
        pollers: 8,
        eventBatch: 256,
        callbackWorkers: 8,
        callbackQueue: 64,
        ioBudgetBytes: 256 << 10,
        ioBudgetOps: 64,
    }
    if got != want { t.Fatalf("got %+v want %+v", got, want) }
}

func TestResolveEpollConfigExplicitValues(t *testing.T) {
    cfg := EpollConfig{Pollers: 2, EventBatch: 33, CallbackWorkers: 3, CallbackQueue: 7, IOBudgetBytes: 4096, IOBudgetOps: 9}
    got, err := resolveEpollConfig(cfg, 99)
    if err != nil { t.Fatal(err) }
    want := resolvedEpollConfig{pollers: 2, eventBatch: 33, callbackWorkers: 3, callbackQueue: 7, ioBudgetBytes: 4096, ioBudgetOps: 9}
    if got != want { t.Fatalf("got %+v want %+v", got, want) }
}

func TestResolveEpollConfigRejectsNegativeValues(t *testing.T) {
    cases := []EpollConfig{{Pollers: -1}, {EventBatch: -1}, {CallbackWorkers: -1}, {CallbackQueue: -1}, {IOBudgetBytes: -1}, {IOBudgetOps: -1}}
    for _, cfg := range cases {
        if _, err := resolveEpollConfig(cfg, 4); !errors.Is(err, ErrInvalidEpollConfig) {
            t.Fatalf("cfg=%+v err=%v", cfg, err)
        }
    }
}
```

Also assert `gomaxprocs <= 0` resolves to one poller/worker. Add an overflow case using `CallbackWorkers: math.MaxInt` with default `CallbackQueue` and require `ErrInvalidEpollConfig` rather than integer wrap.

- [ ] **Step 2: Run RED config tests**

```bash
go test ./transport -run '^TestResolveEpollConfig' -count=1
```

Expected: FAIL because the new types/functions do not exist.

- [ ] **Step 3: Implement config + sentinels**

Add to the existing `transport/errors.go` var block:

```go
ErrBackendUnsupported  = errors.New("transport: backend unsupported on this platform")
ErrProtocolUnsupported = errors.New("transport: protocol unsupported by backend")
ErrInvalidEpollConfig  = errors.New("transport: invalid epoll configuration")
```

`resolveEpollConfig` resolves zero values exactly as the spec states:

```text
Pollers         = max(1, gomaxprocs)
EventBatch      = 256
CallbackWorkers = max(1, gomaxprocs)
CallbackQueue   = min(1024, max(64, 4 * CallbackWorkers))
IOBudgetBytes   = 256 KiB
IOBudgetOps     = 64
```

Check `4 * CallbackWorkers` before multiplying. Explicit positive `CallbackQueue` is accepted as-is; only the default formula is clamped.

- [ ] **Step 4: Run config tests GREEN**

```bash
go test ./transport -run '^TestResolveEpollConfig' -count=1
```

Expected: PASS.

- [ ] **Step 5: Write RED constructor/capability tests**

Cross-platform validation in `epoll_config_test.go`:

```go
func TestNewEpollRejectsInvalidConfig(t *testing.T) {
    _, err := NewEpoll(EpollConfig{Pollers: -1})
    if !errors.Is(err, ErrInvalidEpollConfig) { t.Fatalf("err=%v", err) }
}
```

Linux-only `epoll_capability_linux_test.go`:

```go
func TestEpollRejectsTLSWSWSSWithoutFallback(t *testing.T) {
    var observed atomic.Uint64
    e, err := NewEpoll(EpollConfig{Pollers: 1}, WithObserver(ogrenet.ObserverFunc(func(ogrenet.Event) {
        observed.Add(1)
    })))
    if err != nil { t.Fatal(err) }
    t.Cleanup(func() { _ = e.Close() })

    for _, scheme := range []ogrenet.Scheme{ogrenet.SchemeTLS, ogrenet.SchemeWS, ogrenet.SchemeWSS} {
        ep := ogrenet.Endpoint{Scheme: scheme, Host: "127.0.0.1", Port: 1}
        if _, err := e.Dial(context.Background(), ep, nil); !errors.Is(err, ErrProtocolUnsupported) {
            t.Fatalf("scheme=%s err=%v", scheme, err)
        }
    }
    if got := observed.Load(); got != 0 { t.Fatalf("unsupported operations emitted %d events", got) }
}

func TestEpollMethodProtocolMismatch(t *testing.T) {
    e, err := NewEpoll(EpollConfig{Pollers: 1})
    if err != nil { t.Fatal(err) }
    defer e.Close()
    if _, err := e.Dial(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 1}, nil); !errors.Is(err, ErrProtocolMismatch) {
        t.Fatalf("stream/udp mismatch err=%v", err)
    }
    if _, err := e.DialPacket(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 1}, nil); !errors.Is(err, ErrProtocolMismatch) {
        t.Fatalf("packet/tcp mismatch err=%v", err)
    }
}
```

Non-Linux `epoll_stub_test.go`:

```go
//go:build !linux

package transport

func TestNewEpollUnsupportedPlatformDoesNotApplyOptions(t *testing.T) {
    applied := false
    opt := func(*config) error { applied = true; return nil }
    _, err := NewEpoll(EpollConfig{}, opt)
    if !errors.Is(err, ErrBackendUnsupported) { t.Fatalf("err=%v", err) }
    if applied { t.Fatal("option applied on unsupported platform") }
}
```

- [ ] **Step 6: Run RED constructor tests on Linux**

```bash
go test ./transport -run '^Test(NewEpoll|Epoll)' -count=1
```

Expected: FAIL because `NewEpoll` does not exist.

- [ ] **Step 7: Implement constructors and the phase-6A Engine scaffold**

`epoll_constructor_stub.go`:

```go
//go:build !linux

package transport

import "github.com/qigao/ogrenet"

func NewEpoll(cfg EpollConfig, opts ...Option) (ogrenet.Engine, error) {
    if _, err := resolveEpollConfig(cfg, 1); err != nil { return nil, err }
    return nil, ErrBackendUnsupported
}
```

Linux `NewEpoll` must resolve `runtime.GOMAXPROCS(0)`, apply the same `Option` loop as `New`, validate `cfg.limits`, and return `*epollEngine`.

The 6A scaffold implements `ogrenet.Engine` with no socket I/O:

```go
type epollEngine struct {
    cfg      config
    epollCfg resolvedEpollConfig
    done     chan struct{}
    once     sync.Once
}

func (e *epollEngine) Stats() ogrenet.EngineStats { return ogrenet.EngineStats{} }
func (e *epollEngine) Done() <-chan struct{} { return e.done }
func (e *epollEngine) Close() error { e.once.Do(func(){ close(e.done) }); return nil }
func (e *epollEngine) Shutdown(ctx context.Context) error {
    if ctx == nil { return ErrNilContext }
    if cause := context.Cause(ctx); cause != nil { return cause }
    _ = e.Close()
    select {
    case <-e.done:
        return nil
    case <-ctx.Done():
        return context.Cause(ctx)
    }
}
```

For the branch-local 6A scaffold only:

```text
Listen/Dial(tls|ws|wss)        -> ErrProtocolUnsupported
Listen/Dial(udp)               -> ErrProtocolMismatch
ListenPacket/DialPacket(tcp/...) -> ErrProtocolMismatch unless scheme==udp
TCP and UDP matching methods   -> ErrProtocolUnsupported until 6B/6C replace the scaffold
```

Add `var _ ogrenet.Engine = (*epollEngine)(nil)`. No PR/release text may describe TCP/UDP as supported at this checkpoint.

- [ ] **Step 8: Run Linux tests and compile non-Linux test binaries**

```bash
go test ./transport -run '^Test(NewEpoll|Epoll)' -count=1
GOOS=windows GOARCH=amd64 go test -c ./transport -o /tmp/ogrenet-transport-windows-amd64.test.exe
GOOS=darwin GOARCH=arm64 go test -c ./transport -o /tmp/ogrenet-transport-darwin-arm64.test
GOOS=freebsd GOARCH=amd64 go test -c ./transport -o /tmp/ogrenet-transport-freebsd-amd64.test
```

Expected: Linux PASS; all cross-compiled test binaries build successfully. Windows/macOS CI later executes `epoll_stub_test.go` natively.

- [ ] **Step 9: Commit**

```bash
git add -- transport/errors.go transport/epoll_config.go transport/epoll_config_test.go transport/epoll_constructor_linux.go transport/epoll_constructor_stub.go transport/epoll_engine_phase6a_linux.go transport/epoll_capability_linux_test.go transport/epoll_stub_test.go
git commit -m "runtime: define explicit epoll backend contract"
```

---

### Task 2: Add public-only portable contract characterization harness

**Files:**
- Create: `transport/contract_harness_test.go`
- Create: `transport/contract_tcp_test.go`
- Create: `transport/contract_udp_test.go`

**Interfaces:**

```go
type contractProfile struct { TCP, UDP bool }

type engineFactory struct {
    name    string
    profile contractProfile
    new     func(t *testing.T, opts ...transport.Option) ogrenet.Engine
}

func runEngineContracts(t *testing.T, f engineFactory)
func runTCPContract(t *testing.T, f engineFactory)
func runUDPContract(t *testing.T, f engineFactory)
```

All three files use `package transport_test`; they may import root `ogrenet` and public `transport`, but no private helper from package `transport`.

- [ ] **Step 1: Add the harness and portable factory**

```go
func portableFactory() engineFactory {
    return engineFactory{
        name: "portable",
        profile: contractProfile{TCP: true, UDP: true},
        new: func(t *testing.T, opts ...transport.Option) ogrenet.Engine {
            t.Helper()
            e, err := transport.New(opts...)
            if err != nil { t.Fatal(err) }
            t.Cleanup(func() { _ = e.Close() })
            return e
        },
    }
}

func runEngineContracts(t *testing.T, f engineFactory) {
    t.Run(f.name+"/tcp", func(t *testing.T) { if f.profile.TCP { runTCPContract(t, f) } })
    t.Run(f.name+"/udp", func(t *testing.T) { if f.profile.UDP { runUDPContract(t, f) } })
}

func TestEnginePublicContracts(t *testing.T) { runEngineContracts(t, portableFactory()) }
```

This is characterization of already-working public behavior, so it is expected to pass immediately; it is not a production-code RED step.

- [ ] **Step 2: Implement the TCP public contract case**

Use only public APIs. The test shape is fixed:

```go
func runTCPContract(t *testing.T, f engineFactory) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    e := f.new(t)

    serverSession := make(chan ogrenet.Session, 1)
    serverClosed := make(chan error, 1)
    ln, err := e.Listen(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, ogrenet.HandlerFuncs{
        Open: func(s ogrenet.Session) { serverSession <- s },
        Message: func(s ogrenet.Session, m ogrenet.Message) {
            if err := s.Send(context.Background(), m); err != nil { serverClosed <- err }
        },
        Close: func(_ ogrenet.Session, err error) { serverClosed <- err },
    })
    if err != nil { t.Fatal(err) }
    defer ln.Close()

    received := make(chan ogrenet.Message, 1)
    client, err := e.Dial(ctx, ln.Endpoint(), ogrenet.HandlerFuncs{Message: func(_ ogrenet.Session, m ogrenet.Message) { received <- m }})
    if err != nil { t.Fatal(err) }
    peer := <-serverSession

    if err := client.Send(ctx, ogrenet.Text("contract-ping")); err != nil { t.Fatal(err) }
    select {
    case msg := <-received:
        if string(msg.Data) != "contract-ping" { t.Fatalf("payload=%q", msg.Data) }
    case <-ctx.Done():
        t.Fatal(context.Cause(ctx))
    }

    cs := client.Stats()
    if cs.MessagesTX != 1 || cs.MessagesRX != 1 || cs.BytesTX == 0 || cs.BytesRX == 0 { t.Fatalf("client stats=%+v", cs) }

    _ = client.Close()
    _ = peer.Close()
    <-client.Done()
    <-peer.Done()
    if client.Err() != nil { t.Fatalf("client err=%v", client.Err()) }
}
```

If the existing public API uses the bound endpoint through `ln.Endpoint()` as expected, keep it. If the portable characterization proves `Endpoint()` does not carry the bound ephemeral port, construct the dial endpoint from `ln.Addr()` in the harness only; do not change production semantics merely to suit the fixture.

- [ ] **Step 3: Implement connected + unconnected UDP public cases**

Representative connected echo:

```go
func runUDPContract(t *testing.T, f engineFactory) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    e := f.new(t)

    serverRecv := make(chan []byte, 1)
    server, err := e.ListenPacket(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 0}, ogrenet.PacketHandlerFuncs{
        Packet: func(pc ogrenet.PacketConn, peer net.Addr, p ogrenet.Packet) {
            serverRecv <- append([]byte(nil), p.Data...)
            _ = pc.SendTo(context.Background(), peer, p)
        },
    })
    if err != nil { t.Fatal(err) }
    defer server.Close()

    clientRecv := make(chan []byte, 1)
    client, err := e.DialPacket(ctx, server.Endpoint(), ogrenet.PacketHandlerFuncs{
        Packet: func(_ ogrenet.PacketConn, _ net.Addr, p ogrenet.Packet) { clientRecv <- append([]byte(nil), p.Data...) },
    })
    if err != nil { t.Fatal(err) }
    defer client.Close()

    if err := client.Send(ctx, ogrenet.Packet{Data: []byte("udp-contract")}); err != nil { t.Fatal(err) }
    select { case <-serverRecv: case <-ctx.Done(): t.Fatal(context.Cause(ctx)) }
    select { case got := <-clientRecv: if string(got) != "udp-contract" { t.Fatalf("payload=%q", got) }; case <-ctx.Done(): t.Fatal(context.Cause(ctx)) }

    cs := client.Stats()
    if cs.PacketsTX != 1 || cs.PacketsRX != 1 || cs.BytesTX == 0 || cs.BytesRX == 0 { t.Fatalf("client stats=%+v", cs) }

    if err := client.SendTo(ctx, server.LocalAddr(), ogrenet.Packet{Data: []byte("x")}); !errors.Is(err, transport.ErrPeerMismatch) {
        t.Fatalf("connected SendTo mismatch err=%v", err)
    }
}
```

The server side itself exercises the unconnected `ListenPacket` + `SendTo` path. Add deterministic `Close`/`Done` assertions for both sockets after the echo.

- [ ] **Step 4: Run characterization repeatedly**

```bash
go test ./transport -run '^TestEnginePublicContracts$' -count=5
```

Expected: PASS. If the fixture exposes an incorrect assumption, fix only the public test fixture unless an actual root-contract bug is demonstrated.

- [ ] **Step 5: Commit**

```bash
git add -- transport/contract_harness_test.go transport/contract_tcp_test.go transport/contract_udp_test.go
git commit -m "test: add backend-neutral transport contract harness"
```

---

### Task 3: Extract `SendGate` into `internal/runtimecore`

**Files:**
- Create: `internal/runtimecore/gate.go`
- Create: `internal/runtimecore/gate_test.go`
- Modify: `transport/gate.go`

**Interfaces:**

```go
type SendGate struct { /* private */ }
func NewSendGate() SendGate
func (g *SendGate) Enter() bool
func (g *SendGate) Leave()
func (g *SendGate) Close() <-chan struct{}
func (g *SendGate) Done() <-chan struct{}
```

Returning the value keeps the transport wrapper at one heap object per gate rather than allocating a wrapper plus a separately allocated core object.

- [ ] **Step 1: Write RED core tests**

```go
func TestSendGateCloseWaitsForActiveOwners(t *testing.T) {
    g := NewSendGate()
    if !g.Enter() || !g.Enter() { t.Fatal("enter rejected") }
    done := g.Close()
    select { case <-done: t.Fatal("closed with active owners"); default: }
    g.Leave()
    select { case <-done: t.Fatal("closed with one active owner"); default: }
    g.Leave()
    select { case <-done: default: t.Fatal("did not close after final leave") }
}

func TestSendGateRejectsEnterAfterClose(t *testing.T) {
    g := NewSendGate(); <-g.Close()
    if g.Enter() { t.Fatal("enter succeeded after close") }
}

func TestSendGateCloseIsIdempotent(t *testing.T) {
    g := NewSendGate()
    a, b := g.Close(), g.Close()
    if a != b { t.Fatal("close returned different barriers") }
}
```

- [ ] **Step 2: Run RED**

```bash
go test ./internal/runtimecore -run '^TestSendGate' -count=1
```

Expected: FAIL because the package/type does not exist.

- [ ] **Step 3: Implement exact current gate semantics**

Copy the current mutex/closed/active/drained-channel ownership only; no new states, contexts, callbacks, or atomics.

- [ ] **Step 4: Run core tests GREEN**

```bash
go test ./internal/runtimecore -run '^TestSendGate' -count=1
```

- [ ] **Step 5: Replace `transport/gate.go` with a compatibility wrapper**

```go
type sendGate struct { core runtimecore.SendGate }
func newSendGate() *sendGate { return &sendGate{core: runtimecore.NewSendGate()} }
func (g *sendGate) enter() bool { return g.core.Enter() }
func (g *sendGate) leave() { g.core.Leave() }
func (g *sendGate) close() <-chan struct{} { return g.core.Close() }
func (g *sendGate) done() <-chan struct{} { return g.core.Done() }
```

Do not touch `conn.go`, `packet.go`, or `websocket.go` call sites.

- [ ] **Step 6: Verify portable behavior/races**

```bash
go test ./transport -run 'Graceful|Send|TrySend' -count=1
go test -race ./transport -run 'Graceful|Send|TrySend' -count=5
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add -- internal/runtimecore/gate.go internal/runtimecore/gate_test.go transport/gate.go
git commit -m "runtime: share send gate ownership core"
```

---

### Task 4: Extract Session lifecycle ownership into `internal/runtimecore`

**Files:**
- Create: `internal/runtimecore/lifecycle.go`
- Create: `internal/runtimecore/lifecycle_test.go`
- Modify: `transport/lifecycle.go`
- Modify: `transport/lifecycle_test.go`

**Interfaces:**

```go
type CloseGoal uint8
const (
    GoalRunning CloseGoal = iota
    GoalWrite
    GoalFull
    GoalAbort
)

type AbortReason uint8
const (
    AbortNone AbortReason = iota
    AbortExplicit
    AbortCaller
    AbortFailure
)

type Lifecycle struct { /* private */ }
func NewLifecycle() Lifecycle
func (l *Lifecycle) Request(CloseGoal) bool
func (l *Lifecycle) RequestWithPrevious(CloseGoal) (bool, CloseGoal)
func (l *Lifecycle) Abort(AbortReason) bool
func (l *Lifecycle) AbortWith(AbortReason, func()) bool
func (l *Lifecycle) Reason() AbortReason
func (l *Lifecycle) WriteRequested() <-chan struct{}
func (l *Lifecycle) FullRequested() <-chan struct{}
func (l *Lifecycle) Aborted() <-chan struct{}
func (l *Lifecycle) ReadDone() <-chan struct{}
func (l *Lifecycle) WriteDone() <-chan struct{}
func (l *Lifecycle) TerminalDone() <-chan struct{}
func (l *Lifecycle) MarkReadClosed()
func (l *Lifecycle) MarkWriteClosed()
func (l *Lifecycle) TryMarkTerminal() bool
func (l *Lifecycle) MarkTerminal()
```

- [ ] **Step 1: Write RED core lifecycle tests**

Required deterministic tests:

```go
func TestLifecycleWriteThenFullEscalation(t *testing.T)
func TestLifecycleAbortPublishesBeforeSignals(t *testing.T)
func TestLifecycleFirstAbortOwnerWins(t *testing.T)
func TestLifecycleTerminalClosesReadWriteAndTerminal(t *testing.T)
func TestLifecycleAbortCannotBeReplacedByTerminalMark(t *testing.T)
```

For publish-before-signal:

```go
func TestLifecycleAbortPublishesBeforeSignals(t *testing.T) {
    l := NewLifecycle()
    var published atomic.Bool
    seen := make(chan bool, 1)
    go func() { <-l.Aborted(); seen <- published.Load() }()
    if !l.AbortWith(AbortFailure, func(){ published.Store(true) }) { t.Fatal("abort lost") }
    if !<-seen { t.Fatal("abort signal visible before publish") }
}
```

- [ ] **Step 2: Run RED**

```bash
go test ./internal/runtimecore -run '^TestLifecycle' -count=1
```

Expected: FAIL because `Lifecycle` does not exist.

- [ ] **Step 3: Implement lifecycle by semantic copy, not redesign**

Preserve current mutex + `sync.Once` signaling exactly. `AbortWith` keeps the winning publish closure serialized before abort/read/write signals become observable; a losing abort cannot return while a winning publish is partially applied.

- [ ] **Step 4: Run core lifecycle tests GREEN**

```bash
go test ./internal/runtimecore -run '^TestLifecycle' -count=1
```

- [ ] **Step 5: Wrap core behind the existing transport-private surface**

```go
type sessionLifecycle struct { core runtimecore.Lifecycle }
func newSessionLifecycle() *sessionLifecycle { return &sessionLifecycle{core: runtimecore.NewLifecycle()} }
```

Use explicit conversion helpers:

```go
func coreGoal(g closeGoal) runtimecore.CloseGoal {
    switch g { case closeGoalWrite: return runtimecore.GoalWrite; case closeGoalFull: return runtimecore.GoalFull; case closeGoalAbort: return runtimecore.GoalAbort; default: return runtimecore.GoalRunning }
}

func coreReason(r abortReason) runtimecore.AbortReason {
    switch r { case abortExplicit: return runtimecore.AbortExplicit; case abortCaller: return runtimecore.AbortCaller; case abortFailure: return runtimecore.AbortFailure; default: return runtimecore.AbortNone }
}
```

Add `transport/lifecycle_test.go` mapping coverage:

```go
func TestLifecycleCoreMapping(t *testing.T) {
    goals := []struct{ local closeGoal; core runtimecore.CloseGoal }{{closeGoalRunning, runtimecore.GoalRunning}, {closeGoalWrite, runtimecore.GoalWrite}, {closeGoalFull, runtimecore.GoalFull}, {closeGoalAbort, runtimecore.GoalAbort}}
    for _, tc := range goals { if got := coreGoal(tc.local); got != tc.core { t.Fatalf("goal %v -> %v", tc.local, got) } }
    reasons := []struct{ local abortReason; core runtimecore.AbortReason }{{abortNone, runtimecore.AbortNone}, {abortExplicit, runtimecore.AbortExplicit}, {abortCaller, runtimecore.AbortCaller}, {abortFailure, runtimecore.AbortFailure}}
    for _, tc := range reasons { if got := coreReason(tc.local); got != tc.core { t.Fatalf("reason %v -> %v", tc.local, got) } }
}
```

Keep all existing lower-case lifecycle methods as delegators, so `conn.go`/`websocket.go` do not change.

- [ ] **Step 6: Verify P0-3/P0-4 ownership + allocation**

```bash
go test ./transport -run 'Lifecycle|Graceful|HalfClose|Terminal|TypedError|ErrorOwnership' -count=1
go test -race ./transport -run 'Lifecycle|Graceful|HalfClose|Terminal|TypedError|ErrorOwnership' -count=10
go test ./transport -run '^$' -bench 'BenchmarkGraceful(SendRunning|TrySendRunning)' -benchmem -benchtime=1x -count=5
```

Expected: all tests/races PASS and existing Go-version-specific graceful allocation gates remain satisfied.

- [ ] **Step 7: Commit**

```bash
git add -- internal/runtimecore/lifecycle.go internal/runtimecore/lifecycle_test.go transport/lifecycle.go transport/lifecycle_test.go
git commit -m "runtime: share session lifecycle ownership core"
```

---

### Task 5: Extract bounded Observer delivery into `internal/runtimecore`

**Files:**
- Create: `internal/runtimecore/observer.go`
- Create: `internal/runtimecore/observer_test.go`
- Modify: `transport/observer.go`
- Modify: `transport/stats.go`

**Interfaces:**

```go
type ObserverDispatcher struct { /* private */ }
func NewObserverDispatcher(ogrenet.Observer, int) *ObserverDispatcher
func (d *ObserverDispatcher) Emit(ogrenet.Event) bool
func (d *ObserverDispatcher) Stop()
func (d *ObserverDispatcher) Dropped() uint64
func (d *ObserverDispatcher) Panics() uint64
```

`NewObserverDispatcher(nil, n)` returns nil. There is deliberately no Wait/Join API: a blocked Observer callback stays outside the Engine `Done()` barrier.

- [ ] **Step 1: Write RED core dispatcher tests**

Required tests:

```go
func TestObserverDispatcherOverflowIsNonBlockingAndCounted(t *testing.T)
func TestObserverDispatcherRecoversPanicsAndContinues(t *testing.T)
func TestObserverDispatcherStopMakesFutureEmitNoop(t *testing.T)
func TestObserverDispatcherStopDoesNotWaitForBlockedCallback(t *testing.T)
```

Use synchronization channels, not sleeps. A blocked observer test should enter `Observe`, signal `entered`, block on `release`, call `Stop`, assert `Stop` returned immediately, then release the callback.

- [ ] **Step 2: Run RED**

```bash
go test ./internal/runtimecore -run '^TestObserverDispatcher' -count=1
```

Expected: FAIL because dispatcher does not exist.

- [ ] **Step 3: Implement exact P0-5 dispatcher semantics**

Use one bounded event channel, one worker, `stopped/dropped/panics` atomics, and a separate stop channel. Never close the event queue. `Emit` is nonblocking and increments dropped count only for queue saturation; emits after stopped are no-ops. Recover around every Observer callback and continue after panic. `Stop` marks stopped before signaling stop and never waits for user code.

- [ ] **Step 4: Run core tests GREEN**

```bash
go test ./internal/runtimecore -run '^TestObserverDispatcher' -count=10
```

- [ ] **Step 5: Preserve the transport wrapper and disabled path**

`transport/observer.go` retains `defaultObserverBuffer`, `WithObserver`, `WithObserverBuffer`, and `Engine.observeSetup`.

```go
type observerDispatcher struct { core *runtimecore.ObserverDispatcher }

func newObserverDispatcher(o ogrenet.Observer, n int) *observerDispatcher {
    core := runtimecore.NewObserverDispatcher(o, n)
    if core == nil { return nil }
    return &observerDispatcher{core: core}
}
func (d *observerDispatcher) emit(e ogrenet.Event) bool { return d != nil && d.core.Emit(e) }
func (d *observerDispatcher) stop() { if d != nil { d.core.Stop() } }
func (d *observerDispatcher) droppedCount() uint64 { if d == nil { return 0 }; return d.core.Dropped() }
func (d *observerDispatcher) panicCount() uint64 { if d == nil { return 0 }; return d.core.Panics() }
```

`transport/stats.go` changes only direct dispatcher-field loads to `droppedCount()` / `panicCount()`. Do not change public Stats counting points.

- [ ] **Step 6: Verify P0-5 semantics/races/allocations**

```bash
go test ./transport -run 'Observer|Observability' -count=1
go test -race ./transport -run '^TestObservabilityRace' -count=20
go test ./transport -run '^$' -bench 'BenchmarkObserver|Benchmark.*StatsSnapshot' -benchmem -benchtime=100x -count=3
```

Expected: observer-disabled and Stats snapshot deterministic benchmarks remain 0 allocs/op; saturation, panic isolation, and blocked-observer independence remain unchanged.

- [ ] **Step 7: Commit**

```bash
git add -- internal/runtimecore/observer.go internal/runtimecore/observer_test.go transport/observer.go transport/stats.go
git commit -m "runtime: share bounded observer dispatcher core"
```

---

### Task 6: Lock runtimecore dependency direction and prepare the epoll factory seam

**Files:**
- Create: `internal/runtimecore/dependency_test.go`
- Create: `transport/contract_native_linux_test.go`

**Interfaces:**

```go
func epollFactory(profile contractProfile) engineFactory
```

- [ ] **Step 1: Add a direct-import boundary test with the Go parser**

Avoid spawning nested `go list` processes and avoid adding `go/packages`. `dependency_test.go` parses only `.go` files in its own directory and rejects direct imports of implementation packages:

```go
func TestRuntimecoreDoesNotImportTransportOrNativePollers(t *testing.T) {
    forbidden := map[string]bool{
        "github.com/qigao/ogrenet/transport": true,
        "github.com/qigao/ogrenet/epoll": true,
        "github.com/qigao/ogrenet/kqueue": true,
        "github.com/qigao/ogrenet/iocp": true,
    }
    entries, err := os.ReadDir(".")
    if err != nil { t.Fatal(err) }
    fset := token.NewFileSet()
    for _, ent := range entries {
        if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".go") { continue }
        f, err := parser.ParseFile(fset, ent.Name(), nil, parser.ImportsOnly)
        if err != nil { t.Fatal(err) }
        for _, imp := range f.Imports {
            path, err := strconv.Unquote(imp.Path.Value)
            if err != nil { t.Fatal(err) }
            if forbidden[path] { t.Fatalf("%s imports forbidden %s", ent.Name(), path) }
        }
    }
}
```

The root `github.com/qigao/ogrenet` import is allowed for public Observer/Event types.

- [ ] **Step 2: Run the dependency test**

```bash
go test ./internal/runtimecore -run '^TestRuntimecoreDoesNotImport' -count=1
```

Expected: PASS.

- [ ] **Step 3: Add the Linux epoll contract factory without enabling TCP/UDP**

`transport/contract_native_linux_test.go`:

```go
//go:build linux

package transport_test

func epollFactory(profile contractProfile) engineFactory {
    return engineFactory{
        name: "epoll",
        profile: profile,
        new: func(t *testing.T, opts ...transport.Option) ogrenet.Engine {
            t.Helper()
            e, err := transport.NewEpoll(transport.EpollConfig{Pollers: 1}, opts...)
            if err != nil { t.Fatal(err) }
            t.Cleanup(func() { _ = e.Close() })
            return e
        },
    }
}

func TestEpollPhase6AContractProfile(t *testing.T) {
    f := epollFactory(contractProfile{})
    if f.profile.TCP || f.profile.UDP { t.Fatalf("6A profile unexpectedly enables TCP/UDP: %+v", f.profile) }
    e := f.new(t)
    ep := ogrenet.Endpoint{Scheme: ogrenet.SchemeTLS, Host: "127.0.0.1", Port: 1}
    if _, err := e.Dial(context.Background(), ep, nil); !errors.Is(err, transport.ErrProtocolUnsupported) {
        t.Fatalf("TLS capability err=%v", err)
    }
}
```

Do not register epoll in `TestEnginePublicContracts` with a true protocol bit during 6A. 6B turns on `TCP:true`; 6C turns on `UDP:true`. Once a bit is true, contract failures must fail the test rather than being dynamically skipped.

- [ ] **Step 4: Verify contract seam + full core tests**

```bash
go test ./transport -run 'EnginePublicContracts|EpollPhase6A|EpollRejects' -count=5
go test ./internal/runtimecore -count=5
```

Expected: portable contract characterization PASS; epoll capability-only profile PASS.

- [ ] **Step 5: Commit**

```bash
git add -- internal/runtimecore/dependency_test.go transport/contract_native_linux_test.go
git commit -m "test: lock native runtime semantic boundaries"
```

---

### Task 7: Full 6A verification and draft-PR checkpoint

**Files:**
- No expected production changes; only demonstrated verification fixes may be added.

- [ ] **Step 1: Format + module hygiene**

```bash
gofmt -w internal/runtimecore transport
git diff --check
go mod tidy
git diff --exit-code -- go.mod go.sum
```

Expected: formatting clean and module files unchanged. If `go mod tidy` changes them, inspect the cause and either include a justified dependency change (none is expected in 6A) or restore the module files before continuing.

- [ ] **Step 2: Full vet/unit/race verification**

```bash
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
go test -race ./internal/runtimecore -count=20
go test -race ./transport -run 'ObservabilityRace|TypedError|ErrorOwnership|Graceful' -count=20
```

Expected: PASS with no races/deadlocks.

- [ ] **Step 3: Re-run deterministic allocation gates**

```bash
go test ./transport -run '^$' -bench 'BenchmarkGraceful(SendRunning|TrySendRunning)|BenchmarkObserverDisabledEmitPath|Benchmark(Session|Packet|Engine)StatsSnapshot' -benchmem -benchtime=1x -count=5
```

Expected: existing Go-version-specific graceful Send/TrySend allocation gates remain satisfied; observer-disabled emit and Session/Packet/Engine Stats snapshots remain 0 allocs/op. Do not add a noisy ns/op percentage threshold.

- [ ] **Step 4: Cross-compile test binaries**

```bash
GOOS=windows GOARCH=amd64 go test -c ./transport -o /tmp/ogrenet-transport-windows-amd64.test.exe
GOOS=darwin GOARCH=arm64 go test -c ./transport -o /tmp/ogrenet-transport-darwin-arm64.test
GOOS=freebsd GOARCH=amd64 go test -c ./transport -o /tmp/ogrenet-transport-freebsd-amd64.test
```

Expected: compile PASS. Native Windows/macOS CI will execute the non-Linux constructor test.

- [ ] **Step 5: Verify narrow diff scope**

```bash
git diff --stat master...HEAD
git diff --name-only master...HEAD
```

Allowed scope:

```text
docs/superpowers/specs/2026-08-22-linux-native-engine-design.md
docs/superpowers/plans/2026-08-22-linux-native-engine-6a.md
internal/runtimecore/*
transport/epoll_*
transport/contract_*
transport/gate.go
transport/lifecycle.go
transport/lifecycle_test.go
transport/observer.go
transport/stats.go
transport/errors.go
```

No native socket I/O, TLS/WS implementation, kqueue/IOCP high-level Engine, or unrelated refactor is allowed.

- [ ] **Step 6: Create/update one draft PR**

Title:

```text
runtime: add Linux epoll native Engine
```

Checkpoint body:

```text
P1-6A complete: explicit backend contract, public parity harness, and shared semantic ownership core only.
No native TCP/UDP support claim yet; reactor/socket I/O begins in 6B.
Refs #57
Refs #56
Refs #38
```

Keep it draft. Do not close #57 or #56.

- [ ] **Step 7: Require exact-head CI before 6B**

Fetch the branch head SHA after all 6A commits. Require fresh CI for that exact SHA, including Linux Go 1.25/1.26, full race, Windows/macOS, FreeBSD runtime, GmSSL, and existing cross-compiles. Fix demonstrated failures before adding reactor code.

## 6A Completion Boundary

6A is complete only when:

- `transport.New()` behavior and allocation gates are unchanged;
- `EpollConfig` and capability errors are stable;
- shared construction code compiles on non-Linux and returns `ErrBackendUnsupported` there;
- Linux epoll selection is explicit and TLS/WS/WSS never fall back;
- public TCP/UDP contract characterization runs through `package transport_test` against the portable reference backend;
- the Linux epoll factory exists but advertises no TCP/UDP support yet;
- `SendGate`, Session `Lifecycle`, and bounded `ObserverDispatcher` each have one implementation under `internal/runtimecore` plus compatibility wrappers;
- `runtimecore` directly imports neither `transport` nor native poller packages;
- quota/admission/stats extraction was not forced through artificial abstraction;
- exact-head CI is green;
- the PR stays draft and #57/#56 stay open.

Only after that checkpoint may 6B add the reactor, intrusive inbox/wake handshake, deadline heap, callback executor, and native TCP socket I/O.