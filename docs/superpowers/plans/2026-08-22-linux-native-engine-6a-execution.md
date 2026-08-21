# Linux Native Engine 6A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish the explicit epoll backend contract, a backend-neutral public parity harness, and a small shared semantic core while leaving all native socket I/O for 6B/6C.

**Architecture:** `transport.New()` stays the portable reference backend. `transport.NewEpoll(EpollConfig, ...Option)` is explicit and never selected automatically. 6A moves only clean, protocol-independent ownership primitives (`SendGate`, `Lifecycle`, `ObserverDispatcher`) into `internal/runtimecore`, with private wrappers preserving the existing portable call surface and P0 semantics.

**Tech Stack:** Go 1.25+, standard library, existing `golang.org/x/sys`, root `ogrenet` contracts, race detector and existing benchmark/CI gates.

**Spec:** `docs/superpowers/specs/2026-08-22-linux-native-engine-design.md`

## Global Constraints

- No native accept/connect/read/write in 6A and no TCP/UDP native-support claim.
- TLS/WS/WSS never silently fall back to portable I/O.
- `internal/runtimecore` may import root `github.com/qigao/ogrenet`, but never `transport`, `epoll`, `kqueue`, `iocp`, TLS, WebSocket, DNS, or syscall ownership code.
- Portable `transport.New()` behavior, lifecycle/error ownership, Stats/Observer semantics, and deterministic allocation gates must not regress.
- Do not extract quota/admission/stats merely for directory symmetry; they currently depend on `transport` error/snapshot semantics and are directly reusable by a native implementation living in the same package.
- No fake unified poller abstraction, lock-free queue, pooling, scatter/gather, resolver redesign, proxy, QUIC, or HTTP work.
- Production changes use TDD; characterization-only tests for already-existing portable behavior are expected to pass immediately.

---

### Task 1: Epoll config and capability contract

**Files:**
- Create: `transport/epoll_config.go`
- Create: `transport/epoll_config_test.go`
- Create: `transport/epoll_constructor_linux.go`
- Create: `transport/epoll_constructor_stub.go`
- Create: `transport/epoll_engine_phase6a_linux.go`
- Create: `transport/epoll_capability_linux_test.go`
- Create: `transport/epoll_stub_test.go`
- Modify: `transport/errors.go`

**Produces:**

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

and direct sentinels `ErrBackendUnsupported`, `ErrProtocolUnsupported`, `ErrInvalidEpollConfig`.

- [ ] **Step 1: Write RED config tests**

```go
func TestResolveEpollConfigDefaults(t *testing.T) {
    got, err := resolveEpollConfig(EpollConfig{}, 8)
    if err != nil { t.Fatal(err) }
    want := resolvedEpollConfig{pollers: 8, eventBatch: 256, callbackWorkers: 8, callbackQueue: 64, ioBudgetBytes: 256 << 10, ioBudgetOps: 64}
    if got != want { t.Fatalf("got %+v want %+v", got, want) }
}

func TestResolveEpollConfigExplicitValues(t *testing.T) {
    cfg := EpollConfig{Pollers: 2, EventBatch: 33, CallbackWorkers: 3, CallbackQueue: 7, IOBudgetBytes: 4096, IOBudgetOps: 9}
    got, err := resolveEpollConfig(cfg, 99)
    if err != nil { t.Fatal(err) }
    want := resolvedEpollConfig{pollers: 2, eventBatch: 33, callbackWorkers: 3, callbackQueue: 7, ioBudgetBytes: 4096, ioBudgetOps: 9}
    if got != want { t.Fatalf("got %+v want %+v", got, want) }
}

func TestResolveEpollConfigRejectsInvalidValues(t *testing.T) {
    cases := []EpollConfig{{Pollers: -1}, {EventBatch: -1}, {CallbackWorkers: -1}, {CallbackQueue: -1}, {IOBudgetBytes: -1}, {IOBudgetOps: -1}, {CallbackWorkers: math.MaxInt}}
    for _, cfg := range cases {
        if _, err := resolveEpollConfig(cfg, 4); !errors.Is(err, ErrInvalidEpollConfig) {
            t.Fatalf("cfg=%+v err=%v", cfg, err)
        }
    }
}
```

Also assert `resolveEpollConfig(EpollConfig{}, 0)` resolves one poller/worker.

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^TestResolveEpollConfig' -count=1
```

Expected: compile FAIL because the config types/functions do not exist.

- [ ] **Step 3: Implement exact config resolver**

```go
const (
    defaultEpollEventBatch    = 256
    defaultEpollCallbackQueue = 64
    defaultEpollMaxCallbackQueue = 1024
    defaultEpollIOBudgetBytes = 256 << 10
    defaultEpollIOBudgetOps   = 64
)

type resolvedEpollConfig struct {
    pollers, eventBatch, callbackWorkers, callbackQueue, ioBudgetBytes, ioBudgetOps int
}

func resolveEpollConfig(cfg EpollConfig, gomaxprocs int) (resolvedEpollConfig, error) {
    if cfg.Pollers < 0 || cfg.EventBatch < 0 || cfg.CallbackWorkers < 0 || cfg.CallbackQueue < 0 || cfg.IOBudgetBytes < 0 || cfg.IOBudgetOps < 0 {
        return resolvedEpollConfig{}, ErrInvalidEpollConfig
    }
    if gomaxprocs < 1 { gomaxprocs = 1 }
    r := resolvedEpollConfig{
        pollers: cfg.Pollers, eventBatch: cfg.EventBatch, callbackWorkers: cfg.CallbackWorkers,
        callbackQueue: cfg.CallbackQueue, ioBudgetBytes: cfg.IOBudgetBytes, ioBudgetOps: cfg.IOBudgetOps,
    }
    if r.pollers == 0 { r.pollers = gomaxprocs }
    if r.eventBatch == 0 { r.eventBatch = defaultEpollEventBatch }
    if r.callbackWorkers == 0 { r.callbackWorkers = gomaxprocs }
    if r.callbackQueue == 0 {
        if r.callbackWorkers > math.MaxInt/4 { return resolvedEpollConfig{}, ErrInvalidEpollConfig }
        r.callbackQueue = 4 * r.callbackWorkers
        if r.callbackQueue < defaultEpollCallbackQueue { r.callbackQueue = defaultEpollCallbackQueue }
        if r.callbackQueue > defaultEpollMaxCallbackQueue { r.callbackQueue = defaultEpollMaxCallbackQueue }
    }
    if r.ioBudgetBytes == 0 { r.ioBudgetBytes = defaultEpollIOBudgetBytes }
    if r.ioBudgetOps == 0 { r.ioBudgetOps = defaultEpollIOBudgetOps }
    return r, nil
}
```

Add the three sentinels to `transport/errors.go` using `errors.New` and no typed P0-4 wrapper.

- [ ] **Step 4: Run config GREEN**

```bash
go test ./transport -run '^TestResolveEpollConfig' -count=1
```

Expected: PASS.

- [ ] **Step 5: Write RED constructor/capability tests**

Linux test:

```go
func TestEpollRejectsTLSWSWSSWithoutFallback(t *testing.T) {
    var observed atomic.Uint64
    e, err := NewEpoll(EpollConfig{Pollers: 1}, WithObserver(ogrenet.ObserverFunc(func(ogrenet.Event) { observed.Add(1) })))
    if err != nil { t.Fatal(err) }
    t.Cleanup(func() { _ = e.Close() })
    for _, scheme := range []ogrenet.Scheme{ogrenet.SchemeTLS, ogrenet.SchemeWS, ogrenet.SchemeWSS} {
        ep := ogrenet.Endpoint{Scheme: scheme, Host: "127.0.0.1", Port: 1}
        if _, err := e.Dial(context.Background(), ep, nil); !errors.Is(err, ErrProtocolUnsupported) {
            t.Fatalf("scheme=%s err=%v", scheme, err)
        }
    }
    if got := observed.Load(); got != 0 { t.Fatalf("unsupported operation emitted %d events", got) }
}

func TestEpollMethodProtocolMismatch(t *testing.T) {
    e, err := NewEpoll(EpollConfig{Pollers: 1})
    if err != nil { t.Fatal(err) }
    defer e.Close()
    if _, err := e.Dial(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 1}, nil); !errors.Is(err, ErrProtocolMismatch) { t.Fatalf("err=%v", err) }
    if _, err := e.DialPacket(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 1}, nil); !errors.Is(err, ErrProtocolMismatch) { t.Fatalf("err=%v", err) }
}
```

Cross-platform validation:

```go
func TestNewEpollRejectsInvalidConfig(t *testing.T) {
    if _, err := NewEpoll(EpollConfig{Pollers: -1}); !errors.Is(err, ErrInvalidEpollConfig) { t.Fatalf("err=%v", err) }
}
```

Non-Linux native CI test:

```go
//go:build !linux

func TestNewEpollUnsupportedPlatformDoesNotApplyOptions(t *testing.T) {
    applied := false
    opt := func(*config) error { applied = true; return nil }
    _, err := NewEpoll(EpollConfig{}, opt)
    if !errors.Is(err, ErrBackendUnsupported) { t.Fatalf("err=%v", err) }
    if applied { t.Fatal("option applied on unsupported platform") }
}
```

- [ ] **Step 6: Run RED constructor tests**

```bash
go test ./transport -run '^Test(NewEpoll|Epoll)' -count=1
```

Expected: compile FAIL because `NewEpoll` does not exist.

- [ ] **Step 7: Implement constructors and branch-local 6A Engine scaffold**

Non-Linux constructor validates `EpollConfig` and then immediately returns `ErrBackendUnsupported` without applying options.

Linux constructor applies options exactly like `New`:

```go
func NewEpoll(epcfg EpollConfig, opts ...Option) (ogrenet.Engine, error) {
    resolved, err := resolveEpollConfig(epcfg, runtime.GOMAXPROCS(0))
    if err != nil { return nil, err }
    cfg := defaultConfig()
    for _, opt := range opts {
        if opt == nil { continue }
        if err := opt(&cfg); err != nil { return nil, err }
    }
    if err := cfg.limits.validate(); err != nil { return nil, err }
    return &epollEngine{cfg: cfg, epollCfg: resolved, done: make(chan struct{})}, nil
}
```

6A Engine lifecycle is exact:

```go
type epollEngine struct {
    cfg config
    epollCfg resolvedEpollConfig
    done chan struct{}
    doneOnce sync.Once
}

func (e *epollEngine) Stats() ogrenet.EngineStats { return ogrenet.EngineStats{} }
func (e *epollEngine) Done() <-chan struct{} { return e.done }
func (e *epollEngine) Close() error { e.doneOnce.Do(func(){ close(e.done) }); return nil }
func (e *epollEngine) Shutdown(ctx context.Context) error {
    if ctx == nil { return ErrNilContext }
    if cause := context.Cause(ctx); cause != nil { return cause }
    _ = e.Close()
    select { case <-e.done: return nil; case <-ctx.Done(): return context.Cause(ctx) }
}
```

Method routing during 6A:

```text
Listen/Dial with udp            -> ErrProtocolMismatch
Listen/Dial with tls/ws/wss     -> ErrProtocolUnsupported
Listen/Dial with tcp            -> ErrProtocolUnsupported (temporary 6A scaffold)
ListenPacket/DialPacket with udp -> ErrProtocolUnsupported (temporary 6A scaffold)
all other packet schemes        -> ErrProtocolMismatch
```

No resource/admission/DNS/observer setup event occurs for these scaffold errors. Add `var _ ogrenet.Engine = (*epollEngine)(nil)`.

- [ ] **Step 8: Run Linux tests + cross-compile tests correctly**

```bash
go test ./transport -run '^Test(NewEpoll|Epoll)' -count=1
GOOS=windows GOARCH=amd64 go test -c ./transport -o /tmp/ogrenet-transport-windows-amd64.test.exe
GOOS=darwin GOARCH=arm64 go test -c ./transport -o /tmp/ogrenet-transport-darwin-arm64.test
GOOS=freebsd GOARCH=amd64 go test -c ./transport -o /tmp/ogrenet-transport-freebsd-amd64.test
```

Expected: PASS/build success.

- [ ] **Step 9: Commit**

```bash
git add -- transport/errors.go transport/epoll_config.go transport/epoll_config_test.go transport/epoll_constructor_linux.go transport/epoll_constructor_stub.go transport/epoll_engine_phase6a_linux.go transport/epoll_capability_linux_test.go transport/epoll_stub_test.go
git commit -m "runtime: define explicit epoll backend contract"
```

---

### Task 2: Public-only portable contract characterization

**Files:**
- Create: `transport/contract_harness_test.go`
- Create: `transport/contract_tcp_test.go`
- Create: `transport/contract_udp_test.go`

**Produces:** `engineFactory`, `contractProfile`, `runEngineContracts`, `runTCPContract`, `runUDPContract` in `package transport_test`.

- [ ] **Step 1: Add harness**

```go
type contractProfile struct { TCP, UDP bool }
type engineFactory struct {
    name string
    profile contractProfile
    new func(t *testing.T, opts ...transport.Option) ogrenet.Engine
}

func portableFactory() engineFactory {
    return engineFactory{name: "portable", profile: contractProfile{TCP: true, UDP: true}, new: func(t *testing.T, opts ...transport.Option) ogrenet.Engine {
        t.Helper(); e, err := transport.New(opts...); if err != nil { t.Fatal(err) }; t.Cleanup(func(){ _ = e.Close() }); return e
    }}
}

func runEngineContracts(t *testing.T, f engineFactory) {
    if f.profile.TCP { t.Run(f.name+"/tcp", func(t *testing.T){ runTCPContract(t, f) }) }
    if f.profile.UDP { t.Run(f.name+"/udp", func(t *testing.T){ runUDPContract(t, f) }) }
}
func TestEnginePublicContracts(t *testing.T) { runEngineContracts(t, portableFactory()) }
```

- [ ] **Step 2: Add TCP characterization**

Create one public-only loopback echo fixture. Use `Listener.Endpoint()` when it contains the bound port; if characterization proves otherwise, construct the dial endpoint from `Listener.Addr()` only in test code.

```go
func runTCPContract(t *testing.T, f engineFactory) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second); defer cancel()
    e := f.new(t)
    accepted := make(chan ogrenet.Session, 1)
    recv := make(chan ogrenet.Message, 1)
    ln, err := e.Listen(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, ogrenet.HandlerFuncs{
        Open: func(s ogrenet.Session){ accepted <- s },
        Message: func(s ogrenet.Session, m ogrenet.Message){ _ = s.Send(context.Background(), m) },
    })
    if err != nil { t.Fatal(err) }
    defer ln.Close()
    client, err := e.Dial(ctx, ln.Endpoint(), ogrenet.HandlerFuncs{Message: func(_ ogrenet.Session, m ogrenet.Message){ recv <- m }})
    if err != nil { t.Fatal(err) }
    peer := <-accepted
    if err := client.Send(ctx, ogrenet.Text("contract-ping")); err != nil { t.Fatal(err) }
    select { case m := <-recv: if string(m.Data) != "contract-ping" { t.Fatalf("payload=%q", m.Data) }; case <-ctx.Done(): t.Fatal(context.Cause(ctx)) }
    s := client.Stats()
    if s.MessagesTX != 1 || s.MessagesRX != 1 || s.BytesTX == 0 || s.BytesRX == 0 { t.Fatalf("stats=%+v", s) }
    _ = client.Close(); _ = peer.Close(); <-client.Done(); <-peer.Done()
    if client.Err() != nil { t.Fatalf("client err=%v", client.Err()) }
}
```

- [ ] **Step 3: Add UDP characterization**

```go
func runUDPContract(t *testing.T, f engineFactory) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second); defer cancel()
    e := f.new(t)
    serverSeen := make(chan []byte, 1)
    server, err := e.ListenPacket(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 0}, ogrenet.PacketHandlerFuncs{Packet: func(pc ogrenet.PacketConn, peer net.Addr, p ogrenet.Packet){
        serverSeen <- append([]byte(nil), p.Data...); _ = pc.SendTo(context.Background(), peer, p)
    }})
    if err != nil { t.Fatal(err) }
    clientSeen := make(chan []byte, 1)
    client, err := e.DialPacket(ctx, server.Endpoint(), ogrenet.PacketHandlerFuncs{Packet: func(_ ogrenet.PacketConn, _ net.Addr, p ogrenet.Packet){ clientSeen <- append([]byte(nil), p.Data...) }})
    if err != nil { t.Fatal(err) }
    if err := client.Send(ctx, ogrenet.Packet{Data: []byte("udp-contract")}); err != nil { t.Fatal(err) }
    select { case <-serverSeen: case <-ctx.Done(): t.Fatal(context.Cause(ctx)) }
    select { case got := <-clientSeen: if string(got) != "udp-contract" { t.Fatalf("payload=%q", got) }; case <-ctx.Done(): t.Fatal(context.Cause(ctx)) }
    s := client.Stats(); if s.PacketsTX != 1 || s.PacketsRX != 1 || s.BytesTX == 0 || s.BytesRX == 0 { t.Fatalf("stats=%+v", s) }
    mismatch := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1}
    if _, ok := client.RemoteAddr().(*net.UDPAddr); !ok { t.Fatalf("remote=%T", client.RemoteAddr()) }
    if err := client.SendTo(ctx, mismatch, ogrenet.Packet{Data: []byte("x")}); !errors.Is(err, transport.ErrPeerMismatch) { t.Fatalf("mismatch err=%v", err) }
    _ = client.Close(); _ = server.Close(); <-client.Done(); <-server.Done()
}
```

This one case covers connected `DialPacket` plus unconnected `ListenPacket`/`SendTo`.

- [ ] **Step 4: Run characterization repeatedly**

```bash
go test ./transport -run '^TestEnginePublicContracts$' -count=5
```

Expected: PASS; this test characterizes existing behavior and does not require a RED production failure.

- [ ] **Step 5: Commit**

```bash
git add -- transport/contract_harness_test.go transport/contract_tcp_test.go transport/contract_udp_test.go
git commit -m "test: add backend-neutral transport contract harness"
```

---

### Task 3: Share `SendGate`

**Files:** Create `internal/runtimecore/gate.go`, `gate_test.go`; modify `transport/gate.go`.

- [ ] **Step 1: Write RED tests**

```go
func TestSendGateCloseWaitsForOwners(t *testing.T) {
    g := NewSendGate(); if !g.Enter() || !g.Enter() { t.Fatal("enter") }; done := g.Close()
    select { case <-done: t.Fatal("early close"); default: }
    g.Leave(); select { case <-done: t.Fatal("early close"); default: }
    g.Leave(); select { case <-done: default: t.Fatal("not closed") }
}
func TestSendGateRejectsAfterClose(t *testing.T) { g := NewSendGate(); <-g.Close(); if g.Enter(){ t.Fatal("entered") } }
func TestSendGateCloseIdempotent(t *testing.T) { g := NewSendGate(); if g.Close() != g.Close(){ t.Fatal("barrier changed") } }
```

- [ ] **Step 2: Run RED**

```bash
go test ./internal/runtimecore -run '^TestSendGate' -count=1
```

- [ ] **Step 3: Implement exact core**

```go
type SendGate struct { mu sync.Mutex; closed bool; active int; drained chan struct{} }
func NewSendGate() SendGate { return SendGate{drained: make(chan struct{})} }
func (g *SendGate) Enter() bool { g.mu.Lock(); defer g.mu.Unlock(); if g.closed { return false }; g.active++; return true }
func (g *SendGate) Leave() { g.mu.Lock(); defer g.mu.Unlock(); g.active--; if g.closed && g.active == 0 { close(g.drained) } }
func (g *SendGate) Close() <-chan struct{} { g.mu.Lock(); defer g.mu.Unlock(); if !g.closed { g.closed = true; if g.active == 0 { close(g.drained) } }; return g.drained }
func (g *SendGate) Done() <-chan struct{} { g.mu.Lock(); defer g.mu.Unlock(); return g.drained }
```

- [ ] **Step 4: Run GREEN, then wrap portable surface**

```bash
go test ./internal/runtimecore -run '^TestSendGate' -count=1
```

`transport/gate.go` becomes:

```go
type sendGate struct { core runtimecore.SendGate }
func newSendGate() *sendGate { return &sendGate{core: runtimecore.NewSendGate()} }
func (g *sendGate) enter() bool { return g.core.Enter() }
func (g *sendGate) leave(){ g.core.Leave() }
func (g *sendGate) close() <-chan struct{} { return g.core.Close() }
func (g *sendGate) done() <-chan struct{} { return g.core.Done() }
```

- [ ] **Step 5: Regression/race**

```bash
go test ./transport -run 'Graceful|Send|TrySend' -count=1
go test -race ./transport -run 'Graceful|Send|TrySend' -count=5
```

- [ ] **Step 6: Commit**

```bash
git add -- internal/runtimecore/gate.go internal/runtimecore/gate_test.go transport/gate.go
git commit -m "runtime: share send gate ownership core"
```

---

### Task 4: Share Session `Lifecycle`

**Files:** Create `internal/runtimecore/lifecycle.go`, `lifecycle_test.go`; modify `transport/lifecycle.go`, `transport/lifecycle_test.go`.

- [ ] **Step 1: Write RED core tests**

Required names: `TestLifecycleWriteThenFullEscalation`, `TestLifecycleAbortPublishesBeforeSignals`, `TestLifecycleFirstAbortOwnerWins`, `TestLifecycleTerminalClosesAllBarriers`, `TestLifecycleAbortCannotBeReplacedByTerminalMark`.

Publish ordering test:

```go
func TestLifecycleAbortPublishesBeforeSignals(t *testing.T) {
    l := NewLifecycle(); var published atomic.Bool; seen := make(chan bool, 1)
    go func(){ <-l.Aborted(); seen <- published.Load() }()
    if !l.AbortWith(AbortFailure, func(){ published.Store(true) }) { t.Fatal("lost abort") }
    if !<-seen { t.Fatal("signal preceded publication") }
}
```

- [ ] **Step 2: Run RED**

```bash
go test ./internal/runtimecore -run '^TestLifecycle' -count=1
```

- [ ] **Step 3: Implement exact lifecycle state machine**

Use these exact enum values and fields:

```go
type CloseGoal uint8
const ( GoalRunning CloseGoal = iota; GoalWrite; GoalFull; GoalAbort )
type AbortReason uint8
const ( AbortNone AbortReason = iota; AbortExplicit; AbortCaller; AbortFailure )

type Lifecycle struct {
    mu sync.Mutex; goal CloseGoal; why AbortReason; terminal bool
    writeReq, fullReq, abortCh, readCh, writeCh, termCh chan struct{}
    writeReqOnce, fullReqOnce, abortOnce, readOnce, writeOnce, termOnce sync.Once
}
func NewLifecycle() Lifecycle { return Lifecycle{writeReq: make(chan struct{}), fullReq: make(chan struct{}), abortCh: make(chan struct{}), readCh: make(chan struct{}), writeCh: make(chan struct{}), termCh: make(chan struct{})} }
```

Implement `Request`, `RequestWithPrevious`, `Abort`, `AbortWith`, `Reason`, six barrier accessors, `MarkReadClosed`, `MarkWriteClosed`, `TryMarkTerminal`, `MarkTerminal` by copying the current `transport/sessionLifecycle` algorithm exactly, with exported names only. In particular, `AbortWith` runs `publish()` while `mu` still owns the winning abort before any channels close.

- [ ] **Step 4: Run GREEN**

```bash
go test ./internal/runtimecore -run '^TestLifecycle' -count=1
```

- [ ] **Step 5: Keep existing transport names through explicit mapping**

```go
type sessionLifecycle struct { core runtimecore.Lifecycle }
func newSessionLifecycle() *sessionLifecycle { return &sessionLifecycle{core: runtimecore.NewLifecycle()} }
func coreGoal(g closeGoal) runtimecore.CloseGoal { switch g { case closeGoalWrite: return runtimecore.GoalWrite; case closeGoalFull: return runtimecore.GoalFull; case closeGoalAbort: return runtimecore.GoalAbort; default: return runtimecore.GoalRunning } }
func coreReason(r abortReason) runtimecore.AbortReason { switch r { case abortExplicit: return runtimecore.AbortExplicit; case abortCaller: return runtimecore.AbortCaller; case abortFailure: return runtimecore.AbortFailure; default: return runtimecore.AbortNone } }
```

All current lower-case methods delegate to `core`. Map `RequestWithPrevious`'s returned core goal back to the local enum with a second explicit helper.

Add a table test `TestLifecycleCoreMapping` covering all four goals in both directions and all four abort reasons.

- [ ] **Step 6: P0 lifecycle/error/allocation regression**

```bash
go test ./transport -run 'Lifecycle|Graceful|HalfClose|Terminal|TypedError|ErrorOwnership' -count=1
go test -race ./transport -run 'Lifecycle|Graceful|HalfClose|Terminal|TypedError|ErrorOwnership' -count=10
go test ./transport -run '^$' -bench 'BenchmarkGraceful(SendRunning|TrySendRunning)' -benchmem -benchtime=1x -count=5
```

Expected: tests/race PASS; existing Go-version-specific allocation gates remain satisfied.

- [ ] **Step 7: Commit**

```bash
git add -- internal/runtimecore/lifecycle.go internal/runtimecore/lifecycle_test.go transport/lifecycle.go transport/lifecycle_test.go
git commit -m "runtime: share session lifecycle ownership core"
```

---

### Task 5: Share bounded `ObserverDispatcher`

**Files:** Create `internal/runtimecore/observer.go`, `observer_test.go`; modify `transport/observer.go`, `transport/stats.go`.

- [ ] **Step 1: Write RED tests**

Required names: `TestObserverDispatcherOverflowIsCounted`, `TestObserverDispatcherRecoversPanicAndContinues`, `TestObserverDispatcherStopMakesEmitNoop`, `TestObserverDispatcherStopDoesNotWaitForBlockedCallback`. Use channels for all synchronization; no sleep is the correctness oracle.

- [ ] **Step 2: Run RED**

```bash
go test ./internal/runtimecore -run '^TestObserverDispatcher' -count=1
```

- [ ] **Step 3: Implement exact core dispatcher**

```go
type ObserverDispatcher struct {
    observer ogrenet.Observer
    queue chan ogrenet.Event
    stopCh chan struct{}
    stopped atomic.Bool
    stopOnce sync.Once
    dropped atomic.Uint64
    panics atomic.Uint64
}
func NewObserverDispatcher(o ogrenet.Observer, size int) *ObserverDispatcher {
    if o == nil { return nil }
    d := &ObserverDispatcher{observer:o, queue:make(chan ogrenet.Event, size), stopCh:make(chan struct{})}
    go d.run(); return d
}
func (d *ObserverDispatcher) Emit(e ogrenet.Event) bool {
    if d == nil || d.stopped.Load() { return false }
    select { case d.queue <- e: return true; default: d.dropped.Add(1); return false }
}
func (d *ObserverDispatcher) Stop(){ if d == nil { return }; d.stopped.Store(true); d.stopOnce.Do(func(){ close(d.stopCh) }) }
func (d *ObserverDispatcher) Dropped() uint64 { if d == nil { return 0 }; return d.dropped.Load() }
func (d *ObserverDispatcher) Panics() uint64 { if d == nil { return 0 }; return d.panics.Load() }
func (d *ObserverDispatcher) run(){ for { select { case <-d.stopCh: return; case e := <-d.queue: d.observe(e) } } }
func (d *ObserverDispatcher) observe(e ogrenet.Event){ defer func(){ if recover()!=nil { d.panics.Add(1) } }(); d.observer.Observe(e) }
```

This is a semantic move of the current P0-5 algorithm, not a shutdown redesign.

- [ ] **Step 4: Run GREEN and wrap transport**

```bash
go test ./internal/runtimecore -run '^TestObserverDispatcher' -count=10
```

Transport wrapper:

```go
type observerDispatcher struct { core *runtimecore.ObserverDispatcher }
func newObserverDispatcher(o ogrenet.Observer, n int) *observerDispatcher { c := runtimecore.NewObserverDispatcher(o,n); if c==nil{return nil}; return &observerDispatcher{core:c} }
func (d *observerDispatcher) emit(e ogrenet.Event) bool { return d != nil && d.core.Emit(e) }
func (d *observerDispatcher) stop(){ if d!=nil { d.core.Stop() } }
func (d *observerDispatcher) droppedCount() uint64 { if d==nil{return 0}; return d.core.Dropped() }
func (d *observerDispatcher) panicCount() uint64 { if d==nil{return 0}; return d.core.Panics() }
```

`stats.go` changes only direct dispatcher field reads to these accessors.

- [ ] **Step 5: P0-5 regression/race/alloc**

```bash
go test ./transport -run 'Observer|Observability' -count=1
go test -race ./transport -run '^TestObservabilityRace' -count=20
go test ./transport -run '^$' -bench 'BenchmarkObserver|Benchmark.*StatsSnapshot' -benchmem -benchtime=100x -count=3
```

Expected: observer-disabled and Stats deterministic snapshots remain 0 allocs/op; saturation/panic/blocked-observer behavior unchanged.

- [ ] **Step 6: Commit**

```bash
git add -- internal/runtimecore/observer.go internal/runtimecore/observer_test.go transport/observer.go transport/stats.go
git commit -m "runtime: share bounded observer dispatcher core"
```

---

### Task 6: Lock dependency direction and add epoll factory seam

**Files:** Create `internal/runtimecore/dependency_test.go`, `transport/contract_native_linux_test.go`.

- [ ] **Step 1: Add direct-import architecture test**

```go
func TestRuntimecoreDoesNotImportImplementationPackages(t *testing.T) {
    forbidden := map[string]bool{
        "github.com/qigao/ogrenet/transport":true,
        "github.com/qigao/ogrenet/epoll":true,
        "github.com/qigao/ogrenet/kqueue":true,
        "github.com/qigao/ogrenet/iocp":true,
    }
    entries, err := os.ReadDir("."); if err != nil { t.Fatal(err) }
    fset := token.NewFileSet()
    for _, ent := range entries {
        if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".go") { continue }
        f, err := parser.ParseFile(fset, ent.Name(), nil, parser.ImportsOnly); if err != nil { t.Fatal(err) }
        for _, imp := range f.Imports {
            p, err := strconv.Unquote(imp.Path.Value); if err != nil { t.Fatal(err) }
            if forbidden[p] { t.Fatalf("%s imports %s", ent.Name(), p) }
        }
    }
}
```

- [ ] **Step 2: Run architecture test**

```bash
go test ./internal/runtimecore -run '^TestRuntimecoreDoesNotImport' -count=1
```

- [ ] **Step 3: Add Linux epoll factory seam with zero supported profile**

```go
//go:build linux

package transport_test

func epollFactory(profile contractProfile) engineFactory {
    return engineFactory{name:"epoll", profile:profile, new:func(t *testing.T, opts ...transport.Option) ogrenet.Engine {
        t.Helper(); e, err := transport.NewEpoll(transport.EpollConfig{Pollers:1}, opts...); if err!=nil { t.Fatal(err) }; t.Cleanup(func(){ _=e.Close() }); return e
    }}
}
func TestEpollPhase6AContractProfile(t *testing.T) {
    f := epollFactory(contractProfile{})
    if f.profile.TCP || f.profile.UDP { t.Fatalf("profile=%+v", f.profile) }
    e := f.new(t)
    _, err := e.Dial(context.Background(), ogrenet.Endpoint{Scheme:ogrenet.SchemeTLS, Host:"127.0.0.1", Port:1}, nil)
    if !errors.Is(err, transport.ErrProtocolUnsupported) { t.Fatalf("err=%v", err) }
}
```

6B changes profile to `TCP:true`; 6C adds `UDP:true`. Once enabled, a failing common contract must fail normally—never runtime-skip by probing errors.

- [ ] **Step 4: Run seam tests**

```bash
go test ./transport -run 'EnginePublicContracts|EpollPhase6A|EpollRejects' -count=5
go test ./internal/runtimecore -count=5
```

- [ ] **Step 5: Commit**

```bash
git add -- internal/runtimecore/dependency_test.go transport/contract_native_linux_test.go
git commit -m "test: lock native runtime semantic boundaries"
```

---

### Task 7: Full 6A verification and draft PR

- [ ] **Step 1: Format/check module state**

```bash
gofmt -w internal/runtimecore/*.go transport/*.go
git diff --check
go mod tidy
git diff --exit-code -- go.mod go.sum
```

No module change is expected. If tidy changes module files, inspect the cause and restore them unless a real new dependency was intentionally introduced (none is planned).

- [ ] **Step 2: Full vet/unit/race**

```bash
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
go test -race ./internal/runtimecore -count=20
go test -race ./transport -run 'ObservabilityRace|TypedError|ErrorOwnership|Graceful' -count=20
```

Expected: PASS.

- [ ] **Step 3: Allocation gates**

```bash
go test ./transport -run '^$' -bench 'BenchmarkGraceful(SendRunning|TrySendRunning)|BenchmarkObserverDisabledEmitPath|Benchmark(Session|Packet|Engine)StatsSnapshot' -benchmem -benchtime=1x -count=5
```

Expected: existing Go-version-specific graceful gates remain within their configured limits; observer-disabled emit and Session/Packet/Engine Stats snapshots remain 0 allocs/op.

- [ ] **Step 4: Correct cross-compilation**

```bash
GOOS=windows GOARCH=amd64 go test -c ./transport -o /tmp/ogrenet-transport-windows-amd64.test.exe
GOOS=darwin GOARCH=arm64 go test -c ./transport -o /tmp/ogrenet-transport-darwin-arm64.test
GOOS=freebsd GOARCH=amd64 go test -c ./transport -o /tmp/ogrenet-transport-freebsd-amd64.test
```

Expected: build success; Windows/macOS CI execute non-Linux stub tests natively.

- [ ] **Step 5: Scope audit**

```bash
git diff --stat master...HEAD
git diff --name-only master...HEAD
```

Allowed production scope is only `transport/epoll_*`, `transport/errors.go`, the three runtimecore wrappers (`gate.go`, `lifecycle.go`, `observer.go` plus the accessor-only `stats.go` change), and `internal/runtimecore/*`. Contract tests/docs are allowed. No socket reactor I/O, TLS/WS work, kqueue/IOCP high-level code, or unrelated refactor.

- [ ] **Step 6: Open/update one draft PR**

Title: `runtime: add Linux epoll native Engine`

Body must contain:

```text
P1-6A complete: explicit backend contract, public parity harness, and shared semantic ownership core only.
No native TCP/UDP support claim yet; reactor/socket I/O begins in 6B.
Refs #57
Refs #56
Refs #38
```

Do not close #57 or #56 and do not mark Ready.

- [ ] **Step 7: Exact-head CI gate**

Fetch the final 6A head SHA and require fresh green CI for that SHA: Linux Go 1.25/1.26 including full race, Windows, macOS, FreeBSD runtime, GmSSL, and existing cross-compiles. Fix any demonstrated failure before starting 6B.

## 6A Completion Boundary

6A ends only when `transport.New()` remains unchanged, the explicit Epoll contract compiles cross-platform, TLS/WS/WSS never fall back, portable TCP/UDP characterization exists, epoll advertises zero TCP/UDP capability for this checkpoint, the three shared semantic owners live in `internal/runtimecore`, architecture/race/allocation gates are green, and the PR remains draft. Only then may 6B add the reactor, intrusive inbox/wake handshake, deadline heap, callback executor, and native TCP fd ownership.