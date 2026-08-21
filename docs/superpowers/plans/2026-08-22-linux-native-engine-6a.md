# Linux Native Engine 6A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish the Linux native Engine public construction/capability contract, a reusable public-contract parity harness, and a small shared semantic core without implementing native socket I/O or changing portable transport behavior.

**Architecture:** `transport.New()` stays the portable reference backend. `transport.NewEpoll(EpollConfig, ...Option)` is introduced explicitly, but P1-6A only provides construction/capability scaffolding; no TCP/UDP native-support claim is made yet. Pure ownership primitives (`SendGate`, `Lifecycle`, `ObserverDispatcher`) move to `internal/runtimecore` behind compatibility wrappers so the existing portable implementation keeps its private call shape and P0 behavior while later native reactors can consume the same semantic owners directly.

**Tech Stack:** Go 1.25+, standard library, `golang.org/x/sys` as already present, existing `ogrenet` root contracts, existing `transport` package, Go race detector and benchmark gates.

**Spec:** `docs/superpowers/specs/2026-08-22-linux-native-engine-design.md`

## Global Constraints

- `transport.New(opts...)` always remains the portable backend; no backend switch or automatic selection is introduced.
- P1-6A performs no native socket accept/connect/read/write work and makes no native TCP/UDP support claim.
- The eventual Linux native backend supports TCP + UDP only; TLS/WS/WSS must never silently fall back to portable I/O.
- `internal/runtimecore` must not import `transport`, `epoll`, `kqueue`, `iocp`, TLS, WebSocket, DNS, or own socket syscalls.
- Existing P0 limits, timeout, graceful lifecycle, typed error, Stats, Observer, allocation, and `Done()` semantics must remain unchanged for `transport.New()`.
- Do not introduce a generic cross-platform poller abstraction.
- Do not introduce lock-free queues, pooling, scatter/gather, Happy Eyeballs, proxy, QUIC, or HTTP work in 6A.
- Every production-code task follows RED -> GREEN -> focused regression/race test -> commit.
- Existing Linux Go 1.25/1.26, Windows, macOS, FreeBSD runtime, GmSSL, and cross-compile gates remain intact.

---

## File Structure Locked by 6A

```text
internal/runtimecore/
    gate.go                  pure send-admission close/drain ownership
    gate_test.go
    lifecycle.go             protocol-independent graceful/abort state ownership
    lifecycle_test.go
    observer.go              bounded best-effort observer dispatcher
    observer_test.go

transport/
    epoll_config.go          cross-platform EpollConfig/default/validation contract
    epoll_constructor_linux.go
    epoll_constructor_stub.go
    epoll_engine_phase6a_linux.go
    epoll_config_test.go
    epoll_capability_linux_test.go
    gate.go                  compatibility wrapper over runtimecore.SendGate
    lifecycle.go             compatibility wrapper over runtimecore.Lifecycle
    observer.go              transport options/setup + compatibility wrapper
    stats.go                 only adjusted to read observer health through wrapper methods

transport_test package files:
    contract_harness_test.go backend-neutral factory/profile harness
    contract_tcp_test.go     public TCP reference-contract cases
    contract_udp_test.go     public UDP reference-contract cases
```

`quota.go`, `limits.go`, admission leases, and Stats counters intentionally remain in `transport` during 6A. They directly encode public `transport` error identities or snapshots; extracting them now would require artificial factories/interfaces. The spec explicitly allows incremental extraction only where the boundary is clean.

---

### Task 1: Freeze the Epoll constructor, config, and capability contract

**Files:**
- Create: `transport/epoll_config.go`
- Create: `transport/epoll_config_test.go`
- Create: `transport/epoll_constructor_stub.go`
- Create: `transport/epoll_constructor_linux.go`
- Create: `transport/epoll_engine_phase6a_linux.go`
- Create: `transport/epoll_capability_linux_test.go`
- Modify: `transport/errors.go`

**Interfaces:**
- Consumes: existing `Option`, `config`, `defaultConfig()`, `Limits.validate()`, root `ogrenet.Engine` interfaces.
- Produces:

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

Package-private resolved config:

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

In `transport/epoll_config_test.go`, add table-driven tests that lock exact defaults and invalid values:

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

func TestResolveEpollConfigRejectsNegativeValues(t *testing.T) {
    cases := []EpollConfig{
        {Pollers: -1}, {EventBatch: -1}, {CallbackWorkers: -1},
        {CallbackQueue: -1}, {IOBudgetBytes: -1}, {IOBudgetOps: -1},
    }
    for _, cfg := range cases {
        if _, err := resolveEpollConfig(cfg, 4); !errors.Is(err, ErrInvalidEpollConfig) {
            t.Fatalf("cfg=%+v err=%v", cfg, err)
        }
    }
}
```

Also test `gomaxprocs <= 0` resolves as one worker/poller and explicit positive values override defaults exactly.

- [ ] **Step 2: Run RED config tests**

Run:

```bash
go test ./transport -run '^TestResolveEpollConfig' -count=1
```

Expected: FAIL because `EpollConfig`, `resolvedEpollConfig`, and `resolveEpollConfig` do not exist.

- [ ] **Step 3: Implement config + sentinels minimally**

Add to `transport/errors.go`:

```go
ErrBackendUnsupported  = errors.New("transport: backend unsupported on this platform")
ErrProtocolUnsupported = errors.New("transport: protocol unsupported by backend")
ErrInvalidEpollConfig  = errors.New("transport: invalid epoll configuration")
```

Create `transport/epoll_config.go` with the public struct and deterministic resolver. Use checked arithmetic for `4 * CallbackWorkers`; if multiplication would overflow `int`, return `ErrInvalidEpollConfig`. Clamp only the default callback queue formula to `[64, 1024]`; explicit positive `CallbackQueue` is accepted as-is.

- [ ] **Step 4: Run config tests GREEN**

Run:

```bash
go test ./transport -run '^TestResolveEpollConfig' -count=1
```

Expected: PASS.

- [ ] **Step 5: Write RED cross-platform constructor/capability tests**

In `transport/epoll_config_test.go`, verify negative config fails before any backend-specific construction:

```go
func TestNewEpollRejectsInvalidConfig(t *testing.T) {
    _, err := NewEpoll(EpollConfig{Pollers: -1})
    if !errors.Is(err, ErrInvalidEpollConfig) { t.Fatalf("err=%v", err) }
}
```

In Linux-only `transport/epoll_capability_linux_test.go`, lock final unsupported-scheme semantics even though TCP/UDP I/O is not implemented yet:

```go
func TestEpollRejectsTLSWSWSSWithoutFallback(t *testing.T) {
    e, err := NewEpoll(EpollConfig{Pollers: 1})
    if err != nil { t.Fatal(err) }
    t.Cleanup(func() { _ = e.Close() })

    for _, scheme := range []ogrenet.Scheme{ogrenet.SchemeTLS, ogrenet.SchemeWS, ogrenet.SchemeWSS} {
        ep := ogrenet.Endpoint{Scheme: scheme, Host: "127.0.0.1", Port: 1}
        if _, err := e.Dial(context.Background(), ep, nil); !errors.Is(err, ErrProtocolUnsupported) {
            t.Fatalf("scheme=%s err=%v", scheme, err)
        }
    }
}
```

Add non-Linux build coverage in `epoll_constructor_stub.go` through existing cross-compilation: valid config returns `ErrBackendUnsupported`; options are deliberately not applied on unsupported platforms.

- [ ] **Step 6: Run RED constructor tests**

Run on Linux:

```bash
go test ./transport -run '^Test(NewEpoll|EpollRejects)' -count=1
```

Expected: FAIL because `NewEpoll` does not exist.

- [ ] **Step 7: Implement constructor scaffolding without native I/O**

`transport/epoll_constructor_stub.go`:

```go
//go:build !linux

package transport

import "github.com/qigao/ogrenet"

func NewEpoll(cfg EpollConfig, opts ...Option) (ogrenet.Engine, error) {
    if _, err := resolveEpollConfig(cfg, 1); err != nil { return nil, err }
    return nil, ErrBackendUnsupported
}
```

`transport/epoll_constructor_linux.go` resolves `runtime.GOMAXPROCS(0)`, applies the same `Option` loop and limits validation as `New`, then returns a phase-6A `epollEngine` scaffold.

`transport/epoll_engine_phase6a_linux.go` implements the root `Engine` interface with Engine lifecycle/Stats zero-value behavior but no native socket operations. For the temporary 6A scaffold:

- `tls/ws/wss` from stream methods return `ErrProtocolUnsupported` exactly;
- stream/packet method mismatch continues to return `ErrProtocolMismatch`;
- TCP/UDP entry points also return `ErrProtocolUnsupported` **only as a branch-local 6A scaffold**; no release/PR may describe TCP/UDP as supported until 6B/6C replace those paths;
- `Close` is idempotent, `Done` closes once, `Shutdown(ctx)` respects nil/canceled contexts using existing sentinels/context causes, and `Stats()` returns zero.

This temporary scaffold is permitted because `feat/linux-native-engine` is not released at 6A and the #57 definition of done requires 6B/6C before readiness.

- [ ] **Step 8: Run constructor/capability tests GREEN and compile all platforms**

Run:

```bash
go test ./transport -run '^Test(NewEpoll|EpollRejects)' -count=1
GOOS=windows GOARCH=amd64 go test ./transport -run '^$'
GOOS=darwin GOARCH=arm64 go test ./transport -run '^$'
GOOS=freebsd GOARCH=amd64 go test ./transport -run '^$'
```

Expected: PASS/compile PASS.

- [ ] **Step 9: Commit**

```bash
git add -- transport/errors.go transport/epoll_config.go transport/epoll_config_test.go transport/epoll_constructor_stub.go transport/epoll_constructor_linux.go transport/epoll_engine_phase6a_linux.go transport/epoll_capability_linux_test.go
git commit -m "runtime: define explicit epoll backend contract"
```

---

### Task 2: Add the backend-neutral public contract harness

**Files:**
- Create: `transport/contract_harness_test.go`
- Create: `transport/contract_tcp_test.go`
- Create: `transport/contract_udp_test.go`

**Interfaces:**
- Consumes: only public `ogrenet` root interfaces plus public `transport.New`/options.
- Produces:

```go
type contractProfile struct {
    TCP bool
    UDP bool
}

type engineFactory struct {
    name    string
    profile contractProfile
    new     func(t *testing.T, opts ...transport.Option) ogrenet.Engine
}

func runEngineContracts(t *testing.T, factory engineFactory)
```

The files use `package transport_test` so contract tests cannot depend on portable/private implementation details.

- [ ] **Step 1: Write the harness and RED reference contract cases**

`contract_harness_test.go` defines only the portable factory initially:

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

func TestEnginePublicContracts(t *testing.T) {
    runEngineContracts(t, portableFactory())
}
```

`contract_tcp_test.go` should include at least one complete public-only TCP lifecycle case with:

- loopback listener on port 0;
- accepted child + outbound Dial;
- `OnOpen -> OnMessage -> OnClose` order;
- payload echo;
- `Stats().BytesRX/TX` and `MessagesRX/TX` nonzero/consistent;
- `Done()` and stable `Err()` after close.

`contract_udp_test.go` should include both connected and unconnected UDP cases:

- `DialPacket` Send/receive path;
- `ListenPacket` SendTo/receive path;
- payload/packet Stats accounting;
- deterministic Close/Done.

Do not copy portable implementation helpers from package `transport`; build fixtures with public APIs and standard `net` addresses only.

- [ ] **Step 2: Run the contract suite**

Run:

```bash
go test ./transport -run '^TestEnginePublicContracts$' -count=1
```

Expected on the first attempt: any failure identifies an assumption in the new public-only harness. Fix the harness/fixture, not production semantics, unless the test demonstrates a real root-contract violation.

- [ ] **Step 3: Add contract-profile skips as explicit capability gates**

Each protocol-specific helper starts with an explicit profile check:

```go
if !factory.profile.TCP { return }
```

Do not use runtime error probing as capability discovery. In 6B the Linux factory will join with `TCP:true`; in 6C it will become `TCP:true, UDP:true`.

- [ ] **Step 4: Run public contract suite + existing package tests**

Run:

```bash
go test ./transport -run '^TestEnginePublicContracts$' -count=5
go test ./transport -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -- transport/contract_harness_test.go transport/contract_tcp_test.go transport/contract_udp_test.go
git commit -m "test: add backend-neutral transport contract harness"
```

---

### Task 3: Extract SendGate into `internal/runtimecore` without changing portable call sites

**Files:**
- Create: `internal/runtimecore/gate.go`
- Create: `internal/runtimecore/gate_test.go`
- Modify: `transport/gate.go`

**Interfaces:**
- Produces:

```go
package runtimecore

type SendGate struct { /* private state */ }
func NewSendGate() *SendGate
func (g *SendGate) Enter() bool
func (g *SendGate) Leave()
func (g *SendGate) Close() <-chan struct{}
func (g *SendGate) Done() <-chan struct{}
```

- `transport.sendGate` remains package-private and preserves `enter/leave/close/done` call sites through delegation.

- [ ] **Step 1: Write RED runtimecore gate tests**

Cover:

```go
func TestSendGateCloseWaitsForActiveOwners(t *testing.T)
func TestSendGateRejectsEnterAfterClose(t *testing.T)
func TestSendGateCloseIsIdempotent(t *testing.T)
```

The first test must deterministically enter twice, close, assert `Done` is not closed, leave once, assert still open, leave second time, assert closed.

- [ ] **Step 2: Run RED gate tests**

```bash
go test ./internal/runtimecore -run '^TestSendGate' -count=1
```

Expected: FAIL because package/type does not exist.

- [ ] **Step 3: Implement `runtimecore.SendGate`**

Move the current mutex/active/drained ownership semantics exactly, renaming methods to exported internal-package names. Do not add atomics, contexts, callbacks, or new states.

- [ ] **Step 4: Run runtimecore gate tests GREEN**

```bash
go test ./internal/runtimecore -run '^TestSendGate' -count=1
```

Expected: PASS.

- [ ] **Step 5: Replace `transport/gate.go` implementation with compatibility delegation**

Use:

```go
type sendGate struct { core *runtimecore.SendGate }
func newSendGate() *sendGate { return &sendGate{core: runtimecore.NewSendGate()} }
func (g *sendGate) enter() bool { return g.core.Enter() }
func (g *sendGate) leave() { g.core.Leave() }
func (g *sendGate) close() <-chan struct{} { return g.core.Close() }
func (g *sendGate) done() <-chan struct{} { return g.core.Done() }
```

Do not touch `conn.go`, `packet.go`, or `websocket.go` call sites.

- [ ] **Step 6: Run focused and race regression**

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

**Interfaces:**
- Produces internal enums and owner:

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

type Lifecycle struct { /* private state */ }
func NewLifecycle() *Lifecycle
func (l *Lifecycle) Request(goal CloseGoal) bool
func (l *Lifecycle) RequestWithPrevious(goal CloseGoal) (bool, CloseGoal)
func (l *Lifecycle) Abort(reason AbortReason) bool
func (l *Lifecycle) AbortWith(reason AbortReason, publish func()) bool
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

`transport/lifecycle.go` retains the existing lower-case enums/methods and maps them to the core, so no session call site changes.

- [ ] **Step 1: Write RED lifecycle tests in runtimecore**

Cover at minimum:

```go
func TestLifecycleWriteThenFullEscalation(t *testing.T)
func TestLifecycleAbortPublishesBeforeSignals(t *testing.T)
func TestLifecycleFirstAbortOwnerWins(t *testing.T)
func TestLifecycleTerminalClosesReadWriteAndTerminal(t *testing.T)
func TestLifecycleAbortCannotBeReplacedByTerminalMark(t *testing.T)
```

`TestLifecycleAbortPublishesBeforeSignals` must use a publish closure that stores an atomic marker and a waiter on `Aborted()` that asserts the marker is already visible.

- [ ] **Step 2: Run RED lifecycle tests**

```bash
go test ./internal/runtimecore -run '^TestLifecycle' -count=1
```

Expected: FAIL because `Lifecycle` does not exist.

- [ ] **Step 3: Implement the lifecycle core by semantic copy, not redesign**

Move current synchronization/`sync.Once` channel-closing behavior exactly. `AbortWith` must keep the winning publish callback inside lifecycle ownership before any completion channel closes; losing aborts cannot observe a partially published terminal cause.

- [ ] **Step 4: Run runtimecore lifecycle tests GREEN**

```bash
go test ./internal/runtimecore -run '^TestLifecycle' -count=1
```

Expected: PASS.

- [ ] **Step 5: Convert `transport/sessionLifecycle` to a compatibility wrapper**

Keep the existing transport names:

```go
type sessionLifecycle struct { core *runtimecore.Lifecycle }
```

Map each `closeGoal`/`abortReason` explicitly with conversion helpers. Do not alias enum integer values implicitly across packages; tests should lock the mapping.

- [ ] **Step 6: Run the P0-3/P0-4 ownership regression suite**

```bash
go test ./transport -run 'Lifecycle|Graceful|HalfClose|Terminal|TypedError|ErrorOwnership' -count=1
go test -race ./transport -run 'Lifecycle|Graceful|HalfClose|Terminal|TypedError|ErrorOwnership' -count=10
```

Expected: PASS with no changed terminal owner, callback barrier, or close semantics.

- [ ] **Step 7: Run graceful allocation smoke**

```bash
go test ./transport -run '^$' -bench 'BenchmarkGraceful(SendRunning|TrySendRunning)' -benchmem -benchtime=1x -count=5
```

Expected: remain within the existing repository CI allocation gates; the wrapper must not add per-Send allocation.

- [ ] **Step 8: Commit**

```bash
git add -- internal/runtimecore/lifecycle.go internal/runtimecore/lifecycle_test.go transport/lifecycle.go
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
- Consumes: root `ogrenet.Observer` and `ogrenet.Event` only.
- Produces:

```go
type ObserverDispatcher struct { /* private state */ }
func NewObserverDispatcher(observer ogrenet.Observer, size int) *ObserverDispatcher
func (d *ObserverDispatcher) Emit(event ogrenet.Event) bool
func (d *ObserverDispatcher) Stop()
func (d *ObserverDispatcher) Dropped() uint64
func (d *ObserverDispatcher) Panics() uint64
```

The core dispatcher deliberately has no wait/join API because P0-5 requires blocked Observer callbacks to stay outside the Engine `Done()` barrier.

- [ ] **Step 1: Write RED dispatcher tests in runtimecore**

Add deterministic tests:

```go
func TestObserverDispatcherOverflowIsNonBlockingAndCounted(t *testing.T)
func TestObserverDispatcherRecoversPanicsAndContinues(t *testing.T)
func TestObserverDispatcherStopMakesFutureEmitNoop(t *testing.T)
func TestObserverDispatcherStopDoesNotWaitForBlockedCallback(t *testing.T)
```

Use channels for synchronization; do not use sleeps as correctness oracles.

- [ ] **Step 2: Run RED observer tests**

```bash
go test ./internal/runtimecore -run '^TestObserverDispatcher' -count=1
```

Expected: FAIL because dispatcher does not exist.

- [ ] **Step 3: Implement core dispatcher with exact P0-5 semantics**

Use one bounded `chan ogrenet.Event`, one worker, atomic stopped/dropped/panics, and a separate stop signal. Never close the event queue. `Emit` is nonblocking and increments Dropped on full queue. Recover around each `Observe` callback and continue. `Stop` marks stopped before closing the stop signal and does not wait for a callback to return.

- [ ] **Step 4: Run runtimecore observer tests GREEN**

```bash
go test ./internal/runtimecore -run '^TestObserverDispatcher' -count=10
```

Expected: PASS.

- [ ] **Step 5: Keep the transport wrapper/API stable**

`transport/observer.go` retains:

- `defaultObserverBuffer = 1024`;
- `WithObserver`;
- `WithObserverBuffer`;
- `Engine.observeSetup`;
- a package-private `observerDispatcher` wrapper used by existing code.

Wrapper shape:

```go
type observerDispatcher struct { core *runtimecore.ObserverDispatcher }
func newObserverDispatcher(o ogrenet.Observer, n int) *observerDispatcher { ... }
func (d *observerDispatcher) emit(e ogrenet.Event) bool { ... }
func (d *observerDispatcher) stop() { ... }
func (d *observerDispatcher) droppedCount() uint64 { ... }
func (d *observerDispatcher) panicCount() uint64 { ... }
```

Update `transport/stats.go` only from direct field access to `droppedCount()` / `panicCount()`; do not change any public Stats semantics.

- [ ] **Step 6: Run P0-5 observer/race/alloc regression**

```bash
go test ./transport -run 'Observer|Observability' -count=1
go test -race ./transport -run '^TestObservabilityRace' -count=20
go test ./transport -run '^$' -bench 'BenchmarkObserver|Benchmark.*StatsSnapshot' -benchmem -benchtime=100x -count=3
```

Expected:

- observer-disabled path remains 0 allocs/op;
- Session/Packet/Engine Stats snapshots remain 0 allocs/op;
- saturation and panic/blocked-callback behavior remain unchanged.

- [ ] **Step 7: Commit**

```bash
git add -- internal/runtimecore/observer.go internal/runtimecore/observer_test.go transport/observer.go transport/stats.go
git commit -m "runtime: share bounded observer dispatcher core"
```

---

### Task 6: Prove shared-core dependency direction and keep extraction intentionally narrow

**Files:**
- Create: `internal/runtimecore/dependency_test.go`
- No production file movement beyond Tasks 3-5.

**Interfaces:**
- Consumes: package source tree.
- Produces: a regression test that prevents `internal/runtimecore` from importing backend/transport implementation packages.

- [ ] **Step 1: Write a dependency-boundary test**

In `internal/runtimecore/dependency_test.go`, use `go list -deps -json` or `go/packages` is **not** allowed because it would add a dependency. Prefer invoking the Go tool through `os/exec` in the test:

```go
func TestRuntimecoreDoesNotDependOnTransportOrNativePollers(t *testing.T) {
    cmd := exec.Command("go", "list", "-deps", "github.com/qigao/ogrenet/internal/runtimecore")
    out, err := cmd.Output()
    if err != nil { t.Fatal(err) }
    forbidden := []string{
        "github.com/qigao/ogrenet/transport",
        "github.com/qigao/ogrenet/epoll",
        "github.com/qigao/ogrenet/kqueue",
        "github.com/qigao/ogrenet/iocp",
    }
    for _, dep := range forbidden {
        if bytes.Contains(out, []byte("\n"+dep+"\n")) || strings.TrimSpace(string(out)) == dep {
            t.Fatalf("runtimecore depends on %s", dep)
        }
    }
}
```

Allow the root `github.com/qigao/ogrenet` package because Observer/Event types are public contracts, not backend implementation.

- [ ] **Step 2: Run the dependency test**

```bash
go test ./internal/runtimecore -run '^TestRuntimecoreDoesNotDepend' -count=1
```

Expected: PASS.

- [ ] **Step 3: Record deliberate non-extractions in the plan/checkpoint, not production comments**

Confirm by inspection that these still live in `transport`:

```text
transport/quota.go
transport/limits.go
transport/stats.go counters/admission snapshots
```

Reason: quota/admission currently return or compose `transport` public error identities; Stats counters are already directly reusable by the future epoll implementation because that implementation also lives in `transport`. Do not create error factories or generic interfaces solely to move files.

- [ ] **Step 4: Commit**

```bash
git add -- internal/runtimecore/dependency_test.go
git commit -m "test: lock runtimecore dependency boundary"
```

---

### Task 7: Add a Linux-native factory registration seam without enabling native parity yet

**Files:**
- Modify: `transport/contract_harness_test.go`
- Create: `transport/contract_native_linux_test.go`

**Interfaces:**
- Produces a Linux-only factory builder that can be switched on protocol-by-protocol in 6B/6C:

```go
func epollFactory(profile contractProfile) engineFactory
```

- [ ] **Step 1: Add the Linux factory helper**

`transport/contract_native_linux_test.go` uses only public API:

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
```

Do **not** register this factory with `TCP:true` or `UDP:true` in 6A. The current scaffold has no socket I/O.

- [ ] **Step 2: Add a capability-only 6A invocation**

The harness may instantiate `epollFactory(contractProfile{})` only to verify constructor lifecycle and explicit unsupported TLS/WS/WSS behavior. It must not skip failures after a profile says a protocol is supported.

- [ ] **Step 3: Run Linux contract/capability tests**

```bash
go test ./transport -run 'EnginePublicContracts|Epoll' -count=5
```

Expected: portable TCP/UDP contract cases PASS; epoll capability-only cases PASS; no test claims native TCP/UDP parity.

- [ ] **Step 4: Commit**

```bash
git add -- transport/contract_harness_test.go transport/contract_native_linux_test.go
git commit -m "test: prepare epoll contract factory seam"
```

---

### Task 8: Full 6A regression, race, allocation, and cross-platform checkpoint

**Files:**
- No new production files expected.
- Update only tests if verification exposes a real regression.

**Interfaces:**
- Consumes all 6A tasks.
- Produces the evidence required before beginning native reactor I/O in 6B.

- [ ] **Step 1: Format and module hygiene**

Run:

```bash
gofmt -w internal/runtimecore transport
git diff --check
go mod tidy
git diff --exit-code -- go.mod go.sum
```

Expected: formatting clean; module files unchanged.

- [ ] **Step 2: Unit/vet full repository**

```bash
go vet ./...
go test -count=1 ./...
```

Expected: PASS.

- [ ] **Step 3: Race full repository plus repeated shared-core/observability ownership loops**

```bash
go test -race -count=1 ./...
go test -race ./internal/runtimecore -count=20
go test -race ./transport -run 'ObservabilityRace|TypedError|ErrorOwnership|Graceful' -count=20
```

Expected: PASS with no race/deadlock/leak symptom.

- [ ] **Step 4: Re-run deterministic allocation gates**

```bash
go test ./transport -run '^$' -bench 'BenchmarkGraceful(SendRunning|TrySendRunning)|BenchmarkObserverDisabledEmitPath|Benchmark(Session|Packet|Engine)StatsSnapshot' -benchmem -benchtime=1x -count=5
```

Expected:

- existing Go-version-specific graceful Send/TrySend CI limits remain satisfied;
- observer-disabled emit remains 0 allocs/op;
- Session/Packet/Engine Stats snapshots remain 0 allocs/op.

Do not invent a new ns/op percentage threshold for runtimecore wrappers.

- [ ] **Step 5: Cross-compile the transport package on non-Linux targets**

```bash
GOOS=windows GOARCH=amd64 go test ./transport -run '^$'
GOOS=darwin GOARCH=arm64 go test ./transport -run '^$'
GOOS=freebsd GOARCH=amd64 go test ./transport -run '^$'
```

Expected: compile PASS and no Linux implementation leakage.

- [ ] **Step 6: Verify branch diff scope**

Run:

```bash
git diff --stat master...HEAD
git diff --name-only master...HEAD
```

Expected scope is limited to:

- P1-6 design/plan docs;
- epoll config/constructor/capability scaffolding;
- public contract harness;
- `internal/runtimecore` pure primitives/tests;
- compatibility wrapper changes in `transport`.

No native socket I/O file, TLS/WS implementation, kqueue/IOCP high-level implementation, or unrelated refactor is allowed.

- [ ] **Step 7: Commit any verification-only fixes, if needed**

Stage only the exact files changed by a demonstrated failure. If no fixes are needed, create no empty commit.

- [ ] **Step 8: Create/update one draft PR for `feat/linux-native-engine`**

PR title:

```text
runtime: add Linux epoll native Engine
```

Body at the 6A checkpoint must say explicitly:

```text
P1-6A complete: public backend contract, parity harness, and shared semantic core only.
No native TCP/UDP support claim yet; reactor/socket I/O begins in 6B.
Refs #57
Refs #56
Refs #38
```

Keep the PR draft. Do not close #57 or #56.

- [ ] **Step 9: Verify exact-head CI before starting 6B**

Fetch the feature branch head and require fresh CI on that exact SHA. Existing matrix plus the new package must be green before adding reactor I/O. If CI fails, fix the demonstrated failure first; do not start 6B on a red 6A base.

---

## 6A Completion Boundary

P1-6A is complete when all of the following are true:

- `transport.New()` behavior and allocation gates are unchanged;
- `EpollConfig`/capability errors are stable and cross-platform construction code compiles;
- Linux epoll selection is explicit and TLS/WS/WSS never fall back;
- public TCP/UDP contract tests exist in `package transport_test` and currently run against the portable reference backend;
- Linux epoll has a factory registration seam but does not claim TCP/UDP support;
- `SendGate`, Session `Lifecycle`, and bounded `ObserverDispatcher` have one shared semantic implementation under `internal/runtimecore` with compatibility wrappers;
- `runtimecore` has no dependency on `transport` or native poller packages;
- quota/admission/stats extraction was not forced through artificial abstraction;
- full tests/race/allocation/cross-platform checks are green on the exact feature head;
- the PR remains draft and #57/#56 remain open.

Only after this checkpoint may 6B introduce the reactor, inbox/wake protocol, deadline heap, callback executor, and native TCP socket I/O.