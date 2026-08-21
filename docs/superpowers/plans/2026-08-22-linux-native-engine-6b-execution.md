# Linux Native Engine 6B Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a real Linux epoll-owned TCP backend behind `transport.NewEpoll`, with fixed reactor ownership, bounded setup/callback execution, non-blocking accept/connect/read/write, graceful half-close, typed-error/Stats/Observer parity, and TCP race/stress/benchmark evidence.

**Architecture:** `transport.New()` remains the portable reference implementation. Linux `NewEpoll` starts N fixed epoll reactors plus one bounded setup/callback executor; every native listener/session fd is owned by exactly one reactor and only that reactor performs physical socket I/O. Application goroutines perform admission/encoding/state publication only, then signal the owning reactor through a deduplicated intrusive inbox. UDP remains explicitly unsupported until 6C.

**Tech Stack:** Go 1.25+, `golang.org/x/sys/unix`, existing top-level `epoll` poller, root `ogrenet` contracts, existing `transport` admission/quota/error/stats/observer helpers, `internal/runtimecore`, race detector, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-22-linux-native-engine-design.md`

## Global Constraints

- Linux TCP only in 6B. `ListenPacket`/`DialPacket` remain `ErrProtocolUnsupported`; TLS/WS/WSS remain `ErrProtocolUnsupported` with no portable fallback.
- `transport.New()` must remain unchanged as the portable correctness/reference backend.
- Exactly one reactor goroutine owns each listener/session fd. No native Session gets a reader goroutine, writer goroutine, timeout goroutine, or direct application-goroutine syscall path.
- `epoll.Event.Data` stores one Engine-local monotonic non-zero ID; `math.MaxUint64` is never allocated because the low-level epoll package reserves it for wakeups.
- A resource never migrates between reactors in 6B. Accepted sessions are assigned round-robin once; outbound sessions select one reactor before socket creation.
- Cross-goroutine control uses per-resource intrusive inbox nodes plus coalesced `Poller.Wake()`. Do not introduce a generic command channel, unbounded queue, lock-free MPSC, or per-signal allocation.
- Edge-triggered work deliberately left before `EAGAIN` must be requeued on a reactor-local runnable list. Never wait for a second edge after budget yield, worker-capacity pause, codec pause, or lifecycle pause.
- User `Handler` callbacks never run on reactor goroutines. Per-session `FramerFactory`/`CipherFactory` construction is also application-supplied code and runs as bounded setup work on the worker executor rather than inside a reactor.
- Custom framer encode runs synchronously on the Send/TrySend caller after codec-token admission; decode runs on the owning reactor only after non-blocking codec-token acquisition.
- A decoded `ogrenet.Message` submitted to an asynchronous worker must own `Data` independently of reactor read buffers: `append([]byte(nil), msg.Data...)` before buffer compaction/reuse.
- Stats ownership/counting points and Observer ordering remain P0-5 exact: counters first, optional Observer event second, application callback/Send ack third.
- Caller context cancellation/deadline is returned unchanged. Operational socket failures use existing `classifyOperational`; configuration/capability errors remain direct sentinels.
- Admission, `byteQuota`, `listenerCapacity`, session/listener counters, `sendGate`, `sessionLifecycle`, Observer dispatcher, and public error types are reused rather than copied into a second native semantic stack.
- TCP `Send(ctx)` may return the caller context cause after queue ownership transfer while the frame remains eligible for physical write, matching portable semantics. `TrySend` never waits for reactor/network progress.
- Worker capacity is exactly `CallbackWorkers + CallbackQueue` retained running+queued tasks. When all workers are executing, queued tasks are at most `CallbackQueue`.
- `Engine.Done()` waits for application/setup work needed to finish a resource, but never waits for a blocked Observer callback.
- Correctness tests use channels/barriers derived from real state transitions, not sleeps as ordering/success oracles. Deadline tests may use elapsed time only after setup is synchronously established.
- Every task below must compile and pass its stated verification before the next task begins; do not rely on Go forward declarations or a later task to make an intermediate commit buildable.
- No UDP, TLS, WS/WSS, kqueue, IOCP, pooling, buffer-pool redesign, `writev`, `sendfile`, resolver racing/Happy Eyeballs, proxy, QUIC, or HTTP scope expansion.

## Planned file map

```text
transport/
    epoll_engine_linux.go              Engine state, resource registry, shutdown barrier
    epoll_reactor_linux.go             reactor loop, event dispatch, runnable work
    epoll_reactor_inbox_linux.go       intrusive inbox + lost-wake handshake
    epoll_deadline_linux.go            generation-based min-heap scheduler
    epoll_callback_linux.go            exact-bounded setup/callback executor
    epoll_fd_linux.go                  sockaddr/socket/TCP option helpers
    epoll_listener_linux.go            native TCP listener + accept/handoff
    epoll_session_linux.go             native TCP Session identity/state/bootstrap
    epoll_session_task_linux.go        codec setup + Handler worker tasks
    epoll_dial_linux.go                resolver + reactor-owned connect attempts
    epoll_session_send_linux.go        codec admission + Send/TrySend + partial write
    epoll_session_lifecycle_linux.go   half-close/abort/finalization
    epoll_session_read_linux.go        read/decode + callback serialization
    epoll_native_test_helpers_linux.go deterministic native test helpers
```

`transport/epoll_engine_phase6a_linux.go` is removed only after `epoll_engine_linux.go` supplies the same `ogrenet.Engine` surface. Existing portable files are not reorganized.

---

### Task 1: Engine-independent reactor inbox, lost-wake handshake, resource registry, and runnable fairness

**Files:**
- Create: `transport/epoll_reactor_inbox_linux.go`
- Create: `transport/epoll_reactor_linux.go`
- Create: `transport/epoll_reactor_linux_test.go`

**Consumes:** `epoll.Open`, `Poller.Wait`, `Poller.Add/Mod/Del`, `Poller.Wake`, `resolvedEpollConfig`.

**Produces:**

```go
type epollInboxItem interface {
    inboxNode() *epollInboxNode
    onReactorInbox(*epollReactor)
}

type epollEventResource interface {
    epollInboxItem
    resourceID() uint64
    resourceFD() int
    onReactorEvent(*epollReactor, epoll.Events)
    onReactorRunnable(*epollReactor)
}

type epollInboxNode struct {
    owner          epollInboxItem
    next           *epollInboxNode
    queued         bool // inboxMu
    runnableQueued bool // reactor goroutine only
}

type epollReactor struct {
    index int
    cfg   resolvedEpollConfig
    poller *epoll.Poller
    events []epoll.Event

    resources map[uint64]epollEventResource

    inboxMu     sync.Mutex
    inboxHead   *epollInboxNode
    inboxTail   *epollInboxNode
    waiting     bool
    wakePending bool

    controlFlags atomic.Uint32
    runnable     []*epollInboxNode

    onFatal func(error) // optional until Task 4 attaches Engine ownership
}
```

Control bit defined now:

```go
const epollControlStop uint32 = 1 << 0
```

- [ ] **Step 1: Write RED dedupe/lost-wake tests**

```go
func TestEpollReactorSignalDeduplicatesQueuedItem(t *testing.T) {}
func TestEpollReactorSignalWakesBlockedWait(t *testing.T) {}
func TestEpollReactorControlWakeCannotBeLost(t *testing.T) {}
```

Synthetic items publish explicit barriers from `onReactorInbox`; tests do not use sleep to infer Wait state.

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^TestEpollReactor(Signal|Control)' -count=1
```

Expected: compile FAIL because reactor types are absent.

- [ ] **Step 3: Implement intrusive signal + Wait handshake**

Required signal shape:

```go
func (r *epollReactor) signal(item epollInboxItem) {
    n := item.inboxNode()
    r.inboxMu.Lock()
    if !n.queued {
        n.queued = true
        n.next = nil
        if r.inboxTail == nil { r.inboxHead = n } else { r.inboxTail.next = n }
        r.inboxTail = n
    }
    wake := r.waiting && !r.wakePending
    if wake { r.wakePending = true }
    r.inboxMu.Unlock()
    if wake { _ = r.poller.Wake() }
}
```

`armWait` checks inbox/control/runnable while holding `inboxMu`, then sets `waiting=true` and `wakePending=false`. `disarmWait` clears both under the same mutex. `signalControl` uses CAS to set a bit then performs the identical wake handshake without allocating a node.

- [ ] **Step 4: Write RED registry/runnable tests**

```go
func TestEpollReactorIgnoresStaleEventData(t *testing.T) {}
func TestEpollReactorRunnableContinuesWithoutSecondEdge(t *testing.T) {}
func TestEpollReactorResourceIDRegistryRejectsDuplicate(t *testing.T) {}
```

- [ ] **Step 5: Implement event loop**

Loop order:

```text
drain inbox
drain control
drain runnable
if stop requested AND registry/inbox/runnable empty => return
arm Wait handshake
epoll.Wait
clear Wait handshake
dispatch only IDs still present in registry
```

A resource that intentionally yields calls `reactor.requeue(item)`; requeue is local and deduplicated by `runnableQueued`.

- [ ] **Step 6: Run GREEN + race**

```bash
go test ./transport -run '^TestEpollReactor' -count=1
go test -race ./transport -run '^TestEpollReactor' -count=20
```

- [ ] **Step 7: Commit**

```bash
git add transport/epoll_reactor_inbox_linux.go transport/epoll_reactor_linux.go transport/epoll_reactor_linux_test.go
git commit -m "runtime: add epoll reactor core"
```

---

### Task 2: Generation-based deadline scheduler, added without changing Task 1 interfaces prematurely

**Files:**
- Create: `transport/epoll_deadline_linux.go`
- Create: `transport/epoll_deadline_linux_test.go`
- Modify: `transport/epoll_reactor_linux.go`

**Produces:**

```go
type epollDeadlineKind uint8
const (
    epollDeadlineConnect epollDeadlineKind = iota + 1
    epollDeadlineWrite
    epollDeadlineReadIdle
    epollDeadlineConnectionIdle
    epollDeadlineMaxLifetime
)

type epollDeadlineTarget interface {
    currentDeadlineGeneration(epollDeadlineKind) uint64
    onReactorDeadline(*epollReactor, epollDeadlineKind, uint64)
}

type epollDeadlineEntry struct {
    at         time.Time
    resourceID uint64
    kind       epollDeadlineKind
    generation uint64
}
```

Deadline dispatch looks up `reactor.resources[id]`, then type-asserts `epollDeadlineTarget`; Task 1's `epollEventResource` does not need a deadline method.

- [ ] **Step 1: Write RED heap/stale-generation tests**

```go
func TestEpollDeadlineHeapOrdersEarliest(t *testing.T) {}
func TestEpollDeadlineIgnoresStaleGeneration(t *testing.T) {}
func TestEpollDeadlineWaitTimeoutZeroWhenExpired(t *testing.T) {}
func TestEpollDeadlineWaitTimeoutNegativeWhenEmpty(t *testing.T) {}
```

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^TestEpollDeadline' -count=1
```

- [ ] **Step 3: Implement `container/heap` scheduler**

Do not remove old heap entries on update. A resource increments its domain generation and pushes a new entry. Pop discards entries when resource is absent, does not implement `epollDeadlineTarget`, or current generation differs.

Reactor order becomes:

```text
drain inbox/control
run expired live deadlines
drain runnable
if runnable remains => poll timeout 0/continue
otherwise Wait(min(next live deadline, infinity))
```

- [ ] **Step 4: Run GREEN + race**

```bash
go test ./transport -run '^TestEpoll(Deadline|Reactor)' -count=1
go test -race ./transport -run '^TestEpoll(Deadline|Reactor)' -count=20
```

- [ ] **Step 5: Commit**

```bash
git add transport/epoll_deadline_linux.go transport/epoll_deadline_linux_test.go transport/epoll_reactor_linux.go
git commit -m "runtime: add epoll deadline scheduler"
```

---

### Task 3: Exact-bounded setup/callback worker executor

**Files:**
- Create: `transport/epoll_callback_linux.go`
- Create: `transport/epoll_callback_linux_test.go`

**Produces:**

```go
type epollWorkerTask interface { runEpollWorkerTask() }

type epollCallbackExecutor struct {
    mu sync.Mutex
    workers []*epollCallbackWorker
    idle    []*epollCallbackWorker

    queue []epollWorkerTask // fixed ring of CallbackQueue entries
    head  int
    size  int

    reserved int // running + queued
    limit    int // CallbackWorkers + CallbackQueue

    onCapacity func()
}
```

Each worker owns a one-slot private channel. Reservation and submission are separate so a reactor can reserve capacity before consuming socket bytes.

- [ ] **Step 1: Write RED exact-bound tests**

```go
func TestEpollCallbackExecutorReservationBound(t *testing.T) {}
func TestEpollCallbackExecutorQueueNeverExceedsConfiguredQueue(t *testing.T) {}
func TestEpollCallbackExecutorBlockedTaskDoesNotBlockSubmitter(t *testing.T) {}
func TestEpollCallbackExecutorCapacityReleaseNotifies(t *testing.T) {}
func TestEpollCallbackExecutorStopsOnlyWhenIdle(t *testing.T) {}
```

Tasks block on explicit channels; no sleep-based inference.

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^TestEpollCallbackExecutor' -count=1
```

- [ ] **Step 3: Implement direct-idle-worker + fixed-ring scheduling**

Completion algorithm:

```text
run task outside executor mutex
lock
reserved-- for completed task
if fixed queue non-empty: pop next already-reserved task; same worker continues
else mark worker idle
unlock
call onCapacity outside lock
```

`stopIdle()` is legal only when `reserved==0 && size==0`; it closes idle worker channels and waits for worker goroutines. It never unwinds running user Go code.

- [ ] **Step 4: Run GREEN + race**

```bash
go test ./transport -run '^TestEpollCallbackExecutor' -count=1
go test -race ./transport -run '^TestEpollCallbackExecutor' -count=20
```

- [ ] **Step 5: Commit**

```bash
git add transport/epoll_callback_linux.go transport/epoll_callback_linux_test.go
git commit -m "runtime: add bounded epoll worker executor"
```

---

### Task 4: Real epoll Engine shell using only already-defined types

**Files:**
- Create: `transport/epoll_engine_linux.go`
- Create: `transport/epoll_engine_linux_test.go`
- Modify: `transport/epoll_reactor_inbox_linux.go`
- Modify: `transport/epoll_reactor_linux.go`
- Modify: `transport/epoll_constructor_linux.go`
- Modify: `transport/stats.go`
- Delete after GREEN: `transport/epoll_engine_phase6a_linux.go`

**Produces:** Engine startup/shutdown for an empty native runtime. TCP public methods still return `ErrProtocolUnsupported`.

Define the managed-resource boundary before concrete listener/session types exist:

```go
type epollManagedResource interface {
    managedID() uint64
    requestEngineShutdown()
    requestEngineAbort(abortReason)
}

type epollEngine struct {
    cfg      config
    epollCfg resolvedEpollConfig

    admission *admissionController
    observer  *observerDispatcher
    callbacks *epollCallbackExecutor
    reactors  []*epollReactor

    mu             sync.Mutex
    state          engineState
    shutdownReason abortReason
    shutdownErr    error
    activeOps      int
    managed        map[uint64]epollManagedResource

    nextReactor atomic.Uint64
    nextID      atomic.Uint64

    quiescent     chan struct{}
    quiescentOnce sync.Once
    reactorWG     sync.WaitGroup
    done          chan struct{}
    doneOnce      sync.Once
}
```

- [ ] **Step 1: Write RED Engine boot/barrier/ID tests**

```go
func TestNewEpollStartsConfiguredReactorCount(t *testing.T) {}
func TestEpollEngineResourceIDNeverReturnsZeroOrReservedWakeValue(t *testing.T) {}
func TestEpollEngineCloseStopsEmptyReactorsAndWorkers(t *testing.T) {}
func TestEpollEngineStatsUsesAdmissionAndObserverOwners(t *testing.T) {}
```

Package test sets `nextID` to `math.MaxUint64-1` and expects private `errNativeResourceIDExhausted` before any registration.

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^Test(EpollEngine|NewEpollStarts)' -count=1
```

- [ ] **Step 3: Add generic worker-capacity wait list to reactor**

Extend `epollInboxNode` with `workerBlocked bool` (reactor-only) and reactor with:

```go
workerBlocked    []*epollInboxNode
hasWorkerBlocked atomic.Bool
```

`blockOnWorker(item)` deduplicates. `epollControlWorkerCapacity` is added as a control bit. On that control, move blocked nodes to runnable and clear `hasWorkerBlocked`. Executor `onCapacity` calls Engine `wakeWorkerWaiters`, which signals only reactors whose flag is true.

- [ ] **Step 4: Implement Engine construction/finalizer**

Construction order:

```text
resolve config/options
create admission + observer
open all N epoll reactors (cleanup earlier reactors on failure)
create callback executor with onCapacity=e.wakeWorkerWaiters
attach onFatal callback to reactors
start reactor goroutines
start exactly one Engine finalizer goroutine
return Engine
```

`maybeQuiescentLocked` closes `quiescent` only when:

```text
state != engineRunning
activeOps == 0
len(managed) == 0
admission.idle()
```

Finalizer waits quiescent, sends stop control to all reactors, waits `reactorWG`, requires worker executor idle, stops workers, calls `observer.stop()` without waiting for Observer return, then closes Engine.Done.

- [ ] **Step 5: Implement operation/resource helpers and immediate Close**

Add `beginOp/endOp`, `addManaged/removeManaged`, `selectReactor`, `nextResourceID`, `wakeAll`, `wakeWorkerWaiters`.

`Close()` transitions to aborting once, snapshots current managed interface values, calls `requestEngineAbort(abortExplicit)` outside Engine mutex, wakes reactors, and returns without waiting. This is sufficient cleanup for Tasks 5-10; graceful `Shutdown` is completed in Task 11.

- [ ] **Step 6: Share EngineStats formatting**

Create:

```go
func engineStatsSnapshot(admission *admissionController, observer *observerDispatcher) ogrenet.EngineStats
```

Portable and epoll `Stats()` both call it. Admission remains authoritative.

- [ ] **Step 7: Run GREEN + full portable regression**

```bash
go test ./transport -run '^Test(EpollEngine|NewEpollStarts|EngineStats)' -count=1
go test -race ./transport -run '^Test(EpollEngine|NewEpollStarts)' -count=20
go test ./... -count=1
```

- [ ] **Step 8: Remove 6A scaffold and commit**

```bash
git rm transport/epoll_engine_phase6a_linux.go
git add transport/epoll_engine_linux.go transport/epoll_engine_linux_test.go transport/epoll_reactor_inbox_linux.go transport/epoll_reactor_linux.go transport/epoll_constructor_linux.go transport/stats.go
git commit -m "runtime: boot epoll engine reactors"
```

---

### Task 5: Native TCP fd helpers, bootstrap Session/setup task, listener accept, and handoff

**Files:**
- Create: `transport/epoll_fd_linux.go`
- Create: `transport/epoll_session_linux.go`
- Create: `transport/epoll_session_task_linux.go`
- Create: `transport/epoll_listener_linux.go`
- Create: `transport/epoll_listener_linux_test.go`
- Create: `transport/epoll_native_test_helpers_linux.go`

**Produces:** private `listenNativeTCP`; public `Listen` remains unsupported.

- [ ] **Step 1: Write RED sockaddr/TCP option tests**

```go
func TestNativeTCPSockaddrRoundTripIPv4(t *testing.T) {}
func TestNativeTCPSockaddrRoundTripIPv6(t *testing.T) {}
func TestNativeTCPConfigAppliesNoDelayKeepaliveAndBuffers(t *testing.T) {}
```

Helpers:

```go
func nativeTCPAddrToSockaddr(*net.TCPAddr) (unix.Sockaddr, int, error)
func nativeSockaddrToTCPAddr(unix.Sockaddr) (*net.TCPAddr, error)
func nativeSocketAddr(fd int, peer bool) (*net.TCPAddr, error)
func configureNativeTCP(fd int, cfg TCPConfig) error
```

- [ ] **Step 2: Implement helpers and run GREEN**

```bash
go test ./transport -run '^TestNativeTCP(Sockaddr|Config)' -count=1
```

First run RED; after implementation PASS.

- [ ] **Step 3: Create bootstrap `epollSession` and codec-setup worker task under RED tests**

Initial Session struct contains all fields needed by listener/dial setup, but not Send/read/lifecycle fields added later:

```go
type epollSessionState uint8
const (
    epollSessionHandoff epollSessionState = iota + 1
    epollSessionConnecting
    epollSessionCodecSetup
    epollSessionOpening
    epollSessionActive
    epollSessionTerminal
    epollSessionClosed
)

type epollSession struct {
    engine  *epollEngine
    reactor *epollReactor
    node    epollInboxNode
    id      uint64
    fd      int

    state    epollSessionState // reactor-owned
    endpoint ogrenet.Endpoint
    local    *net.TCPAddr
    remote   *net.TCPAddr
    handler  ogrenet.Handler
    lease    *connectionLease
    parent   *epollListener

    framer     wire.Framer
    wireFramer bool
    setupMu    sync.Mutex
    setupDone  bool
    setupErr   error
    setupFramer wire.Framer

    done chan struct{}
}
```

`epollCodecSetupTask{session *epollSession}` implements `epollWorkerTask`. It calls `session.engine.cfg.newFramer()` on a worker, stores result, then signals the session reactor.

RED tests:

```go
func TestEpollCodecFactoryDoesNotRunOnReactor(t *testing.T) {}
func TestEpollCodecFactoryCapacityPauseRetriesWithoutSecondSignal(t *testing.T) {}
```

Block `FramerFactory` on a channel while another synthetic item on the same reactor progresses.

- [ ] **Step 4: Write RED listener/handoff tests**

```go
func TestEpollNativeListenerOwnedByOneReactor(t *testing.T) {}
func TestEpollNativeAcceptHandoffRegistersOnSelectedReactor(t *testing.T) {}
func TestEpollNativeAcceptEventUsesSessionAndParentIDs(t *testing.T) {}
func TestEpollNativeListenerAdmissionRejectsAndReleasesLease(t *testing.T) {}
func TestEpollAcceptedCodecFactoryFailureClosesWithoutHandlerCallback(t *testing.T) {}
```

`Pollers:2` handoff test waits on Observer `EventAccept`, then checks package-private session reactor index; no polling sleep.

- [ ] **Step 5: Implement listener**

```go
type epollListener struct {
    engine   *epollEngine
    reactor  *epollReactor
    node     epollInboxNode
    id       uint64
    fd       int
    endpoint ogrenet.Endpoint
    local    *net.TCPAddr
    handler  ogrenet.Handler
    capacity *listenerCapacity
    stats    *listenerCounters
    done     chan struct{}
    closeReq atomic.Bool
    errMu    sync.RWMutex
    err      error
}
```

Listener implements `epollEventResource` + `epollManagedResource`. `requestEngineShutdown` and `requestEngineAbort` both request listener close; neither caller closes the fd.

Accept uses `unix.Accept4(...SOCK_NONBLOCK|SOCK_CLOEXEC)` under `IOBudgetOps`. For each fd: resolve addresses, acquire opening lease with listener capacity, configure TCP, allocate ID, select fixed target reactor, create bootstrap session, `engine.addManaged(session)`, signal target.

Target first registers the fd. Before registration, the handoff object owns the fd and no reactor performs socket I/O; target is merely the registration/cleanup executor. Successful `Poller.Add` transfers fd ownership to target. Registration failure closes fd/releases lease/removes managed session exactly once.

After registration, schedule codec setup through worker reservation. Setup failure closes/release/removes without Accept or Handler callbacks. Setup success activates lease, increments listener Accepted, emits `EventAccept` with Session ID + Listener ParentID, and leaves Session in `epollSessionOpening` for Task 9.

- [ ] **Step 6: Run listener/setup GREEN + race**

```bash
go test ./transport -run '^TestEpoll(Native(Listen|Accept)|Codec)' -count=1
go test -race ./transport -run '^TestEpoll(Native(Listen|Accept)|Codec)' -count=20
```

- [ ] **Step 7: Commit**

```bash
git add transport/epoll_fd_linux.go transport/epoll_session_linux.go transport/epoll_session_task_linux.go transport/epoll_listener_linux.go transport/epoll_listener_linux_test.go transport/epoll_native_test_helpers_linux.go
git commit -m "runtime: add epoll TCP listener handoff"
```

---

### Task 6: Caller-side DNS and reactor-owned non-blocking Dial/connect

**Files:**
- Create: `transport/epoll_dial_linux.go`
- Create: `transport/epoll_dial_linux_test.go`
- Modify: `transport/epoll_session_linux.go`
- Modify: `transport/epoll_session_task_linux.go`

**Produces:** private `dialNativeTCP`; public `Dial` remains unsupported.

- [ ] **Step 1: Write RED resolver/connect tests**

```go
func TestResolveNativeDialTCPLiteralBypassesResolver(t *testing.T) {}
func TestEpollNativeDialCompletesThroughSOError(t *testing.T) {}
func TestEpollNativeDialRefusedReturnsTypedDialError(t *testing.T) {}
func TestEpollNativeDialCallerCancellationReturnsCauseUnwrapped(t *testing.T) {}
func TestEpollNativeDialCancellationNeverClosesFDFromCaller(t *testing.T) {}
func TestEpollNativeDialTriesResolvedAddressesSequentially(t *testing.T) {}
func TestEpollDialCodecFactoryFailureReturnsDirectConfigError(t *testing.T) {}
```

Private resolver signature:

```go
func resolveNativeDialTCP(ctx context.Context, endpoint ogrenet.Endpoint) ([]*net.TCPAddr, error)
```

IP literals bypass DNS; hostnames use `net.Resolver`; no Happy Eyeballs.

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^Test(EpollNativeDial|ResolveNativeDial)' -count=1
```

- [ ] **Step 3: Implement Dial caller flow**

```text
beginOp
create one bounded Connect context before DNS
resolve all addresses caller-side in resolver order
allocate tentative ID + fixed reactor
create connecting epollSession + engine.addManaged
signal reactor
wait buffered result OR bounded context
on context completion publish cancellation state + signal reactor; caller never closes/mutates fd
endOp
```

Failed dials consume tentative IDs but Observer failure events expose `ResourceID:0`.

- [ ] **Step 4: Implement reactor address attempts**

For each address:

```text
socket NONBLOCK|CLOEXEC
connect
0 => connected
EINPROGRESS => Add(fd, Writable|Error|EdgeTriggered, id), schedule Connect generation/deadline
other errno => close, next address
EPOLLOUT/ERR => SO_ERROR
0 => connected
errno => Del/close, next address
```

On connect success: capture local/remote, configure TCP, acquire opening lease, schedule codec setup worker. After setup success activate lease, emit Connect success with stable Session ID/duration, set `epollSessionOpening`, send buffered Dial result. On setup/admission failure close/release/remove and return parity error.

Final socket failure is classified once as `OpDial`; caller cancellation remains direct.

- [ ] **Step 5: Run GREEN + cancellation race**

```bash
go test ./transport -run '^Test(EpollNativeDial|ResolveNativeDial)' -count=1
go test -race ./transport -run '^TestEpollNativeDial' -count=20
```

- [ ] **Step 6: Commit**

```bash
git add transport/epoll_dial_linux.go transport/epoll_dial_linux_test.go transport/epoll_session_linux.go transport/epoll_session_task_linux.go
git commit -m "runtime: add epoll nonblocking TCP dial"
```

---

### Task 7: Send/TrySend, codec token, partial write, and fixed write deadline

**Files:**
- Create: `transport/epoll_session_send_linux.go`
- Create: `transport/epoll_session_send_linux_test.go`
- Modify: `transport/epoll_session_linux.go`

**Produces:** outbound native TCP path tested against raw TCP peers; public Engine capability remains gated.

- [ ] **Step 1: Write RED admission/codec/queue tests**

```go
func TestEpollNativeTrySendCodecContentionReturnsTypedWouldBlockOnce(t *testing.T) {}
func TestEpollNativeTrySendQueuePressureCountsOneBackpressure(t *testing.T) {}
func TestEpollNativeSendCancellationAfterQueueTransferMayStillWrite(t *testing.T) {}
func TestEpollNativePartialWriteRetainsQuotaUntilFrameComplete(t *testing.T) {}
func TestEpollNativeWriteEAGAINDoesNotBlockReactor(t *testing.T) {}
```

Partial-write test shrinks `SO_SNDBUF`, blocks raw peer reads, queues a large allowed frame, and proves another synthetic/session item on the same reactor progresses. The oracle is the second completion channel, not a millisecond threshold.

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^TestEpollNative(Send|TrySend|Partial|Write)' -count=1
```

- [ ] **Step 3: Add existing ownership primitives to Session**

```go
queue      chan outbound
quota      *byteQuota
gate       *sendGate
frameSlots chan struct{}
codecSlot  chan struct{} // capacity 1; send=acquire, receive=release
stats      *sessionCounters

decodeWaiting atomic.Bool
```

`Send`/`TrySend` copy portable admission ordering. Encoding is synchronous on caller, under codec token, and returned frame is copied before ownership transfer. `releaseCodec` signals reactor when decoder was paused.

- [ ] **Step 4: Add reactor-only write machine**

```go
writeCurrent outbound
writeOffset  int
writeActive  bool
writeBlocked bool
writeGen     uint64
```

`driveWrite`:

```text
pop current non-blockingly
start one fixed Write deadline generation
unix.Write(frame[offset:])
progress => touch activity/charge per-turn budget
complete => cancel deadline generation; release quota+frame slot; Stats TX; Observer Write; ack
EAGAIN => enable Writable interest and return
EINTR => retry
budget hit => reactor.requeue(session)
other error => typed OpWrite terminal path, fail current/pending once
```

Writable interest is disabled when no incomplete output remains.

- [ ] **Step 5: Run GREEN + race**

```bash
go test ./transport -run '^TestEpollNative(Send|TrySend|Partial|Write)' -count=1
go test -race ./transport -run '^TestEpollNative(Send|TrySend|Partial|Write)' -count=20
```

- [ ] **Step 6: Commit**

```bash
git add transport/epoll_session_send_linux.go transport/epoll_session_send_linux_test.go transport/epoll_session_linux.go
git commit -m "runtime: add epoll TCP send path"
```

---

### Task 8: Session lifecycle API, reactor-only close/shutdown syscalls, and half-close ownership

**Files:**
- Create: `transport/epoll_session_lifecycle_linux.go`
- Create: `transport/epoll_session_lifecycle_linux_test.go`
- Modify: `transport/epoll_session_linux.go`
- Modify: `transport/epoll_session_send_linux.go`

**Produces:** `epollSession` implements `ogrenet.HalfCloseSession`; public Engine TCP remains gated until Task 9 adds OnOpen/read callbacks.

- [ ] **Step 1: Write compile RED**

```go
var _ ogrenet.Session = (*epollSession)(nil)
var _ ogrenet.HalfCloseSession = (*epollSession)(nil)
```

Run:

```bash
go test ./transport -run '^$'
```

Expected: compile FAIL for missing lifecycle/public methods.

- [ ] **Step 2: Write RED lifecycle ownership tests with raw peer**

```go
func TestEpollNativeCloseDoesNotCloseFDFromCaller(t *testing.T) {}
func TestEpollNativeCloseWriteDrainsThenSendsFIN(t *testing.T) {}
func TestEpollNativeShutdownWaitsForPeerFIN(t *testing.T) {}
func TestEpollNativePeerFINLeavesWriteHalfUsable(t *testing.T) {}
func TestEpollNativeLifecycleFirstAbortWins(t *testing.T) {}
```

For peer FIN before Task 9 read path, reactor uses a non-consuming `MSG_PEEK|MSG_DONTWAIT` probe: n==0 marks FIN; n>0 leaves payload untouched for Task 9.

- [ ] **Step 3: Implement public identity/Stats/error methods**

Add `ID/Protocol/Endpoint/LocalAddr/RemoteAddr/Stats/Done/Err/ReadClosed` with stored addresses; Stats does no socket syscall.

Add `life *sessionLifecycle`, logical `closing chan struct{}`, error lock, age/stats, and close-signal once fields.

- [ ] **Step 4: Implement caller-side lifecycle publication**

```text
CloseWrite => request GoalWrite; winner closes send gate; signal reactor; wait WriteDone/abort/ctx
Shutdown => request GoalFull; winner closes send gate; signal reactor; wait WriteDone then Done/abort/ctx
Close => abortWith AbortExplicit publishing Err=nil; close logical closing signal + gate; signal reactor; return
owner ctx expiry => abortWith AbortCaller; signal reactor; return caller cause
```

No caller path invokes `unix.Close`/`unix.Shutdown`.

- [ ] **Step 5: Implement reactor lifecycle drive**

```text
abort: Del if registered -> close fd -> fail all output/release quota -> cancel deadlines -> mark halves closed -> terminal cleanup
graceful write: wait gate drained and write queue/current empty -> unix.Shutdown(SHUT_WR) -> MarkWriteClosed
peer FIN: MarkReadClosed without Err; keep writable if write half open
both halves closed: TryMarkTerminal -> Del/close fd -> zero queues -> freeze age -> EventClose
```

At this task, sessions whose Handler lifecycle has not started skip Handler OnClose. Task 9 starts Handler lifecycle before the public TCP capability is enabled.

- [ ] **Step 6: Run GREEN + race**

```bash
go test ./transport -run '^TestEpollNative(Close|Shutdown|PeerFIN|Lifecycle)' -count=1
go test -race ./transport -run '^TestEpollNative(Close|Shutdown|PeerFIN|Lifecycle)' -count=20
```

- [ ] **Step 7: Commit**

```bash
git add transport/epoll_session_lifecycle_linux.go transport/epoll_session_lifecycle_linux_test.go transport/epoll_session_linux.go transport/epoll_session_send_linux.go
git commit -m "runtime: add epoll TCP lifecycle"
```

---

### Task 9: OnOpen/read/decode/callback serialization, async Message ownership, OnClose barrier, and public TCP capability flip

**Files:**
- Create: `transport/epoll_session_read_linux.go`
- Create: `transport/epoll_session_read_linux_test.go`
- Modify: `transport/epoll_session_task_linux.go`
- Modify: `transport/epoll_session_linux.go`
- Modify: `transport/epoll_session_lifecycle_linux.go`
- Modify: `transport/epoll_listener_linux.go`
- Modify: `transport/epoll_dial_linux.go`
- Modify: `transport/epoll_engine_linux.go`
- Modify: `transport/contract_native_linux_test.go`

- [ ] **Step 1: Write RED callback/order/ownership tests**

```go
func TestEpollNativeOnOpenCompletesBeforeFirstOnMessage(t *testing.T) {}
func TestEpollNativeDecodedMessageOwnsBytesOutsideReadBuffer(t *testing.T) {}
func TestEpollNativeStatsAndObserverReadCommitBeforeCallback(t *testing.T) {}
func TestEpollNativeBlockedHandlerDoesNotBlockReactor(t *testing.T) {}
func TestEpollNativeOneSessionNeverRunsConcurrentCallbacks(t *testing.T) {}
func TestEpollNativeDoneWaitsForOnCloseReturn(t *testing.T) {}
```

Ownership test retains Message #1 while later network/read-buffer work occurs and verifies its bytes remain unchanged.

- [ ] **Step 2: Write RED executor-saturation/edge-retry test**

With `CallbackWorkers:1`, `CallbackQueue:1`, fill one running + one queued Handler task behind barriers, make a third session readable, release one slot, and assert the third message arrives without sending more bytes or relying on another edge.

- [ ] **Step 3: Run RED**

```bash
go test ./transport -run '^TestEpollNative(OnOpen|Decoded|StatsAndObserverRead|BlockedHandler|OneSession|DoneWaits|Callback)' -count=1
```

- [ ] **Step 4: Extend worker tasks with Handler lifecycle**

Reactor-owned state:

```go
type epollSessionCallbackState uint8
const (
    epollCallbackNeedOpen epollSessionCallbackState = iota + 1
    epollCallbackOpenInFlight
    epollCallbackIdle
    epollCallbackMessageInFlight
    epollCallbackNeedClose
    epollCallbackCloseInFlight
    epollCallbackClosed
)
```

Worker task kinds: codec setup (existing), OnOpen, OnMessage, OnClose. At most one Handler task is in flight per Session. Worker completion stores one completion code then signals reactor.

After listener/dial codec setup succeeds, schedule OnOpen; Dial may return after setup/adoption while OnOpen is queued/running, matching portable asynchronous OnOpen timing. Reads remain paused until OnOpen completion.

- [ ] **Step 5: Implement non-blocking read/decode**

Session owns `readScratch []byte` and `readPending []byte`.

Before work that may produce a Handler callback, reserve worker capacity. If unavailable, add Session to reactor worker-blocked list without reading.

Algorithm:

```text
try codec token; if unavailable set decodeWaiting and return
try DecodeOne(existing pending)
NeedMore => unix.Read under byte/op budget and append
complete => Validate; copy Message.Data to independent slice; consume/compact pending; release codec token
            Stats BytesRX/MessagesRX; Observer Read; submit reserved OnMessage; pause Session
EAGAIN/NeedMore without message => release reservation/token
read 0 => release reservation/token; clean peer FIN
invalid/decode error => release reservation/token; DecodeErrors++; typed OpRead terminal path
budget hit before EAGAIN => release temporary reservation/token and reactor.requeue(session)
```

Callback completion explicitly retries pending decode/read before waiting for another edge.

- [ ] **Step 6: Complete OnClose barrier**

Terminal resource closes fd/queues/deadlines first, freezes age and emits Close, then schedules OnClose only after no later Message callback can be created. After OnClose returns: close Session.Done, release connection lease, `engine.removeManaged(id)`. A blocked OnClose therefore holds Engine.Done; Observer does not.

- [ ] **Step 7: Flip public TCP capability and shared basic contract**

`epollEngine.Listen/Dial` route validated TCP to native methods. TLS/WS/WSS remain `ErrProtocolUnsupported`; UDP mismatch remains `ErrProtocolMismatch`; packet methods remain unsupported.

Change native factory test to:

```go
func TestEpollPublicTCPContracts(t *testing.T) {
    runEngineContracts(t, epollFactory(contractProfile{TCP: true}))
}
```

Run:

```bash
go test ./transport -run '^TestEnginePublicContracts|^TestEpollPublicTCPContracts' -count=1
```

Expected: portable TCP/UDP + native TCP basic echo/lifecycle/Stats PASS.

- [ ] **Step 8: Run callback/native race**

```bash
go test -race ./transport -run '^TestEpollNative(OnOpen|Decoded|StatsAndObserverRead|BlockedHandler|OneSession|DoneWaits|Callback)|^TestEpollPublicTCPContracts' -count=20
```

- [ ] **Step 9: Commit**

```bash
git add transport/epoll_session_read_linux.go transport/epoll_session_read_linux_test.go transport/epoll_session_task_linux.go transport/epoll_session_linux.go transport/epoll_session_lifecycle_linux.go transport/epoll_listener_linux.go transport/epoll_dial_linux.go transport/epoll_engine_linux.go transport/contract_native_linux_test.go
git commit -m "runtime: add epoll TCP read callbacks"
```

---

### Task 10: Shared graceful/limits/timeouts/errors/Stats/Observer parity

**Files:**
- Create: `transport/contract_tcp_graceful_test.go`
- Create: `transport/contract_tcp_limits_test.go`
- Create: `transport/contract_tcp_observer_test.go`
- Create: `transport/contract_tcp_timeout_error_test.go`
- Create: `transport/epoll_tcp_parity_linux_test.go`
- Modify: `transport/contract_harness_test.go`
- Modify: native Engine/listener/session/dial files only where a RED parity test exposes a missing seam

- [ ] **Step 1: Add shared graceful contracts**

Portable and epoll TCP run the same helpers for CloseWrite, full Shutdown, and peer FIN. Assert drain-before-FIN, write-after-peer-FIN, clean FIN `Err()==nil`, single OnClose, Done after OnClose, and lifecycle-owner cancellation precedence.

Run portable subtests first: they must characterize GREEN; native subtests should expose only genuine parity gaps.

- [ ] **Step 2: Add shared limits/Stats contracts**

Cover:

```text
MaxConnections / per-peer / per-listener
MaxQueuedBytesTotal
listener Accepted/Rejected/Current
engine Opening/Active/Draining/GlobalQueuedBytes
session payload BytesRX/TX + MessagesRX/TX
Backpressure exactly once per TrySend pressure failure
queue gauges zero after finalization
Age stable after Done
```

- [ ] **Step 3: Add shared Observer contracts**

Cover Accept IDs/ParentID, Connect success/failure/duration, Read/Write payload bytes, Backpressure, one Close after final state, observer saturation independence, and observer panic counters.

- [ ] **Step 4: Add timeout/error parity tests**

Cover synchronized ReadIdle, ConnectionIdle, MaxLifetime, write timeout with raw peer not reading, connection refused raw errno reachability, reset/error first-owner precedence, and direct caller cancellation.

Connect-deadline arbitration also has internal deterministic Task 2/6 tests; do not depend only on an external unroutable address.

- [ ] **Step 5: Fill only demonstrated native seams**

Deadline/activity rules:

```text
Connect deadline covers DNS + sequential attempts
Write deadline fixed from current-frame start; partial progress does not reset it
ReadIdle begins after OnOpen return and is suspended while that Session's Handler callback executes
ConnectionIdle resets only on successful network read/write progress
MaxLifetime never resets
```

Stats/Observer order:

```text
RX decode+validate -> Stats -> Observer -> Handler
TX complete write -> release quota/slot -> Stats -> Observer -> Send ack
Close stable Err + zero queue + age freeze -> Observer -> OnClose -> Done
```

- [ ] **Step 6: Run full parity + native race**

```bash
go test ./transport -run '^Test.*(Contract|Parity|Graceful|Observer|Stats|Limit|Timeout|Error)' -count=1
go test -race ./transport -run '^TestEpoll.*(Parity|Graceful|Observer|Stats|Limit|Timeout|Error)' -count=20
```

- [ ] **Step 7: Commit**

```bash
git add transport/contract_tcp_graceful_test.go transport/contract_tcp_limits_test.go transport/contract_tcp_observer_test.go transport/contract_tcp_timeout_error_test.go transport/epoll_tcp_parity_linux_test.go transport/contract_harness_test.go transport/epoll_*.go
git commit -m "runtime: align epoll TCP semantics"
```

Before committing, inspect `git diff --name-only` and replace the broad `transport/epoll_*.go` staging expression with the exact native files actually changed by RED tests; do not stage unrelated files.

---

### Task 11: Graceful Engine Shutdown/Close and cross-reactor stress invariants

**Files:**
- Create: `transport/epoll_engine_shutdown_linux.go`
- Create: `transport/epoll_engine_shutdown_linux_test.go`
- Create: `transport/epoll_tcp_stress_linux_test.go`
- Modify: `transport/epoll_engine_linux.go`
- Modify: concrete listener/session managed-resource methods

- [ ] **Step 1: Write RED Engine lifecycle tests**

```go
func TestEpollEngineShutdownStopsListenersThenGracefullyDrainsSessions(t *testing.T) {}
func TestEpollEngineShutdownOwnerCancellationAbortsRemaining(t *testing.T) {}
func TestEpollEngineCloseNeverClosesFDFromCaller(t *testing.T) {}
func TestEpollEngineDoneWaitsForBlockedApplicationOnClose(t *testing.T) {}
func TestEpollEngineDoneDoesNotWaitForBlockedObserver(t *testing.T) {}
```

- [ ] **Step 2: Write deterministic race cases**

Use package barriers for accept handoff vs Shutdown, connect completion vs cancellation, callback completion vs abort, Wake vs Close, peer FIN vs CloseWrite, write timeout vs reset, and listener close during accept flood.

- [ ] **Step 3: Implement graceful Engine owner model using `epollManagedResource`**

Each concrete resource implements:

```text
listener requestEngineShutdown => close listener
active session requestEngineShutdown => graceful full Session shutdown
connecting/handoff/setup session requestEngineShutdown => abort (no established protocol to drain)
requestEngineAbort => abort/close request only; physical fd action remains reactor-owned
```

Engine `Shutdown` mirrors portable owner precedence:

```text
first caller running->draining, snapshot managed resources, request shutdown, wake all
wait Done
if owning ctx ends first => abort remaining AbortCaller, return caller cause
non-owner concurrent Shutdown never steals abort ownership
```

`Close` remains immediate abort and idempotent. `shutdownResult` matches portable `ErrClosed` precedence.

- [ ] **Step 4: Add invariant snapshot for tests only**

```go
type epollNativeInvariantSnapshot struct {
    ReactorResources int
    ReactorInbox     int
    ReactorRunnable  int
    WorkerBlocked    int
    CallbackReserved int
    ManagedResources int
    Admission        admissionSnapshot
}
```

After Engine.Done assert every structural count zero plus Opening/Active/Draining/GlobalQueuedBytes zero. Do not expose these as public Stats.

- [ ] **Step 5: Add multi-reactor short-lived stress**

`Pollers:4`, thousands of loopback TCP sessions, one framed exchange, mixed close side, WaitGroups/channels for completion, final invariant snapshot zero. No sleeps.

- [ ] **Step 6: Run GREEN**

```bash
go test -race ./transport -run '^TestEpoll(Engine|Native).*' -count=20
go test ./transport -run '^TestEpollNativeShortLivedConnections' -count=10
```

- [ ] **Step 7: Commit**

```bash
git add transport/epoll_engine_shutdown_linux.go transport/epoll_engine_shutdown_linux_test.go transport/epoll_tcp_stress_linux_test.go transport/epoll_engine_linux.go
git commit -m "runtime: harden epoll engine shutdown"
```

Include exact concrete resource files in the commit only if Step 3 changed them.

---

### Task 12: TCP backend benchmarks, permanent CI gates, exact-head verification, and 6B checkpoint

**Files:**
- Create: `transport/epoll_tcp_benchmark_test.go`
- Modify: `.github/workflows/netpoll-v2.yml`

- [ ] **Step 1: Add identical portable-vs-epoll TCP benchmarks**

Required dimensions:

```text
1 KiB echo throughput
4 KiB echo throughput
64 KiB echo throughput
request/echo latency harness
connection setup rate
Send allocs/bytes-op
TrySend allocs/bytes-op
graceful Engine shutdown fan-out
```

Sub-benchmarks use identical framing/options. Report speed data; do not hard-gate ns/op or percent improvement on hosted CI.

- [ ] **Step 2: Keep only deterministic hard gates**

Hard checks may cover: existing portable allocation limits unchanged, observer-disabled/Stats zero-allocation unchanged, native Stats snapshot zero allocations, no disabled-observer event-path allocation, and no quota/lease/task leaks after benchmark teardown.

- [ ] **Step 3: Extend Linux Go 1.26 CI**

Add:

```yaml
- name: Native TCP race loop
  if: matrix.go == '1.26.x'
  run: go test -race ./transport -run '^TestEpoll' -count=20

- name: Native TCP benchmark smoke
  if: matrix.go == '1.26.x'
  run: >-
    go test ./transport -run '^$'
    -bench 'BenchmarkTCPBackend|BenchmarkEpoll'
    -benchmem -benchtime=1x
```

Keep existing Linux 1.25/1.26, Windows, macOS, FreeBSD runtime, GmSSL, and cross-compile jobs intact.

- [ ] **Step 4: Full verification**

```bash
gofmt -w .
git diff --exit-code
go mod tidy
git diff --exit-code -- go.mod go.sum
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
go test -race ./transport -run '^TestEpoll' -count=20
go test ./transport -run '^$' -bench 'BenchmarkTCPBackend|BenchmarkEpoll' -benchmem -benchtime=1x
```

- [ ] **Step 5: Cross-build without executing foreign binaries**

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c ./transport -o /tmp/transport-linux-amd64.test
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go test -c ./transport -o /tmp/transport-linux-arm64.test
GOOS=windows GOARCH=arm64 CGO_ENABLED=0 go test -c ./transport -o /tmp/transport-windows-arm64.test.exe
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go test -c ./transport -o /tmp/transport-darwin-arm64.test
```

- [ ] **Step 6: Commit benchmark/CI work**

```bash
git add transport/epoll_tcp_benchmark_test.go .github/workflows/netpoll-v2.yml
git commit -m "test: gate epoll TCP runtime"
```

- [ ] **Step 7: Require exact-head Actions evidence**

Exact head must pass:

```text
Linux Go 1.25 format/module/vet + full race
Linux Go 1.26 format/module/vet
existing graceful allocation gate
existing observability allocation gate
typed-error 20x race
observability 20x race
runtimecore 20x race
native TCP 20x race
full repository race
Windows/macOS tests
FreeBSD runtime classifier
GmSSL
all existing cross-compiles
native TCP benchmark smoke
```

- [ ] **Step 8: Update PR #58 and tracking issues**

Record:

```text
P1-6B Linux TCP complete
P1-6C UDP not implemented
TLS/WS/WSS still explicit unsupported/no fallback
PR remains Draft because #57 still requires UDP and later productionization evidence
```

Do not mark PR Ready and do not close #57, #56, or #38 at the 6B checkpoint.
