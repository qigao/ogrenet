# Linux Native Engine 6B Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a real Linux epoll-owned TCP backend behind `transport.NewEpoll`, with fixed reactor ownership, bounded setup/callback execution, non-blocking accept/connect/read/write, graceful half-close, typed-error/Stats/Observer parity, and TCP race/stress/benchmark evidence.

**Architecture:** `transport.New()` remains the portable reference implementation. Linux `NewEpoll` starts N fixed epoll reactors plus one exact-bounded setup/callback executor. Every listener/session fd is assigned to exactly one reactor; only that reactor performs socket creation after assignment, accept/connect progress, read/write, `shutdown(2)`, and terminal close. Application goroutines perform validation, admission, synchronous encoding, ownership publication, and waiting only. Cross-goroutine progress is published through embedded intrusive inbox nodes with coalesced `Poller.Wake()`; work intentionally left ready under EPOLLET is requeued locally rather than waiting for another edge.

**Tech Stack:** Go 1.25+, `golang.org/x/sys/unix`, existing top-level `epoll` poller, root `ogrenet` contracts, existing `transport` admission/quota/error/stats/observer helpers, `internal/runtimecore`, race detector, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-22-linux-native-engine-design.md`

## Global Constraints

- Linux TCP only in 6B. `ListenPacket`/`DialPacket` remain `ErrProtocolUnsupported`; TLS/WS/WSS remain `ErrProtocolUnsupported` with no portable fallback.
- `transport.New()` remains unchanged as the portable correctness/reference backend.
- Exactly one reactor goroutine owns each assigned listener/session fd. No native Session gets a reader goroutine, writer goroutine, timeout goroutine, or application-goroutine fd syscall path.
- Listener socket/bind/listen and outbound socket/connect are executed by the selected reactor. Caller-side address/DNS resolution is allowed because it owns no fd.
- Accepted fd handoff preserves one logical owner until target registration succeeds; fd close and lease release on handoff failure happen exactly once.
- `epoll.Event.Data` is one Engine-local monotonic ID. ID 0 and `math.MaxUint64` are forbidden. Allocation uses CAS and permanently fails at `math.MaxUint64-1`; it must never wrap and reuse low IDs under concurrency.
- A resource never migrates between reactors in 6B. Accepted sessions are assigned round-robin once; outbound sessions select one reactor before socket creation.
- Every embedded `epollInboxNode` sets `owner` exactly once before first signal. Producers never allocate a command object per signal.
- Cross-goroutine control uses intrusive deduplicated inbox nodes plus coalesced `Poller.Wake()`. No generic command channel, unbounded queue, lock-free MPSC, or per-signal allocation.
- Edge-triggered work deliberately left before `EAGAIN` is placed on the reactor-local runnable list. Never rely on a second edge after fairness yield, worker-capacity pause, codec pause, lifecycle pause, or callback completion.
- User `Handler` callbacks never run on reactors. Per-session `FramerFactory`/`CipherFactory` construction is also application-supplied work and executes on the exact-bounded worker executor, not on reactors.
- Custom framer encode runs synchronously on `Send`/`TrySend` callers after codec-token admission; decode runs on the owning reactor only after non-blocking codec-token acquisition.
- A decoded `ogrenet.Message` submitted asynchronously owns its `Data`: copy with `append([]byte(nil), msg.Data...)` before pending-buffer compaction/reuse.
- Stats ownership/counting points and Observer ordering remain P0-5 exact: counters first, optional Observer event second, Handler/Send ack third.
- Caller context cancellation/deadline is returned unchanged. Operational socket failures use existing `classifyOperational`; configuration/capability errors remain direct sentinels.
- Admission, `connectionLease`, `listenerCapacity`, `byteQuota`, `sendGate`, `sessionLifecycle`, counters, Observer dispatcher, and public error types are reused rather than duplicated.
- Engine graceful shutdown moves active connection leases to draining before publishing Session graceful requests, matching portable `beginGracefulShutdown` ordering.
- `Send(ctx)` may return caller cancellation after queue ownership transfer while the frame remains eligible for physical write. `TrySend` never waits for reactor/network progress.
- Worker retained capacity is exactly `CallbackWorkers + CallbackQueue` running+queued+reserved tasks. Queued tasks never exceed `CallbackQueue`.
- `Engine.Done()` waits for application/setup tasks needed to finish resources; it never waits for a blocked Observer callback.
- Listener lifetime continues to honor the `Listen` context without a persistent listener watcher goroutine: use `context.AfterFunc` only to publish close state + signal the owning reactor; stop the AfterFunc when Listener finalizes.
- Correctness tests synchronize on real channels/hooks/state transitions. Sleeps are not correctness or ordering oracles. Timeout tests start clocks only after setup barriers are established.
- Every task below compiles and passes its stated verification before the next task starts. No task depends on Go forward declarations from a subsequent task.
- UDP, TLS, WS/WSS, kqueue, IOCP, pooling, buffer-pool redesign, `writev`, `sendfile`, Happy Eyeballs, proxy, QUIC, and HTTP changes are outside 6B.

## Planned file map

```text
transport/
    epoll_engine_linux.go              Engine state, registry, startup/final barrier
    epoll_engine_shutdown_linux.go     graceful Engine owner semantics
    epoll_reactor_linux.go             Wait loop, event dispatch, runnable fairness
    epoll_reactor_inbox_linux.go       intrusive inbox + lost-wake handshake
    epoll_deadline_linux.go            generation min-heap scheduler
    epoll_callback_linux.go            exact-bounded setup/callback executor
    epoll_fd_linux.go                  sockaddr/socket/TCP option helpers
    epoll_listener_linux.go            native TCP listener + accept/handoff
    epoll_session_linux.go             native TCP Session identity/state/bootstrap
    epoll_session_task_linux.go        codec setup + Handler worker tasks
    epoll_dial_linux.go                DNS/address attempts + non-blocking connect
    epoll_session_send_linux.go        admission + codec token + partial write
    epoll_session_lifecycle_linux.go   half-close/abort/finalization
    epoll_session_read_linux.go        read/decode + callback serialization
    epoll_native_test_helpers_linux.go deterministic package-test helpers
    epoll_tcp_benchmark_test.go        portable/native TCP benchmark harness
```

`transport/epoll_engine_phase6a_linux.go` is removed only after `epoll_engine_linux.go` supplies the same `ogrenet.Engine` surface. Existing portable source files are not reorganized.

---

### Task 1: Engine-independent reactor inbox, lost-wake handshake, registry, and runnable fairness

**Files:**
- Create: `transport/epoll_reactor_inbox_linux.go`
- Create: `transport/epoll_reactor_linux.go`
- Create: `transport/epoll_reactor_linux_test.go`

**Consumes:** `epoll.Open`, `Poller.Wait/Add/Mod/Del/Wake`, `resolvedEpollConfig`.

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
    queued         bool // reactor.inboxMu
    runnableQueued bool // reactor goroutine only
}

type epollReactor struct {
    index  int
    cfg    resolvedEpollConfig
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
    onFatal      func(error)
}

const epollControlStop uint32 = 1 << 0
```

- [ ] **Step 1: Write RED synthetic-item tests**

Test helper in `epoll_reactor_linux_test.go`:

```go
type testInboxItem struct {
    node  epollInboxNode
    calls atomic.Int32
    run   func(*epollReactor)
}

func newTestInboxItem(run func(*epollReactor)) *testInboxItem {
    x := &testInboxItem{run: run}
    x.node.owner = x
    return x
}
func (x *testInboxItem) inboxNode() *epollInboxNode { return &x.node }
func (x *testInboxItem) onReactorInbox(r *epollReactor) {
    x.calls.Add(1)
    if x.run != nil { x.run(r) }
}
```

Write these exact scenarios:

```go
// dedupe: signal one embedded node 100 times before drain; drain once; calls == 1.
for i := 0; i < 100; i++ { r.signal(item) }
r.drainInbox()
if got := item.calls.Load(); got != 1 { t.Fatalf("calls=%d", got) }

// lost wake: start real r.run in a goroutine; a package-test hook closes waitArmed
// immediately after armWait commits waiting=true. After <-waitArmed, signal(item).
// item callback must close processed without any additional network event.
select { case <-processed: case <-ctx.Done(): t.Fatal(ctx.Err()) }

// control: after <-waitArmed call r.signalControl(epollControlStop); reactor exits.
select { case <-reactorDone: case <-ctx.Done(): t.Fatal(ctx.Err()) }
```

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^TestEpollReactor(Signal|Control)' -count=1
```

Expected: compile FAIL because reactor types are absent.

- [ ] **Step 3: Implement intrusive signal + Wait handshake**

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

`drainInbox` detaches the whole list under `inboxMu`, clears each `queued` flag before calling `onReactorInbox`, then invokes callbacks without holding `inboxMu`. A producer may therefore requeue the same item while its prior callback executes.

`armWait` checks inbox/control/runnable while holding `inboxMu`; only an empty reactor sets `waiting=true, wakePending=false`. `disarmWait` clears both under the same lock. `signalControl(mask)` CAS-ORs the bit then performs the same wake handshake without an inbox allocation.

- [ ] **Step 4: Write RED registry/runnable tests**

Use a synthetic `epollEventResource` with fixed ID/fd and counters. Execute:

```go
// stale event: dispatch Event{Data:42} with resources[42] absent; counter stays 0.
r.dispatch(epoll.Event{Data: 42, Events: epoll.Readable})
if got := res.events.Load(); got != 0 { t.Fatalf("events=%d", got) }

// duplicate registration: register ID 7 once; second different resource ID 7 returns err.
if err := r.registerResource(a); err != nil { t.Fatal(err) }
if err := r.registerResource(b); err == nil { t.Fatal("duplicate ID replaced owner") }

// runnable continuation: onReactorRunnable increments count and calls r.requeue(res)
// until count == 3. One initial r.requeue + r.drainRunnable must reach exactly 3
// without Poller.Wait or another fd edge.
r.requeue(res)
r.drainRunnable()
if got := res.runs.Load(); got != 3 { t.Fatalf("runs=%d", got) }
```

- [ ] **Step 5: Implement registry/runnable/event loop**

Registry mutates only on the reactor goroutine. `requeue` sets `runnableQueued` and appends once. Loop order:

```text
drain inbox
drain control
drain runnable
if stop requested AND resources/inbox/runnable empty => return
arm Wait handshake
Poller.Wait
clear Wait handshake
dispatch only event IDs still in resources
```

Wait errors other than expected close invoke `onFatal` outside internal locks.

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

### Task 2: Generation-based native deadline scheduler

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

Deadline dispatch looks up `reactor.resources[id]`, then type-asserts `epollDeadlineTarget`; Task 1's interface stays unchanged.

- [ ] **Step 1: Write RED heap tests with exact assertions**

Test target stores `gens [6]uint64` and a channel receiving fired kind. Execute:

```go
base := time.Unix(100, 0)
r.scheduleDeadline(2, epollDeadlineWrite, 1, base.Add(2*time.Second))
r.scheduleDeadline(1, epollDeadlineConnect, 1, base.Add(time.Second))
if got := r.nextWaitTimeout(base); got != time.Second { t.Fatalf("timeout=%v", got) }
r.runExpiredDeadlines(base.Add(time.Second))
if got := <-target1.fired; got != epollDeadlineConnect { t.Fatalf("kind=%v", got) }
```

Stale generation:

```go
target.gens[epollDeadlineWrite] = 2
r.scheduleDeadline(id, epollDeadlineWrite, 1, base)
r.runExpiredDeadlines(base)
select { case got := <-target.fired: t.Fatalf("stale fired %v", got); default: }
```

Also assert empty heap returns negative Wait timeout and expired live head returns zero.

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^TestEpollDeadline' -count=1
```

- [ ] **Step 3: Implement `container/heap` scheduler**

Do not remove old entries on update. Callers increment per-domain generation and push a new entry. Head cleanup discards entries when resource is absent, does not implement `epollDeadlineTarget`, or generation differs. Reactor loop order becomes inbox/control → expired deadlines → runnable → Wait(next live deadline).

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

**Produces exact private API:**

```go
type epollWorkerTask interface { runEpollWorkerTask() }

type epollCallbackExecutor struct { /* fixed workers/ring/reservation state */ }

func newEpollCallbackExecutor(workers, queue int, onCapacity func()) *epollCallbackExecutor
func (x *epollCallbackExecutor) tryReserve() bool
func (x *epollCallbackExecutor) submitReserved(epollWorkerTask)
func (x *epollCallbackExecutor) releaseReserved()
func (x *epollCallbackExecutor) reservedCount() int
func (x *epollCallbackExecutor) queuedCount() int
func (x *epollCallbackExecutor) stopIdle()
```

`submitReserved` is called only after one successful `tryReserve`; violating that invariant panics in package-private code so accounting corruption fails fast in tests.

- [ ] **Step 1: Write RED exact-bound tests**

Use:

```go
type blockingWorkerTask struct { entered chan struct{}; release <-chan struct{} }
func (t *blockingWorkerTask) runEpollWorkerTask() { close(t.entered); <-t.release }
```

For `workers=1, queue=1`:

```go
if !x.tryReserve() || !x.tryReserve() { t.Fatal("two reservations should fit") }
if x.tryReserve() { t.Fatal("third reservation exceeded exact bound") }

x.submitReserved(task1)
<-task1.entered
x.submitReserved(task2)
if got := x.queuedCount(); got != 1 { t.Fatalf("queued=%d", got) }
if got := x.reservedCount(); got != 2 { t.Fatalf("reserved=%d", got) }
```

Start `submitReserved(task2)` in the test goroutine and assert it returns before `task1.release` closes, proving a blocked user task cannot block submitter.

For capacity notification, reserve+submit one task, wait entered, release it, then receive exactly one notification from a buffered `capacity` channel. For an unsubmitted reservation, call `releaseReserved()` and assert the same notification/accounting behavior.

For `stopIdle`, after all releases and completions wait on worker-exit hook and assert `reservedCount()==0 && queuedCount()==0` before stop; a package-test guarded call while reserved>0 must panic rather than silently stop active work.

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^TestEpollCallbackExecutor' -count=1
```

- [ ] **Step 3: Implement direct idle-worker + fixed-ring scheduling**

State contains `workers`, `idle`, fixed `queue []epollWorkerTask`, ring head/size, `reserved`, and `limit=workers+queue`. Each worker owns one private one-slot channel.

Completion algorithm:

```text
run task outside executor mutex
lock
reserved-- for completed task
if fixed queue non-empty: pop next already-reserved task; same worker continues
else: mark worker idle
unlock
invoke onCapacity outside lock
```

`releaseReserved` decrements a reservation that never became a task. `stopIdle` is legal only at zero reservations and an empty ring, closes idle-worker channels, and waits for worker goroutines.

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

### Task 4: Real epoll Engine shell, worker-capacity wake, IDs, admission, Stats, final barrier

**Files:**
- Create: `transport/epoll_engine_linux.go`
- Create: `transport/epoll_engine_linux_test.go`
- Modify: `transport/epoll_reactor_inbox_linux.go`
- Modify: `transport/epoll_reactor_linux.go`
- Modify: `transport/epoll_constructor_linux.go`
- Modify: `transport/stats.go`
- Delete after GREEN: `transport/epoll_engine_phase6a_linux.go`

**Produces:** Engine startup/shutdown for an empty native runtime; TCP public methods still return `ErrProtocolUnsupported`.

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

- [ ] **Step 1: Write RED Engine boot/ID/barrier tests**

```go
e, err := NewEpoll(EpollConfig{Pollers: 3, CallbackWorkers: 2, CallbackQueue: 4})
if err != nil { t.Fatal(err) }
ne := e.(*epollEngine)
if len(ne.reactors) != 3 { t.Fatalf("reactors=%d", len(ne.reactors)) }
```

ID exhaustion test:

```go
ne.nextID.Store(math.MaxUint64 - 2)
id, err := ne.nextResourceID()
if err != nil || id != math.MaxUint64-1 { t.Fatalf("id=%d err=%v", id, err) }
if _, err := ne.nextResourceID(); !errors.Is(err, errNativeResourceIDExhausted) { t.Fatalf("err=%v", err) }
if got := ne.nextID.Load(); got != math.MaxUint64-1 { t.Fatalf("allocator wrapped: %d", got) }
```

Close empty engine, wait `Done`, and assert every reactor test-exit hook + worker exit hook fired. With a configured Observer that panics/blocks only when called, assert empty Close does not call it and `Stats()` reads the shared admission/observer owners.

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^Test(EpollEngine|NewEpollStarts)' -count=1
```

- [ ] **Step 3: Add generic worker-capacity wait list**

Extend node with reactor-only `workerBlocked bool`; reactor gets `workerBlocked []*epollInboxNode` and `hasWorkerBlocked atomic.Bool`. Add `epollControlWorkerCapacity`. `blockOnWorker(item)` deduplicates. Capacity control moves blocked nodes to runnable and clears the flag. Executor `onCapacity` asks Engine to signal only reactors whose `hasWorkerBlocked` is true.

- [ ] **Step 4: Implement Engine construction/finalizer**

Construction order:

```text
resolve options
create admission + observer
open all N Pollers/reactors; cleanup earlier Pollers + observer on failure
create worker executor with onCapacity=e.wakeWorkerWaiters
attach reactor onFatal=e.onReactorFatal
start reactor goroutines
start exactly one Engine finalizer goroutine
return
```

`onReactorFatal(err)` records first shutdown error, transitions to aborting if necessary, snapshots managed resources, publishes abort requests outside Engine mutex, and wakes all reactors. It must not silently let one reactor exit while owning resources.

`maybeQuiescentLocked` closes `quiescent` only when state is non-running, activeOps==0, managed empty, and admission idle. Finalizer waits quiescent, sends stop control, waits reactorWG, requires worker executor idle, stops workers, calls `observer.stop()` without waiting for Observer return, then closes Engine.Done.

- [ ] **Step 5: Implement helpers + immediate Close**

`nextResourceID` uses CAS:

```go
for {
    cur := e.nextID.Load()
    if cur >= math.MaxUint64-1 { return 0, errNativeResourceIDExhausted }
    if e.nextID.CompareAndSwap(cur, cur+1) { return cur+1, nil }
}
```

Add `beginOp/endOp`, `addManaged/removeManaged`, `selectReactor`, `wakeAll`, `wakeWorkerWaiters`. `Close` transitions to aborting once, calls `maybeQuiescentLocked`, snapshots managed values, unlocks, publishes `requestEngineAbort(abortExplicit)`, wakes reactors, returns without waiting.

- [ ] **Step 6: Share EngineStats formatting**

```go
func engineStatsSnapshot(admission *admissionController, observer *observerDispatcher) ogrenet.EngineStats
```

Portable `Engine.Stats` and epoll `Stats` both call this; admission remains authoritative.

- [ ] **Step 7: Run GREEN + portable regression**

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

### Task 5: Native listen socket, TCP fd helpers, bootstrap Session/setup task, accept/handoff

**Files:**
- Create: `transport/epoll_fd_linux.go`
- Create: `transport/epoll_session_linux.go`
- Create: `transport/epoll_session_task_linux.go`
- Create: `transport/epoll_listener_linux.go`
- Create: `transport/epoll_listener_linux_test.go`
- Create: `transport/epoll_native_test_helpers_linux.go`

**Produces:** private `listenNativeTCP`; `epollListener` implements `ogrenet.Listener`; public Engine `Listen` remains capability-gated until Task 9.

- [ ] **Step 1: Write RED address/socket-option tests**

Create a fake resolver:

```go
type testIPResolver struct { called atomic.Int32; addrs []net.IPAddr; err error }
func (r *testIPResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
    r.called.Add(1); return append([]net.IPAddr(nil), r.addrs...), r.err
}
```

Private address helpers:

```go
type nativeIPResolver interface {
    LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}
func resolveNativeListenTCP(context.Context, ogrenet.Endpoint, nativeIPResolver) (*net.TCPAddr, error)
func nativeTCPAddrToSockaddr(*net.TCPAddr) (unix.Sockaddr, int, error)
func nativeSockaddrToTCPAddr(unix.Sockaddr) (*net.TCPAddr, error)
func nativeSocketAddr(fd int, peer bool) (*net.TCPAddr, error)
func configureNativeTCP(fd int, cfg TCPConfig) error
```

Test IPv4/IPv6 sockaddr round-trip exactly by comparing IP/port. For literal `127.0.0.1`, resolver `called` remains zero. For configured TCP options, create a loopback TCP fd, apply `configureNativeTCP`, and assert `TCP_NODELAY`, `SO_KEEPALIVE`, `SO_RCVBUF`, `SO_SNDBUF` using `unix.GetsockoptInt` (buffer sizes may be kernel-scaled; assert configured nonzero lower-bound semantics rather than exact equality).

- [ ] **Step 2: Run RED, implement helpers, run GREEN**

```bash
go test ./transport -run '^TestNativeTCP(Sockaddr|Config|ResolveListen)' -count=1
```

- [ ] **Step 3: Create bootstrap Session + codec setup task under RED worker-isolation tests**

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

    state    epollSessionState
    endpoint ogrenet.Endpoint
    local    *net.TCPAddr
    remote   *net.TCPAddr
    handler  ogrenet.Handler
    lease    *connectionLease
    parent   *epollListener

    framer      wire.Framer
    wireFramer  bool
    setupMu     sync.Mutex
    setupDone   bool
    setupErr    error
    setupFramer wire.Framer

    done chan struct{}
}
```

`epollCodecSetupTask` calls `cfg.newFramer()` on a worker, stores result under `setupMu`, then signals the Session reactor.

Worker-isolation test recipe:

```text
FramerFactory closes factoryEntered then waits factoryRelease.
Create accepted bootstrap Session on reactor 0 and let setup task enter factory.
Signal an independent synthetic inbox item on reactor 0.
Require syntheticProcessed before closing factoryRelease.
```

Capacity-retry recipe: exhaust executor reservation with blocking tasks, place Session in codec-setup-needed state, call reactor handler once, verify it enters workerBlocked; release one worker slot; capacity control requeues Session and codec setup starts with no second Session signal.

- [ ] **Step 4: Write RED native Listener creation/API/handoff tests**

Exact scenarios:

```text
Listen creation: call private listenNativeTCP(ctx, tcp://127.0.0.1:0); result has nonzero ID, bound port, one fixed reactor; that reactor registry contains listener ID.
Context lifetime: after successful Listen, cancel ctx; wait Listener.Done; fd absent from reactor registry and Err nil.
Public Listener API compile assertion: var _ ogrenet.Listener = (*epollListener)(nil).
Handoff: Pollers=2, raw net.Dial listener endpoint; wait Observer EventAccept; event.ResourceID != 0, ParentID == listener Stats.ResourceID; locate managed Session and assert fixed target reactor differs according to deterministic round-robin seed.
Admission reject: MaxConnectionsPerListener=1; hold first accepted Session; second raw connect is rejected; ListenerStats.RejectedConnections==1; CurrentConnections stays 1; Engine opening count returns to 0.
Setup failure: FramerFactory returns nil/error; accepted fd disappears, lease/current gauge returns zero, no EventAccept, no Handler callback.
```

- [ ] **Step 5: Implement reactor-owned listen socket + Listener lifecycle**

`listenNativeTCP` does caller-side `resolveNativeListenTCP`, allocates ID/selects reactor, creates a listener request object with buffered result, adds managed resource, signals target, and waits result or ctx.

Selected reactor performs:

```text
unix.Socket(family, SOCK_STREAM|SOCK_NONBLOCK|SOCK_CLOEXEC, IPPROTO_TCP)
SO_REUSEADDR
Bind
Listen(SOMAXCONN)
Getsockname -> bound endpoint
Poller.Add(fd, Readable|Error|EdgeTriggered, listenerID)
return Listener result
```

If ctx wins before result, caller publishes cancel state + signals reactor; caller never closes the fd. Once returned, `context.AfterFunc(ctx, func(){ listener.requestClose(); reactor.signal(listener) })` preserves lifetime; Listener finalization stops that function.

`epollListener` implements stored-address `Endpoint/Addr/Stats/Done/Err/Close`. `Close` publishes closeReq + signals reactor only. Reactor removes registration, closes fd, freezes listener age, emits Listener Close after stable Err/stats, closes Listener.Done, removes managed resource.

- [ ] **Step 6: Implement accept + exact-once handoff**

Accept reactor uses `unix.Accept4(...SOCK_NONBLOCK|SOCK_CLOEXEC)` under `IOBudgetOps`; EINTR retries, EAGAIN ends turn, budget exhaustion requeues listener locally.

Per accepted fd:

```text
listener reactor is logical handoff owner
capture local/remote sockaddr
acquire opening lease with listenerCapacity
configureNativeTCP
allocate ID + choose immutable target reactor
create bootstrap Session/node and engine.addManaged
publish handoff to target intrusive inbox
```

Target performs `Poller.Add(fd, Readable|PeerClosed|Error|EdgeTriggered, sessionID)`. Successful Add is the exact ownership-transfer point. On failure, target executes delegated handoff cleanup exactly once: close fd, release opening lease, remove managed Session. No Accept event/callback.

After registration reserve worker setup. Setup success activates lease, increments listener Accepted, emits `EventAccept` with Session ID + Listener ParentID, state=`epollSessionOpening`. Setup failure unregisters/closes/releases/removes without Accept/Handler.

- [ ] **Step 7: Run GREEN + race**

```bash
go test ./transport -run '^TestEpoll(Native(Listen|Accept)|Codec)' -count=1
go test -race ./transport -run '^TestEpoll(Native(Listen|Accept)|Codec)' -count=20
```

- [ ] **Step 8: Commit**

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

**Produces:** private `dialNativeTCP`; public Engine `Dial` remains gated until Task 9.

**Resolver API:**

```go
func resolveNativeDialTCP(ctx context.Context, endpoint ogrenet.Endpoint, resolver nativeIPResolver) ([]*net.TCPAddr, error)
```

Production passes `net.DefaultResolver`; tests inject `testIPResolver` from Task 5.

- [ ] **Step 1: Write RED resolver/address-order tests**

```text
Literal 127.0.0.1: resolver.called remains 0 and one TCPAddr is returned.
Fake hostname resolver returns [127.0.0.2, 127.0.0.1]; resulting TCPAddr slice preserves that exact order and endpoint port on both entries.
Resolver error is returned for later existing DNS classifier mapping; caller context cause wins if ctx already canceled.
```

- [ ] **Step 2: Write RED connect/cancel/setup tests**

Use a loopback raw listener for success and a closed loopback port for refusal. Add a package-test hook `afterConnectRegistered` fired after EINPROGRESS Add but before SO_ERROR dispatch. Exact assertions:

```text
success => stored local/remote nonnil; no caller goroutine fd syscall; connection result arrives.
refused => errors.As(*transport.Error), Op==OpDial, Protocol TCP, raw errno reachable via errors.Is/As.
cancel after registration => caller returns the exact sentinel cause; hook records caller did not close fd; reactor later removes/closes it.
sequential addresses => first fake address fails, second loopback listener succeeds; attempt-order hook equals resolver order.
codec setup failure => direct config/framer error, no *transport.Error wrapping, fd/lease/managed resource removed.
```

- [ ] **Step 3: Run RED**

```bash
go test ./transport -run '^Test(EpollNativeDial|ResolveNativeDial)' -count=1
```

- [ ] **Step 4: Implement caller Dial state**

```text
beginOp
create one bounded Connect context before DNS
resolve ordered addresses
allocate tentative ID + immutable reactor
create connecting epollSession + engine.addManaged
signal reactor
wait buffered result OR bounded context
if context wins: publish cancel cause/state + signal reactor; caller never closes/mutates fd
endOp
```

Observer timing starts only if observer != nil; duration spans DNS + all connect attempts. Failed dial IDs are internal only; failure `EventConnect.ResourceID` is 0.

- [ ] **Step 5: Implement reactor address attempts**

For each address:

```text
Socket NONBLOCK|CLOEXEC on owning reactor
Connect
0 => connected
EINPROGRESS => Add Writable|Error|EdgeTriggered; schedule Connect generation/deadline
other errno => close, try next
on EPOLLOUT/ERR => SO_ERROR
0 => connected
errno => Del/close, try next
```

After connect: capture addresses, `configureNativeTCP`, acquire opening lease, schedule codec setup. Setup success activates lease, emits Connect success with stable Session ID, state Opening, sends buffered Dial result. Admission/setup failure closes/releases/removes and reports parity error. Final socket failure is classified once as `OpDial`; direct caller cancellation is never wrapped.

- [ ] **Step 6: Run GREEN + race**

```bash
go test ./transport -run '^Test(EpollNativeDial|ResolveNativeDial)' -count=1
go test -race ./transport -run '^TestEpollNativeDial' -count=20
```

- [ ] **Step 7: Commit**

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

**Produces:** outbound native TCP path tested against raw peers; public capability remains gated.

- [ ] **Step 1: Write RED admission/codec/partial-write tests**

Initialize private active sessions against raw TCP peers through Task 5/6 test helpers.

Codec contention:

```text
Acquire session codecSlot in test.
Call TrySend(valid message).
Assert errors.Is(err, ErrWouldBlock), errors.As(err,*Error), Error.Op==OpSend.
Assert SessionStats.Backpressure increments exactly once and one Backpressure Observer event arrives.
Release token.
```

Queue pressure:

```text
With WriteQueue=1, block physical progress on first frame, fill queue/current ownership, call one TrySend that cannot admit.
Assert exactly one backpressure counter/event for that attempt; no repeated increments while reactor remains blocked.
```

Cancellation after transfer:

```text
Use hook after queue ownership transfer, cancel caller ctx, expect exact ctx cause from Send.
Then allow peer reads; peer receives the complete admitted frame and TX Stats eventually commit once.
```

Partial write/EAGAIN:

```text
Shrink SO_SNDBUF, stop raw peer reads, enqueue large allowed frame.
At first partial/EAGAIN hook assert quota.current()>0 and frame slot held.
Signal independent item on same reactor and require it completes while peer still blocked.
Resume peer reads; after full frame assert quota/slot released and TX counters/event/ack occur once.
```

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^TestEpollNative(Send|TrySend|Partial|Write)' -count=1
```

- [ ] **Step 3: Add existing admission owners + codec token**

Session adds:

```go
queue      chan outbound
quota      *byteQuota
gate       *sendGate
frameSlots chan struct{}
codecSlot  chan struct{} // capacity 1; send=acquire, receive=release
stats      *sessionCounters

decodeWaiting atomic.Bool
```

Initialize quota parent to `engine.admission.bytes`, frameSlots to `writeQueue+1`, queue to `writeQueue`, codecSlot capacity 1, new session counters/gate. `Send`/`TrySend` copy portable validation→frame-slot→codec→encode(copy)→local/global quota→bounded queue ownership. `releaseCodec` signals reactor if a decoder published `decodeWaiting`.

- [ ] **Step 4: Implement reactor-only partial write state**

```go
writeCurrent outbound
writeOffset  int
writeActive  bool
writeBlocked bool
writeGen     uint64
```

`driveWrite`:

```text
nonblocking dequeue current
start one fixed Write deadline generation for this frame
unix.Write(frame[offset:])
n>0 => network activity touch, offset/budgets advance
complete => invalidate write deadline; release quota+slot; Stats TX; Observer Write; buffered ack; next
EAGAIN => enable Writable interest, return
EINTR => retry without consuming operation budget
fairness budget before complete => reactor.requeue(session)
other error => classify OpWrite; first terminal owner wins; fail current/pending exactly once
```

Disable Writable interest when no current/queued frame needs progress.

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

### Task 8: Session public lifecycle, reactor-only close/shutdown syscalls, half-close

**Files:**
- Create: `transport/epoll_session_lifecycle_linux.go`
- Create: `transport/epoll_session_lifecycle_linux_test.go`
- Modify: `transport/epoll_session_linux.go`
- Modify: `transport/epoll_session_send_linux.go`

**Produces:** `epollSession` satisfies `ogrenet.HalfCloseSession`; Engine TCP remains gated until Handler/read lifecycle exists.

- [ ] **Step 1: Write compile RED + stored-address Stats test**

Add:

```go
var _ ogrenet.Session = (*epollSession)(nil)
var _ ogrenet.HalfCloseSession = (*epollSession)(nil)
```

Create active private session, close raw peer fd externally only in test teardown, then call `LocalAddr/RemoteAddr/Stats`; assert these use stored addresses and never require a live fd. First compile run fails on missing methods.

- [ ] **Step 2: Write RED lifecycle ownership tests**

```text
Close syscall ownership: install reactor close hook; call Session.Close from test goroutine; hook records close executed on reactor path, not caller; Close returns after request publication.
CloseWrite: queue admitted frame, call CloseWrite, peer receives frame then EOF; WriteDone closes after SHUT_WR.
Shutdown: local write closes after drain but Shutdown remains blocked until raw peer sends FIN; then returns nil.
Peer FIN: raw peer CloseWrite; Session.ReadClosed closes, Err nil, subsequent local Send succeeds until local write closes.
First abort: race explicit Close and synthetic terminal failure at barriers; assert lifecycle winner Err/reason remains stable and derived fd-close errors cannot replace it.
```

Before Task 9 read/decode exists, peer-FIN probe uses `unix.Recvfrom(fd, peekBuf, MSG_PEEK|MSG_DONTWAIT)` only when EPOLLIN/RDHUP fires: n==0 means FIN; n>0 leaves data untouched.

- [ ] **Step 3: Implement public identity/Stats/error methods**

Add `ID/Protocol/Endpoint/LocalAddr/RemoteAddr/Stats/Done/Err/ReadClosed` using stored values. Session adds `life *sessionLifecycle`, logical `closing`, error lock, counters/age, close/final once fields. Stats performs no fd syscall.

- [ ] **Step 4: Implement caller lifecycle publication**

```text
CloseWrite => request GoalWrite; winner closes send gate; signal reactor; wait WriteDone/abort/ctx
Shutdown => request GoalFull; winner closes send gate; signal reactor; wait WriteDone, then Done/abort/ctx
Close => life.abortWith(AbortExplicit, publish nil Err); close logical closing once; close gate; signal reactor; return
owning ctx expiry => life.abortWith(AbortCaller,...); signal reactor; return exact caller cause
```

No caller invokes `unix.Close`, `Poller.Del`, or `unix.Shutdown`.

- [ ] **Step 5: Implement reactor lifecycle/final cleanup for pre-Handler sessions**

```text
abort => Del if registered; close fd; fail output/release quotas+slots; invalidate deadlines; mark read/write closed; freeze age; emit Close
write graceful => wait send gate drained + write machine empty; unix.Shutdown(SHUT_WR); MarkWriteClosed
peer FIN => MarkReadClosed without Err
both halves => TryMarkTerminal; Del/close; zero queue gauges; freeze age; emit Close once
```

Because Handler lifecycle is not active before Task 9, terminal cleanup now closes Session.Done, releases connection lease, and removes Engine managed resource directly. Task 9 replaces that final step with OnClose scheduling after Handler lifecycle starts.

- [ ] **Step 6: Run GREEN + race**

```bash
go test ./transport -run '^TestEpollNative(Close|Shutdown|PeerFIN|Lifecycle|Stored)' -count=1
go test -race ./transport -run '^TestEpollNative(Close|Shutdown|PeerFIN|Lifecycle)' -count=20
```

- [ ] **Step 7: Commit**

```bash
git add transport/epoll_session_lifecycle_linux.go transport/epoll_session_lifecycle_linux_test.go transport/epoll_session_linux.go transport/epoll_session_send_linux.go
git commit -m "runtime: add epoll TCP lifecycle"
```

---

### Task 9: OnOpen/read/decode/callback serialization, Message ownership, OnClose barrier, public TCP flip

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

- [ ] **Step 1: Write RED callback/order/ownership tests with explicit barriers**

OnOpen ordering:

```text
OnOpen closes openEntered and waits openRelease.
Peer writes a complete framed message while OnOpen is blocked.
Assert messageEntered is still open via nonblocking select.
Close openRelease; require messageEntered.
```

Message ownership:

```text
First OnMessage stores Message #1 and blocks.
After release, send enough later frames to force readPending append/compaction/reuse.
After all callbacks, assert stored Message #1 bytes exactly equal original.
```

Stats/Observer ordering:

```text
Inside OnMessage read Session.Stats and observer channel state.
Assert BytesRX/MessagesRX already include message and EventRead for same ResourceID/Bytes was observed before callback body proceeds.
```

Blocked Handler:

```text
Block Session A OnMessage.
Session B on same reactor sends/receives an echo and closes its completion channel before A is released.
```

Serialization:

```text
Use atomic inCallback CAS in Handler; send 100 messages; any CAS failure reports concurrent callback; final count==100.
```

Done barrier:

```text
OnClose closes closeEntered and waits closeRelease.
After terminal fd cleanup, assert Session.Done is still open.
Release; require Done closes afterward.
```

- [ ] **Step 2: Write RED worker-saturation/edge-retry test**

With `CallbackWorkers=1, CallbackQueue=1`, occupy one running + one queued Handler task. Make third Session readable and wait its reactor `workerBlocked` hook. Release one capacity slot. Require third message callback without any additional peer write or manual reactor signal.

- [ ] **Step 3: Run RED**

```bash
go test ./transport -run '^TestEpollNative(OnOpen|Decoded|StatsAndObserverRead|BlockedHandler|OneSession|DoneWaits|Callback)' -count=1
```

- [ ] **Step 4: Extend worker task lifecycle**

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

Task kinds are codec setup, OnOpen, OnMessage, OnClose. Exactly one Handler task per Session may be in flight. Completion stores one reactor-visible completion state, then signals owning reactor.

After codec setup/adoption, request OnOpen. Dial may return once setup/adoption succeeds while OnOpen is queued/running, matching portable asynchronous timing. Reactor consumes no application bytes until OnOpen completion.

- [ ] **Step 5: Implement non-blocking read/decode**

Session owns `readScratch` and `readPending`. Reserve worker capacity before work that can produce a callback. If unavailable, call `reactor.blockOnWorker(session)` without reading.

```text
try codec token; if unavailable set decodeWaiting and return
DecodeOne(existing pending)
NeedMore => unix.Read under byte/op budget; append bytes
complete => Validate; copy Data; compact pending; release codec token
            Stats RX -> Observer Read -> submit reserved OnMessage; pause Session
EAGAIN/NeedMore without complete message => release reservation + token
read 0 => release reservation + token; clean FIN lifecycle
invalid/decode error => release reservation + token; DecodeErrors++; typed OpRead terminal abort
budget hit before EAGAIN => release temporary reservation/token; reactor.requeue(session)
```

Callback completion always requeues Session to retry pending decode/read before sleeping, so no readiness edge can be lost.

- [ ] **Step 6: Replace pre-Handler finalizer with OnClose barrier**

Terminal order:

```text
stable Err + fd/registration cleanup + fail/release queues + invalidate deadlines + age freeze
Observer Close
wait until no OnOpen/OnMessage task can still produce a later callback
reserve/submit OnClose
OnClose returns
close Session.Done
release lease
engine.removeManaged
```

A blocked OnClose holds Engine.Done; observer dispatcher remains outside this barrier.

- [ ] **Step 7: Flip public TCP capability and run shared basic contract**

`epollEngine.Listen/Dial` use `beginOp/endOp`, validate endpoints exactly like portable, route TCP to native paths, keep TLS/WS/WSS unsupported and stream/UDP mismatch unchanged. Packet methods remain unsupported in 6B.

Native contract becomes:

```go
func TestEpollPublicTCPContracts(t *testing.T) {
    runEngineContracts(t, epollFactory(contractProfile{TCP: true}))
}
```

Run:

```bash
go test ./transport -run '^TestEnginePublicContracts|^TestEpollPublicTCPContracts' -count=1
```

Expected: portable TCP/UDP and native TCP basic echo/lifecycle/Stats pass.

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
- Modify native files only when a RED shared contract identifies a concrete missing seam

- [ ] **Step 1: Add shared graceful contract code**

Create helpers that receive `engineFactory` and execute the same public assertions for portable and epoll TCP:

```text
Half-close: server records complete payload then peer-side EOF; CloseWrite returns; client ReadClosed remains independent until peer FIN.
Full Shutdown: queued writes are observed by peer before FIN; peer FIN releases Shutdown; Err nil on clean path.
Peer FIN: ReadClosed closes, Err nil, one subsequent local Send reaches peer before local CloseWrite.
Owner precedence: first Shutdown ctx owns transition; a second canceled Shutdown returns its own ctx cause but does not abort first owner's drain; first owner timeout does abort remaining resource.
OnClose once and Done only after OnClose return in every case.
```

Portable subtests run first and must pass before enabling native assertions.

- [ ] **Step 2: Add shared limit/Stats code**

For each factory configure one limit at a time and use channels to hold resources at the ownership point being measured:

```text
MaxConnections=1: second Dial fails LimitConnections; Engine rejected++ and first remains active.
MaxConnectionsPerPeer=1: second same-loopback peer fails LimitConnectionsPerPeer.
MaxConnectionsPerListener=1: second inbound is rejected; Listener Rejected==1 Current==1 until first lease releases.
MaxQueuedBytesTotal: block peer reads, fill admitted bytes, one TrySend fails with ErrWouldBlock + LimitQueuedBytes; after drain GlobalQueuedBytes returns zero.
Stats: one payload each direction yields exact application Bytes/Messages, not framing bytes.
Finalization: QueuedFrames/QueuedBytes zero; Age read twice after Done is identical.
```

- [ ] **Step 3: Add shared Observer code**

Use an Observer that sends copied events to buffered channel and assert:

```text
Accept ResourceID==child Stats.ResourceID and ParentID==Listener ResourceID.
Connect success has stable Session ID; refused Connect event has ResourceID 0 and typed Err.
Read/Write Bytes equal application payload.
One pressure TrySend produces one Backpressure event.
Close occurs once and reading Stats/Err inside observer sees final stable values.
A blocking observer fills bounded queue while normal TCP echo continues; EngineStats.ObserverDroppedEvents grows.
A panic observer increments ObserverPanics and the Session remains usable.
```

- [ ] **Step 4: Add timeout/error code**

Synchronize connection/OnOpen first, then assert:

```text
ReadIdle: no network data until terminal *Error wrapping TimeoutReadIdle.
ConnectionIdle: no read/write progress until corresponding TimeoutError domain.
MaxLifetime: activity does not extend fixed lifetime.
Write timeout: tiny SO_SNDBUF + raw peer not reading; current frame remains owned until typed TimeoutWrite terminal failure, then quota zero.
Refused/reset: Op/Protocol/local/remote fields and raw errno remain reachable through errors.Is/As.
Caller cancellation: exact sentinel cause returned, not *transport.Error.
First terminal error: reset/write-timeout race keeps the first committed operational owner; derived close fallout never replaces it.
```

Internal Task 2/6 hooks provide deterministic Connect deadline/cancellation arbitration; external unroutable addresses are not the sole oracle.

- [ ] **Step 5: Fill only RED native seams**

Deadline/activity policy is exact:

```text
Connect deadline covers DNS + all sequential attempts.
Write deadline is fixed from current-frame start; partial write progress does not reset it.
ReadIdle starts after OnOpen returns, is invalidated while that Session Handler callback executes, and is re-armed after callback completion.
ConnectionIdle generation resets only on successful network read/write progress.
MaxLifetime generation never resets.
```

Stats/Observer order remains:

```text
RX decode+validate -> Stats -> Observer -> Handler
TX full physical write -> quota/slot release -> Stats -> Observer -> Send ack
Close stable Err + zero queues + age freeze -> Observer -> OnClose -> Done
```

Listener close path also freezes age/emits one Listener Close before Done; accepted/current/rejected counters retain Task 5/limit ownership semantics.

- [ ] **Step 6: Run parity + native 20x race**

```bash
go test ./transport -run '^Test.*(Contract|Parity|Graceful|Observer|Stats|Limit|Timeout|Error)' -count=1
go test -race ./transport -run '^TestEpoll.*(Parity|Graceful|Observer|Stats|Limit|Timeout|Error)' -count=20
```

- [ ] **Step 7: Commit exact changed files**

```bash
git diff --name-only
git add transport/contract_tcp_graceful_test.go transport/contract_tcp_limits_test.go transport/contract_tcp_observer_test.go transport/contract_tcp_timeout_error_test.go transport/epoll_tcp_parity_linux_test.go transport/contract_harness_test.go
# Add only the exact native .go paths shown by git diff that were changed to satisfy RED parity cases.
git diff --cached --name-only
git commit -m "runtime: align epoll TCP semantics"
```

Do not stage unrelated native files merely because their names share an `epoll_` prefix.

---

### Task 11: Graceful Engine Shutdown/Close and cross-reactor stress invariants

**Files:**
- Create: `transport/epoll_engine_shutdown_linux.go`
- Create: `transport/epoll_engine_shutdown_linux_test.go`
- Create: `transport/epoll_tcp_stress_linux_test.go`
- Modify: `transport/epoll_engine_linux.go`
- Modify concrete listener/session managed-resource methods if required by Engine shutdown requests

- [ ] **Step 1: Write RED Engine lifecycle tests with explicit barriers**

```text
Graceful order: hold one active Session; call Engine.Shutdown; Listener.Done must close/reject new Dial first; active lease Stats moves Active->Draining before Session write drain; release peer FIN; Shutdown returns nil and all gauges zero.
Owner cancellation: first Shutdown uses cancelable ctx and blocks in Session OnClose; cancel owner; remaining native resources receive AbortCaller and first call returns exact ctx cause. A concurrent non-owner cancellation must not abort owner's drain.
Close syscall ownership: call Engine.Close from test goroutine; reactor close hook records all fd closes on reactor paths; Close itself returns before blocked OnClose.
Blocked application OnClose: Engine.Done remains open until closeRelease.
Blocked Observer: observer blocks forever after one event; Engine.Close/Done still complete after application lifecycle finishes.
```

- [ ] **Step 2: Write deterministic cross-race cases**

Provide package hooks immediately before/after the ownership transition under test and coordinate both actors with channels:

```text
accepted fd published vs Engine Shutdown
connect registered vs caller cancellation
Handler completion signal vs Session abort
reactor armWait vs Engine Close wake
peer FIN commit vs CloseWrite request
write deadline firing vs reset event
accept flood turn vs Listener.Close request
```

Each test asserts one fd close, one lease release, stable first error/lifecycle reason, and no blocked goroutine after its barriers release.

- [ ] **Step 3: Implement graceful Engine owner model**

Concrete `requestEngineShutdown` is non-blocking:

```text
Listener: publish close request + reactor signal.
Active Session: call lease.beginDrain() first, then publish internal GoalFull + close send gate + reactor signal; do not call blocking public Shutdown.
Connecting/Handoff/CodecSetup Session: publish abort because no established protocol exists to drain.
Opening Session whose OnOpen is queued/running: treat as active/adopted; beginDrain then GoalFull, and Handler lifecycle still closes in order.
requestEngineAbort: publish abort state + signal only; no caller fd syscall.
```

Engine `Shutdown` mirrors portable ownership:

```text
first running caller => state Draining; snapshot managed; call maybeQuiescentLocked; unlock
publish resource shutdown requests; wake all
wait Done
if owner ctx wins => begin abort with AbortCaller; publish aborts; wake; return exact cause
non-owner caller waits Done or its own ctx and never steals abort ownership
```

`Close` stays immediate idempotent abort. `shutdownResult` uses portable `ErrClosed` precedence.

- [ ] **Step 4: Add test-only invariant snapshot**

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

Snapshot acquires each required internal lock in one documented order and is called only from tests after synchronization. After Engine.Done assert every structural count is 0 and Admission Opening/Active/Draining/GlobalQueuedBytes are 0.

- [ ] **Step 5: Add multi-reactor short-lived stress**

Use `Pollers:4`, 2000 loopback Sessions, one message/echo each, alternating client/server close side. WaitGroups count accepted/open/message/close completion; after all callers finish, Engine.Close then Done, invariant snapshot all zero. Run 10 times without sleeps.

- [ ] **Step 6: Run GREEN**

```bash
go test -race ./transport -run '^TestEpoll(Engine|Native).*' -count=20
go test ./transport -run '^TestEpollNativeShortLivedConnections' -count=10
```

- [ ] **Step 7: Commit**

```bash
git diff --name-only
git add transport/epoll_engine_shutdown_linux.go transport/epoll_engine_shutdown_linux_test.go transport/epoll_tcp_stress_linux_test.go transport/epoll_engine_linux.go
# Add exact listener/session paths only if Step 3 changed their managed-resource methods.
git diff --cached --name-only
git commit -m "runtime: harden epoll engine shutdown"
```

---

### Task 12: TCP backend benchmarks, permanent CI gates, exact-head verification, 6B checkpoint

**Files:**
- Create: `transport/epoll_tcp_benchmark_test.go`
- Modify: `.github/workflows/netpoll-v2.yml`

- [ ] **Step 1: Add identical portable/native TCP benchmarks**

Benchmark factory receives payload size and backend factory. For each backend, create loopback echo once per sub-benchmark, reset timer after setup, then run N Send/echo round trips. Required sub-benchmarks:

```text
BenchmarkTCPBackend/portable/1KiB
BenchmarkTCPBackend/epoll/1KiB
BenchmarkTCPBackend/portable/4KiB
BenchmarkTCPBackend/epoll/4KiB
BenchmarkTCPBackend/portable/64KiB
BenchmarkTCPBackend/epoll/64KiB
BenchmarkTCPBackend/portable/connect
BenchmarkTCPBackend/epoll/connect
BenchmarkEpollSend
BenchmarkEpollTrySend
BenchmarkEpollEngineShutdownFanout
```

Latency harness records per-iteration durations into a preallocated slice and reports p50/p95/p99 using `b.ReportMetric`; it is report-only, not a CI speed threshold.

- [ ] **Step 2: Add deterministic hard benchmark assertions only**

Retain existing portable allocation gates. Add benchmarks for native Stats snapshot and disabled-observer fast branch; parse benchmark output in CI only for deterministic allocation invariants. After each benchmark teardown, inspect package-private invariant snapshot and fail the benchmark if quota/lease/task/resource counts are nonzero.

- [ ] **Step 3: Extend Linux Go 1.26 CI**

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

Do not alter existing Windows/macOS/FreeBSD/GmSSL/cross-compile jobs or hard-gate native-vs-portable ns/op ratios.

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

Record exactly:

```text
P1-6B Linux TCP complete.
P1-6C UDP remains outstanding.
TLS/WS/WSS remain explicit unsupported with no fallback.
PR remains Draft because #57 still requires UDP and 6D productionization evidence.
```

Do not mark PR Ready and do not close #57, #56, or #38 at the 6B checkpoint.
