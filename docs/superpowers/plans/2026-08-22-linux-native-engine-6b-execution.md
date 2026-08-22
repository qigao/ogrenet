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
- `epoll.Event.Data` is one Engine-local monotonic ID. ID 0 and `math.MaxUint64` are forbidden. Allocation uses CAS and permanently fails at `math.MaxUint64-1`; it never wraps/reuses low IDs under concurrency.
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
- Engine graceful shutdown first calls `prepareEngineDrain` on every Session resource, then publishes listener closes, then Session graceful/abort requests. Active leases therefore become draining before any Session graceful request becomes observable.
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

Execute these scenarios:

```go
for i := 0; i < 100; i++ { r.signal(item) }
r.drainInbox()
if got := item.calls.Load(); got != 1 { t.Fatalf("calls=%d", got) }

// Real r.run publishes waitArmed immediately after armWait commits waiting=true.
// After <-waitArmed, signal one item. No other fd event is injected.
select { case <-processed: case <-ctx.Done(): t.Fatal(ctx.Err()) }

// After another <-waitArmed, signalControl(stop). Reactor must exit without another event.
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

`drainInbox` detaches the list under `inboxMu`, clears each `queued` flag before invoking `onReactorInbox`, and invokes resource code with no inbox lock held. `armWait` checks inbox/control/runnable under `inboxMu`; only an empty reactor sets `waiting=true,wakePending=false`. `disarmWait` clears both. `signalControl(mask)` CAS-ORs bits then performs the same wake handshake without an inbox allocation.

- [ ] **Step 4: Write RED registry/runnable tests**

Use a synthetic `epollEventResource` with fixed ID/fd and counters:

```go
r.dispatch(epoll.Event{Data: 42, Events: epoll.Readable})
if got := res.events.Load(); got != 0 { t.Fatalf("stale event dispatched: %d", got) }

if err := r.registerResource(a); err != nil { t.Fatal(err) }
if err := r.registerResource(b); err == nil { t.Fatal("duplicate ID replaced owner") }

// onReactorRunnable increments runs and requeues itself until runs==3.
r.requeue(res)
r.drainRunnable()
if got := res.runs.Load(); got != 3 { t.Fatalf("runs=%d", got) }
```

- [ ] **Step 5: Implement registry/runnable/event loop**

Registry mutates only on reactor. `requeue` appends once through `runnableQueued`. Loop order:

```text
drain inbox
drain control
drain runnable
if stop requested AND resources/inbox/runnable empty => return
arm Wait
Poller.Wait
clear Wait state
dispatch only IDs still in registry
```

Unexpected Wait error calls `onFatal` outside internal locks.

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
    at time.Time
    resourceID uint64
    kind epollDeadlineKind
    generation uint64
}
```

Deadline dispatch looks up `resources[id]`, then type-asserts `epollDeadlineTarget`; Task 1's resource interface stays unchanged.

- [ ] **Step 1: Write RED heap/stale tests**

```go
base := time.Unix(100, 0)
r.scheduleDeadline(2, epollDeadlineWrite, 1, base.Add(2*time.Second))
r.scheduleDeadline(1, epollDeadlineConnect, 1, base.Add(time.Second))
if got := r.nextWaitTimeout(base); got != time.Second { t.Fatalf("timeout=%v", got) }
r.runExpiredDeadlines(base.Add(time.Second))
if got := <-target1.fired; got != epollDeadlineConnect { t.Fatalf("kind=%v", got) }

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

- [ ] **Step 3: Implement heap + reactor integration**

Use `container/heap`. Never remove old entries on update; resource generations invalidate them. Head cleanup drops absent resources/non-targets/generation mismatches. Loop becomes inbox/control → expired deadlines → runnable → Wait(next live deadline).

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

### Task 3: Exact-bounded setup/callback executor

**Files:**
- Create: `transport/epoll_callback_linux.go`
- Create: `transport/epoll_callback_linux_test.go`

**Exact private API/state:**

```go
type epollWorkerTask interface { runEpollWorkerTask() }
type epollCallbackWorker struct { tasks chan epollWorkerTask }
type epollCallbackExecutor struct {
    mu sync.Mutex
    workers []*epollCallbackWorker
    idle []*epollCallbackWorker
    queue []epollWorkerTask
    head int
    size int
    reserved int
    limit int
    stopped bool
    onCapacity func()
    wg sync.WaitGroup
}
func newEpollCallbackExecutor(workers, queue int, onCapacity func()) *epollCallbackExecutor
func (x *epollCallbackExecutor) tryReserve() bool
func (x *epollCallbackExecutor) submitReserved(epollWorkerTask)
func (x *epollCallbackExecutor) releaseReserved()
func (x *epollCallbackExecutor) reservedCount() int
func (x *epollCallbackExecutor) queuedCount() int
func (x *epollCallbackExecutor) stopIdle()
```

`queue` is allocated once at exact `CallbackQueue` length and used as a ring. `limit=CallbackWorkers+CallbackQueue`. `submitReserved` without a prior successful reservation panics.

- [ ] **Step 1: Write RED exact-bound tests**

```go
type blockingWorkerTask struct { entered chan struct{}; release <-chan struct{} }
func (t *blockingWorkerTask) runEpollWorkerTask() { close(t.entered); <-t.release }

if !x.tryReserve() || !x.tryReserve() { t.Fatal("two reservations should fit") }
if x.tryReserve() { t.Fatal("third reservation exceeded bound") }
x.submitReserved(task1)
<-task1.entered
x.submitReserved(task2)
if got := x.queuedCount(); got != 1 { t.Fatalf("queued=%d", got) }
if got := x.reservedCount(); got != 2 { t.Fatalf("reserved=%d", got) }
```

For capacity notification, release task1 and require one buffered notification. For an unsubmitted reservation, `releaseReserved` decrements accounting and notifies. `submitReserved(task2)` must return while task1 is blocked. Calling `stopIdle` while reserved>0 must panic in package tests; at zero reservations it closes idle workers and waits `wg`.

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^TestEpollCallbackExecutor' -count=1
```

- [ ] **Step 3: Implement worker scheduling**

Completion:

```text
run task outside executor mutex
lock
reserved--
if ring nonempty: pop already-reserved next task; same worker continues
else: append worker to idle
unlock
onCapacity outside lock
```

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

### Task 4: Real epoll Engine shell, worker wake, IDs, admission, Stats, final barrier

**Files:**
- Create: `transport/epoll_engine_linux.go`
- Create: `transport/epoll_engine_linux_test.go`
- Modify: reactor inbox/reactor/constructor/stats files
- Delete after GREEN: `transport/epoll_engine_phase6a_linux.go`

**Generic boundary before concrete resource types exist:**

```go
type epollManagedKind uint8
const (
    epollManagedListener epollManagedKind = iota + 1
    epollManagedSession
)
type epollManagedResource interface {
    managedID() uint64
    managedKind() epollManagedKind
    prepareEngineDrain()
    requestEngineShutdown()
    requestEngineAbort(abortReason)
}
type epollEngine struct {
    cfg config
    epollCfg resolvedEpollConfig
    admission *admissionController
    observer *observerDispatcher
    callbacks *epollCallbackExecutor
    reactors []*epollReactor
    mu sync.Mutex
    state engineState
    shutdownReason abortReason
    shutdownErr error
    activeOps int
    managed map[uint64]epollManagedResource
    nextReactor atomic.Uint64
    nextID atomic.Uint64
    quiescent chan struct{}
    quiescentOnce sync.Once
    reactorWG sync.WaitGroup
    done chan struct{}
    doneOnce sync.Once
}
```

- [ ] **Step 1: Write RED Engine boot/ID/barrier tests**

```go
e, err := NewEpoll(EpollConfig{Pollers:3, CallbackWorkers:2, CallbackQueue:4})
if err != nil { t.Fatal(err) }
ne := e.(*epollEngine)
if len(ne.reactors) != 3 { t.Fatalf("reactors=%d", len(ne.reactors)) }
ne.nextID.Store(math.MaxUint64-2)
id, err := ne.nextResourceID()
if err != nil || id != math.MaxUint64-1 { t.Fatalf("id=%d err=%v", id, err) }
if _, err := ne.nextResourceID(); !errors.Is(err, errNativeResourceIDExhausted) { t.Fatalf("err=%v", err) }
if got := ne.nextID.Load(); got != math.MaxUint64-1 { t.Fatalf("wrapped=%d", got) }
```

Close empty Engine; wait Done; require reactor/worker exit hooks. Assert `Stats` uses admission/observer owners and empty Close never invokes Observer callback.

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^Test(EpollEngine|NewEpollStarts)' -count=1
```

- [ ] **Step 3: Add worker-capacity wait list**

Node gains reactor-only `workerBlocked`; reactor gains fixed/dynamic list plus `hasWorkerBlocked atomic.Bool`; add `epollControlWorkerCapacity`. `blockOnWorker` deduplicates. Capacity control moves entries to runnable. Executor callback wakes only reactors whose flag is set.

- [ ] **Step 4: Implement Engine construction/finalizer/fatal path**

```text
resolve options
create admission+observer
open all Pollers/reactors; cleanup earlier Pollers+observer on failure
create executor with onCapacity=e.wakeWorkerWaiters
attach onFatal=e.onReactorFatal
start reactors
start one Engine finalizer
```

`onReactorFatal` records shutdown error, transitions to aborting, snapshots managed resources, unlocks, publishes abort requests, wakes all. `maybeQuiescentLocked` requires non-running + activeOps 0 + managed empty + admission idle. Finalizer waits quiescent, stops reactors, waits reactorWG, requires executor idle, stops workers, calls observer.stop without waiting for Observer, closes Done.

- [ ] **Step 5: Implement helpers and CAS ID allocator**

```go
for {
    cur := e.nextID.Load()
    if cur >= math.MaxUint64-1 { return 0, errNativeResourceIDExhausted }
    if e.nextID.CompareAndSwap(cur, cur+1) { return cur+1, nil }
}
```

Add `beginOp/endOp`, `addManaged/removeManaged`, `selectReactor`, `wakeAll`, `wakeWorkerWaiters`. `Close` transitions aborting once, calls `maybeQuiescentLocked`, snapshots managed, unlocks, publishes `requestEngineAbort(abortExplicit)`, wakes reactors, returns.

- [ ] **Step 6: Share EngineStats formatting**

```go
func engineStatsSnapshot(admission *admissionController, observer *observerDispatcher) ogrenet.EngineStats
```

Portable and native call it; admission remains authoritative.

- [ ] **Step 7: Run GREEN + portable regression**

```bash
go test ./transport -run '^Test(EpollEngine|NewEpollStarts|EngineStats)' -count=1
go test -race ./transport -run '^Test(EpollEngine|NewEpollStarts)' -count=20
go test ./... -count=1
```

- [ ] **Step 8: Remove 6A scaffold + commit**

```bash
git rm transport/epoll_engine_phase6a_linux.go
git add transport/epoll_engine_linux.go transport/epoll_engine_linux_test.go transport/epoll_reactor_inbox_linux.go transport/epoll_reactor_linux.go transport/epoll_constructor_linux.go transport/stats.go
git commit -m "runtime: boot epoll engine reactors"
```

---

### Task 5: Native listen socket, fd helpers, bootstrap Session/setup, accept/handoff

**Files:** create `epoll_fd_linux.go`, `epoll_session_linux.go`, `epoll_session_task_linux.go`, `epoll_listener_linux.go`, tests/helpers.

**Produces:** private `listenNativeTCP`; Listener satisfies `ogrenet.Listener`; public Engine Listen stays gated until Task 9.

- [ ] **Step 1: Write RED address/socket option tests**

```go
type nativeIPResolver interface { LookupIPAddr(context.Context,string) ([]net.IPAddr,error) }
type testIPResolver struct { called atomic.Int32; addrs []net.IPAddr; err error }
func (r *testIPResolver) LookupIPAddr(context.Context,string) ([]net.IPAddr,error) {
    r.called.Add(1); return append([]net.IPAddr(nil), r.addrs...), r.err
}
func resolveNativeListenTCP(context.Context, ogrenet.Endpoint, nativeIPResolver) (*net.TCPAddr,error)
func nativeTCPAddrToSockaddr(*net.TCPAddr) (unix.Sockaddr,int,error)
func nativeSockaddrToTCPAddr(unix.Sockaddr) (*net.TCPAddr,error)
func nativeSocketAddr(fd int, peer bool) (*net.TCPAddr,error)
func configureNativeTCP(fd int, cfg TCPConfig) error
```

Assert v4/v6 round-trip IP/port; literal address does not call resolver; configured TCP_NODELAY/KEEPALIVE/buffers observable via getsockopt (kernel-scaled buffers checked by lower-bound, not exact equality).

- [ ] **Step 2: Run RED/implement helpers/GREEN**

```bash
go test ./transport -run '^TestNativeTCP(Sockaddr|Config|ResolveListen)' -count=1
```

- [ ] **Step 3: Create bootstrap Session + setup task with nonblocking engine-abort contract**

```go
type epollSessionState uint8
const (
    epollSessionHandoff epollSessionState = iota+1
    epollSessionConnecting
    epollSessionCodecSetup
    epollSessionOpening
    epollSessionActive
    epollSessionTerminal
    epollSessionClosed
)
type epollSession struct {
    engine *epollEngine
    reactor *epollReactor
    node epollInboxNode
    id uint64
    fd int
    state epollSessionState
    endpoint ogrenet.Endpoint
    local, remote *net.TCPAddr
    handler ogrenet.Handler
    lease *connectionLease
    parent *epollListener
    framer wire.Framer
    wireFramer bool
    setupMu sync.Mutex
    setupDone bool
    setupErr error
    setupFramer wire.Framer
    engineAbort atomic.Uint32 // abortReason publication before Task 8 lifecycle wrapper exists
    done chan struct{}
}
```

Bootstrap Session already implements `epollManagedResource`: kind Session; `prepareEngineDrain` calls `lease.beginDrain()` when lease nonnil; `requestEngineAbort` CAS-publishes first reason then signals reactor; `requestEngineShutdown` signals reactor and pre-Task11 reactor logic treats non-active setup states as abort. Task 8 moves established Session requests onto `sessionLifecycle`; Task 11 completes graceful Engine semantics.

`epollCodecSetupTask` executes `cfg.newFramer()` on worker, stores result, signals reactor. Test blocks FramerFactory, then requires an independent item on same reactor to process before factory release. Exhaust executor reservations, verify Session enters workerBlocked, release one capacity slot, require setup starts without a second Session signal.

- [ ] **Step 4: Write RED Listener creation/API/handoff cases**

```text
private Listen on 127.0.0.1:0 => nonzero ID/bound port/fixed reactor; registry contains ID.
var _ ogrenet.Listener = (*epollListener)(nil).
Cancel Listen ctx after success => Done closes, Err nil, registry entry removed.
Pollers=2 raw Dial => EventAccept ResourceID nonzero, ParentID listener ID; managed Session target matches deterministic round-robin selection.
MaxConnectionsPerListener=1 => second inbound reject increments Listener rejected once, Current stays 1, opening gauge returns zero.
FramerFactory failure => no Accept event/Handler, fd removed, lease/current gauge zero.
```

- [ ] **Step 5: Implement reactor-owned listen + Listener lifecycle**

Caller resolves address, allocates ID/selects reactor, creates request/managed listener, signals reactor, waits buffered result or ctx. Reactor does Socket NONBLOCK|CLOEXEC, SO_REUSEADDR, Bind, Listen, Getsockname, Poller.Add(Readable|Error|EdgeTriggered). Context-winning caller only publishes cancel+signal. After successful return use `context.AfterFunc` to publish later close; stop it on finalization.

Listener stored-address API: Endpoint/Addr/Stats/Done/Err/Close. `prepareEngineDrain` no-op; kind Listener; engine shutdown/abort publish close+signal. Reactor Del/close, freezes age, emits Listener Close after stable state, closes Done, removes managed.

- [ ] **Step 6: Implement accept + exact-once handoff**

Accept4 under op budget; EINTR retry; EAGAIN return; budget yield local requeue. For accepted fd: listener reactor logical owner → addresses → opening lease/listener capacity → TCP options → ID/target → bootstrap Session/addManaged → signal target. Target `Poller.Add(Readable|PeerClosed|Error|EdgeTriggered)` is ownership transfer. Failed Add performs delegated one-time close/lease release/remove. Successful registration reserves worker setup; setup success activates lease, listener Accepted++, EventAccept(SessionID,ParentID), state Opening; setup failure unregister/close/release/remove with no Accept/Handler.

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

### Task 6: Caller DNS + reactor-owned non-blocking Dial/connect

**Files:** create `epoll_dial_linux.go`, tests; modify Session/setup task.

```go
func resolveNativeDialTCP(ctx context.Context, endpoint ogrenet.Endpoint, resolver nativeIPResolver) ([]*net.TCPAddr,error)
```

Production uses `net.DefaultResolver`; tests inject Task 5 resolver.

- [ ] **Step 1: Write RED resolver/order tests**

Literal IP keeps resolver.called==0. Fake resolver [127.0.0.2,127.0.0.1] preserves exact order and endpoint port. Resolver error with live ctx is mapped by native Dial to existing typed OpDial DNS classification; if caller ctx already has a cause, that exact cause wins.

- [ ] **Step 2: Write RED connect/cancel/setup tests**

Use loopback raw listener, closed loopback port, and `afterConnectRegistered` hook. Assert success stored addresses; refused gives `*Error{Op:OpDial,Protocol:TCP}` with raw errno reachable; cancellation after registration returns exact cause and caller never closes fd; ordered first-fail/second-success follows resolver order; codec setup failure is direct config error and leaves no fd/lease/managed resource.

- [ ] **Step 3: Run RED**

```bash
go test ./transport -run '^Test(EpollNativeDial|ResolveNativeDial)' -count=1
```

- [ ] **Step 4: Implement Dial caller flow**

```text
beginOp
create one bounded Connect context before DNS
start observer clock only when observer enabled
resolve ordered addresses
on resolver failure: caller cause direct else classify OpDial; emit failed Connect ID0 if observer
allocate tentative ID + immutable reactor
create connecting managed Session
signal reactor
wait buffered result OR bounded context
ctx winner => publish cause + signal; caller never closes/mutates fd
endOp
```

- [ ] **Step 5: Implement reactor attempts + Connect event semantics**

Each address: Socket on reactor → Connect → immediate success or EINPROGRESS Add Writable|Error|EdgeTriggered + Connect deadline → SO_ERROR → next address on failure. After transport connect capture addresses/configure TCP/acquire opening lease/schedule codec setup.

Connect Observer semantics match portable:

```text
all transport attempts fail => EventConnect Err typed, ResourceID 0
TCP connected but admission/setup later fails => EventConnect success Err nil, ResourceID 0, then return admission/config error
full adoption succeeds => EventConnect success Err nil with stable Session ID
```

Setup success activates lease/state Opening/result. Final socket error classified once OpDial; direct caller cancellation never wrapped.

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

### Task 7: Send/TrySend, codec token, partial write, fixed write deadline

**Files:** create send file/tests; modify Session.

- [ ] **Step 1: Write RED ownership tests**

```text
codec contention: hold codecSlot; TrySend => errors.Is ErrWouldBlock + typed OpSend; backpressure Stats/event exactly once.
queue pressure: WriteQueue=1 and blocked peer; one non-admitted TrySend => exactly one backpressure increment/event.
cancel after transfer: hook after queue ownership, cancel ctx => exact cause; later peer receives complete admitted frame and TX commits once.
partial/EAGAIN: tiny SO_SNDBUF + blocked peer; at partial hook quota/slot still held; independent same-reactor item completes while blocked; resume peer => quota/slot release + Stats->Observer->ack once.
```

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^TestEpollNative(Send|TrySend|Partial|Write)' -count=1
```

- [ ] **Step 3: Add existing admission owners + codec token**

```go
queue chan outbound
quota *byteQuota
gate *sendGate
frameSlots chan struct{}
codecSlot chan struct{}
stats *sessionCounters
decodeWaiting atomic.Bool
```

Initialize same sizes/parent quota as portable. Send/TrySend ordering is validation→frame slot→codec token→encode(copy)→quota→bounded queue. releaseCodec signals reactor when decoder published waiting.

- [ ] **Step 4: Implement reactor-only partial write**

```go
writeCurrent outbound
writeOffset int
writeActive bool
writeBlocked bool
writeGen uint64
```

Drive: dequeue → fixed write deadline → unix.Write; progress touches activity/budget; complete invalidates deadline, releases quota/slot, Stats TX, Observer Write, ack; EAGAIN enables Writable; EINTR retry; budget yield requeue; other error typed OpWrite first-terminal cleanup. Disable Writable when drained.

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

### Task 8: Session public lifecycle, reactor-only close/shutdown, half-close

**Files:** create lifecycle file/tests; modify Session/send.

- [ ] **Step 1: Compile RED + stored-address test**

```go
var _ ogrenet.Session = (*epollSession)(nil)
var _ ogrenet.HalfCloseSession = (*epollSession)(nil)
```

Private active Session must return stored Local/Remote/Stats without fd syscall.

- [ ] **Step 2: RED lifecycle cases**

```text
Close publishes request; reactor hook proves physical close on reactor, not caller.
CloseWrite drains admitted frame then peer observes EOF.
Shutdown waits after local FIN until peer FIN.
Peer FIN closes ReadClosed with Err nil and local write remains usable.
Explicit Close vs synthetic terminal error barrier preserves first lifecycle/error owner; derived close errors cannot replace it.
```

Before read path, FIN detection on EPOLLIN/RDHUP uses non-consuming MSG_PEEK|DONTWAIT; n>0 is left untouched.

- [ ] **Step 3: Implement public fields/methods + lifecycle**

Add stored identity/addresses, Stats/Done/Err/ReadClosed, `life`, logical closing/error locks/once fields. Replace bootstrap engineAbort handling for established Sessions with sessionLifecycle while retaining pre-establishment abort publication.

- [ ] **Step 4: Caller publication**

CloseWrite GoalWrite, Shutdown GoalFull, Close AbortExplicit, owner ctx expiry AbortCaller; close send gate as portable; signal reactor only. No caller Close/Del/Shutdown syscall.

- [ ] **Step 5: Reactor lifecycle + pre-Handler finalizer**

Abort: Del/close/fail+release/invalidate/halves closed/freeze/EventClose. Graceful write waits gate+writer drain then SHUT_WR. Peer FIN clean. Both halves terminal Del/close/zero queues/freeze/EventClose. Before Handler lifecycle exists, close Done, release lease, remove managed directly; Task 9 replaces this last step with OnClose.

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

**Files:** create read file/tests; modify task/Session/lifecycle/listener/dial/Engine/native contract.

- [ ] **Step 1: RED callback/ownership cases with barriers**

```text
OnOpen blocks; peer writes complete frame; OnMessage must not enter until OnOpen release.
First OnMessage retains Message; later frames force pending buffer reuse; retained bytes unchanged.
Inside OnMessage, Stats already count RX and corresponding EventRead was observed.
Session A Handler blocks; Session B same reactor completes echo before A release.
100 messages use atomic inCallback CAS; no concurrent callbacks and count exactly 100.
OnClose blocks; Done remains open until OnClose release.
```

- [ ] **Step 2: RED worker-saturation edge-retry**

Workers=1, Queue=1: occupy running+queued tasks, make third Session readable, wait workerBlocked hook, release capacity, require third callback without extra peer write/manual signal.

- [ ] **Step 3: Run RED**

```bash
go test ./transport -run '^TestEpollNative(OnOpen|Decoded|StatsAndObserverRead|BlockedHandler|OneSession|DoneWaits|Callback)' -count=1
```

- [ ] **Step 4: Handler task state**

```go
type epollSessionCallbackState uint8
const (
    epollCallbackNeedOpen epollSessionCallbackState = iota+1
    epollCallbackOpenInFlight
    epollCallbackIdle
    epollCallbackMessageInFlight
    epollCallbackNeedClose
    epollCallbackCloseInFlight
    epollCallbackClosed
)
```

Worker task kinds codec setup/OnOpen/OnMessage/OnClose; one Handler task/session. Completion stores state then signals reactor. Dial may return after setup/adoption while OnOpen queued/running; reactor consumes no app bytes until OnOpen completion.

- [ ] **Step 5: Non-blocking read/decode**

Reserve worker capacity before callback-producing read. No capacity => workerBlocked, no read. Try codec token. Decode pending; NeedMore read under budget. Complete => Validate + copy Data + compact + release token + Stats RX + Observer Read + submit OnMessage + pause. EAGAIN/NeedMore releases reservation/token. read0 clean FIN. Decode failure DecodeErrors + typed OpRead. Budget yield releases temporary ownership and local requeue. Callback completion always requeues to retry pending work.

- [ ] **Step 6: OnClose barrier**

Stable terminal state/fd/queues/deadlines/age → Observer Close → wait no earlier Handler callback can be created → OnClose → Done → lease release → remove managed. Blocked OnClose holds Engine.Done; blocked Observer does not.

- [ ] **Step 7: Flip TCP public capability**

Listen/Dial beginOp/endOp and endpoint validation match portable; TCP routes native; TLS/WS/WSS unsupported; stream/UDP mismatch unchanged; Packet methods unsupported.

```go
func TestEpollPublicTCPContracts(t *testing.T) {
    runEngineContracts(t, epollFactory(contractProfile{TCP:true}))
}
```

```bash
go test ./transport -run '^TestEnginePublicContracts|^TestEpollPublicTCPContracts' -count=1
go test -race ./transport -run '^TestEpollNative(OnOpen|Decoded|StatsAndObserverRead|BlockedHandler|OneSession|DoneWaits|Callback)|^TestEpollPublicTCPContracts' -count=20
```

- [ ] **Step 8: Commit**

```bash
git add transport/epoll_session_read_linux.go transport/epoll_session_read_linux_test.go transport/epoll_session_task_linux.go transport/epoll_session_linux.go transport/epoll_session_lifecycle_linux.go transport/epoll_listener_linux.go transport/epoll_dial_linux.go transport/epoll_engine_linux.go transport/contract_native_linux_test.go
git commit -m "runtime: add epoll TCP read callbacks"
```

---

### Task 10: Shared graceful/limits/timeouts/errors/Stats/Observer parity

**Files:** create `contract_tcp_graceful_test.go`, `contract_tcp_limits_test.go`, `contract_tcp_observer_test.go`, `contract_tcp_timeout_error_test.go`, `epoll_tcp_parity_linux_test.go`; modify harness and only RED-exposed native seams.

- [ ] **Step 1: Shared graceful contracts**

Same factory helper runs portable then epoll: half-close drains before FIN; full Shutdown drains then waits peer FIN; peer FIN leaves write half usable/Err nil; lifecycle owner cancellation precedence; OnClose once/Done after OnClose.

- [ ] **Step 2: Shared limit/Stats contracts**

Use channel-held ownership points: MaxConnections=1, per-peer=1, per-listener=1, global queued bytes pressure; assert typed limit kinds/rejections/gauges. One payload each direction gives exact application Bytes/Messages. Final queue gauges zero and Age stable after Done.

- [ ] **Step 3: Shared Observer contracts**

Buffered copied events: Accept child/parent IDs; Connect stable ID or failed ID0; Read/Write payload bytes; one backpressure event per attempt; Close after final state; blocking observer drops but TCP progresses; panic observer increments counter without harming Session.

- [ ] **Step 4: Timeout/error contracts**

After setup barriers: ReadIdle typed timeout, ConnectionIdle, MaxLifetime, write timeout with tiny SNDBUF/raw peer no read, refused/reset errno reachability, direct caller cancellation, first terminal owner preserved. Internal Task2/6 hooks are the Connect timeout/cancel arbitration oracle.

- [ ] **Step 5: Fill only demonstrated native seams**

Policy: Connect deadline DNS+all attempts; Write fixed per current frame; ReadIdle begins after OnOpen and suspended during that Session Handler callback; ConnectionIdle only network progress; MaxLifetime fixed. Ordering RX Stats→Observer→Handler, TX release→Stats→Observer→ack, Close stable state→Observer→OnClose→Done. Listener final close freezes age/emits once.

- [ ] **Step 6: Run parity + native race**

```bash
go test ./transport -run '^Test.*(Contract|Parity|Graceful|Observer|Stats|Limit|Timeout|Error)' -count=1
go test -race ./transport -run '^TestEpoll.*(Parity|Graceful|Observer|Stats|Limit|Timeout|Error)' -count=20
```

- [ ] **Step 7: Commit exact files**

```bash
git diff --name-only
git add transport/contract_tcp_graceful_test.go transport/contract_tcp_limits_test.go transport/contract_tcp_observer_test.go transport/contract_tcp_timeout_error_test.go transport/epoll_tcp_parity_linux_test.go transport/contract_harness_test.go
# Add only exact native paths shown by git diff that changed for RED parity cases.
git diff --cached --name-only
git commit -m "runtime: align epoll TCP semantics"
```

---

### Task 11: Graceful Engine Shutdown/Close and cross-reactor stress

**Files:** create engine shutdown/tests/stress; modify Engine and exact resource methods.

- [ ] **Step 1: RED Engine lifecycle barriers**

```text
Graceful: active Session held; Shutdown closes listener/rejects new Dial; active lease becomes Draining before Session drain; peer FIN completes; all gauges zero.
Owner cancellation: first Shutdown owner blocked in OnClose, cancel it => exact cause and AbortCaller remaining; concurrent non-owner cancel cannot steal.
Close: reactor close hook proves all fd closes reactor-side; Close returns before blocked OnClose.
Blocked OnClose holds Engine.Done.
Blocked Observer does not hold Engine.Done.
```

- [ ] **Step 2: Deterministic ownership races**

Barriers around accepted handoff vs Shutdown, connect registration vs cancel, Handler completion vs abort, armWait vs Close wake, FIN vs CloseWrite, write deadline vs reset, accept flood vs Listener.Close. Each asserts one fd close/lease release and stable terminal owner.

- [ ] **Step 3: Implement ordered graceful owner model**

Engine starts drain under mutex and snapshots managed resources partitioned by `managedKind`. After unlock:

```text
for every Session snapshot: prepareEngineDrain()        // active lease -> draining; no blocking
for every Listener snapshot: requestEngineShutdown()    // close accept surface first
for every Session snapshot: requestEngineShutdown()     // reactor decides graceful vs pre-adoption abort
wake all
```

Concrete Session request is nonblocking: active/opening adopted Session publishes internal GoalFull/closes gate/signals; connecting/handoff/codec-setup publishes abort; `prepareEngineDrain` calls lease.beginDrain if current lease is active. Listener prepare is no-op. `requestEngineAbort` only publishes + signals.

Shutdown owner waits Done; owner ctx expiry transitions Engine abort and publishes AbortCaller to remaining resources; non-owner never changes shared shutdown reason. Close stays immediate AbortExplicit. `shutdownResult` matches portable precedence.

- [ ] **Step 4: Test-only invariant snapshot**

```go
type epollNativeInvariantSnapshot struct {
    ReactorResources int
    ReactorInbox int
    ReactorRunnable int
    WorkerBlocked int
    CallbackReserved int
    ManagedResources int
    Admission admissionSnapshot
}
```

After Done all structural values and Opening/Active/Draining/GlobalQueuedBytes are zero.

- [ ] **Step 5: Stress**

Pollers=4, 2000 loopback Sessions, one message/echo, alternating close side, WaitGroups/channels only. After completion Engine.Close→Done→zero invariant snapshot. Run 10 times.

- [ ] **Step 6: Run GREEN**

```bash
go test -race ./transport -run '^TestEpoll(Engine|Native).*' -count=20
go test ./transport -run '^TestEpollNativeShortLivedConnections' -count=10
```

- [ ] **Step 7: Commit exact files**

```bash
git diff --name-only
git add transport/epoll_engine_shutdown_linux.go transport/epoll_engine_shutdown_linux_test.go transport/epoll_tcp_stress_linux_test.go transport/epoll_engine_linux.go
# Add listener/session paths only if managed-resource methods changed.
git diff --cached --name-only
git commit -m "runtime: harden epoll engine shutdown"
```

---

### Task 12: TCP benchmarks, permanent CI gates, exact-head 6B checkpoint

**Files:** create `epoll_tcp_benchmark_test.go`; modify `.github/workflows/netpoll-v2.yml`.

- [ ] **Step 1: Identical portable/native benchmarks**

Sub-benchmarks: portable/epoll 1KiB,4KiB,64KiB echo; portable/epoll connect; EpollSend; EpollTrySend; EpollEngineShutdownFanout. Latency harness uses preallocated samples and `ReportMetric` p50/p95/p99; no hosted speed threshold.

- [ ] **Step 2: Deterministic hard gates only**

Keep portable allocation gates. Add native Stats snapshot/disabled-observer allocation checks and teardown invariant leak check. No hard native-vs-portable ns/op ratio.

- [ ] **Step 3: Extend Go1.26 CI**

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

- [ ] **Step 5: Cross-build, never execute foreign binary**

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c ./transport -o /tmp/transport-linux-amd64.test
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go test -c ./transport -o /tmp/transport-linux-arm64.test
GOOS=windows GOARCH=arm64 CGO_ENABLED=0 go test -c ./transport -o /tmp/transport-windows-arm64.test.exe
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go test -c ./transport -o /tmp/transport-darwin-arm64.test
```

- [ ] **Step 6: Commit benchmark/CI**

```bash
git add transport/epoll_tcp_benchmark_test.go .github/workflows/netpoll-v2.yml
git commit -m "test: gate epoll TCP runtime"
```

- [ ] **Step 7: Exact-head Actions evidence**

Must pass Linux Go1.25/1.26 format/module/vet/full race, existing graceful+observability allocations, typed-error 20x, observability 20x, runtimecore 20x, native TCP 20x, Windows/macOS, FreeBSD runtime, GmSSL, all cross-compiles, native benchmark smoke.

- [ ] **Step 8: Update PR/issues**

Record: P1-6B Linux TCP complete; P1-6C UDP outstanding; TLS/WS/WSS explicit unsupported/no fallback; PR stays Draft because #57 still requires UDP and 6D evidence. Do not close #57/#56/#38 and do not mark PR Ready.
