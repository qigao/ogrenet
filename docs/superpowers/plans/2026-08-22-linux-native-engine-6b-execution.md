# Linux Native Engine 6B Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a real Linux epoll-owned TCP backend behind `transport.NewEpoll`, with fixed reactor ownership, bounded callback execution, non-blocking accept/connect/read/write, graceful half-close, typed-error/Stats/Observer parity, and TCP race/stress/benchmark evidence.

**Architecture:** `transport.New()` remains the portable reference implementation. Linux `NewEpoll` starts N fixed epoll reactors plus one bounded callback/setup executor; every native listener/session fd is owned by exactly one reactor and only that reactor performs physical socket I/O. Application goroutines perform admission/encoding/state publication only, then signal the owning reactor through the deduplicated intrusive inbox established by this plan. UDP remains explicitly unsupported until 6C.

**Tech Stack:** Go 1.25+, `golang.org/x/sys/unix`, existing top-level `epoll` poller, root `ogrenet` contracts, existing `transport` admission/quota/error/stats/observer helpers, `internal/runtimecore`, race detector, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-22-linux-native-engine-design.md`

## Global Constraints

- Linux TCP only in 6B. `ListenPacket`/`DialPacket` remain `ErrProtocolUnsupported`; TLS/WS/WSS remain `ErrProtocolUnsupported` with no portable fallback.
- `transport.New()` must remain unchanged as the portable correctness/reference backend.
- Exactly one reactor goroutine owns each listener/session fd. No native Session gets a reader goroutine, writer goroutine, timeout goroutine, or direct application-goroutine syscall path.
- `epoll.Event.Data` stores one Engine-local monotonic non-zero ID; `math.MaxUint64` is never allocated because the low-level epoll package reserves it for wakeups.
- A resource never migrates between reactors in 6B. Accepted sessions are assigned round-robin once; outbound sessions select one reactor before socket creation.
- Cross-goroutine control uses per-resource intrusive inbox nodes plus coalesced `Poller.Wake()`. Do not introduce a generic command channel, unbounded queue, lock-free MPSC, or per-signal allocation.
- Edge-triggered work deliberately left before `EAGAIN` must be requeued on a reactor-local runnable list. Never wait for a second edge after budget yield, callback pause, codec pause, or lifecycle pause.
- User `Handler` callbacks never run on reactor goroutines. `FramerFactory`/`CipherFactory` construction is also application-supplied code and must run as bounded setup work on the same worker executor, not inside a reactor.
- Custom framer encode runs synchronously on the Send/TrySend caller after codec-token admission; decode runs on the owning reactor only after non-blocking codec-token acquisition.
- A decoded `ogrenet.Message` submitted to an asynchronous worker must own its `Data` independently of reactor read buffers (`append([]byte(nil), msg.Data...)`).
- Stats ownership/counting points and Observer ordering remain P0-5 exact: counters first, optional Observer event second, application callback/Send ack third.
- Caller context cancellation/deadline is returned unchanged. Operational socket failures are classified with existing `classifyOperational`; configuration/capability errors remain direct sentinels.
- Admission, `byteQuota`, `listenerCapacity`, session/listener counters, `sendGate`, `sessionLifecycle`, Observer dispatcher, and public error types are reused rather than copied into a second native semantic stack.
- TCP `Send(ctx)` may return the caller context cause after queue ownership transfer while the frame remains eligible for physical write, matching portable semantics. `TrySend` never waits for reactor/network progress.
- Callback executor capacity is exactly `CallbackWorkers + CallbackQueue` retained tasks; when all workers are executing callbacks, queued tasks are at most `CallbackQueue`.
- `Engine.Done()` waits for every application/setup task required to finish a resource, but never waits for a blocked Observer callback.
- Correctness tests use channels/barriers/hooks derived from real state transitions, not sleeps as success or ordering oracles. Time-based timeout tests may use deadlines, but must synchronize setup before starting the timeout assertion.
- No UDP, TLS, WS/WSS, kqueue, IOCP, pooling, buffer-pool redesign, `writev`, `sendfile`, resolver racing/Happy Eyeballs, proxy, QUIC, or HTTP scope expansion.

## Planned file map

```text
transport/
    epoll_engine_linux.go              Engine state, construction, shutdown barrier
    epoll_reactor_linux.go             reactor loop, event dispatch, runnable work
    epoll_reactor_inbox_linux.go       intrusive inbox + lost-wake handshake
    epoll_deadline_linux.go            generation-based min-heap scheduler
    epoll_callback_linux.go            bounded worker/callback/setup executor
    epoll_fd_linux.go                  sockaddr/socket/TCP option helpers
    epoll_listener_linux.go            native TCP listener + accept/handoff
    epoll_session_linux.go             native TCP Session state/public identity
    epoll_session_send_linux.go        codec admission + Send/TrySend + partial write
    epoll_session_read_linux.go        read/decode + callback serialization
    epoll_session_lifecycle_linux.go   half-close/abort/finalization
    epoll_native_test_helpers_linux.go test-only deterministic native helpers
```

`transport/epoll_engine_phase6a_linux.go` is removed only after `epoll_engine_linux.go` supplies the same `ogrenet.Engine` surface. Existing portable files are not reorganized.

---

### Task 1: Reactor inbox, wake handshake, resource registry, and fairness runnable list

**Files:**
- Create: `transport/epoll_reactor_inbox_linux.go`
- Create: `transport/epoll_reactor_linux.go`
- Create: `transport/epoll_reactor_linux_test.go`

**Consumes:** existing `epoll.Open`, `Poller.Wait`, `Poller.Add/Mod/Del`, `Poller.Wake`, `resolvedEpollConfig`.

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
    onReactorDeadline(*epollReactor, epollDeadlineKind, uint64)
}

type epollInboxNode struct {
    owner  epollInboxItem
    next   *epollInboxNode
    queued bool // protected only by reactor.inboxMu
}

type epollReactor struct {
    engine *epollEngine
    index  int
    poller *epoll.Poller
    events []epoll.Event

    resources map[uint64]epollEventResource

    inboxMu      sync.Mutex
    inboxHead    *epollInboxNode
    inboxTail    *epollInboxNode
    waiting      bool
    wakePending  bool
    controlFlags atomic.Uint32

    runnable []*epollInboxNode
}
```

Control bits:

```go
const (
    epollControlStop uint32 = 1 << iota
    epollControlCallbackCapacity
)
```

- [ ] **Step 1: Write RED dedupe and lost-wake tests**

Use a synthetic `epollInboxItem` whose `onReactorInbox` publishes to a channel. Tests must assert:

```go
func TestEpollReactorSignalDeduplicatesQueuedItem(t *testing.T) { /* signal same node 100 times before drain; one invocation */ }
func TestEpollReactorSignalWakesBlockedWait(t *testing.T) { /* wait until reactor publishes wait-armed barrier; signal once; item runs */ }
func TestEpollReactorControlWakeCannotBeLost(t *testing.T) { /* arm Wait, set control bit, observe control processing */ }
```

Do not infer reactor sleeping with `time.Sleep`; expose a test-only `waitArmed` channel on the synthetic harness around the real `armWait` transition.

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^TestEpollReactor(Signal|Control)' -count=1
```

Expected: compile FAIL because `epollReactor`/`epollInboxNode` do not exist.

- [ ] **Step 3: Implement intrusive signal and wait handshake**

Required shape:

```go
func (r *epollReactor) signal(item epollInboxItem) {
    n := item.inboxNode()
    r.inboxMu.Lock()
    if !n.queued {
        n.queued = true
        n.next = nil
        if r.inboxTail == nil {
            r.inboxHead = n
        } else {
            r.inboxTail.next = n
        }
        r.inboxTail = n
    }
    wake := r.waiting && !r.wakePending
    if wake {
        r.wakePending = true
    }
    r.inboxMu.Unlock()
    if wake {
        _ = r.poller.Wake()
    }
}

func (r *epollReactor) armWait() bool {
    r.inboxMu.Lock()
    defer r.inboxMu.Unlock()
    if r.inboxHead != nil || r.controlFlags.Load() != 0 || len(r.runnable) != 0 {
        return false
    }
    r.waiting = true
    r.wakePending = false
    return true
}

func (r *epollReactor) disarmWait() {
    r.inboxMu.Lock()
    r.waiting = false
    r.wakePending = false
    r.inboxMu.Unlock()
}
```

`signalControl(mask)` sets bits with CAS and then performs the same wake handshake without allocating an inbox node.

- [ ] **Step 4: Add reactor registry/runnable RED tests**

```go
func TestEpollReactorIgnoresStaleEventData(t *testing.T) { /* event ID absent from resources => no callback/panic */ }
func TestEpollReactorRunnableContinuesWithoutSecondEdge(t *testing.T) { /* synthetic resource requeues itself 3 times and completes */ }
func TestEpollReactorResourceIDRegistryRejectsDuplicate(t *testing.T) { /* duplicate ID cannot replace existing owner */ }
```

- [ ] **Step 5: Implement registry/runnable/event loop**

`registerResource`/`unregisterResource` mutate `resources` only on the reactor goroutine. `requeue(item)` is reactor-local and uses a reactor-only `runnableQueued` bit in the concrete resource/node so one resource appears at most once.

The loop order is exact:

```go
for {
    r.drainInbox()
    r.drainControl()
    r.runExpiredDeadlines(time.Now())
    r.drainRunnable()
    if r.shouldStop() { return }
    if len(r.runnable) != 0 { continue }
    timeout := r.nextWaitTimeout(time.Now())
    if !r.armWait() { continue }
    n, err := r.poller.Wait(r.events, timeout)
    r.disarmWait()
    if err != nil { /* ErrClosed only during final stop; otherwise engine-fatal */ }
    for i := 0; i < n; i++ {
        ev := r.events[i]
        if res := r.resources[ev.Data]; res != nil {
            res.onReactorEvent(r, ev.Events)
        }
    }
}
```

- [ ] **Step 6: Run GREEN + race**

```bash
go test ./transport -run '^TestEpollReactor' -count=1
go test -race ./transport -run '^TestEpollReactor' -count=20
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add transport/epoll_reactor_inbox_linux.go transport/epoll_reactor_linux.go transport/epoll_reactor_linux_test.go
git commit -m "runtime: add epoll reactor core"
```

---

### Task 2: Generation-based native deadline heap

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

type epollDeadlineEntry struct {
    at         time.Time
    resourceID uint64
    kind       epollDeadlineKind
    generation uint64
}
```

Every deadline-owning resource stores a generation per domain and returns the current generation when the reactor checks staleness.

- [ ] **Step 1: Write RED heap tests**

Required tests:

```go
func TestEpollDeadlineHeapOrdersEarliest(t *testing.T) {}
func TestEpollDeadlineIgnoresStaleGeneration(t *testing.T) {}
func TestEpollDeadlineWaitTimeoutZeroWhenExpired(t *testing.T) {}
func TestEpollDeadlineWaitTimeoutNegativeWhenEmpty(t *testing.T) {}
```

A stale entry must not invoke `onReactorDeadline` after the resource has incremented that domain's generation.

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^TestEpollDeadline' -count=1
```

Expected: compile FAIL because deadline types do not exist.

- [ ] **Step 3: Implement heap with stale-entry discard**

Use `container/heap`; do not remove old entries on update. Scheduling increments the resource generation and pushes a new entry. Pop logic discards entries when:

```text
resource absent OR resource.currentDeadlineGeneration(kind) != entry.generation
```

`nextWaitTimeout(now)` repeatedly discards stale head entries and returns:

```text
-1        no live deadline
0         earliest deadline <= now
ceil-ish duration handled by epoll.Poller.Wait for positive values
```

- [ ] **Step 4: Integrate with reactor loop and rerun race**

```bash
go test ./transport -run '^TestEpoll(Deadline|Reactor)' -count=1
go test -race ./transport -run '^TestEpoll(Deadline|Reactor)' -count=20
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add transport/epoll_deadline_linux.go transport/epoll_deadline_linux_test.go transport/epoll_reactor_linux.go
git commit -m "runtime: add epoll deadline scheduler"
```

---

### Task 3: Exact-bounded callback/setup executor

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

    queue []epollWorkerTask // fixed ring length CallbackQueue
    head  int
    size  int

    reserved int // running + queued
    limit    int // CallbackWorkers + CallbackQueue

    onCapacity func()
}
```

Each worker has a single-slot private channel. A reservation succeeds only when `reserved < limit`. `submitReserved` dispatches directly to an idle worker or appends to the fixed queue. It never blocks on a user callback and never allocates an unbounded task list.

- [ ] **Step 1: Write RED capacity tests**

Required deterministic cases:

```go
func TestEpollCallbackExecutorReservationBound(t *testing.T) {}
func TestEpollCallbackExecutorQueueNeverExceedsConfiguredQueue(t *testing.T) {}
func TestEpollCallbackExecutorBlockedTaskDoesNotBlockSubmitter(t *testing.T) {}
func TestEpollCallbackExecutorCapacityReleaseNotifies(t *testing.T) {}
func TestEpollCallbackExecutorStopsOnlyWhenIdle(t *testing.T) {}
```

Use tasks that block on explicit channels; do not use sleep to infer worker state.

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^TestEpollCallbackExecutor' -count=1
```

Expected: compile FAIL.

- [ ] **Step 3: Implement worker scheduling**

Worker completion logic is exact:

```text
run task outside executor mutex
lock executor
reserved-- for completed task
if fixed queue non-empty:
    pop next already-reserved task; worker continues directly
else:
    mark worker idle
unlock
invoke onCapacity outside lock
```

`stopIdle()` is legal only when `reserved == 0 && size == 0`; then all workers are idle, their private channels are closed, and the method waits for worker goroutines to exit. It never attempts to unwind running application code.

- [ ] **Step 4: Run GREEN + race**

```bash
go test ./transport -run '^TestEpollCallbackExecutor' -count=1
go test -race ./transport -run '^TestEpollCallbackExecutor' -count=20
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add transport/epoll_callback_linux.go transport/epoll_callback_linux_test.go
git commit -m "runtime: add bounded epoll callback executor"
```

---

### Task 4: Native Engine ownership, reactor startup, IDs, admission, Stats, and final barrier

**Files:**
- Create: `transport/epoll_engine_linux.go`
- Create: `transport/epoll_engine_linux_test.go`
- Modify: `transport/epoll_constructor_linux.go`
- Modify: `transport/stats.go`
- Delete after GREEN: `transport/epoll_engine_phase6a_linux.go`

**Produces:** a real engine shell with N reactor goroutines and bounded worker executor, but TCP public methods still return `ErrProtocolUnsupported` until Task 8 flips the capability.

Required Engine state:

```go
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
    listeners      map[*epollListener]struct{}
    sessions       map[*epollSession]struct{} // includes connect/handoff/setup states

    nextReactor atomic.Uint64
    nextID      atomic.Uint64

    quiescent     chan struct{}
    quiescentOnce sync.Once
    reactorWG     sync.WaitGroup
    done          chan struct{}
    doneOnce      sync.Once
}
```

- [ ] **Step 1: Write RED construction/barrier tests**

Required tests:

```go
func TestNewEpollStartsConfiguredReactorCount(t *testing.T) {}
func TestEpollEngineResourceIDNeverReturnsZeroOrReservedWakeValue(t *testing.T) {}
func TestEpollEngineCloseStopsEmptyReactorsAndWorkers(t *testing.T) {}
func TestEpollEngineStatsUsesAdmissionAndObserverOwners(t *testing.T) {}
```

For ID exhaustion, set `nextID` to `math.MaxUint64-1` in package test and assert the next allocation fails with a private error before any poller registration.

- [ ] **Step 2: Run RED**

```bash
go test ./transport -run '^Test(EpollEngine|NewEpollStarts)' -count=1
```

Expected: FAIL because the 6A Engine has no reactors/admission/worker barrier.

- [ ] **Step 3: Implement Engine boot and finalizer**

Constructor order:

```text
resolve config/options
create epollEngine + admission + observer
open all pollers/reactors; on failure close already-open pollers + stop observer
create callback executor with onCapacity=e.wakeCallbackWaiters
start reactor goroutines
start exactly one Engine finalizer goroutine
return Engine
```

The finalizer waits on `quiescent`, signals `epollControlStop` to every reactor, waits `reactorWG`, asserts callback executor is idle, stops workers, calls `observer.stop()` without waiting for Observer return, then closes `done`.

`maybeQuiescentLocked` may close `quiescent` only when:

```text
state != engineRunning
activeOps == 0
len(listeners) == 0
len(sessions) == 0
admission.idle() == true
```

- [ ] **Step 4: Add operation/state helpers**

Implement `beginOp/endOp`, `add/removeListener`, `add/removeSession`, `selectReactor`, `nextResourceID`, `wakeAll`, and `wakeCallbackWaiters`.

`selectReactor` is round-robin and immutable for a resource lifetime.

- [ ] **Step 5: Share EngineStats formatting**

Refactor only the formatting layer in `transport/stats.go`:

```go
func engineStatsSnapshot(admission *admissionController, observer *observerDispatcher) ogrenet.EngineStats
```

Portable `(*Engine).Stats()` and native `(*epollEngine).Stats()` call the same function. Admission remains the sole gauge/rejection owner.

- [ ] **Step 6: Run GREEN + portable regression**

```bash
go test ./transport -run '^Test(EpollEngine|NewEpollStarts|EngineStats)' -count=1
go test -race ./transport -run '^Test(EpollEngine|NewEpollStarts)' -count=20
go test ./... -count=1
```

Expected: PASS. Existing portable allocation gates are rerun in final Task 12.

- [ ] **Step 7: Delete 6A Engine scaffold and commit**

```bash
git rm transport/epoll_engine_phase6a_linux.go
git add transport/epoll_engine_linux.go transport/epoll_engine_linux_test.go transport/epoll_constructor_linux.go transport/stats.go
git commit -m "runtime: boot epoll engine reactors"
```

---

### Task 5: Linux TCP fd helpers, listener ownership, accept drain, and exact-once handoff

**Files:**
- Create: `transport/epoll_fd_linux.go`
- Create: `transport/epoll_listener_linux.go`
- Create: `transport/epoll_listener_linux_test.go`
- Create: `transport/epoll_native_test_helpers_linux.go`

**Consumes:** Engine/reactor/inbox/callback executor; existing `listenerCapacity`, `connectionLease`, `listenerCounters`, `classifyOperational`, `boundEndpoint`, `normalizeHandler`, `config.newFramer`.

**Produces:** private `listenNativeTCP` used by internal tests; public `Listen` is not switched yet.

- [ ] **Step 1: Write RED sockaddr/TCP-option tests**

Required tests cover IPv4 and IPv6 loopback:

```go
func TestNativeTCPSockaddrRoundTripIPv4(t *testing.T) {}
func TestNativeTCPSockaddrRoundTripIPv6(t *testing.T) {}
func TestNativeTCPConfigAppliesNoDelayKeepaliveAndBuffers(t *testing.T) {}
```

Implementation helpers:

```go
func nativeTCPAddrToSockaddr(*net.TCPAddr) (unix.Sockaddr, int, error)
func nativeSockaddrToTCPAddr(unix.Sockaddr) (*net.TCPAddr, error)
func nativeSocketAddr(fd int, peer bool) (*net.TCPAddr, error)
func configureNativeTCP(fd int, cfg TCPConfig) error
```

Use `unix.Socket(family, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, unix.IPPROTO_TCP)`.

- [ ] **Step 2: Run RED, implement helpers, run GREEN**

```bash
go test ./transport -run '^TestNativeTCP(Sockaddr|Config)' -count=1
```

Expected first run: compile FAIL; second run after implementation: PASS.

- [ ] **Step 3: Write RED listener/handoff tests**

Required deterministic cases:

```go
func TestEpollNativeListenerOwnedByOneReactor(t *testing.T) {}
func TestEpollNativeAcceptHandoffRegistersOnSelectedReactor(t *testing.T) {}
func TestEpollNativeAcceptEventUsesSessionAndParentIDs(t *testing.T) {}
func TestEpollNativeListenerAdmissionRejectsAndReleasesLease(t *testing.T) {}
func TestEpollNativeListenerCloseLeavesNoFDOrCurrentConnectionGauge(t *testing.T) {}
```

For handoff, configure `Pollers: 2`, connect with a raw `net.Dialer`, wait on Observer `EventAccept`, then inspect package-private session ownership after the event. No sleep/polling loop.

- [ ] **Step 4: Implement listener resource**

`epollListener` owns:

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

Base interest: `epoll.Readable | epoll.EdgeTriggered`.

Accept loop uses `unix.Accept4(fd, unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC)` and stops only on `EAGAIN`, lifecycle close, or the reactor operation budget. `EINTR` retries without consuming an accepted-resource slot.

- [ ] **Step 5: Implement accepted-session handoff state**

For each accepted fd:

```text
convert remote/local sockaddr
acquire listener/global/per-peer opening lease
configure native TCP
allocate non-reserved resource ID
choose target reactor round-robin
create epollSession in setup-pending state and add to Engine sessions map
signal target reactor intrusive inbox
```

The target reactor is authorized to register or close the handoff fd, but the session is not application-active until registration + codec setup succeed. On target registration failure: `Del` if necessary, close fd, release lease once, remove session once, emit no Accept event, invoke no Handler callback.

- [ ] **Step 6: Run listener GREEN + race**

```bash
go test ./transport -run '^TestEpollNative(Listen|Accept)' -count=1
go test -race ./transport -run '^TestEpollNative(Listen|Accept)' -count=20
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add transport/epoll_fd_linux.go transport/epoll_listener_linux.go transport/epoll_listener_linux_test.go transport/epoll_native_test_helpers_linux.go
git commit -m "runtime: add epoll TCP listener handoff"
```

---

### Task 6: Caller-side DNS + reactor-owned non-blocking Dial/connect completion

**Files:**
- Create: `transport/epoll_dial_linux.go`
- Create: `transport/epoll_dial_linux_test.go`
- Modify: `transport/epoll_session_linux.go` (create here if Task 5 used a bootstrap declaration)

**Produces:** private `dialNativeTCP`; public `Dial` remains gated until Task 8.

- [ ] **Step 1: Write RED resolver/address-order tests**

Required helper contract:

```go
func resolveNativeDialTCP(ctx context.Context, endpoint ogrenet.Endpoint) ([]*net.TCPAddr, error)
```

Tests assert IP literals bypass resolver helper logic and hostname results are retained in resolver order. Do not add Happy Eyeballs/racing.

- [ ] **Step 2: Write RED non-blocking connect tests**

Required deterministic cases:

```go
func TestEpollNativeDialImmediateOrEINPROGRESSCompletesViaSOError(t *testing.T) {}
func TestEpollNativeDialRefusedReturnsTypedDialError(t *testing.T) {}
func TestEpollNativeDialCallerCancellationReturnsCauseUnwrapped(t *testing.T) {}
func TestEpollNativeDialCancellationNeverClosesFDFromCaller(t *testing.T) {}
func TestEpollNativeDialTriesResolvedAddressesSequentially(t *testing.T) {}
```

Use a loopback listener for success, a closed loopback port for refusal, and a package-private connect operation barrier to race cancellation after fd registration but before completion processing.

- [ ] **Step 3: Run RED**

```bash
go test ./transport -run '^TestEpollNativeDial' -count=1
```

Expected: compile FAIL because `dialNativeTCP`/connect state do not exist.

- [ ] **Step 4: Implement `epollSession` connecting/setup states**

Session identity/state includes:

```go
type epollSessionState uint8
const (
    epollSessionConnecting epollSessionState = iota + 1
    epollSessionCodecSetup
    epollSessionOpening
    epollSessionActive
    epollSessionTerminal
    epollSessionClosed
)
```

The same tentative resource ID is used as epoll Data during connect and becomes the Session ID on success. Failed dials consume the ID but expose Observer `ResourceID: 0`.

- [ ] **Step 5: Implement reactor-owned connect attempt loop**

Caller flow:

```text
beginOp
create bounded Connect context once (covers DNS + all attempts)
resolve addresses on caller/setup plane
allocate tentative ID + choose reactor
create epollSession(connecting) + add Engine session map
signal owning reactor
wait buffered result OR caller/internal context
on context completion publish cancel state + signal reactor; never close fd on caller\endOp
```

Reactor flow per address:

```text
socket(nonblock/cloexec)
connect
0 => connected
EINPROGRESS => Add(fd, Writable|Error|EdgeTriggered, id), schedule Connect deadline
other errno => close and try next address
EPOLLOUT/ERR => GetsockoptInt(SOL_SOCKET, SO_ERROR)
0 => connected
errno => Del/close and try next address
```

After final failure, classify once as `OpDial`. Observer Connect duration covers DNS + all address attempts when observer is enabled.

- [ ] **Step 6: Run GREEN + cancellation race**

```bash
go test ./transport -run '^TestEpollNativeDial' -count=1
go test -race ./transport -run '^TestEpollNativeDial' -count=20
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add transport/epoll_dial_linux.go transport/epoll_dial_linux_test.go transport/epoll_session_linux.go
git commit -m "runtime: add epoll nonblocking TCP dial"
```

---

### Task 7: Codec/setup ownership, Send/TrySend admission, partial writes, and write deadlines

**Files:**
- Create: `transport/epoll_session_send_linux.go`
- Create: `transport/epoll_session_send_linux_test.go`
- Modify: `transport/epoll_session_linux.go`
- Modify: `transport/epoll_listener_linux.go`
- Modify: `transport/epoll_dial_linux.go`

**Produces:** native Session outbound path; no application callback/read path yet.

- [ ] **Step 1: Write RED codec-setup executor tests**

Tests use a `FramerFactory` blocked on a channel and prove:

```go
func TestEpollCodecFactoryDoesNotRunOnReactor(t *testing.T) { /* while factory blocks, same reactor processes control event */ }
func TestEpollAcceptedCodecFactoryFailureClosesWithoutOnOpen(t *testing.T) {}
func TestEpollDialCodecFactoryFailureReturnsDirectConfigError(t *testing.T) {}
```

Codec setup is represented as an `epollWorkerTask`; completion stores `{framer, wireFramer, err}` on the session and signals its reactor. Only after successful setup does the lease activate and accepted/dial adoption complete.

- [ ] **Step 2: Write RED Send/TrySend ownership tests**

Required tests:

```go
func TestEpollNativeTrySendCodecContentionReturnsTypedWouldBlockOnce(t *testing.T) {}
func TestEpollNativeTrySendQueuePressureCountsOneBackpressure(t *testing.T) {}
func TestEpollNativeSendCancellationAfterQueueTransferMayStillWrite(t *testing.T) {}
func TestEpollNativePartialWriteRetainsQuotaUntilFrameComplete(t *testing.T) {}
func TestEpollNativeWriteEAGAINDoesNotBlockReactor(t *testing.T) {}
```

For partial write/EAGAIN, connect to a raw TCP peer with a deliberately small `SO_SNDBUF`, stop peer reads behind a barrier, queue a large allowed frame, and simultaneously prove another resource on the same reactor makes progress. Do not assert a wall-clock latency threshold; use a second resource completion channel.

- [ ] **Step 3: Run RED**

```bash
go test ./transport -run '^TestEpoll(Native(Send|TrySend|Partial|Write)|Codec)' -count=1
```

Expected: compile/behavior FAIL.

- [ ] **Step 4: Implement codec token and caller-side encoding**

Native Session fields reuse existing owners:

```go
queue      chan outbound
quota      *byteQuota
gate       *sendGate
frameSlots chan struct{}
codecSlot  chan struct{} // capacity 1; holder sends token, release receives
framer     wire.Framer
wireFramer bool

decodeWaiting atomic.Bool
```

`Send` waits on frame slot + codec token + quota subject to ctx/lifecycle. `TrySend` performs non-blocking versions. Encoding calls `framer.Encode` synchronously and copies the returned frame before ownership transfer.

`releaseCodec` must signal the reactor when `decodeWaiting.Swap(false)` reports a paused decoder.

- [ ] **Step 5: Implement reactor-only partial write machine**

Session reactor-owned fields:

```go
writeCurrent outbound
writeOffset  int
writeActive  bool
writeBlocked bool
writeGen     uint64
```

`driveWrite` loops under one per-turn `epollBudget`:

```text
if no current: non-blocking receive next queued frame
schedule fixed Write deadline for current frame
unix.Write(frame[offset:])
 n>0 => touch activity, advance offset, charge budget
 complete => cancel deadline generation; release quota + frame slot; update Stats; emit Write; ack; next
 EAGAIN => enable Writable interest and return
 EINTR => retry
 other => classify OpWrite, terminal-abort, fail current/pending exactly once
 budget exhausted before completion => reactor.requeue(session)
```

Disable Writable interest after output fully drains.

- [ ] **Step 6: Implement API compile contract**

Add compile RED first:

```go
var _ ogrenet.Session = (*epollSession)(nil)
var _ ogrenet.HalfCloseSession = (*epollSession)(nil)
```

Then add `ID/Protocol/Endpoint/LocalAddr/RemoteAddr/Stats/Done/Err/Send/TrySend/Close/Shutdown/CloseWrite/ReadClosed` methods. Lifecycle methods may call the state machinery completed in Task 9; they must not perform fd syscalls on the caller goroutine.

- [ ] **Step 7: Run GREEN + race**

```bash
go test ./transport -run '^TestEpoll(Native(Send|TrySend|Partial|Write)|Codec)' -count=1
go test -race ./transport -run '^TestEpoll(Native(Send|TrySend|Partial|Write)|Codec)' -count=20
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add transport/epoll_session_linux.go transport/epoll_session_send_linux.go transport/epoll_session_send_linux_test.go transport/epoll_listener_linux.go transport/epoll_dial_linux.go
git commit -m "runtime: add epoll TCP send path"
```

---

### Task 8: Inbound read/decode, async message ownership, callback serialization, and TCP capability flip

**Files:**
- Create: `transport/epoll_session_read_linux.go`
- Create: `transport/epoll_session_read_linux_test.go`
- Modify: `transport/epoll_session_linux.go`
- Modify: `transport/epoll_callback_linux.go`
- Modify: `transport/epoll_engine_linux.go`
- Modify: `transport/contract_native_linux_test.go`

**Produces:** first end-to-end native TCP public contract; UDP still false.

- [ ] **Step 1: Write RED callback-order/ownership tests**

Required tests:

```go
func TestEpollNativeOnOpenCompletesBeforeFirstOnMessage(t *testing.T) {}
func TestEpollNativeDecodedMessageOwnsBytesOutsideReadBuffer(t *testing.T) {}
func TestEpollNativeStatsAndObserverReadCommitBeforeCallback(t *testing.T) {}
func TestEpollNativeBlockedHandlerDoesNotBlockReactor(t *testing.T) {}
func TestEpollNativeOneSessionNeverRunsConcurrentCallbacks(t *testing.T) {}
```

The ownership test must retain the first callback Message while subsequent socket reads and pending-buffer compaction occur, then assert the first `Data` is unchanged.

- [ ] **Step 2: Write RED callback-saturation/edge-retry test**

Configure `CallbackWorkers: 1`, `CallbackQueue: 1`. Fill one running + one queued task with explicit barriers, cause a third session to become readable, then release one slot. Assert the third Session receives its message without sending additional network data or manufacturing a second readiness edge.

- [ ] **Step 3: Run RED**

```bash
go test ./transport -run '^TestEpollNative(OnOpen|Decoded|StatsAndObserverRead|BlockedHandler|OneSession|CallbackSaturation)' -count=1
```

Expected: FAIL because read/callback path is absent.

- [ ] **Step 4: Implement Session callback state machine**

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

Worker completion uses one atomic completion code because one callback is in flight per Session. Completion stores state first, then `reactor.signal(session)`.

When callback capacity is unavailable, add the Session once to a reactor-local callback-wait list and set a reactor atomic waiter flag. Executor `onCapacity` wakes reactors whose waiter flag is set; the reactor moves waiters to runnable before sleeping.

- [ ] **Step 5: Implement non-blocking read/decode**

Session owns `readScratch []byte` and `readPending []byte`. Before a read/decode turn that may produce a callback, reserve executor capacity. If reservation fails, pause without reading.

Decode algorithm:

```text
try codec token; if unavailable set decodeWaiting and return
first attempt DecodeOne on existing pending bytes
if NeedMore, read nonblocking under budget and append
on complete Message:
    validate
    owned := ogrenet.Message{Type: msg.Type, Data: append([]byte(nil), msg.Data...)}
    consume/compact pending
    release codec token
    increment BytesRX/MessagesRX
    emit EventRead
    submit reserved OnMessage task
    pause this Session until callback completion
on EAGAIN/NeedMore without Message:
    release reserved callback slot immediately
on decode/protocol failure:
    release reservation/token; increment DecodeErrors; terminal-abort typed OpRead
on read 0:
    release reservation/token; mark clean peer FIN
```

If byte/op budget expires while readable work remains, release temporary reservations/tokens and reactor-local requeue immediately.

- [ ] **Step 6: Flip public TCP capability and run shared basic contract RED/GREEN**

Change `contract_native_linux_test.go` to:

```go
func TestEpollPublicTCPContracts(t *testing.T) {
    runEngineContracts(t, epollFactory(contractProfile{TCP: true}))
}
```

Change `epollEngine.Listen/Dial` so validated TCP routes to native methods, TLS/WS/WSS still return `ErrProtocolUnsupported`, and stream/UDP mismatch stays `ErrProtocolMismatch`.

Run:

```bash
go test ./transport -run '^TestEnginePublicContracts|^TestEpollPublicTCPContracts' -count=1
```

Expected: portable and epoll TCP basic echo/lifecycle/Stats contract PASS; UDP runs only portable.

- [ ] **Step 7: Run native read/callback race**

```bash
go test -race ./transport -run '^TestEpollNative(OnOpen|Decoded|StatsAndObserverRead|BlockedHandler|OneSession|CallbackSaturation)|^TestEpollPublicTCPContracts' -count=20
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add transport/epoll_session_read_linux.go transport/epoll_session_read_linux_test.go transport/epoll_session_linux.go transport/epoll_callback_linux.go transport/epoll_engine_linux.go transport/contract_native_linux_test.go
git commit -m "runtime: add epoll TCP read callbacks"
```

---

### Task 9: Graceful Shutdown, CloseWrite, peer FIN, abort, and final OnClose barrier

**Files:**
- Create: `transport/epoll_session_lifecycle_linux.go`
- Create: `transport/epoll_session_lifecycle_linux_test.go`
- Create: `transport/contract_tcp_graceful_test.go`
- Modify: `transport/epoll_session_read_linux.go`
- Modify: `transport/epoll_session_send_linux.go`
- Modify: `transport/contract_harness_test.go`

- [ ] **Step 1: Write shared portable/native graceful contract tests**

Backend-neutral tests must cover:

```go
func runTCPHalfCloseContract(t *testing.T, f engineFactory) {}
func runTCPShutdownContract(t *testing.T, f engineFactory) {}
func runTCPPeerFINContract(t *testing.T, f engineFactory) {}
```

Assertions include:

```text
CloseWrite drains already-admitted writes before peer EOF
ReadClosed closes on clean peer FIN
peer FIN does not populate Session.Err
session remains writable after peer FIN until local write closes
OnClose happens after final I/O and exactly once
Done closes only after OnClose returns
Shutdown caller timeout only aborts when that caller owns the transition, preserving existing owner precedence
```

- [ ] **Step 2: Run RED against epoll + GREEN against portable**

Use subtests to prove portable characterization passes and native fails before implementation.

```bash
go test ./transport -run '^TestTCP(Portable|Epoll).*Graceful|^TestEpoll.*HalfClose' -count=1
```

- [ ] **Step 3: Implement caller-side lifecycle publication**

Native methods mirror portable ownership:

```text
requestWriteClose => life.requestWithPrevious(GoalWrite); winner closes send gate; signal reactor
requestShutdown  => GoalFull; winner closes send gate; signal reactor
Close            => life.abortWith(AbortExplicit, publish Err=nil); close logical closing signal; close gate; signal reactor
ctx expiry by owning CloseWrite/Shutdown => life.abortWith(AbortCaller,...); signal reactor; return caller cause
```

No caller path calls `unix.Close` or `unix.Shutdown`.

- [ ] **Step 4: Implement reactor graceful/abort drive**

Exact order:

```text
abort:
  Del fd if registered
  close fd
  fail current + queued writes; release all local/global quota + frame slots
  cancel deadline generations
  mark read/write closed
  schedule terminal close event/callback

graceful write:
  wait send gate drained
  drain current + queued frames
  unix.Shutdown(fd, SHUT_WR)
  mark WriteDone
  continue read side

peer FIN:
  drain buffered readable bytes/messages first
  mark ReadDone
  keep fd writable if WriteDone is still open

both halves done:
  life.TryMarkTerminal
  Del/close fd
  zero queue gauges
  freeze age
  emit EventClose after stable Err/stats
  schedule OnClose only after no Message callback can still be created
```

After OnClose worker completion, close Session.Done, release connection lease, remove Engine session. Only then may Engine become quiescent.

- [ ] **Step 5: Run graceful GREEN + races**

```bash
go test ./transport -run 'Graceful|HalfClose|PeerFIN|EpollPublicTCPContracts' -count=1
go test -race ./transport -run '^TestEpoll.*(Graceful|HalfClose|PeerFIN|Close)|^TestEpollPublicTCPContracts' -count=20
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add transport/epoll_session_lifecycle_linux.go transport/epoll_session_lifecycle_linux_test.go transport/contract_tcp_graceful_test.go transport/epoll_session_read_linux.go transport/epoll_session_send_linux.go transport/contract_harness_test.go
git commit -m "runtime: preserve graceful TCP lifecycle on epoll"
```

---

### Task 10: Timeout, typed-error, Stats, Observer, limits, and listener parity

**Files:**
- Create: `transport/contract_tcp_limits_test.go`
- Create: `transport/contract_tcp_observer_test.go`
- Create: `transport/contract_tcp_timeout_error_test.go`
- Create: `transport/epoll_tcp_parity_linux_test.go`
- Modify: `transport/epoll_session_linux.go`
- Modify: `transport/epoll_session_send_linux.go`
- Modify: `transport/epoll_session_read_linux.go`
- Modify: `transport/epoll_listener_linux.go`
- Modify: `transport/epoll_dial_linux.go`
- Modify: `transport/epoll_engine_linux.go`

- [ ] **Step 1: Write shared limit/Stats RED tests**

Backend-neutral TCP cases:

```text
MaxConnections
MaxConnectionsPerPeer
MaxConnectionsPerListener
MaxQueuedBytesTotal
listener Accepted/Rejected/Current
engine opening/active/draining/global queued parity
session payload BytesRX/TX + MessagesRX/TX
Backpressure exactly once per TrySend local-pressure failure
queue gauges zero after finalization
Age freezes after Done
```

Portable subtests must pass immediately; epoll subtests are RED until native accounting seams are complete.

- [ ] **Step 2: Write shared Observer RED tests**

Assert for epoll and portable:

```text
Accept: Session ResourceID + Listener ParentID
Connect success/failure and duration only with observer enabled
Read/Write payload bytes
Backpressure event once per failed TrySend attempt
Close exactly once after stable final Stats/Err
observer saturation never blocks socket progress
observer panic increments EngineStats.ObserverPanics and does not affect Session
```

- [ ] **Step 3: Write timeout/error RED tests**

Cover:

```text
ReadIdle: synchronized open, no data, typed TimeoutReadIdle
ConnectionIdle: no network progress, typed timeout domain
MaxLifetime: fixed age timeout
Write timeout: peer stops reading after connection barrier + tiny send buffer
connection refused: typed OpDial and raw errno reachable
peer reset: first real terminal failure wins derived close fallout
caller cancellation: unchanged direct cause
```

Internal connect deadline tests from Task 2/6 cover deterministic connect-timeout arbitration; do not rely on an external unroutable address as the only oracle.

- [ ] **Step 4: Implement deadline/activity seams**

Rules:

```text
Connect deadline uses original bounded Connect context deadline (DNS + all attempts)
Write deadline starts when a frame becomes writeCurrent and is not reset by partial progress
ReadIdle starts after OnOpen completes; suspend it while that Session's Handler callback is executing; resume after callback completion
ConnectionIdle resets on successful read/write network progress
MaxLifetime never resets
stale generations are harmless
```

- [ ] **Step 5: Implement exact Stats/Observer ownership points**

Reuse `sessionCounters`, `listenerCounters`, `listenerCapacity`, `byteQuota`, `admissionController`, and `sessionStatsSnapshot`. Native address fields are stored once and Stats performs no socket syscalls.

Ordering remains:

```text
RX: decode+validate -> stats -> Observer Read -> Handler task
TX: full physical frame -> release quota/slot -> stats -> Observer Write -> Send ack
close: terminal Err + zero queues + age freeze -> Observer Close -> OnClose task -> Done
```

- [ ] **Step 6: Run shared parity suite + native 20x race**

```bash
go test ./transport -run '^Test.*(Contract|Parity|Observer|Stats|Limit|Timeout|Error)' -count=1
go test -race ./transport -run '^TestEpoll.*(Parity|Observer|Stats|Limit|Timeout|Error)' -count=20
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add transport/contract_tcp_limits_test.go transport/contract_tcp_observer_test.go transport/contract_tcp_timeout_error_test.go transport/epoll_tcp_parity_linux_test.go transport/epoll_session_linux.go transport/epoll_session_send_linux.go transport/epoll_session_read_linux.go transport/epoll_listener_linux.go transport/epoll_dial_linux.go transport/epoll_engine_linux.go
git commit -m "runtime: align epoll TCP runtime semantics"
```

---

### Task 11: Engine Shutdown/Close ownership and cross-reactor stress invariants

**Files:**
- Create: `transport/epoll_engine_shutdown_linux.go`
- Create: `transport/epoll_engine_shutdown_linux_test.go`
- Create: `transport/epoll_tcp_stress_linux_test.go`
- Modify: `transport/epoll_engine_linux.go`
- Modify: `transport/epoll_listener_linux.go`
- Modify: `transport/epoll_session_lifecycle_linux.go`

- [ ] **Step 1: Write RED Engine lifecycle tests**

Required cases:

```go
func TestEpollEngineShutdownStopsListenersThenGracefullyDrainsSessions(t *testing.T) {}
func TestEpollEngineShutdownOwnerCancellationAbortsRemaining(t *testing.T) {}
func TestEpollEngineCloseNeverClosesFDFromCaller(t *testing.T) {}
func TestEpollEngineDoneWaitsForBlockedApplicationOnClose(t *testing.T) {}
func TestEpollEngineDoneDoesNotWaitForBlockedObserver(t *testing.T) {}
```

- [ ] **Step 2: Write RED handoff/connect race tests**

Use deterministic barriers to cover:

```text
accept handoff vs Engine Shutdown
Dial connect completion vs caller cancellation
callback completion vs abort
reactor Wake vs Close
peer FIN vs CloseWrite
write timeout vs reset
listener close during accept flood
```

- [ ] **Step 3: Implement native Engine shutdown owner model**

Mirror portable semantics with native resource types:

```text
first Shutdown caller: running -> draining; snapshot listener/session set
listeners: request close
active sessions: request graceful full shutdown
connecting/setup sessions: abort (no established protocol to drain)
wake every reactor
wait Done
if owning Shutdown ctx ends first: abort remaining with AbortCaller; return caller cause
non-owning concurrent Shutdown caller never steals abort ownership

Close:
state -> aborting once
publish AbortExplicit if no earlier reason
request abort on every listener/session
wake every reactor
return without waiting
```

`shutdownResult` matches portable `ErrClosed` precedence.

- [ ] **Step 4: Add post-shutdown invariant probe**

Package-private test snapshot:

```go
type epollNativeInvariantSnapshot struct {
    ReactorResources int
    ReactorInbox     int
    ReactorRunnable  int
    CallbackReserved int
    EngineSessions   int
    EngineListeners  int
    Admission        admissionSnapshot
}
```

After Done, assert:

```text
ReactorResources == 0
ReactorInbox == 0
ReactorRunnable == 0
CallbackReserved == 0
EngineSessions == 0
EngineListeners == 0
Opening/Active/Draining == 0
GlobalQueuedBytes == 0
```

This is test diagnostics only; do not add public Stats fields.

- [ ] **Step 5: Add short-lived multi-reactor stress**

Create thousands of loopback connections across `Pollers: 4`, exchange one framed message, close from mixed sides, and assert final invariant snapshot. Synchronize completion with WaitGroups/channels; do not use sleeps.

- [ ] **Step 6: Run race/stress GREEN**

```bash
go test -race ./transport -run '^TestEpoll(Engine|Native).*' -count=20
go test ./transport -run '^TestEpollNativeShortLivedConnections' -count=10
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add transport/epoll_engine_shutdown_linux.go transport/epoll_engine_shutdown_linux_test.go transport/epoll_tcp_stress_linux_test.go transport/epoll_engine_linux.go transport/epoll_listener_linux.go transport/epoll_session_lifecycle_linux.go
git commit -m "runtime: harden epoll engine shutdown"
```

---

### Task 12: TCP backend benchmarks, permanent CI gates, exact-head verification, and 6B checkpoint metadata

**Files:**
- Create: `transport/epoll_tcp_benchmark_test.go`
- Modify: `.github/workflows/netpoll-v2.yml`
- Modify: `docs/superpowers/plans/2026-08-22-linux-native-engine-6b-execution.md` only to check completed boxes if the execution workflow tracks them in-repo; otherwise leave plan immutable.

**Produces:** evidence only; no UDP or production-ready claim.

- [ ] **Step 1: Add portable-vs-epoll TCP benchmark harness**

Required benchmark dimensions for 6B:

```text
1 KiB echo throughput
4 KiB echo throughput
64 KiB echo throughput
request/echo latency harness (report distribution outside hard CI gate)
connection setup rate
Send allocation/bytes-op
TrySend allocation/bytes-op
graceful Engine shutdown fan-out
```

Use sub-benchmarks named `portable/...` and `epoll/...` with identical framing/options. Do not add hosted-runner ns/op or percentage-improvement thresholds.

- [ ] **Step 2: Add deterministic hard benchmark checks only**

Allowed hard gates:

```text
existing portable graceful allocation limits unchanged
existing observer-disabled/Stats zero-allocation limits unchanged
native Stats snapshot zero allocations
native disabled-observer event branch does not allocate
no quota/lease/task leak after benchmark teardown
```

Do not hard-gate native-vs-portable speed ratio.

- [ ] **Step 3: Extend Linux Go 1.26 CI**

Add after existing observability/runtimecore loops:

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

Keep Windows/macOS/FreeBSD/GmSSL and existing cross-compile jobs unchanged. Native runtime tests remain Linux-only.

- [ ] **Step 4: Run local/full verification where environment permits**

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

Expected: all commands exit 0.

- [ ] **Step 5: Cross-build from Linux-capable environment**

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c ./transport -o /tmp/transport-linux-amd64.test
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go test -c ./transport -o /tmp/transport-linux-arm64.test
GOOS=windows GOARCH=arm64 CGO_ENABLED=0 go test -c ./transport -o /tmp/transport-windows-arm64.test.exe
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go test -c ./transport -o /tmp/transport-darwin-arm64.test
```

Do not try to execute foreign binaries.

- [ ] **Step 6: Commit CI/benchmark work**

```bash
git add transport/epoll_tcp_benchmark_test.go .github/workflows/netpoll-v2.yml
git commit -m "test: gate epoll TCP runtime"
```

- [ ] **Step 7: Verify exact PR head in Actions**

Required exact-head evidence before declaring 6B complete:

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

- [ ] **Step 8: Update PR #58 and issues #57/#56**

State explicitly:

```text
P1-6B Linux TCP complete
P1-6C UDP not implemented
TLS/WS/WSS still unsupported with no fallback
PR remains Draft while #57 still requires UDP + 6D productionization evidence
```

Do not mark PR Ready and do not close #57/#56/#38 at the 6B checkpoint.
