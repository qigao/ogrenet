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

## Execution checkpoint

Tasks 1-4 are implemented on `feat/linux-native-engine` and verified by exact implementation head `e49879ed632f9baad93c348bf608ca2b2586af00` / workflow #441 (`32540064652`) success. The following Tasks 1-4 sections are retained as the implementation contract and historical RED/GREEN checklist. Task 5 is the next executable task. No native TCP listener/session fd support is claimed at this checkpoint; public epoll `Listen`/`Dial` still return `ErrProtocolUnsupported` for matching stream protocols.

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

**Status:** Complete.

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
```

Required behavior remains: deduplicated intrusive inbox, no lost wake, stale event IDs ignored, duplicate IDs rejected, and locally requeued runnable work continues without a second readiness edge.

---

### Task 2: Generation-based native deadline scheduler

**Status:** Complete.

Uses `container/heap`, live-resource lookup, generation invalidation rather than O(n) deletion, and the nearest live deadline as the epoll wait timeout. Empty heap waits indefinitely; an expired live deadline produces zero timeout.

---

### Task 3: Exact-bounded setup/callback executor

**Status:** Complete.

The executor has exactly `CallbackWorkers` worker goroutines, one one-slot handoff per worker, a fixed `CallbackQueue` ring, and a reservation bound of `CallbackWorkers + CallbackQueue`. Submission never waits for user task completion. Capacity release is signaled outside the executor mutex.

---

### Task 4: Real epoll Engine shell, worker wake, IDs, admission, Stats, final barrier

**Status:** Complete.

The 6A scaffold has been replaced by the real Engine shell. It starts fixed reactors + bounded workers, shares authoritative admission/Observer Stats formatting, uses a CAS resource ID allocator that never emits zero or `math.MaxUint64`, and finalizes only after quiescence. Empty `Close` stops reactors, closes pollers, stops idle workers, stops Observer delivery without waiting for a user Observer callback, then closes `Done`.

Worker-capacity retry uses a release-before-block-safe handshake: after publishing a resource to the reactor worker-blocked list, the reactor rechecks executor capacity and self-signals if capacity was released immediately before publication.

---

### Task 5: Native listen socket, fd helpers, bootstrap Session/setup, accept/handoff

**Status:** Next.

**Files:** create `epoll_fd_linux.go`, `epoll_session_linux.go`, `epoll_session_task_linux.go`, `epoll_listener_linux.go`, tests/helpers.

**Produces:** private `listenNativeTCP`; Listener satisfies `ogrenet.Listener`; public Engine Listen stays gated until Task 9.

- [ ] Write RED IPv4/IPv6 sockaddr, resolver bypass, and TCP option tests.
- [ ] Implement reactor-owned socket/bind/listen and stored Listener API.
- [ ] Add bootstrap Session + bounded worker codec setup; custom factories never execute on a reactor.
- [ ] Add deterministic accept/handoff tests, per-listener admission tests, setup failure cleanup tests, and Accept event ID/ParentID assertions.
- [ ] Implement `Accept4` fairness and exact ownership transfer at target `Poller.Add`.
- [ ] Run focused tests/race and exact-head full regression before Task 6.

Exact ownership requirements remain:

```text
listener reactor: Accept4 -> addresses -> opening lease -> TCP config -> ID/target -> handoff
handoff owner: accepted fd until target Poller.Add succeeds
target reactor: Poller.Add success is fd ownership transfer
registration/setup failure: one close + one lease release + one managed-resource removal
setup success: lease activate -> listener Accepted++ -> EventAccept(child ID,parent listener ID)
```

---

### Task 6: Caller DNS + reactor-owned non-blocking Dial/connect

**Status:** Pending Task 5.

Literal IP bypasses resolver; hostname resolution preserves resolver order; one bounded Connect context spans DNS + sequential attempts. Owning reactor performs socket/connect, EINPROGRESS registration, and `SO_ERROR` completion. Caller cancellation only publishes state + wakes reactor and never closes/mutates the fd.

---

### Task 7: Send/TrySend, codec token, partial write, fixed write deadline

**Status:** Pending Task 6.

Caller performs validation/admission/codec/encode/queue ownership; reactor alone physically writes. Partial writes retain quota/frame ownership until full completion or terminal cleanup. EAGAIN enables writable interest; fairness yields local-requeue. TX ordering is quota/slot release -> Stats -> Observer -> Send ack.

---

### Task 8: Session public lifecycle, reactor-only close/shutdown, half-close

**Status:** Pending Task 7.

`epollSession` must satisfy `ogrenet.HalfCloseSession`. Caller methods publish lifecycle state and signal only; `Poller.Del`, `unix.Shutdown`, and `unix.Close` stay reactor-owned. Clean peer FIN closes read-half without terminal error and local write remains usable.

---

### Task 9: OnOpen/read/decode/callback serialization, Message ownership, OnClose barrier, public TCP flip

**Status:** Pending Task 8.

Worker tasks cover OnOpen/OnMessage/OnClose with at most one Handler task per Session. Reactor reserves worker capacity before callback-producing reads. Complete decoded messages copy `Data` before asynchronous delivery. Callback completion requeues the resource so ET readiness is never lost. Only after this task may public epoll TCP capability flip to supported.

---

### Task 10: Shared graceful/limits/timeouts/errors/Stats/Observer parity

**Status:** Pending Task 9.

Portable and epoll factories run the same public contracts for half-close, shutdown, limits, global/local byte quota, Stats, Observer ordering/saturation/panics, timeout domains, typed errors, and first-terminal ownership.

---

### Task 11: Graceful Engine Shutdown/Close and cross-reactor stress

**Status:** Pending Task 10.

Engine drain order is:

```text
all Session prepareEngineDrain() -> close listeners -> publish Session shutdown/abort -> wake reactors
```

The owner caller controls abort escalation. Post-Done invariant snapshot must show no reactor resources/inbox/runnable/worker-blocked tasks, no callback reservations, no managed resources, no admission leases, and zero global queued bytes. Stress uses four pollers and thousands of short-lived loopback Sessions without sleep-based correctness oracles.

---

### Task 12: TCP benchmarks, permanent CI gates, exact-head 6B checkpoint

**Status:** Pending Task 11.

Add identical portable/native TCP throughput/connect/latency/Send/TrySend/shutdown benchmarks. Hosted CI uses no hard performance ratio; deterministic allocation/leak invariants may be hard gates. Go 1.26 gets a native TCP 20x race loop and native benchmark smoke. At the 6B checkpoint PR #58 remains Draft, P1-6C UDP remains outstanding, and #57/#56/#38 stay open.
