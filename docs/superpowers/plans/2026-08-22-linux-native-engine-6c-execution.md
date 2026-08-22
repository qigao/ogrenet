# P1-6C Linux Native UDP Execution Plan

> **For Codex/ChatGPT:** Execute this plan with `executing-plans`, TDD, and verification-before-completion. Do not change portable `transport.New()` semantics. Do not add TLS/WS/WSS fallback. Keep PR #58 Draft until the later 6D closure required by #57.

**Goal:** Complete Linux epoll-native UDP for both connected `DialPacket` and unconnected `ListenPacket`, preserving the root `ogrenet.PacketConn` contract and the portable UDP backend as the semantic reference.

**Architecture:** Reuse the existing epoll reactor/inbox/deadline/callback/Engine lifecycle primitives built by P1-6B. Introduce a reactor-owned `epollPacketConn` resource with fixed poller affinity. Callers perform bounded packet admission and copy payload ownership before queue publication; only the owning reactor performs socket/bind/connect/send/sendto/recv/close syscalls. UDP datagrams are atomic writes: no TCP-style partial-write continuation. Connected UDP participates in ReadIdle/ConnectionIdle/MaxLifetime; unconnected `ListenPacket` does not idle-close. Handler callbacks are bounded and serialized per PacketConn. Public UDP capability stays closed until shared parity is green.

**Reference semantics pinned from portable UDP:**

- `Send` / `TrySend` require connected UDP; unconnected returns `ErrNotConnected`.
- `SendTo` / `TrySendTo` require a peer; connected UDP accepts only the configured peer and returns `ErrPeerMismatch` for a different target.
- admission order is packet slot -> byte quota -> copy payload -> bounded queue publication.
- `Send` waits for physical datagram completion; `TrySend` returns after admission/publication and never waits for network I/O.
- caller cancellation after queue publication does not revoke the admitted datagram.
- write completion order is retained ownership release -> TX Stats -> Observer Write -> blocking Send ack.
- write failure is terminal; UDP never continues a partial datagram.
- oversize application sends return `ErrDatagramTooLarge` before queue ownership.
- oversize received datagrams are dropped, increment `DroppedDatagrams`, emit one `EventDrop` with actual datagram bytes, and do not increment RX payload counters or invoke `OnPacket`.
- RX order is copy datagram ownership -> RX Stats -> Observer Read -> serialized `OnPacket`.
- clean explicit Close has `Err()==nil`; first terminal failure wins.
- graceful Engine Shutdown closes the send gate, drains already-admitted datagrams, then cleanly finalizes PacketConn resources.
- connected UDP alone owns ReadIdle/ConnectionIdle/MaxLifetime; unconnected ListenPacket remains alive until explicit Close/Engine shutdown.

---

## Task 1 — Private reactor-owned UDP resource shell and fd adoption

**Files:**
- Create: `transport/epoll_packet_linux.go`
- Create: `transport/epoll_packet_linux_test.go`
- Modify: `transport/epoll_engine_linux.go`
- Modify: `transport/epoll_engine_shutdown_linux.go`

### RED

Add Linux package tests for private helpers, without opening public `ListenPacket`/`DialPacket` yet:

1. unconnected native UDP socket is created/bound on the selected reactor and publishes actual `:0` bound address;
2. connected native UDP socket is created/connected on the selected reactor and stores local/remote snapshots;
3. caller goroutine never closes a reactor-owned fd after `Poller.Add`;
4. `epollPacketConn` satisfies `ogrenet.PacketConn` surface for identity/address/Done/Err/Stats/Close;
5. explicit Close only publishes intent; package test hook proves physical `poller.Del` / `unix.Close` occurs on owning reactor;
6. fixed reactor affinity does not change after adoption.

Run:

```bash
go test ./transport -run '^TestEpollNativePacket(Adopt|Bind|Dial|Close|Affinity)' -count=1
```

Expected RED: missing private packet resource/native helpers.

### GREEN

Implement:

- `epollManagedPacket` kind;
- `epollPacketConn` with Engine/reactor/inbox node/id/fd/registered/endpoint/local/remote/handler;
- reactor-only socket creation using `SOCK_DGRAM|SOCK_NONBLOCK|SOCK_CLOEXEC`;
- native UDP sockaddr conversion using existing native address helpers where possible;
- unconnected bind + actual bound address snapshot;
- connected UDP `connect(2)` on reactor; preserve caller context cause and existing Connect timeout/error taxonomy;
- `Poller.Add(Readable|PeerClosed|Error|EdgeTriggered)` as fd ownership-transfer point;
- stored address snapshots for caller-safe `LocalAddr`/`RemoteAddr`/`Stats`;
- Close publication + reactor-only final DEL/close;
- Engine managed-resource integration, including packet classification in native shutdown snapshots.

Do **not** route public `ListenPacket` / `DialPacket` yet.

Run targeted test + race loop 20x before commit.

Commit:

```text
runtime: add reactor-owned epoll UDP resources
```

---

## Task 2 — Packet admission and Send/TrySend/SendTo/TrySendTo ownership

**Files:**
- Create: `transport/epoll_packet_send_linux.go`
- Create: `transport/epoll_packet_send_linux_test.go`
- Modify: `transport/epoll_packet_linux.go`

### RED

Pin caller-side semantics:

- unconnected `Send` / `TrySend` -> `ErrNotConnected`;
- nil peer -> `ErrPeerRequired`;
- connected `SendTo` same peer succeeds; different peer -> `ErrPeerMismatch`;
- oversize send -> typed `OpSend` wrapping `ErrDatagramTooLarge`, without acquiring quota/slot;
- payload is detached before queue publication and may be mutated by caller afterward;
- `TrySend` never waits across packet slot, byte quota, and queue admission;
- one failed `TrySend` pressure attempt increments authoritative Backpressure Stats once and emits at most one EventBackpressure;
- queue/byte Stats read directly from retained ownership sources;
- `Send(ctx)` cancellation after publication returns exact caller cause but does not revoke packet ownership.

### GREEN

Implement caller side:

```text
validate mode/peer/size
  -> sendGate enter
  -> packet slot
  -> local byte quota(parent = Engine global bytes)
  -> copy Packet.Data
  -> bounded queue publication
  -> reactor signal
```

Rules:

- queue capacity = existing `writeQueue`; slots = `writeQueue+1` (current + queued), matching portable packet accounting;
- no codec token for UDP;
- resolve/copy `*net.UDPAddr` before publication; non-UDP `net.Addr` may use the same portable resolve semantics outside reactor because peer-name resolution is caller work, not fd I/O;
- publication is packet ownership-transfer point;
- blocking Send gets an ack channel; TrySend does not.

Run targeted race 20x.

Commit:

```text
runtime: add epoll UDP send admission
```

---

## Task 3 — Reactor-only atomic datagram write and fixed write deadline

**Files:**
- Modify: `transport/epoll_packet_send_linux.go`
- Modify: `transport/epoll_deadline_linux.go` only if a packet-specific dispatch seam is required; prefer reusing `epollDeadlineWrite`.
- Extend: `transport/epoll_packet_send_linux_test.go`

### RED

Tests:

- owning reactor alone performs `send` / `sendto`;
- `EINTR` retries;
- `EAGAIN/EWOULDBLOCK` enables EPOLLOUT and yields to other same-reactor work;
- write readiness retry does not re-admit/copy the datagram;
- successful datagram must report `n == len(data)`; any short positive result is terminal protocol/runtime failure (no partial continuation);
- one fixed write deadline is attached to the current datagram and is not refreshed by readiness retries;
- write timeout releases current + queued quota/slots and becomes typed `OpWrite` / `TimeoutWrite` first-terminal error;
- completion ordering: release quota/slot -> TX Stats -> EventWrite -> Send ack.

### GREEN

Implement reactor write state:

- current `packetOutbound` + blocked/interested/write generation;
- connected path uses `unix.Sendto(fd, data, 0, nil)` or equivalent connected datagram syscall; unconnected uses sockaddr target;
- atomic datagram completion only;
- EPOLLOUT interest is temporary and removed after completion/terminal failure;
- fixed write deadline via existing reactor heap/generation invalidation;
- first-terminal owner preserved across Close/Engine abort/write failure;
- failure releases all queued ownership exactly once.

Run targeted race 20x.

Commit:

```text
runtime: add reactor-owned epoll UDP writes
```

---

## Task 4 — ET receive/drain, oversize detection, serialized PacketHandler

**Files:**
- Create: `transport/epoll_packet_read_linux.go`
- Create: `transport/epoll_packet_callback_linux.go`
- Create: `transport/epoll_packet_read_linux_test.go`
- Modify: `transport/epoll_packet_linux.go`

### RED

Pin:

- Readable edge drains multiple datagrams to `EAGAIN` within reactor op/byte budgets and requeues on fairness yield;
- callback capacity is reserved before consuming a callback-producing datagram;
- one `OnPacket` callback per PacketConn at a time;
- blocked PacketConn handler does not block reactor I/O or unrelated PacketConn callback progress when worker capacity exists;
- worker saturation enters `workerBlocked` and capacity release retries without another peer datagram;
- Packet.Data owns detached bytes across later receives;
- RX Stats + EventRead are visible before Handler entry;
- actual peer address is delivered for ListenPacket; connected receive uses configured remote snapshot;
- oversized datagram is consumed/dropped atomically, actual datagram size is reported in EventDrop, `DroppedDatagrams++`, RX counters unchanged, no Handler call;
- ET readiness arriving before first Handler capacity is available is preserved locally and retried.

### GREEN

Implement:

- `recvmsg`/`recvfrom` reactor path with `MSG_DONTWAIT`; use Linux truncation information (`MSG_TRUNC`) or an equivalent deterministic strategy so actual oversize datagram byte length is known and the full datagram is consumed once;
- scratch buffer sized to configured `maxDatagramBytes` (plus only what is needed for deterministic truncation detection);
- reserve callback executor capacity before consuming a normal deliverable datagram;
- copy payload into Handler-owned slice before buffer reuse;
- Stats -> EventRead -> `OnPacket` ordering;
- one packet callback in flight per PacketConn; completion signal returns ownership to reactor;
- callback panic isolation follows existing bounded executor behavior;
- terminal ordering is stable fd/queue/deadline/Stats/Observer -> serialized `OnClose` -> Done -> remove managed.

Run targeted race 20x.

Commit:

```text
runtime: add epoll UDP receive callbacks
```

---

## Task 5 — Connected UDP runtime deadlines and typed error parity

**Files:**
- Create: `transport/epoll_packet_timeout_linux.go`
- Create: `transport/epoll_packet_timeout_linux_test.go`
- Modify: `transport/epoll_packet_linux.go`

### RED

Connected DialPacket:

- ReadIdle closes with typed `TimeoutReadIdle`;
- ConnectionIdle refreshes only on real network read/write progress;
- MaxLifetime is fixed from PacketConn birth and traffic cannot extend it;
- fixed current-datagram Write timeout remains independent of runtime idle generations;
- stale deadline heap entries are ignored by generation mismatch;
- explicit Close first wins over later timeout/error publication.

Unconnected ListenPacket:

- ReadIdle/ConnectionIdle/MaxLifetime options never auto-close it.

Error tests:

- caller cancellation is exact `context.Cause`;
- connected ICMP/reset-style reachable errors remain typed operational errors where Linux exposes them;
- raw errno/cause remains discoverable through errors.Is/As;
- first terminal failure wins and `OnClose`/Err agree.

### GREEN

Reuse `epollDeadlineReadIdle`, `epollDeadlineConnectionIdle`, `epollDeadlineMaxLifetime`, and `epollDeadlineWrite` with packet-specific generation methods if necessary. No packet timer goroutines.

Commit:

```text
runtime: add epoll UDP deadlines and errors
```

---

## Task 6 — Engine graceful drain/abort and final PacketConn completion barrier

**Files:**
- Modify: `transport/epoll_engine_shutdown_linux.go`
- Modify: `transport/epoll_packet_linux.go`
- Create: `transport/epoll_packet_shutdown_linux_test.go`
- Modify: `transport/epoll_engine_invariant_linux_test.go`

### RED

- graceful Engine Shutdown changes packet send gate to draining before shutdown publication;
- datagrams already admitted before drain are physically written before clean PacketConn close;
- new sends after drain begins are rejected;
- Engine.Shutdown owner context expiry publishes exact AbortCaller and aborts remaining packet resources;
- non-owner cancellation cannot steal shutdown ownership;
- Engine.Close is immediate AbortExplicit and does not wait for blocked `OnClose`, while Engine.Done waits Handler completion;
- PacketConn OnClose exactly once and precedes PacketConn.Done;
- Engine.Done waits all packet queue/quota/callback/resource ownership to release;
- zero-invariant snapshot includes packet resources and leaves global queued bytes zero.

### GREEN

Extend native shutdown grouping with packet resources:

- `prepareEngineDrain`: transition packet gate to drain mode;
- `requestEngineShutdown`: reactor drains current + queued datagrams and then cleanly terminalizes PacketConn;
- `requestEngineAbort`: immediate terminal cleanup on reactor;
- OnClose completion barrier mirrors the TCP callback ownership rule but has no half-close state.

Commit:

```text
runtime: integrate epoll UDP engine shutdown
```

---

## Task 7 — Shared UDP parity + public capability flip

**Files:**
- Modify: `transport/contract_harness_test.go` if UDP profile extension is needed
- Modify: `transport/contract_native_linux_test.go`
- Add/extend shared UDP contract files for graceful/limits/Stats/Observer/timeouts/errors without duplicating portable-only tests
- Modify: `transport/epoll_engine_linux.go`

### RED

Add:

```go
func TestEpollPublicUDPContracts(t *testing.T) {
    runEngineContracts(t, epollFactory(contractProfile{TCP: true, UDP: true}))
}
```

or the repository's current equivalent shared UDP harness. Confirm RED is exactly public `ErrProtocolUnsupported` or parity gaps, not test harness failure.

Shared parity must include at minimum:

- connected DialPacket + unconnected ListenPacket echo;
- method legality (`ErrNotConnected`, `ErrPeerRequired`, `ErrPeerMismatch`);
- Send/TrySend/SendTo/TrySendTo backpressure and quota release;
- oversize send/drop semantics;
- exact PacketConn Stats and Engine global queued-byte gauges;
- Observer Read/Write/Drop/Backpressure/Close ordering and isolation;
- connected runtime timeouts; ListenPacket no-idle-close;
- exact cancellation and terminal error ownership;
- graceful Engine drain and Close/Done barriers.

### GREEN

Only after shared parity passes:

- route public `epollEngine.ListenPacket` / `DialPacket` for `SchemeUDP` to native private helpers;
- keep non-UDP packet schemes as protocol mismatch/unsupported per current root contract;
- TCP unchanged;
- TLS/WS/WSS unchanged and no fallback.

Commit:

```text
runtime: advertise epoll native UDP
```

---

## Task 8 — Deterministic UDP race/stress evidence

**Files:**
- Create: `transport/epoll_packet_race_linux_test.go`
- Create: `transport/epoll_packet_stress_linux_test.go` (`//go:build linux && !race` for heavy stress)
- Modify: `.github/workflows/netpoll-v2.yml`

### Deterministic race barriers

Cover:

- Send/TrySend publication vs Close/Engine Shutdown;
- write timeout vs explicit Close/ICMP error first-terminal owner;
- Readable edge vs callback-capacity saturation;
- worker capacity release-before-block-registration lost wake;
- unconnected SendTo peer snapshot ownership vs caller mutation;
- reactor wait arm vs cross-goroutine packet publication;
- close while ET receive drain is active;
- stale write/runtime deadline entries after resource close;
- graceful drain while queue/global quota are saturated.

Run:

```bash
go test -race ./transport -run '^TestEpoll.*(Packet|UDP)' -count=20
```

### Heavy stress

Use 4 reactors and public UDP APIs. Exercise thousands of datagrams across both:

- connected client PacketConns;
- one or more unconnected ListenPacket sockets;
- mixed small/medium payloads;
- concurrent Send/TrySend/SendTo/TrySendTo;
- callback saturation and backpressure;
- alternating explicit Close and Engine Shutdown.

Use per-operation progress deadlines, not one global throughput stopwatch. Final state must satisfy zero invariants: no reactor resources/inbox/runnable/workerBlocked entries, no callback reservations, no managed packet resources, zero global queued bytes.

Keep heavy stress non-race and run repeated count separately.

Commit:

```text
test: stress epoll native UDP
```

---

## Task 9 — UDP benchmark matrix, permanent gates, P1-6C closure

**Files:**
- Create: `transport/epoll_udp_benchmark_test.go`
- Modify: `.github/workflows/netpoll-v2.yml`
- Update PR #58 / #57 / #56 / #38 checkpoint comments after exact-head verification

### Benchmarks

Identical public API portable/epoll sub-benchmarks:

- connected UDP round-trip / packets-per-second: 64 B and 1200 B;
- unconnected SendTo/echo: 64 B and 1200 B;
- connected setup rate;
- Epoll Send / TrySend;
- Engine Shutdown fanout;
- PacketConn Stats snapshot and disabled Observer allocation probes.

Preallocate latency samples; report p50/p95/p99 as informational metrics only. No hosted-runner ns/op ratio threshold.

### Deterministic hard gates

- preserve all existing portable allocation gates;
- native disabled Observer must remain zero-allocation in repeated samples;
- PacketConn Stats allocation gate is based only on measured stable evidence from benchmark smoke (do not guess threshold);
- teardown invariant gate must remain zero;
- no fd/resource/quota leak.

### CI

Keep permanent broad Linux native race surface:

```bash
go test -race ./transport -run '^TestEpoll' -count=20
```

Heavy TCP and UDP stress remain separate `!race` repeated gates.

Add UDP benchmark smoke:

```bash
go test ./transport -run '^$' -bench 'BenchmarkUDPBackend|BenchmarkEpollUDP' -benchmem -benchtime=1x
```

Retain transport/backend cross-build matrix; never execute foreign binaries.

### Full exact-head verification

```bash
gofmt -w .
git diff --exit-code
go mod tidy
git diff --exit-code -- go.mod go.sum
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
go test -race ./transport -run '^TestEpoll' -count=20
go test ./transport -run '^TestEpoll.*(Stress|ShortLived)' -count=10
go test ./transport -run '^$' -bench 'BenchmarkTCPBackend|BenchmarkEpoll|BenchmarkUDPBackend|BenchmarkEpollUDP' -benchmem -benchtime=1x
```

Also require Windows/macOS, FreeBSD runtime, GmSSL, and all configured cross-build jobs green on the exact head.

### Closure

Record in PR/issues:

- P1-6C native UDP complete;
- Linux epoll now publicly supports native TCP + UDP only;
- TLS/WS/WSS explicit unsupported/no fallback;
- P1-6D final evidence/umbrella closure remains outstanding if still required by #57;
- PR #58 remains Draft until the umbrella issue's final readiness conditions are met;
- do not close #57/#56/#38 unless their complete acceptance criteria are independently verified.

Commit:

```text
test: gate epoll UDP runtime
```
