# Runtime Timeout and Deadline Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a complete production timeout/deadline model for the portable `transport.Engine` across TCP, TLS, WS, WSS, and UDP without changing `Session` or `PacketConn` public interfaces.

**Architecture:** Introduce one Engine-wide `Timeouts` policy plus a small typed timeout taxonomy, then apply it through existing transport ownership paths. TCP/TLS and UDP use independent read/write socket deadlines; WS/WSS use bounded contexts; optional connection-idle/max-lifetime behavior is implemented by one internal per-resource watchdog that participates in the existing `Done()` barrier.

**Tech Stack:** Go 1.25/1.26, standard `context`, `net`, `crypto/tls`, `sync`, `sync/atomic`, `time`, `github.com/coder/websocket`, existing `transport` admission/quota lifecycle, GitHub Actions cross-platform matrix.

**Spec:** `docs/superpowers/specs/2026-08-21-runtime-timeout-model-design.md`

## Global Constraints

- Scope is limited to the portable `transport.Engine`; do not modify HTTP/QUIC client runtime behavior.
- `Session` and `PacketConn` public interfaces remain unchanged.
- Effective defaults: Connect 10s, Handshake 10s, Write 10s, ReadIdle disabled, ConnectionIdle disabled, MaxLifetime disabled.
- Negative timeout durations are rejected before Engine creation succeeds.
- Existing `WithTLSHandshakeTimeout` and `WebSocketConfig` handshake/write options remain supported.
- Effective precedence is protocol-specific explicit override > `Timeouts` base > production default and is independent of Option call order.
- Caller context cancellation/deadline wins deterministically over Engine Connect/Handshake timeout.
- Engine-generated timeouts use `TimeoutError`; they do not masquerade as `context.DeadlineExceeded`.
- TCP/TLS and UDP must use `SetReadDeadline` / `SetWriteDeadline`, never `SetDeadline`.
- Partial TCP/TLS write progress refreshes ConnectionIdle but never extends one-frame WriteTimeout.
- Handler execution time is excluded from ReadIdle.
- WS ping/pong does not refresh business ConnectionIdle.
- UDP `ListenPacket` does not receive ReadIdle, ConnectionIdle, or MaxLifetime policy.
- First terminal cause wins through existing exact-once close ownership.
- Timeout cleanup must preserve #39/#40 admission, quota, queue-slot, socket, goroutine, and `Done()` invariants.
- No broad P0-4 typed error model, graceful drain, observer API, native Engine work, or protocol expansion is part of this plan.

---

## File Structure

- Create `transport/timeouts.go`: public timeout policy, normalization, protocol-effective helpers, typed timeout errors, caller-vs-runtime timeout mapping.
- Create `transport/timeouts_test.go`: policy defaults, validation, override order, error taxonomy, mapping helpers.
- Create `transport/activity_clock.go`: optional ConnectionIdle/MaxLifetime activity clock and one-watchdog state machine.
- Create `transport/activity_clock_test.go`: timer/touch/close races and deterministic deadline arbitration.
- Modify `transport/options.go`: store base timeout policy and explicit protocol override flags without Option-order dependence.
- Modify `transport/errors.go`: expose `ErrTimeout` if the implementation keeps the sentinel alongside existing package errors.
- Modify `transport/tcp.go`: bounded DialContext helper for TCP.
- Modify `transport/tls.go`: effective TLS handshake policy and deterministic caller-context precedence.
- Modify `transport/engine_stream_dial.go`: connect/handshake mapping and stream timeout policy injection.
- Modify `transport/engine_stream_listener.go`: accepted TLS handshake mapping and policy injection.
- Modify `transport/conn.go`: TCP/TLS read/write deadline handling, activity touch, watchdog lifecycle.
- Modify `transport/websocket_dial_admission.go`: bounded WS/WSS TCP connect and TLS handshake stages.
- Modify `transport/websocket_client.go`: effective WebSocket upgrade timeout and wsSession policy injection.
- Modify `transport/websocket_server.go`: effective WebSocket server handshake timeout mapping.
- Modify `transport/websocket.go`: WS read/write timeout mapping, activity touch, watchdog lifecycle.
- Modify `transport/packet.go`: UDP Connect/Write/ReadIdle and connected-socket watchdog policy.
- Create `transport/timeout_integration_test.go`: deterministic real-socket TCP/TLS/UDP timeout behavior and cleanup.
- Create `transport/websocket_timeout_integration_test.go`: WS/WSS timeout behavior including ping/pong idle semantics.
- Create `transport/timeout_race_test.go`: timeout-vs-send/close/shutdown races and accounting assertions.
- Create `transport/timeout_benchmark_test.go`: activity touch and disabled/enabled policy overhead.
- Create `docs/runtime-timeouts.md`: public semantics, defaults, precedence, protocol matrix, error handling.

---

### Task 1: Timeout policy, typed errors, and option precedence

**Files:**
- Create: `transport/timeouts.go`
- Create: `transport/timeouts_test.go`
- Modify: `transport/options.go`
- Modify: `transport/errors.go`

**Interfaces:**
- Produces:
  - `type Timeouts struct { Connect, Handshake, Write, ReadIdle, ConnectionIdle, MaxLifetime time.Duration }`
  - `func WithTimeouts(Timeouts) Option`
  - `var ErrTimeout error`
  - `type TimeoutKind uint8`
  - constants `TimeoutConnect`, `TimeoutHandshake`, `TimeoutWrite`, `TimeoutReadIdle`, `TimeoutConnectionIdle`, `TimeoutMaxLifetime`
  - `type TimeoutError struct { Kind TimeoutKind; Cause error }`
  - internal normalized policy stored on `config`
  - internal helpers returning effective TLS/WS handshake/write values without Option-order dependence.
- Consumes: existing `Option`, `config`, `WithTLSHandshakeTimeout`, `WithWebSocketConfig`, `ErrInvalidTimeout`.

- [ ] **Step 1: Write failing policy/error tests**

Add tests that assert exact defaults and zero/negative semantics:

```go
func TestTimeoutDefaults(t *testing.T) {
    cfg := defaultConfig()
    got, err := normalizeTimeouts(cfg.timeouts)
    if err != nil { t.Fatal(err) }
    if got.Connect != 10*time.Second || got.Handshake != 10*time.Second || got.Write != 10*time.Second {
        t.Fatalf("bounded defaults = %+v", got)
    }
    if got.ReadIdle != 0 || got.ConnectionIdle != 0 || got.MaxLifetime != 0 {
        t.Fatalf("idle/lifetime defaults = %+v", got)
    }
}

func TestTimeoutValidationRejectsNegative(t *testing.T) {
    cases := []Timeouts{
        {Connect: -time.Second}, {Handshake: -time.Second}, {Write: -time.Second},
        {ReadIdle: -time.Second}, {ConnectionIdle: -time.Second}, {MaxLifetime: -time.Second},
    }
    for _, tt := range cases {
        if _, err := New(WithTimeouts(tt)); !errors.Is(err, ErrInvalidTimeout) {
            t.Fatalf("New(%+v) = %v, want ErrInvalidTimeout", tt, err)
        }
    }
}
```

Add Option-order tests that build two Engines with reversed `WithTimeouts` / protocol-specific option order and assert identical effective TLS and WS values.

Add `TimeoutError` tests:

```go
func TestTimeoutErrorContract(t *testing.T) {
    cause := errors.New("root")
    err := &TimeoutError{Kind: TimeoutWrite, Cause: cause}
    if !errors.Is(err, ErrTimeout) || !errors.Is(err, cause) { t.Fatal(err) }
    var te *TimeoutError
    if !errors.As(err, &te) || te.Kind != TimeoutWrite || !te.Timeout() || te.Temporary() {
        t.Fatalf("timeout contract = %#v", te)
    }
}
```

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
go test ./transport -run 'TestTimeout(Default|Validation|Error|Option)' -count=1
```

Expected: compile failure for undefined `Timeouts`, `TimeoutError`, or effective timeout helpers.

- [ ] **Step 3: Implement minimal policy and taxonomy**

Implement `timeouts.go` with exact defaults:

```go
const (
    defaultConnectTimeout   = 10 * time.Second
    defaultHandshakeTimeout = 10 * time.Second
    defaultWriteTimeout     = 10 * time.Second
)

type Timeouts struct {
    Connect        time.Duration
    Handshake      time.Duration
    Write          time.Duration
    ReadIdle       time.Duration
    ConnectionIdle time.Duration
    MaxLifetime    time.Duration
}
```

Normalize base fields so bounded zero values become defaults and idle/lifetime zero remains disabled. Add explicit override flags to `config`, for example:

```go
type timeoutOverrides struct {
    tlsHandshake bool
    wsHandshake  bool
    wsWrite      bool
}
```

Set those flags in existing protocol-specific Options. Make effective getters consult flags rather than Option order.

Implement `TimeoutError` so `Unwrap()` returns `Cause` and `Is(ErrTimeout)` returns true even when `Cause` is nil.

- [ ] **Step 4: Run Task 1 tests and package regression**

Run:

```bash
go test ./transport -run 'TestTimeout|TestTLS|TestWebSocketConfig' -count=1
go vet ./transport
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add transport/timeouts.go transport/timeouts_test.go transport/options.go transport/errors.go
git commit -m "transport: add timeout policy and typed timeout errors"
```

---

### Task 2: Activity watchdog and TCP/TLS connect/read/write deadlines

**Files:**
- Create: `transport/activity_clock.go`
- Create: `transport/activity_clock_test.go`
- Modify: `transport/tcp.go`
- Modify: `transport/tls.go`
- Modify: `transport/engine_stream_dial.go`
- Modify: `transport/engine_stream_listener.go`
- Modify: `transport/conn.go`
- Create: `transport/timeout_integration_test.go`

**Interfaces:**
- Consumes Task 1 `Timeouts`, effective timeout helpers, `TimeoutError`.
- Produces internal:
  - `newActivityClock(connectionIdle, maxLifetime time.Duration) *activityClock`
  - lock-free `touch()`
  - watchdog loop that receives `closing <-chan struct{}` and terminal callback
  - operation helper that distinguishes caller context expiration from Engine timeout.

- [ ] **Step 1: Write failing activity-clock tests**

Use short deterministic durations and explicit wake coordination. Cover:

```go
func TestActivityClockTouchWinsOldTimerTick(t *testing.T) { /* touch immediately before old deadline; no close */ }
func TestActivityClockMaxLifetimeWinsEqualDeadline(t *testing.T) { /* equal deadlines => TimeoutMaxLifetime */ }
func TestActivityClockDisabledNeedsNoClock(t *testing.T) { /* newActivityClock(0,0) == nil */ }
```

The timer-fire test must wait beyond the stale deadline and assert that a fresh touch extended only ConnectionIdle, not MaxLifetime.

- [ ] **Step 2: Write failing stream integration tests**

Add deterministic tests for:

1. blocked TCP writer against a peer that stops reading -> Session closes with `TimeoutWrite`;
2. silent TCP peer -> `TimeoutReadIdle`;
3. partial inbound bytes before each idle deadline keep ReadIdle alive;
4. `OnMessage` intentionally sleeps longer than ReadIdle but does not trigger ReadIdle while handler runs;
5. bidirectional progress refreshes ConnectionIdle;
6. continuous traffic does not extend MaxLifetime;
7. slow TLS handshake -> `TimeoutHandshake` and admission counters return to zero;
8. caller deadline shorter than Engine handshake timeout -> caller `context.DeadlineExceeded`, not `TimeoutHandshake`.

Use local loopback servers or `net.Pipe` where the OS deadline behavior is faithfully testable. For write-deadline behavior, prefer real TCP loopback with a tiny socket receive buffer / non-reading peer rather than a mock writer.

- [ ] **Step 3: Run focused tests and verify RED**

Run:

```bash
go test ./transport -run 'TestActivityClock|TestTCP.*Timeout|TestTLS.*Timeout' -count=1
```

Expected: failures because deadlines/watchdog are not yet wired.

- [ ] **Step 4: Implement activity clock**

Implement one timer per enabled resource. Store monotonic elapsed nanoseconds from `born`, use capacity-one non-blocking wake channel, and re-check the deadline after timer fire before closing.

Implement a stop/drain/reset helper and ensure the watchdog selects on `closing`.

- [ ] **Step 5: Implement bounded TCP Dial and TLS handshake mapping**

Wrap TCP Dial in an Engine timeout child context and map return values with deterministic precedence:

```go
if cause := context.Cause(caller); cause != nil { return cause }
if errors.Is(context.Cause(opctx), context.DeadlineExceeded) { return &TimeoutError{Kind: TimeoutConnect, Cause: err} }
return err
```

Apply the same pattern to TLS handshake, using the effective TLS handshake timeout.

- [ ] **Step 6: Wire stream policy into `conn`**

Add effective write/read-idle/activity policy fields to `conn`.

Reader loop rule:

```go
if readIdle > 0 { raw.SetReadDeadline(time.Now().Add(readIdle)) }
n, err := raw.Read(buf)
if readIdle > 0 { raw.SetReadDeadline(time.Time{}) }
```

Touch activity on `n > 0` before decode/handler work. Map actual deadline expiration to `TimeoutReadIdle` only when the connection was not already closing.

Writer loop: set one write deadline before `writeAll`, keep it across all partial writes, clear it afterward, touch activity on every partial progress, and map deadline expiration to `TimeoutWrite`.

Refactor `writeAll` to accept an optional progress callback if needed:

```go
func writeAll(w io.Writer, p []byte, progress func(int)) error
```

Do not allocate a closure on the default disabled-activity path; branch once and call a no-progress helper if necessary.

Include the watchdog in `c.loops` only when activity policy is enabled.

- [ ] **Step 7: Run focused stream tests plus race**

Run:

```bash
go test ./transport -run 'TestActivityClock|TestTCP.*Timeout|TestTLS.*Timeout|Test.*Idle|Test.*Lifetime' -count=1
go test -race ./transport -run 'TestActivityClock|TestTCP.*Timeout|TestTLS.*Timeout' -count=1
```

Expected: PASS with no leaked admission/global-byte counters.

- [ ] **Step 8: Commit**

```bash
git add transport/activity_clock.go transport/activity_clock_test.go transport/tcp.go transport/tls.go transport/engine_stream_dial.go transport/engine_stream_listener.go transport/conn.go transport/timeout_integration_test.go
git commit -m "transport: enforce stream connect and I/O deadlines"
```

---

### Task 3: Apply timeout policy to WS/WSS

**Files:**
- Modify: `transport/websocket_dial_admission.go`
- Modify: `transport/websocket_client.go`
- Modify: `transport/websocket_server.go`
- Modify: `transport/websocket.go`
- Create: `transport/websocket_timeout_integration_test.go`

**Interfaces:**
- Consumes Task 1 effective WS handshake/write helpers and `TimeoutError`.
- Consumes Task 2 optional activity clock.
- Produces WS/WSS behavior consistent with the spec without changing websocket public contracts.

- [ ] **Step 1: Write failing WS/WSS tests**

Cover:

```text
WS TCP connect timeout -> TimeoutConnect
WSS TLS handshake timeout -> TimeoutHandshake
WS/WSS slow HTTP upgrade -> TimeoutHandshake
WS write timeout -> TimeoutWrite
WS read idle -> TimeoutReadIdle
WS business read/write refresh ConnectionIdle
WS ping/pong alone does not refresh ConnectionIdle
WS MaxLifetime closes despite traffic
caller context shorter than upgrade timeout returns caller cause
```

Use local deterministic servers. For ping/pong idle, enable a short `PingInterval` while configuring a longer but finite ConnectionIdle, exchange no business messages, and assert `TimeoutConnectionIdle` still closes the session.

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
go test ./transport -run 'TestWS|TestWSS|TestWebSocket.*Timeout' -count=1
```

Expected: timeout kind mismatch or missing idle/lifetime behavior.

- [ ] **Step 3: Bound WS/WSS connect and handshake stages**

In `newWebSocketHTTPTransport`, give raw TCP Dial the effective Connect timeout. Preserve the existing separate WSS TLS stage and map it to `TimeoutHandshake` with caller precedence.

Use effective WS handshake timeout for the HTTP upgrade. On the server, apply that same effective WS handshake value to `ReadHeaderTimeout`, `WriteTimeout`, and `IdleTimeout` for pre-upgrade HTTP handling.

- [ ] **Step 4: Wire wsSession read/write/activity policy**

Writer: retain one `context.WithTimeout` per actual `ws.Write`, map only runtime deadline expiration to `TimeoutWrite`, and touch activity only after successful business write.

Reader: when ReadIdle is enabled, create one bounded context around one blocking `ws.Read`; cancel it immediately after read returns, before decode/handler. Map runtime expiration to `TimeoutReadIdle`.

Add the optional activity watchdog to the same `loops` WaitGroup. Do not call `touch` from `pingLoop`.

- [ ] **Step 5: Run focused WS/WSS tests plus race**

Run:

```bash
go test ./transport -run 'TestWS|TestWSS|TestWebSocket.*Timeout' -count=1
go test -race ./transport -run 'TestWS|TestWSS|TestWebSocket.*Timeout' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add transport/websocket_dial_admission.go transport/websocket_client.go transport/websocket_server.go transport/websocket.go transport/websocket_timeout_integration_test.go
git commit -m "transport: apply timeout policy to WebSocket runtime"
```

---

### Task 4: Apply timeout policy to UDP

**Files:**
- Modify: `transport/packet.go`
- Extend: `transport/timeout_integration_test.go`

**Interfaces:**
- Consumes Task 1 timeout/error policy and Task 2 activity clock.
- Produces connected UDP Connect/ReadIdle/ConnectionIdle/MaxLifetime and per-datagram Write timeout.

- [ ] **Step 1: Write failing UDP tests**

Cover:

```go
func TestDialPacketConnectCallerDeadlineWins(t *testing.T) { /* caller cause precedence */ }
func TestDialPacketReadIdle(t *testing.T) { /* silent connected peer -> TimeoutReadIdle */ }
func TestDialPacketConnectionIdle(t *testing.T) { /* no successful traffic -> TimeoutConnectionIdle */ }
func TestDialPacketMaxLifetime(t *testing.T) { /* traffic cannot extend */ }
func TestListenPacketDoesNotIdleClose(t *testing.T) { /* wait > idle/lifetime config; still alive */ }
```

Also cover UDP write deadline cleanup with a deterministic internal helper test if the target OS makes actual UDP write blocking non-deterministic. The helper test must still assert that a timeout result maps to `TimeoutWrite` and releases quota/slot ownership.

- [ ] **Step 2: Run focused UDP tests and verify RED**

Run:

```bash
go test ./transport -run 'Test.*Packet.*(Timeout|Idle|Lifetime|Caller)' -count=1
```

Expected: missing policy behavior.

- [ ] **Step 3: Implement DialPacket Connect timeout**

Use the same caller-vs-runtime operation helper as TCP, mapping Engine timeout to `TimeoutConnect`.

- [ ] **Step 4: Implement connected UDP read/activity policy**

Only `DialPacket` creates activity watchdog/read-idle behavior. Set/clear read deadline around the blocking read, touch after successful datagram read, and map timeout to `TimeoutReadIdle`.

- [ ] **Step 5: Implement per-datagram Write timeout**

Before `Write` / `WriteToUDP`, set one write deadline for the datagram and clear it after completion. Map actual runtime deadline expiration to `TimeoutWrite`. Touch connected UDP activity only after successful outbound datagram write; unconnected ListenPacket has no activity clock.

- [ ] **Step 6: Run focused UDP tests plus race**

Run:

```bash
go test ./transport -run 'Test.*Packet.*(Timeout|Idle|Lifetime|Caller)' -count=1
go test -race ./transport -run 'Test.*Packet.*(Timeout|Idle|Lifetime|Caller)' -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add transport/packet.go transport/timeout_integration_test.go
git commit -m "transport: apply timeout policy to UDP runtime"
```

---

### Task 5: Race/stress, documentation, benchmark baseline, and final verification

**Files:**
- Create: `transport/timeout_race_test.go`
- Create: `transport/timeout_benchmark_test.go`
- Create: `docs/runtime-timeouts.md`
- Modify: `README.md` only if the current README maintains a transport feature/documentation index.
- Modify: `.github/workflows/netpoll-v2.yml` only if a bounded benchmark-smoke command is needed to ensure the new benchmark paths execute in CI.

**Interfaces:**
- Consumes all previous tasks.
- Produces final #47 acceptance evidence.

- [ ] **Step 1: Write timeout race/cleanup tests**

Add tests that coordinate the exact races rather than relying on random sleeps:

```text
TimeoutWrite vs explicit Close
TimeoutReadIdle vs Engine.Close
ConnectionIdle watchdog vs successful touch
MaxLifetime vs concurrent Send
TLS handshake timeout vs Listener.Close
hundreds of short-timeout sessions during Engine.Shutdown
```

After each scenario assert:

```go
snap := e.admissionSnapshot()
if snap.OpeningConnections != 0 || snap.ActiveConnections != 0 ||
   snap.ActiveHandshakes != 0 || snap.PendingUpgrades != 0 ||
   snap.GlobalQueuedBytes != 0 {
    t.Fatalf("leaked runtime accounting: %+v", snap)
}
```

For explicit close/timeout races, assert only the first terminal cause is retained and that later `net.ErrClosed` does not overwrite it.

- [ ] **Step 2: Run race suite and verify behavior**

Run:

```bash
go test -race ./transport -run 'TestTimeoutRace|Test.*Shutdown.*Timeout|Test.*Timeout.*Close' -count=1
```

Expected: PASS.

- [ ] **Step 3: Add benchmark baseline**

Add at minimum:

```go
func BenchmarkActivityClockTouch(b *testing.B) {
    c := newActivityClock(time.Second, 0)
    b.ReportAllocs()
    for b.Loop() { c.touch() }
}

func BenchmarkStreamTimeoutPolicyDisabled(b *testing.B) { /* compare hot-path helper with no activity clock */ }
```

The touch benchmark must report `0 allocs/op`. Disabled timeout policy must not add per-message allocations relative to the existing send path.

- [ ] **Step 4: Run benchmark smoke**

Run:

```bash
go test ./transport -run '^$' -bench 'Benchmark(ActivityClockTouch|StreamTimeoutPolicyDisabled)' -benchmem -benchtime=1x
```

Expected: both benchmarks execute; `BenchmarkActivityClockTouch` reports `0 allocs/op`.

- [ ] **Step 5: Write public timeout documentation**

`docs/runtime-timeouts.md` must include:

- the six timeout fields and exact defaults;
- zero/negative semantics;
- protocol-specific override precedence and Option-order independence;
- caller context precedence examples;
- `Send(ctx)` ownership distinction from writer timeout;
- protocol matrix for TCP/TLS/WS/WSS/DialPacket/ListenPacket;
- ReadIdle vs ConnectionIdle vs MaxLifetime definitions;
- WS ping/pong non-refresh rule;
- `TimeoutError` examples with `errors.Is/As`;
- first-terminal-cause and `Done()` cleanup guarantees.

- [ ] **Step 6: Run full local/package verification**

Run:

```bash
gofmt -w .
git diff --exit-code
go mod tidy
git diff --exit-code -- go.mod go.sum
go vet ./...
go test -race -count=1 ./...
go test ./transport -run '^$' -bench 'Benchmark(ActivityClockTouch|StreamTimeoutPolicyDisabled)' -benchmem -benchtime=1x
```

Expected: all commands PASS. If local Go version cannot run repository targets, record the exact local limitation and rely on GitHub Actions for authoritative Go 1.25/1.26 and cross-platform verification; do not claim local execution that did not occur.

- [ ] **Step 7: Commit final hardening artifacts**

```bash
git add transport/timeout_race_test.go transport/timeout_benchmark_test.go docs/runtime-timeouts.md README.md .github/workflows/netpoll-v2.yml
git commit -m "test: harden timeout races stress and benchmarks"
```

Only add README/workflow paths if they were actually changed.

- [ ] **Step 8: Open Draft PR and verify authoritative CI**

Open:

```text
runtime: add complete timeout and deadline model
```

from `feat/runtime-timeout-model` to `master`, Draft, with `Refs #47` and `Refs #38`.

Authoritative final-head requirements:

```text
Linux Go 1.25.x: format, module hygiene, vet, race
Linux Go 1.26.x: format, module hygiene, vet, race
Windows: vet + full tests
macOS: vet + full tests
GmSSL job
all existing cross-compile jobs
benchmark smoke if added to workflow
```

Do not mark complete until a fresh final-head workflow run is `completed / success` for every job.
