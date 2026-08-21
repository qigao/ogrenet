# Typed Transport Error Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a stable typed operational error envelope across TCP, TLS, WS, WSS, UDP, limits, timeouts, listeners, and shutdown paths while preserving established P0-1/P0-2/P0-3 error and lifecycle contracts.

**Architecture:** Introduce one public `transport.Error` envelope with stable `Op` and `ErrorKind` enums. Centralize failure classification in protocol-neutral code plus small Unix/Windows errno adapters; retain `TimeoutError` and `LimitError` as specialized causes, preserve raw OS/TLS/WS causes, and classify only on error paths at the earliest boundary that owns the failure. Resource `Err()` stores the first terminal operational failure; caller context cancellation, explicit local close, and clean peer lifecycle remain control/lifecycle events and do not populate resource `Err()`.

**Tech Stack:** Go 1.25/1.26; standard `errors`, `net`, `os`, `syscall`, `crypto/tls`, `crypto/x509`; `github.com/coder/websocket`; existing ogrenet transport/wire/secure packages; GitHub Actions including pinned FreeBSD VM runtime coverage.

**Spec:** `docs/superpowers/specs/2026-08-21-typed-transport-error-model-design.md`

## Global Constraints

- P0-4 covers operational/runtime failures only; pre-operation configuration, validation, and programmer errors remain direct sentinels.
- Caller `context.Cause(ctx)` wins unchanged and is never wrapped in `*transport.Error`.
- Clean TCP FIN, clean TLS EOF, and normal WS close remain lifecycle events with resource `Err()==nil`.
- `Session.Err()`, `PacketConn.Err()`, and `Listener.Err()` store only the first terminal operational failure.
- Existing `TimeoutError`, `LimitError`, `ErrTimeout`, `ErrResourceExhausted`, `ErrWouldBlock`, `ErrFrameExceedsQueueBudget`, `ErrReadBufferFull`, `ErrMessageTooLarge`, and `ErrDatagramTooLarge` remain reachable through `errors.Is/As`.
- Classification never depends on `Error()` strings.
- `ErrorKind` and `Op` numeric values are append-only after publication.
- Successful Send/TrySend/Read/Receive paths do not allocate `*Error`, snapshot addresses, or run classifiers.
- Running-state graceful allocation baselines from final P0-3 CI #272 are hard gates: Go 1.25 `BenchmarkGracefulSendRunning=29 allocs/op`, `BenchmarkGracefulTrySendRunning=4 allocs/op`; Go 1.26 `BenchmarkGracefulSendRunning=17 allocs/op`, `BenchmarkGracefulTrySendRunning=4 allocs/op`. P0-4 may not increase these values when measured by the same CI benchmark command.
- Engine shutdown continues not to aggregate child failures.
- QUIC and HTTP client errors are out of scope.
- FreeBSD mapping requires runtime evidence; cross-compilation alone is insufficient.
- Branch: `feat/typed-error-model`; tracking issue: #52; base commit: `13c6c4878fad6512d00a4c1e17168f8352546f19`.

---

## File Structure

**Create**

- `transport/error_model.go` — public `Error`, `Op`, `ErrorKind`, address snapshots, category cause wrapper, envelope constructor.
- `transport/error_model_test.go` — public contract, cause chain, duplicate-wrap, address snapshots, unknown behavior.
- `transport/error_classify.go` — protocol-neutral classifier and TLS/WS/wire classification.
- `transport/error_classify_test.go` — protocol-neutral classifier tests.
- `transport/error_classify_unix.go` / `_test.go` — Unix errno mapping.
- `transport/error_classify_windows.go` / `_test.go` — Winsock errno mapping.
- `transport/error_tcp_integration_test.go` — TCP Dial/Listen/Send/Write/Read/FIN behavior.
- `transport/error_tls_integration_test.go` — TLS/x509/reset/close behavior.
- `transport/error_websocket_integration_test.go` — WS/WSS upgrade/read/write/close behavior.
- `transport/error_udp_integration_test.go` — UDP send/write/receive/size behavior.
- `transport/error_resource_lifecycle_test.go` — resource `Err()` ownership and context-control behavior.
- `transport/error_race_test.go` — first-failure owner races.
- `transport/error_benchmark_test.go` — error-path microbenchmarks.
- `docs/runtime-errors.md` — public taxonomy and migration guide.

**Modify**

- `transport/errors.go`
- `transport/tcp.go`
- `transport/tls.go`
- `transport/engine_stream_dial.go`
- `transport/conn.go`
- `transport/stream_graceful.go`
- `transport/listener.go`
- `transport/packet.go`
- `transport/websocket_client.go`
- `transport/websocket.go`
- `transport/websocket_graceful.go`
- `transport/websocket_server.go`
- `transport/websocket_server_admission.go`
- `.github/workflows/netpoll-v2.yml`

Existing tests that use direct `==` against operational sentinels are migrated to `errors.Is` in the same task that changes that behavior; no broad test-only cleanup commit is allowed.

---

### Task 1: Public Error Envelope and Contract

**Files:**
- Create: `transport/error_model.go`
- Create: `transport/error_model_test.go`
- Modify: `transport/errors.go`

**Interfaces:**

```go
type Op uint8
type ErrorKind uint8

type Error struct {
    Op       Op
    Protocol ogrenet.Scheme
    Kind     ErrorKind
    Local    net.Addr
    Remote   net.Addr
    Cause    error
}

func (e *Error) Error() string
func (e *Error) Unwrap() error
func (o Op) String() string
func (k ErrorKind) String() string
func envelopeOperational(op Op, protocol ogrenet.Scheme, local, remote net.Addr, kind ErrorKind, cause error) error
func categorized(category, cause error) error
```

- [ ] **Step 1: Write RED contract tests**

Create `transport/error_model_test.go`:

```go
func TestTransportErrorEnvelopeContract(t *testing.T) {
    raw := syscall.ECONNRESET
    err := envelopeOperational(
        OpRead,
        ogrenet.SchemeTCP,
        &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 10001},
        &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 10002},
        ErrorReset,
        categorized(ErrConnectionReset, raw),
    )
    var te *Error
    if !errors.As(err, &te) { t.Fatalf("not *Error: %T %v", err, err) }
    if te.Op != OpRead || te.Protocol != ogrenet.SchemeTCP || te.Kind != ErrorReset { t.Fatalf("envelope=%+v", te) }
    if !errors.Is(err, ErrConnectionReset) { t.Fatalf("missing category: %v", err) }
    if !errors.Is(err, raw) { t.Fatalf("missing raw cause: %v", err) }
}

func TestEnvelopeOperationalDoesNotDoubleWrap(t *testing.T) {
    first := envelopeOperational(OpRead, ogrenet.SchemeTCP, nil, nil, ErrorUnknown, errors.New("raw"))
    second := envelopeOperational(OpWrite, ogrenet.SchemeTCP, nil, nil, ErrorUnknown, first)
    if second != first { t.Fatalf("identity changed") }
}

func TestTransportErrorSnapshotsTCPAddresses(t *testing.T) {
    local := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1001}
    remote := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 2), Port: 1002}
    err := envelopeOperational(OpRead, ogrenet.SchemeTCP, local, remote, ErrorUnknown, errors.New("raw"))
    local.IP[3] = 9
    remote.IP[3] = 9
    var te *Error
    if !errors.As(err, &te) { t.Fatal("missing *Error") }
    if te.Local.String() != "127.0.0.1:1001" || te.Remote.String() != "127.0.0.2:1002" {
        t.Fatalf("snapshots changed: local=%v remote=%v", te.Local, te.Remote)
    }
}
```

Add table tests for exact `Op.String()` values `dial`, `listen`, `accept`, `handshake`, `upgrade`, `read`, `write`, `send`, `receive`, `close`, `shutdown`, and `unknown`; add exact `ErrorKind.String()` values `unknown`, `closed`, `peer-closed`, `timeout`, `refused`, `reset`, `dns`, `tls`, `protocol`, `resource-exhausted`, `backpressure`, and `too-large`.

- [ ] **Step 2: Verify RED**

```bash
go test ./transport -run 'Test(TransportError|EnvelopeOperational|OpString|ErrorKindString)' -count=1
```

Expected: compile failure for the new public types/helpers/sentinels.

- [ ] **Step 3: Implement the public model**

Add to `transport/errors.go`:

```go
var (
    ErrPeerClosed        = errors.New("transport: peer closed")
    ErrConnectionRefused = errors.New("transport: connection refused")
    ErrConnectionReset   = errors.New("transport: connection reset")
    ErrDNS               = errors.New("transport: DNS failure")
    ErrTLS               = errors.New("transport: TLS failure")
    ErrProtocolViolation = errors.New("transport: protocol violation")
)
```

Create append-only enums:

```go
type Op uint8
const (
    OpDial Op = iota + 1
    OpListen
    OpAccept
    OpHandshake
    OpUpgrade
    OpRead
    OpWrite
    OpSend
    OpReceive
    OpClose
    OpShutdown
)

type ErrorKind uint8
const (
    ErrorUnknown ErrorKind = iota
    ErrorClosed
    ErrorPeerClosed
    ErrorTimeout
    ErrorRefused
    ErrorReset
    ErrorDNS
    ErrorTLS
    ErrorProtocol
    ErrorResourceExhausted
    ErrorBackpressure
    ErrorTooLarge
)
```

Implement the linear envelope and category wrapper:

```go
type Error struct {
    Op       Op
    Protocol ogrenet.Scheme
    Kind     ErrorKind
    Local    net.Addr
    Remote   net.Addr
    Cause    error
}

func (e *Error) Error() string {
    if e == nil { return "transport: operational error" }
    prefix := fmt.Sprintf("transport: %s %s %s", e.Protocol, e.Op, e.Kind)
    if e.Cause == nil { return prefix }
    return prefix + ": " + e.Cause.Error()
}
func (e *Error) Unwrap() error { if e == nil { return nil }; return e.Cause }

type categorizedCause struct { category, cause error }
func (e *categorizedCause) Error() string {
    if e == nil || e.category == nil { return "transport: categorized failure" }
    if e.cause == nil { return e.category.Error() }
    return e.category.Error() + ": " + e.cause.Error()
}
func (e *categorizedCause) Unwrap() error { if e == nil { return nil }; return e.cause }
func (e *categorizedCause) Is(target error) bool { return e != nil && target == e.category }
func categorized(category, cause error) error {
    if cause == nil { return category }
    if errors.Is(cause, category) { return cause }
    return &categorizedCause{category: category, cause: cause}
}
```

`snapshotAddr` deep-copies `*net.TCPAddr` and `*net.UDPAddr` including IP slices, copies `staticAddr` by value, and returns unknown `net.Addr` implementations unchanged because no generic safe clone exists.

`envelopeOperational` returns nil for nil cause, returns an existing nested `*Error` unchanged, otherwise snapshots addresses and creates one `*Error`.

- [ ] **Step 4: Verify GREEN**

```bash
go test ./transport -run 'Test(TransportError|EnvelopeOperational|OpString|ErrorKindString)' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit Task 1**

```bash
git add transport/errors.go transport/error_model.go transport/error_model_test.go
git commit -m "transport: add operational error envelope"
```

- [ ] **Step 6: Open Draft PR for authoritative CI**

Create Draft PR `runtime: add typed transport error taxonomy` from `feat/typed-error-model` to `master` with body:

```text
Implements P0-4 typed operational transport errors using the approved design.

Development proceeds task-by-task with TDD. This Draft PR exists early so GitHub Actions provides the authoritative Go 1.25/1.26, Windows, macOS, GmSSL, and cross-platform harness.

Refs #52
Refs #38
```

Keep it Draft through Task 6 final exact-head verification.

---

### Task 2: Central Classifier and OS Mapping

**Files:**
- Create: `transport/error_classify.go`
- Create: `transport/error_classify_test.go`
- Create: `transport/error_classify_unix.go`
- Create: `transport/error_classify_unix_test.go`
- Create: `transport/error_classify_windows.go`
- Create: `transport/error_classify_windows_test.go`

**Interfaces:**

```go
type classifyHint uint8
const (
    hintNone classifyHint = iota
    hintTLSHandshake
    hintWSUpgrade
    hintWireDecode
    hintMessageDecode
)

func classifyOperational(op Op, protocol ogrenet.Scheme, local, remote net.Addr, cause error, hint classifyHint) error
func classifyPlatformCause(op Op, err error) (ErrorKind, error, bool)
func classifyWSStatus(status websocket.StatusCode) (ErrorKind, error, bool)
```

- [ ] **Step 1: Write RED protocol-neutral tests**

Test exact existing-sentinel mappings:

```go
func TestClassifyOperationalKnownSentinels(t *testing.T) {
    cases := []struct {
        name string
        op Op
        cause error
        kind ErrorKind
        specific error
        broad error
    }{
        {"closed", OpSend, ErrClosed, ErrorClosed, ErrClosed, nil},
        {"would-block", OpSend, ErrWouldBlock, ErrorBackpressure, ErrWouldBlock, nil},
        {"message-too-large", OpSend, ErrMessageTooLarge, ErrorTooLarge, ErrMessageTooLarge, nil},
        {"datagram-too-large", OpSend, ErrDatagramTooLarge, ErrorTooLarge, ErrDatagramTooLarge, nil},
        {"frame-budget", OpSend, ErrFrameExceedsQueueBudget, ErrorResourceExhausted, ErrFrameExceedsQueueBudget, ErrResourceExhausted},
        {"read-buffer", OpRead, ErrReadBufferFull, ErrorResourceExhausted, ErrReadBufferFull, ErrResourceExhausted},
        {"invalid-framer-runtime", OpRead, ErrInvalidFramer, ErrorUnknown, ErrInvalidFramer, nil},
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            err := classifyOperational(tc.op, ogrenet.SchemeTCP, nil, nil, tc.cause, hintNone)
            var te *Error
            if !errors.As(err, &te) || te.Kind != tc.kind { t.Fatalf("err=%#v", err) }
            if !errors.Is(err, tc.specific) { t.Fatalf("missing specific sentinel: %v", err) }
            if tc.broad != nil && !errors.Is(err, tc.broad) { t.Fatalf("missing broad category: %v", err) }
        })
    }
}
```

Test specialized composition:

```go
func TestClassifyOperationalPreservesTimeoutAndLimitTypes(t *testing.T) {
    timeout := &TimeoutError{Kind: TimeoutWrite, Cause: context.DeadlineExceeded}
    terr := classifyOperational(OpWrite, ogrenet.SchemeTCP, nil, nil, timeout, hintNone)
    var te *Error
    var timeoutOut *TimeoutError
    if !errors.As(terr, &te) || te.Kind != ErrorTimeout || !errors.As(terr, &timeoutOut) || timeoutOut.Kind != TimeoutWrite || !errors.Is(terr, ErrTimeout) {
        t.Fatalf("timeout chain=%#v", terr)
    }

    limit := &LimitError{Kind: LimitConnections, Limit: 10}
    lerr := classifyOperational(OpDial, ogrenet.SchemeTCP, nil, nil, limit, hintNone)
    var limitOut *LimitError
    if !errors.As(lerr, &te) || te.Kind != ErrorResourceExhausted || !errors.As(lerr, &limitOut) || !errors.Is(lerr, ErrResourceExhausted) {
        t.Fatalf("limit chain=%#v", lerr)
    }
}
```

Test typed DNS/TLS inputs without network lookup:

```go
func TestClassifyOperationalDNS(t *testing.T) {
    raw := &net.DNSError{Name: "does-not-exist.invalid", Err: "no such host"}
    err := classifyOperational(OpDial, ogrenet.SchemeTCP, nil, nil, raw, hintNone)
    var te *Error
    var dns *net.DNSError
    if !errors.As(err, &te) || te.Kind != ErrorDNS || !errors.Is(err, ErrDNS) || !errors.As(err, &dns) {
        t.Fatalf("dns chain=%#v", err)
    }
}

func TestClassifyTLSCertificateFailure(t *testing.T) {
    raw := x509.UnknownAuthorityError{Cert: &x509.Certificate{}}
    err := classifyOperational(OpHandshake, ogrenet.SchemeTLS, nil, nil, raw, hintTLSHandshake)
    var te *Error
    if !errors.As(err, &te) || te.Kind != ErrorTLS || !errors.Is(err, ErrTLS) { t.Fatalf("tls chain=%#v", err) }
}
```

Test `classifyWSStatus` directly for 1002/1003/1007/1008 => `ErrorProtocol/ErrProtocolViolation`, 1009 => `ErrorTooLarge/ErrMessageTooLarge`, 1011 => `ErrorUnknown`, and 1000/1001 => `ok=false`.

- [ ] **Step 2: Write RED platform tests**

Unix file uses build tag `//go:build !windows` and wraps errno in `*os.SyscallError` plus `*net.OpError`. Required cases:

```go
{OpDial, syscall.ECONNREFUSED, ErrorRefused, ErrConnectionRefused, true}
{OpRead, syscall.ECONNRESET, ErrorReset, ErrConnectionReset, true}
{OpWrite, syscall.EPIPE, ErrorPeerClosed, ErrPeerClosed, true}
{OpWrite, syscall.ENOTCONN, ErrorPeerClosed, ErrPeerClosed, true}
{OpDial, syscall.ENOTCONN, ErrorUnknown, nil, false}
```

Windows file uses build tag `//go:build windows` and `syscall.Errno` values 10061/10054/10053/10058/10057 for WSAECONNREFUSED/RESET/ABORTED/SHUTDOWN/NOTCONN, proving the mappings from the spec.

- [ ] **Step 3: Verify RED**

```bash
go test ./transport -run '^TestClassify' -count=1
```

Expected: compile failure for classifier symbols.

- [ ] **Step 4: Implement fixed classifier order**

`classifyOperational` must execute this exact order:

```text
existing *Error
TimeoutError / ErrTimeout
LimitError / ErrResourceExhausted
known runtime sentinels
*net.DNSError
platform errno
TLS/x509
WebSocket status
wire/message hint
ErrorUnknown fallback
```

Runtime sentinel behavior:

```go
case errors.Is(cause, ErrClosed):
    return envelopeOperational(op, protocol, local, remote, ErrorClosed, cause)
case errors.Is(cause, ErrWouldBlock):
    return envelopeOperational(op, protocol, local, remote, ErrorBackpressure, cause)
case errors.Is(cause, ErrMessageTooLarge), errors.Is(cause, ErrDatagramTooLarge):
    return envelopeOperational(op, protocol, local, remote, ErrorTooLarge, cause)
case errors.Is(cause, ErrFrameExceedsQueueBudget), errors.Is(cause, ErrReadBufferFull):
    return envelopeOperational(op, protocol, local, remote, ErrorResourceExhausted, categorized(ErrResourceExhausted, cause))
case errors.Is(cause, ErrInvalidFramer):
    return envelopeOperational(op, protocol, local, remote, ErrorUnknown, cause)
```

DNS uses `categorized(ErrDNS, cause)`. Platform categories use `categorized(category, cause)`. Typed x509 failures and remaining genuine handshake failures under `hintTLSHandshake` use `ErrorTLS` + `ErrTLS`. WS status uses the table from Step 1. `hintWireDecode` and `hintMessageDecode` use `ErrorProtocol` + `ErrProtocolViolation`. Unmatched causes use `ErrorUnknown` with raw cause.

- [ ] **Step 5: Implement OS adapters**

Unix `classifyPlatformCause` uses `errors.Is` against `ECONNREFUSED`, `ECONNRESET`, `EPIPE`, `ENOTCONN`. `ENOTCONN` only maps to peer-closed for `OpRead`, `OpWrite`, `OpSend`, `OpReceive`, `OpClose`.

Windows `classifyPlatformCause` uses the Winsock errno identities above and the same established-I/O restriction for WSAENOTCONN.

- [ ] **Step 6: Verify GREEN**

```bash
go test ./transport -run '^TestClassify' -count=1
go test ./transport -run 'TestTransportErrorEnvelopeContract|TestClassifyOperationalPreservesTimeoutAndLimitTypes' -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit Task 2**

```bash
git add transport/error_classify.go transport/error_classify_test.go transport/error_classify_unix.go transport/error_classify_unix_test.go transport/error_classify_windows.go transport/error_classify_windows_test.go
git commit -m "transport: classify operational transport failures"
```

---

### Task 3: TCP/TLS Boundaries and Session Error Ownership

**Files:**
- Create: `transport/error_tcp_integration_test.go`
- Create: `transport/error_tls_integration_test.go`
- Modify: `transport/tcp.go`
- Modify: `transport/tls.go`
- Modify: `transport/engine_stream_dial.go`
- Modify: `transport/conn.go`
- Modify: `transport/stream_graceful.go`

**Interfaces:** Task 2 classifier; adds internal `wireFramer bool` to `conn` solely to distinguish default-wire remote decode from custom-framer plugin/invariant failure.

- [ ] **Step 1: Write TCP RED tests**

Define reusable assertion:

```go
func assertTransportError(t *testing.T, err error, op Op, protocol ogrenet.Scheme, kind ErrorKind) *Error {
    t.Helper()
    var te *Error
    if !errors.As(err, &te) { t.Fatalf("not *Error: %T %v", err, err) }
    if te.Op != op || te.Protocol != protocol || te.Kind != kind { t.Fatalf("envelope=%+v", te) }
    return te
}
```

Required cases:

- `TrySend` saturation with `WithWriteQueue(1)` + deterministic blocking writer => `OpSend/ErrorBackpressure`, `errors.Is(ErrWouldBlock)`.
- application message over max => `OpSend/ErrorTooLarge`, `errors.Is(ErrMessageTooLarge)`.
- deterministic write wrapper returns `*net.OpError` -> `*os.SyscallError` -> `syscall.ECONNRESET`; Send ack and terminal `Session.Err()` => `OpWrite/ErrorReset`, `errors.Is(ErrConnectionReset)`, raw errno reachable.
- clean peer FIN closes `ReadClosed` and leaves `Session.Err()==nil`.
- write timeout fixture => `OpWrite/ErrorTimeout`, `TimeoutError.Kind==TimeoutWrite`, `errors.Is(ErrTimeout)`.
- bind then close an unused loopback port; Dial => `OpDial/ErrorRefused`, `errors.Is(ErrConnectionRefused)`.
- keep a raw loopback listener bound and call `Engine.Listen` on same address => `OpListen/ErrorUnknown`, raw bind cause preserved; no new address-in-use kind.

- [ ] **Step 2: Write TLS RED tests**

- untrusted loopback certificate => `OpHandshake/ErrorTLS`, `errors.Is(ErrTLS)`, typed x509/tls cause reachable.
- synthetic handshake cause chain containing `ECONNRESET` through `classifyOperational(OpHandshake, SchemeTLS, ..., hintTLSHandshake)` => `ErrorReset`, not TLS; the Engine Dial integration test separately proves handshake wiring with a certificate failure.
- TLS `CloseWrite` timeout/failure => `OpClose/ErrorTimeout`, nested `TimeoutWrite` preserved.
- clean peer `close_notify` => `ReadClosed` with `Session.Err()==nil`.

- [ ] **Step 3: Verify RED**

```bash
go test ./transport -run 'TestTransportErrorTCP|TestTransportErrorTLS' -count=1
```

Expected: raw/specialized/sentinel errors lack the outer envelope.

- [ ] **Step 4: Wrap Dial/Listen/Handshake boundaries**

`dialTCP`: call `mapOperationTimeout`; if caller context won, return `context.Cause(ctx)` unchanged; otherwise classify `OpDial`.

`listenTCP`: if caller context won, return its cause unchanged; otherwise classify raw listen/bind failure as `OpListen`.

`dialStream`: classify outbound admission `LimitError` as `OpDial/ErrorResourceExhausted`. Do not wrap `clientTLSConfig` configuration errors. Classify handshake operational failure as `OpHandshake` with `hintTLSHandshake` and known local/remote TCP addresses.

- [ ] **Step 5: Wrap Send/Write/Read/decode ownership points**

Before physical I/O: only operational send errors (`ErrClosed`, backpressure, too-large, queue/resource, valid-input plugin failure) get `OpSend`. Caller context and local validation stay direct.

`handleOutbound` classifies `writeFrame` result as `OpWrite` before ack and before terminal ownership. `writeFrame` retains `TimeoutError` but does not bury raw errno under redundant strings needed only for classification.

Reader errors classify as `OpRead` before `initiateClose`. Default-wire remote decode uses `hintWireDecode`; custom framer errors and `ErrInvalidFramer` use `ErrorUnknown`. `ErrReadBufferFull` uses `OpRead/ErrorResourceExhausted`, preserving both resource and specific sentinels.

- [ ] **Step 6: Preserve first-failure Session.Err ownership**

`conn.abort` stores `cause` only when `reason==abortFailure` and cause is non-nil. Caller-owned or explicit abort does not write `c.err`. Existing physical close remains: close `c.physical` when non-nil, else close `c.raw`. Do not classify in `finalize`; derivative `net.ErrClosed` remains suppressed.

- [ ] **Step 7: Classify stream protocol close at OpClose**

`closeProtocolWrite`/`closeTLSWrite` return raw or specialized cause. Writer graceful path classifies failure as `OpClose` before `initiateClose`. TLS close timeout keeps nested `TimeoutWrite`.

- [ ] **Step 8: Verify GREEN and race**

```bash
go test ./transport -run 'TestTransportErrorTCP|TestTransportErrorTLS' -count=1
go test -race ./transport -run 'TestTransportErrorTCP|TestTransportErrorTLS|TestGracefulRace' -count=1
```

Expected: PASS.

- [ ] **Step 9: Commit Task 3**

```bash
git add transport/tcp.go transport/tls.go transport/engine_stream_dial.go transport/conn.go transport/stream_graceful.go transport/error_tcp_integration_test.go transport/error_tls_integration_test.go
git commit -m "transport: type TCP and TLS operational errors"
```

---

### Task 4: WebSocket/WSS Error Mapping

**Files:**
- Create: `transport/error_websocket_integration_test.go`
- Modify: `transport/websocket_client.go`
- Modify: `transport/websocket.go`
- Modify: `transport/websocket_graceful.go`
- Modify: `transport/websocket_server.go`
- Modify: `transport/websocket_server_admission.go`

**Interfaces:** Task 2 classifier + Task 3 `assertTransportError` test helper.

- [ ] **Step 1: Write WS/WSS RED tests**

Required cases:

- HTTP server returns 403 instead of upgrading => Dial returns `OpUpgrade/ErrorProtocol`, `errors.Is(ErrProtocolViolation)`.
- WSS with untrusted certificate => `OpHandshake/ErrorTLS`, not generic upgrade protocol.
- peer close 1002 => terminal `Session.Err()` `OpRead/ErrorProtocol` + `ErrProtocolViolation`.
- peer close 1009 => terminal `OpRead/ErrorTooLarge` + `ErrMessageTooLarge`.
- peer normal 1000 and 1001 => `Session.Err()==nil`.
- nonresponsive peer + `CloseTimeout=50ms` => `OpClose/ErrorTimeout`, nested `TimeoutClose`.
- `TrySend` saturation => `OpSend/ErrorBackpressure`; physical WS write timeout => `OpWrite/ErrorTimeout`, nested `TimeoutWrite`.

- [ ] **Step 2: Verify RED**

```bash
go test ./transport -run 'TestTransportError(WebSocket|WSS)' -count=1
```

Expected: raw coder/websocket/specialized errors without consistent envelope.

- [ ] **Step 3: Classify outbound WS/WSS Dial**

Caller context bypasses. Use `classifyOperational(OpUpgrade, endpoint.Scheme, nil, nil, err, hintWSUpgrade)` first; because DNS/platform/TLS precede WS classification, transport and TLS roots win. If WSS result is `ErrorTLS`, rebuild exactly one envelope with `OpHandshake` and the same `Kind/Local/Remote/Cause`; do not nest `*Error`.

WS HTTP handshake timeout remains `ErrorTimeout` at `OpUpgrade` unless TLS handshake ownership is known before HTTP upgrade.

- [ ] **Step 4: Classify WS Send/Write/Read/decode**

- local closed/backpressure/too-large before `s.ws.Write` => `OpSend`.
- `s.ws.Write` timeout/reset/unknown => `OpWrite`.
- abnormal close status, malformed remote message/base64, inbound decrypt/auth failure => `OpRead`.
- local cipher `Seal` failure on valid input => `OpSend/ErrorUnknown`.
- normal close statuses do not store terminal error.

Pass original coder/websocket errors into classifier so `CloseStatus` and raw cause remain available.

- [ ] **Step 5: Classify graceful close at OpClose**

`watchCloseTimeout` constructs existing `TimeoutError{Kind: TimeoutClose, Cause: context.DeadlineExceeded}`, classifies it as `OpClose`, then calls `abort(abortFailure, typedErr)`. `abortCaller` and `abortExplicit` leave `s.err` nil. A real close-handshake operational failure is classified before terminal ownership.

- [ ] **Step 6: Preserve listener health on inbound upgrade rejection**

Inbound admission/upgrade rejection closes/releases only the child and does not populate healthy WS listener terminal `Err()`. Fatal listener/server operational failure remains terminal and typed at its actual boundary.

- [ ] **Step 7: Verify GREEN and race**

```bash
go test ./transport -run 'TestTransportError(WebSocket|WSS)' -count=1
go test -race ./transport -run 'TestTransportError(WebSocket|WSS)|TestGracefulRaceWebSocket' -count=1
```

Expected: PASS.

- [ ] **Step 8: Commit Task 4**

```bash
git add transport/websocket_client.go transport/websocket.go transport/websocket_graceful.go transport/websocket_server.go transport/websocket_server_admission.go transport/error_websocket_integration_test.go
git commit -m "transport: type WebSocket operational errors"
```

---

### Task 5: UDP, Listener, Admission, and Context-Owned Resources

**Files:**
- Create: `transport/error_udp_integration_test.go`
- Create: `transport/error_resource_lifecycle_test.go`
- Modify: `transport/packet.go`
- Modify: `transport/listener.go`

**Interfaces:** Task 2 classifier; existing `LimitError` internals remain unchanged.

- [ ] **Step 1: Write UDP RED tests**

Required cases:

- datagram over configured max => `OpSend/ErrorTooLarge`, `errors.Is(ErrDatagramTooLarge)`.
- connected UDP read-idle timeout => terminal `PacketConn.Err()` `OpReceive/ErrorTimeout`, nested `TimeoutReadIdle`.
- TrySend saturation => `OpSend/ErrorBackpressure`, `ErrWouldBlock`.
- explicit `Close()` => `PacketConn.Err()==nil`.

DNS classification is deterministic in Task 2 using `*net.DNSError`; `resolvePeer` must route any real resolver error to `classifyOperational(OpSend, SchemeUDP, ...)`, but no public-network-dependent DNS integration test is allowed.

A physical UDP write timeout is hard to force portably on a real datagram socket without changing the hot-path socket type. Do not introduce an interface dispatch solely for testing. P0-2 already owns timeout generation; Task 2 proves `TimeoutError` composition and Task 5 verifies actual UDP receive timeout plus `OpWrite` classification at `handleOutbound` for any `writeDatagram` error.

- [ ] **Step 2: Write resource lifecycle RED tests**

- cancel a `Listen` owner context, wait `Done`, assert `Listener.Err()==nil`.
- cancel a `ListenPacket` owner context, wait `Done`, assert `PacketConn.Err()==nil`.
- deterministic listener wrapper returns `syscall.EIO` from `Accept`; terminal listener error => `OpAccept/ErrorUnknown`, raw cause reachable.
- `MaxConnections=1`, second outbound Dial => `OpDial/ErrorResourceExhausted`, `errors.As(*LimitError)`, `errors.Is(ErrResourceExhausted)`.
- inbound per-listener rejection does not populate `Listener.Err()`.

- [ ] **Step 3: Verify RED**

```bash
go test ./transport -run 'TestTransportErrorUDP|TestTransportErrorResource|TestTransportErrorListener' -count=1
```

Expected: operational UDP/listener errors are unwrapped and owner context still pollutes resource `Err()`.

- [ ] **Step 4: Wrap UDP operational boundaries**

Keep `ErrNotConnected`, `ErrPeerRequired`, `ErrPeerMismatch`, caller context, and packet validation/programmer errors direct.

`resolvePeer` resolver failures => `OpSend`. Send/TrySend runtime admission failures => `OpSend`. `handleOutbound` classifies `writeDatagram` result => `OpWrite` before ack/terminal ownership. Reader timeout/error => `OpReceive`; use actual datagram peer as Remote when available.

- [ ] **Step 5: Normalize owner-context closure**

Listener and ListenPacket context watchers close as control actions with nil terminal cause. Fatal `Accept` classifies `OpAccept` before `initiateClose`. Local close-derived `net.ErrClosed` remains normalized away.

- [ ] **Step 6: Envelope outbound admission only**

`LimitError` itself is unchanged. Outbound Dial/DialPacket/WS operations classify returned `LimitError` at their public operation boundary. Inbound accept/upgrade policy rejection remains a rejected child/counter event, not listener terminal error.

- [ ] **Step 7: Verify GREEN and full transport race**

```bash
go test ./transport -run 'TestTransportErrorUDP|TestTransportErrorResource|TestTransportErrorListener' -count=1
go test -race ./transport -count=1
```

Expected: PASS.

- [ ] **Step 8: Commit Task 5**

```bash
git add transport/packet.go transport/listener.go transport/error_udp_integration_test.go transport/error_resource_lifecycle_test.go
git commit -m "transport: type UDP and listener operational errors"
```

---

### Task 6: Race Hardening, Benchmarks, Docs, FreeBSD Runtime CI, Final Verification

**Files:**
- Create: `transport/error_race_test.go`
- Create: `transport/error_benchmark_test.go`
- Create: `docs/runtime-errors.md`
- Modify: `.github/workflows/netpoll-v2.yml`

- [ ] **Step 1: Add deterministic first-owner race tests**

Use existing blocking-write/nonresponsive-WS helpers and explicit synchronization channels. Add:

```text
TestTransportErrorRaceTimeoutVsDerivedClose
TestTransportErrorRaceResetVsExplicitClose
TestTransportErrorRaceShutdownDeadlineVsPhysicalClose
TestTransportErrorRaceWebSocketCloseTimeoutVsPhysicalClose
```

Assertions:

- timeout ownership first => resource `Err()` remains typed timeout after derived socket close.
- reset ownership first => resource `Err()` remains `ErrorReset` after explicit Close.
- explicit Close ownership first => resource `Err()==nil`; derived reset/closed errors are suppressed.
- caller Shutdown deadline ownership => method returns caller cause and resource `Err()==nil`.
- WS `TimeoutClose` ownership => terminal `OpClose/ErrorTimeout`, physical-close fallout does not replace it.

- [ ] **Step 2: Run race loop**

```bash
go test -race ./transport -run '^TestTransportErrorRace' -count=20
```

Expected: PASS. Any failure blocks progress and requires `systematic-debugging` before runtime changes.

- [ ] **Step 3: Add error-path microbenchmarks**

```go
func BenchmarkErrorWrapKnown(b *testing.B) {
    raw := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
    b.ReportAllocs()
    for i := 0; i < b.N; i++ {
        _ = envelopeOperational(OpRead, ogrenet.SchemeTCP, nil, nil, ErrorReset, categorized(ErrConnectionReset, raw))
    }
}

func BenchmarkErrorWrapUnknown(b *testing.B) {
    raw := errors.New("opaque")
    b.ReportAllocs()
    for i := 0; i < b.N; i++ {
        _ = envelopeOperational(OpRead, ogrenet.SchemeTCP, nil, nil, ErrorUnknown, raw)
    }
}

func BenchmarkErrorClassifyReset(b *testing.B) {
    raw := &net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}}
    b.ReportAllocs()
    for i := 0; i < b.N; i++ {
        _ = classifyOperational(OpRead, ogrenet.SchemeTCP, nil, nil, raw, hintNone)
    }
}

func BenchmarkErrorClassifyTimeout(b *testing.B) {
    raw := &TimeoutError{Kind: TimeoutWrite, Cause: context.DeadlineExceeded}
    b.ReportAllocs()
    for i := 0; i < b.N; i++ {
        _ = classifyOperational(OpWrite, ogrenet.SchemeTCP, nil, nil, raw, hintNone)
    }
}
```

- [ ] **Step 4: Run benchmark smoke and enforce allocation baselines**

```bash
go test ./transport -run '^$' -bench 'BenchmarkError(WrapKnown|WrapUnknown|ClassifyReset|ClassifyTimeout)' -benchmem -benchtime=1x
go test ./transport -run '^$' -bench 'BenchmarkGraceful(SendRunning|TrySendRunning)' -benchmem -benchtime=1x
```

On Go 1.25, Send must remain <=29 allocs/op and TrySend <=4. On Go 1.26, Send must remain <=17 allocs/op and TrySend <=4. If runner noise changes bytes/ns, ignore those values; allocs/op is the acceptance metric.

- [ ] **Step 5: Write `docs/runtime-errors.md`**

Required sections:

```text
1. Operational Errors vs Configuration Errors
2. Error / Op / ErrorKind
3. errors.Is and errors.As
4. TimeoutError and LimitError Composition
5. Closed vs PeerClosed vs Clean EOF
6. Context Cancellation
7. TCP/TLS Mapping
8. WebSocket Mapping
9. UDP Mapping
10. Resource Limits and Backpressure
11. Error Precedence and Resource Err()
12. Cross-Platform Mapping Guarantees
13. Unknown Errors
```

Include:

```go
var te *transport.Error
if errors.As(err, &te) {
    switch te.Kind {
    case transport.ErrorRefused:
        // connection establishment was refused
    case transport.ErrorTimeout:
        var timeout *transport.TimeoutError
        if errors.As(err, &timeout) {
            // inspect TimeoutWrite, TimeoutHandshake, and other timeout domains
        }
    }
}
```

State explicitly: runtime sentinel checks use `errors.Is`, not `==`; `Error()` text is never a classification API.

- [ ] **Step 6: Add Linux error benchmark smoke**

After graceful benchmark smoke:

```yaml
      - name: Error taxonomy benchmark smoke
        run: >-
          go test ./transport -run '^$'
          -bench 'BenchmarkError(WrapKnown|WrapUnknown|ClassifyReset|ClassifyTimeout)'
          -benchmem -benchtime=1x
```

- [ ] **Step 7: Add pinned FreeBSD runtime classifier job**

Pin `vmactions/freebsd-vm` v1.5.2 commit `77ed28d336d03fe19a3f4f7266c1d2c4714dd79d`:

```yaml
  freebsd-runtime:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - uses: actions/checkout@v7
      - name: Test transport error classifier on FreeBSD
        uses: vmactions/freebsd-vm@77ed28d336d03fe19a3f4f7266c1d2c4714dd79d
        with:
          release: "14.4"
          usesh: true
          prepare: |
            pkg install -y go
          run: |
            go version
            go test ./transport -run 'Test(ClassifyPlatformUnixConnectionErrors|TransportErrorTCP|TransportErrorUDP|TransportErrorResource|TransportErrorListener)' -count=1
```

If the VM cannot install/run the repository minimum Go version reliably, FreeBSD runtime evidence remains a release blocker; do not replace it with cross-compile-only evidence.

- [ ] **Step 8: Commit Task 6 candidate**

```bash
git add transport/error_race_test.go transport/error_benchmark_test.go docs/runtime-errors.md .github/workflows/netpoll-v2.yml
git commit -m "transport: harden and document typed errors"
```

Any existing test migrated from direct operational sentinel equality must already have been committed with Task 3, 4, or 5 that caused the migration.

- [ ] **Step 9: Run exact-head final verification**

```bash
go test ./transport -run '^TestTransportError' -count=1
go test -race ./transport -run '^TestTransportError' -count=5
go test ./... -count=1
go test -race ./... -count=1
```

The exact-head GitHub Actions matrix must be successful for:

```text
Linux Go 1.25: format, module hygiene, vet, HTTP benchmark smoke, timeout benchmark smoke, graceful benchmark smoke, error benchmark smoke, full race
Linux Go 1.26: same
Windows Go 1.26: vet + full tests including Winsock classifier
macOS Go 1.26: vet + full tests including Unix classifier
FreeBSD 14.4 VM: focused runtime classifier/error tests
GmSSL: secure/gmssl + wire + transport
existing epoll/kqueue/IOCP cross-compile matrix
```

- [ ] **Step 10: Final diff/spec review**

Verify all of these directly against the diff:

```text
no config/programmer errors unintentionally wrapped
no caller context wrapped
no clean FIN/TLS EOF/normal WS close turned into terminal errors
no string-based classification
no duplicate Error envelopes
no child failure aggregation into Engine.Shutdown
no QUIC/HTTP scope creep
new enums retain approved numeric ordering
raw causes remain reachable
```

- [ ] **Step 11: Update Draft PR and mark Ready**

Update PR body with final exact head, successful CI run, allocation comparison, and FreeBSD evidence. Mark Ready for Review only after Step 9 and Step 10 pass. Do not merge without explicit user authorization.

---

## Final Acceptance Map

- `Error` / `Op` / `ErrorKind`: Task 1.
- category sentinels + raw cause chain: Tasks 1-2.
- `TimeoutError` / `LimitError` composition: Task 2.
- DNS/Unix/Windows/TLS/WS/wire classifier order: Task 2.
- `OpSend` vs `OpWrite`: Tasks 3-5.
- TCP Dial/Listen/reset/FIN/timeout + TLS handshake/close: Task 3.
- WS/WSS upgrade/protocol/normal close/CloseTimeout: Task 4.
- UDP/listener/admission/context-owned resource semantics: Task 5.
- first-failure race ownership: Task 6.
- Linux/Windows/macOS/FreeBSD runtime evidence + GmSSL: Task 6.
- hot-path allocation gate: Task 6 using CI #272 baselines.
- public error docs and migration to `errors.Is`: Task 6.
- PR remains Draft until exact-head verification; merge requires explicit user authorization.
