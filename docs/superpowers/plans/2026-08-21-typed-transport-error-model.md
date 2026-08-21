# Typed Transport Error Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a stable typed operational error envelope across TCP, TLS, WS, WSS, UDP, limits, timeouts, listeners, and shutdown paths while preserving existing sentinel/specialized error contracts and P0-3 lifecycle semantics.

**Architecture:** Introduce one public `transport.Error` envelope with stable `Op` and `ErrorKind` enums. Centralize classification in protocol-neutral code plus small Unix/Windows errno adapters; retain `TimeoutError` and `LimitError` inside the cause chain, preserve raw OS/TLS/WS causes, and classify only on error paths at the earliest boundary that owns the failure. Resource `Err()` becomes the canonical first terminal operational failure source; caller context cancellation, explicit close, and clean peer lifecycle remain control/lifecycle events and do not populate resource `Err()`.

**Tech Stack:** Go 1.25/1.26, standard `errors`/`net`/`syscall`/`crypto/tls`/`crypto/x509`, `github.com/coder/websocket`, existing ogrenet transport/wire/secure packages, GitHub Actions including a pinned FreeBSD VM action for runtime classifier coverage.

**Spec:** `docs/superpowers/specs/2026-08-21-typed-transport-error-model-design.md`

## Global Constraints

- P0-4 covers operational/runtime failures only; configuration, validation, and programmer errors remain direct sentinels.
- Caller `context.Cause(ctx)` wins unchanged and is never wrapped in `*transport.Error`.
- Clean TCP FIN, clean TLS EOF, and normal WS close remain lifecycle events with terminal resource `Err()==nil`.
- `Session.Err()`, `PacketConn.Err()`, and `Listener.Err()` store only the first terminal operational failure.
- Existing `TimeoutError` / `LimitError` and existing sentinels such as `ErrWouldBlock`, `ErrMessageTooLarge`, `ErrDatagramTooLarge`, `ErrFrameExceedsQueueBudget`, and `ErrReadBufferFull` remain reachable through `errors.Is/As`.
- Classification must never depend on `Error()` strings.
- `ErrorKind` and `Op` numeric values are append-only after publication.
- Successful Send/TrySend/Read/Receive paths do not allocate `*Error`, snapshot addresses, or run errno/TLS/WS classifiers.
- Running-state `BenchmarkGracefulSendRunning` and `BenchmarkGracefulTrySendRunning` allocations/op must not increase.
- Engine shutdown continues not to aggregate child failures.
- QUIC and HTTP client errors are out of scope.
- FreeBSD mapping requires runtime evidence; cross-compilation alone is insufficient.
- Branch: `feat/typed-error-model`; tracking issue: #52; base commit: `13c6c4878fad6512d00a4c1e17168f8352546f19`.

---

## File Structure

**Create**

- `transport/error_model.go` — public `Error`, `Op`, `ErrorKind`, address snapshotting, categorized cause wrapper, envelope constructor.
- `transport/error_model_test.go` — public contract, cause-chain, duplicate-wrap, address snapshot, unknown behavior.
- `transport/error_classify.go` — protocol-neutral classifier, hints, sentinel/DNS/TLS/WS/wire classification order.
- `transport/error_classify_test.go` — protocol-neutral classifier unit tests.
- `transport/error_classify_unix.go` — Unix-family errno classification.
- `transport/error_classify_unix_test.go` — Unix errno identity tests.
- `transport/error_classify_windows.go` — Windows Winsock errno classification.
- `transport/error_classify_windows_test.go` — Windows errno identity tests.
- `transport/error_tcp_integration_test.go` — TCP dial/listen/refused/reset/FIN/write-timeout/send-vs-write behavior.
- `transport/error_tls_integration_test.go` — x509/TLS vs reset classification.
- `transport/error_websocket_integration_test.go` — WS/WSS upgrade/close/protocol/too-large classification.
- `transport/error_udp_integration_test.go` — UDP send/receive/size classification.
- `transport/error_resource_lifecycle_test.go` — Listener/PacketConn/Session `Err()` ownership and context-control behavior.
- `transport/error_race_test.go` — first-failure owner races.
- `transport/error_benchmark_test.go` — error-path microbenchmarks.
- `docs/runtime-errors.md` — public taxonomy and migration guide.

**Modify**

- `transport/errors.go` — add stable category sentinels only.
- `transport/tcp.go` — wrap dial/listen/socket operational failures with correct `Op`/`Protocol` and preserve caller context precedence.
- `transport/tls.go` — handshakes return typed operational errors while configuration TLS errors remain direct sentinels.
- `transport/engine_stream_dial.go` — wrap outbound admission and TLS handshake boundaries; record framer origin for remote wire classification.
- `transport/conn.go` — Send/TrySend, stream write/read/decode, read-buffer exhaustion, plugin failures, terminal `Err()` storage.
- `transport/stream_graceful.go` — classify close/close-notify failures at `OpClose`; keep control arbitration direct.
- `transport/listener.go` — fatal accept typed error; owner context cancellation leaves `Listener.Err()==nil`; inbound admission rejection stays nonterminal.
- `transport/packet.go` — Send/TrySend/SendTo, UDP write/receive, peer DNS mapping, owner context semantics, terminal `Err()` storage.
- `transport/websocket_client.go` — outbound upgrade/DNS/TLS/resource classification.
- `transport/websocket.go` — WS send/write/read/decode/protocol/too-large classification.
- `transport/websocket_graceful.go` — `TimeoutClose` becomes `OpClose/ErrorTimeout`; control aborts remain error-free in resource `Err()`.
- `transport/websocket_server.go` / `transport/websocket_server_admission.go` — inbound upgrade operational failures remain nonterminal to healthy listener; transfer failures retain cause ownership.
- `.github/workflows/netpoll-v2.yml` — add pinned FreeBSD runtime classifier job and error benchmark smoke.

---

### Task 1: Public Error Envelope and Pure Contract

**Files:**
- Create: `transport/error_model.go`
- Create: `transport/error_model_test.go`
- Modify: `transport/errors.go`

**Interfaces:**
- Produces:
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
- Later tasks consume these exact names.

- [ ] **Step 1: Add compile-time/public contract tests first**

Create `transport/error_model_test.go` with tests that reference not-yet-defined public types:

```go
func TestTransportErrorEnvelopeContract(t *testing.T) {
    raw := syscall.ECONNRESET
    cause := categorized(ErrConnectionReset, raw)
    err := envelopeOperational(OpRead, ogrenet.SchemeTCP,
        &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 10001},
        &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 10002},
        ErrorReset, cause)

    var te *Error
    if !errors.As(err, &te) {
        t.Fatalf("errors.As(*Error) = false: %T %v", err, err)
    }
    if te.Op != OpRead || te.Protocol != ogrenet.SchemeTCP || te.Kind != ErrorReset {
        t.Fatalf("envelope = %+v", te)
    }
    if !errors.Is(err, ErrConnectionReset) {
        t.Fatalf("errors.Is(ErrConnectionReset) = false: %v", err)
    }
    if !errors.Is(err, raw) {
        t.Fatalf("raw cause not reachable: %v", err)
    }
}
```

Add table tests for exact `String()` values:

```go
func TestOpString(t *testing.T) {
    tests := []struct{ op Op; want string }{
        {OpDial, "dial"}, {OpListen, "listen"}, {OpAccept, "accept"},
        {OpHandshake, "handshake"}, {OpUpgrade, "upgrade"},
        {OpRead, "read"}, {OpWrite, "write"}, {OpSend, "send"},
        {OpReceive, "receive"}, {OpClose, "close"}, {OpShutdown, "shutdown"},
        {Op(255), "unknown"},
    }
    for _, tc := range tests {
        if got := tc.op.String(); got != tc.want { t.Fatalf("%d.String()=%q want %q", tc.op, got, tc.want) }
    }
}

func TestErrorKindString(t *testing.T) {
    tests := []struct{ kind ErrorKind; want string }{
        {ErrorUnknown, "unknown"}, {ErrorClosed, "closed"}, {ErrorPeerClosed, "peer-closed"},
        {ErrorTimeout, "timeout"}, {ErrorRefused, "refused"}, {ErrorReset, "reset"},
        {ErrorDNS, "dns"}, {ErrorTLS, "tls"}, {ErrorProtocol, "protocol"},
        {ErrorResourceExhausted, "resource-exhausted"}, {ErrorBackpressure, "backpressure"},
        {ErrorTooLarge, "too-large"}, {ErrorKind(255), "unknown"},
    }
    for _, tc := range tests {
        if got := tc.kind.String(); got != tc.want { t.Fatalf("%d.String()=%q want %q", tc.kind, got, tc.want) }
    }
}
```

Add tests for nil behavior, duplicate wrapping, and address snapshot isolation:

```go
func TestEnvelopeOperationalDoesNotDoubleWrap(t *testing.T) {
    first := envelopeOperational(OpRead, ogrenet.SchemeTCP, nil, nil, ErrorUnknown, errors.New("raw"))
    second := envelopeOperational(OpWrite, ogrenet.SchemeTCP, nil, nil, ErrorUnknown, first)
    if second != first { t.Fatalf("double wrap changed identity: %p != %p", second, first) }
}

func TestTransportErrorSnapshotsTCPAddresses(t *testing.T) {
    local := &net.TCPAddr{IP: net.IPv4(127,0,0,1), Port: 1001}
    remote := &net.TCPAddr{IP: net.IPv4(127,0,0,2), Port: 1002}
    err := envelopeOperational(OpRead, ogrenet.SchemeTCP, local, remote, ErrorUnknown, errors.New("raw"))
    local.IP[3] = 9
    remote.IP[3] = 9
    var te *Error
    if !errors.As(err, &te) { t.Fatal("missing *Error") }
    if te.Local.String() != "127.0.0.1:1001" || te.Remote.String() != "127.0.0.2:1002" {
        t.Fatalf("snapshots mutated: local=%v remote=%v", te.Local, te.Remote)
    }
}
```

- [ ] **Step 2: Run contract tests and verify RED**

Run:

```bash
go test ./transport -run 'Test(TransportError|OpString|ErrorKindString|EnvelopeOperational)' -count=1
```

Expected: compile failure for undefined `Error`, `Op`, `ErrorKind`, category sentinels, `categorized`, and `envelopeOperational`.

- [ ] **Step 3: Implement the public model minimally**

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

Create `transport/error_model.go` with append-only enums:

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

Implement the envelope exactly with a linear cause chain:

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

func (e *Error) Unwrap() error {
    if e == nil { return nil }
    return e.Cause
}
```

Implement category composition:

```go
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

Implement `snapshotAddr` for `*net.TCPAddr`, `*net.UDPAddr`, and value `staticAddr`; retain unknown `net.Addr` implementations unchanged because no generic safe clone exists.

Implement `envelopeOperational` so `nil` stays `nil` and an existing nested `*Error` is returned unchanged:

```go
func envelopeOperational(op Op, protocol ogrenet.Scheme, local, remote net.Addr, kind ErrorKind, cause error) error {
    if cause == nil { return nil }
    var existing *Error
    if errors.As(cause, &existing) { return cause }
    return &Error{Op: op, Protocol: protocol, Kind: kind, Local: snapshotAddr(local), Remote: snapshotAddr(remote), Cause: cause}
}
```

- [ ] **Step 4: Run contract tests GREEN**

Run:

```bash
go test ./transport -run 'Test(TransportError|OpString|ErrorKindString|EnvelopeOperational)' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit Task 1**

```bash
git add transport/errors.go transport/error_model.go transport/error_model_test.go
git commit -m "transport: add operational error envelope"
```

- [ ] **Step 6: Open the Draft PR as the authoritative Go 1.25/1.26 CI harness**

Create Draft PR from `feat/typed-error-model` to `master`:

```text
Title: runtime: add typed transport error taxonomy
Body:
Implements P0-4 typed operational transport errors using the approved design.

Development proceeds task-by-task with TDD. This Draft PR exists early so GitHub Actions provides the authoritative Go 1.25/1.26, Windows, macOS, GmSSL, and cross-platform harness.

Refs #52
Refs #38
```

Keep the PR Draft until Task 6 final verification.

---

### Task 2: Central Classifier and OS-Specific Mapping

**Files:**
- Create: `transport/error_classify.go`
- Create: `transport/error_classify_test.go`
- Create: `transport/error_classify_unix.go`
- Create: `transport/error_classify_unix_test.go`
- Create: `transport/error_classify_windows.go`
- Create: `transport/error_classify_windows_test.go`

**Interfaces:**
- Consumes: Task 1 `Error`, `Op`, `ErrorKind`, `categorized`, `envelopeOperational`.
- Produces:
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
  func classifyPlatformCause(op Op, err error) (kind ErrorKind, category error, ok bool)
  func classifyWSStatus(status websocket.StatusCode) (kind ErrorKind, category error, ok bool)
  ```

- [ ] **Step 1: Write RED classifier tests**

Create protocol-neutral table tests:

```go
func TestClassifyOperationalKnownSentinels(t *testing.T) {
    tests := []struct {
        name string; op Op; cause error; wantKind ErrorKind; wantIs error
    }{
        {"closed", OpSend, ErrClosed, ErrorClosed, ErrClosed},
        {"would-block", OpSend, ErrWouldBlock, ErrorBackpressure, ErrWouldBlock},
        {"message-too-large", OpSend, ErrMessageTooLarge, ErrorTooLarge, ErrMessageTooLarge},
        {"datagram-too-large", OpSend, ErrDatagramTooLarge, ErrorTooLarge, ErrDatagramTooLarge},
        {"frame-budget", OpSend, ErrFrameExceedsQueueBudget, ErrorResourceExhausted, ErrFrameExceedsQueueBudget},
        {"read-buffer", OpRead, ErrReadBufferFull, ErrorResourceExhausted, ErrReadBufferFull},
        {"runtime-invalid-framer", OpRead, ErrInvalidFramer, ErrorUnknown, ErrInvalidFramer},
    }
    for _, tc := range tests {
        t.Run(tc.name, func(t *testing.T) {
            err := classifyOperational(tc.op, ogrenet.SchemeTCP, nil, nil, tc.cause, hintNone)
            var te *Error
            if !errors.As(err, &te) || te.Kind != tc.wantKind { t.Fatalf("err=%#v", err) }
            if !errors.Is(err, tc.wantIs) { t.Fatalf("missing specific sentinel: %v", err) }
            if tc.wantKind == ErrorResourceExhausted && !errors.Is(err, ErrResourceExhausted) {
                t.Fatalf("missing ErrResourceExhausted: %v", err)
            }
        })
    }
}
```

Add timeout/limit composition tests:

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

Add DNS/TLS unit tests using typed errors only:

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
    raw := x509.UnknownAuthorityError{}
    err := classifyOperational(OpHandshake, ogrenet.SchemeTLS, nil, nil, raw, hintTLSHandshake)
    var te *Error
    if !errors.As(err, &te) || te.Kind != ErrorTLS || !errors.Is(err, ErrTLS) {
        t.Fatalf("tls chain=%#v", err)
    }
}
```

Test WS status classification directly through `classifyWSStatus` so unit tests do not synthesize library-private errors:

```go
func TestClassifyWSStatus(t *testing.T) {
    tests := []struct{ status websocket.StatusCode; kind ErrorKind; category error; ok bool }{
        {websocket.StatusProtocolError, ErrorProtocol, ErrProtocolViolation, true},
        {websocket.StatusUnsupportedData, ErrorProtocol, ErrProtocolViolation, true},
        {websocket.StatusInvalidFramePayloadData, ErrorProtocol, ErrProtocolViolation, true},
        {websocket.StatusPolicyViolation, ErrorProtocol, ErrProtocolViolation, true},
        {websocket.StatusMessageTooBig, ErrorTooLarge, ErrMessageTooLarge, true},
        {websocket.StatusInternalError, ErrorUnknown, nil, true},
        {websocket.StatusNormalClosure, ErrorUnknown, nil, false},
        {websocket.StatusGoingAway, ErrorUnknown, nil, false},
    }
    for _, tc := range tests {
        kind, category, ok := classifyWSStatus(tc.status)
        if kind != tc.kind || category != tc.category || ok != tc.ok { t.Fatalf("status=%v got=(%v,%v,%v)", tc.status, kind, category, ok) }
    }
}
```

- [ ] **Step 2: Add platform RED tests**

Unix test (build tag `//go:build !windows`):

```go
func TestClassifyPlatformUnixConnectionErrors(t *testing.T) {
    tests := []struct{ op Op; raw error; kind ErrorKind; category error; ok bool }{
        {OpDial, syscall.ECONNREFUSED, ErrorRefused, ErrConnectionRefused, true},
        {OpRead, syscall.ECONNRESET, ErrorReset, ErrConnectionReset, true},
        {OpWrite, syscall.EPIPE, ErrorPeerClosed, ErrPeerClosed, true},
        {OpWrite, syscall.ENOTCONN, ErrorPeerClosed, ErrPeerClosed, true},
        {OpDial, syscall.ENOTCONN, ErrorUnknown, nil, false},
    }
    for _, tc := range tests {
        kind, category, ok := classifyPlatformCause(tc.op, &net.OpError{Op: "test", Err: &os.SyscallError{Syscall: "test", Err: tc.raw}})
        if kind != tc.kind || category != tc.category || ok != tc.ok { t.Fatalf("%v: got %v %v %v", tc.raw, kind, category, ok) }
    }
}
```

Windows test (build tag `//go:build windows`) uses `syscall.Errno` values matching Winsock constants defined in `error_classify_windows.go` and wraps them in `*os.SyscallError`/`*net.OpError`. Verify refused/reset/aborted/shutdown/not-connected exactly as specified.

- [ ] **Step 3: Run focused classifier tests RED**

```bash
go test ./transport -run '^TestClassify' -count=1
```

Expected: compile failures for classifier symbols.

- [ ] **Step 4: Implement fixed classification order**

Create `transport/error_classify.go` with this control flow, preserving the order from the spec:

```go
func classifyOperational(op Op, protocol ogrenet.Scheme, local, remote net.Addr, cause error, hint classifyHint) error {
    if cause == nil { return nil }
    var existing *Error
    if errors.As(cause, &existing) { return cause }

    var timeout *TimeoutError
    if errors.As(cause, &timeout) || errors.Is(cause, ErrTimeout) {
        return envelopeOperational(op, protocol, local, remote, ErrorTimeout, cause)
    }
    var limit *LimitError
    if errors.As(cause, &limit) || errors.Is(cause, ErrResourceExhausted) {
        return envelopeOperational(op, protocol, local, remote, ErrorResourceExhausted, cause)
    }

    switch {
    case errors.Is(cause, ErrClosed):
        return envelopeOperational(op, protocol, local, remote, ErrorClosed, cause)
    case errors.Is(cause, ErrWouldBlock):
        return envelopeOperational(op, protocol, local, remote, ErrorBackpressure, cause)
    case errors.Is(cause, ErrMessageTooLarge), errors.Is(cause, ErrDatagramTooLarge):
        return envelopeOperational(op, protocol, local, remote, ErrorTooLarge, cause)
    case errors.Is(cause, ErrFrameExceedsQueueBudget):
        return envelopeOperational(op, protocol, local, remote, ErrorResourceExhausted, categorized(ErrResourceExhausted, cause))
    case errors.Is(cause, ErrReadBufferFull):
        return envelopeOperational(op, protocol, local, remote, ErrorResourceExhausted, categorized(ErrResourceExhausted, cause))
    case errors.Is(cause, ErrInvalidFramer):
        return envelopeOperational(op, protocol, local, remote, ErrorUnknown, cause)
    }

    var dns *net.DNSError
    if errors.As(cause, &dns) {
        return envelopeOperational(op, protocol, local, remote, ErrorDNS, categorized(ErrDNS, cause))
    }
    if kind, category, ok := classifyPlatformCause(op, cause); ok {
        return envelopeOperational(op, protocol, local, remote, kind, categorized(category, cause))
    }
    if kind, category, ok := classifyTLSCause(cause, hint); ok {
        return envelopeOperational(op, protocol, local, remote, kind, categorized(category, cause))
    }
    if kind, category, ok := classifyWebSocketCause(cause, hint); ok {
        if category != nil { cause = categorized(category, cause) }
        return envelopeOperational(op, protocol, local, remote, kind, cause)
    }
    if hint == hintWireDecode || hint == hintMessageDecode {
        return envelopeOperational(op, protocol, local, remote, ErrorProtocol, categorized(ErrProtocolViolation, cause))
    }
    return envelopeOperational(op, protocol, local, remote, ErrorUnknown, cause)
}
```

Implement TLS recognition with typed `x509` errors and `*tls.CertificateVerificationError`; only fall back to `ErrorTLS` for other handshake causes when `hint == hintTLSHandshake` after platform/DNS/timeout checks have failed.

Implement WS status classification via `websocket.CloseStatus(cause)` and the table tested above; only use status mapping when `CloseStatus(cause) != -1` and the status is not normal/going-away.

- [ ] **Step 5: Implement OS adapters**

Unix file uses `errors.Is` against `syscall.ECONNREFUSED`, `ECONNRESET`, `EPIPE`, and `ENOTCONN`. Gate `ENOTCONN` to established I/O operations:

```go
func establishedIOOp(op Op) bool {
    switch op {
    case OpRead, OpWrite, OpSend, OpReceive, OpClose:
        return true
    default:
        return false
    }
}
```

Windows file defines/uses Winsock error identities for WSAECONNREFUSED (10061), WSAECONNRESET (10054), WSAECONNABORTED (10053), WSAESHUTDOWN (10058), WSAENOTCONN (10057) through `syscall.Errno` and the same `errors.Is` traversal.

- [ ] **Step 6: Run Task 2 tests GREEN**

```bash
go test ./transport -run '^TestClassify' -count=1
go test ./transport -run 'TestTransportErrorEnvelopeContract|TestClassifyOperationalPreservesTimeoutAndLimitTypes' -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit Task 2**

```bash
git add transport/error_classify*.go transport/error_classify*_test.go
git commit -m "transport: classify operational transport failures"
```

---

### Task 3: TCP/TLS Operational Boundaries and Resource Error Ownership

**Files:**
- Create: `transport/error_tcp_integration_test.go`
- Create: `transport/error_tls_integration_test.go`
- Modify: `transport/tcp.go`
- Modify: `transport/tls.go`
- Modify: `transport/engine_stream_dial.go`
- Modify: `transport/conn.go`
- Modify: `transport/stream_graceful.go`

**Interfaces:**
- Consumes: Task 2 `classifyOperational` and hints.
- Produces: TCP/TLS public API failures and terminal `Session.Err()` values consistently use the typed envelope; `CloseWrite`/TLS close-notify physical failures use `OpClose`.

- [ ] **Step 1: Write TCP RED tests for OpSend vs OpWrite and clean FIN**

Use existing `dialSessionPair` plus deterministic wrappers where kernel behavior would otherwise be flaky.

Required helper:

```go
func assertTransportError(t *testing.T, err error, op Op, protocol ogrenet.Scheme, kind ErrorKind) *Error {
    t.Helper()
    var te *Error
    if !errors.As(err, &te) { t.Fatalf("error %T %v is not *transport.Error", err, err) }
    if te.Op != op || te.Protocol != protocol || te.Kind != kind { t.Fatalf("error envelope=%+v", te) }
    return te
}
```

Test queue backpressure with `WithWriteQueue(1)` and a blocking writer: `TrySend` after admission saturation must be `OpSend/ErrorBackpressure` and `errors.Is(err, ErrWouldBlock)`.

Test message-too-large on Send: `OpSend/ErrorTooLarge` and `errors.Is(err, ErrMessageTooLarge)`.

Inject a deterministic writer returning a wrapped `syscall.ECONNRESET`; `Send` ack must be `OpWrite/ErrorReset`, `errors.Is(err, ErrConnectionReset)`, and `errors.Is(err, syscall.ECONNRESET)`.

Keep the existing half-close proof and add:

```go
serverHalf.CloseWrite(ctx)
waitClosed(t, clientHalf.ReadClosed(), "client read-half")
if err := client.Err(); err != nil { t.Fatalf("clean FIN Err=%v", err) }
```

- [ ] **Step 2: Write TCP Dial/Listen/timeout RED tests**

For refused Dial, bind then close a loopback TCP listener to obtain a concrete unused address and dial it immediately. Assert `OpDial/ErrorRefused`, `errors.Is(err, ErrConnectionRefused)`, and `Remote==nil` if no established remote `net.Addr` exists.

For listener bind failure, keep a raw listener bound on a loopback address and call `Engine.Listen` on the same address. Assert `OpListen/ErrorUnknown` and that the raw bind error remains reachable through the cause chain. Do not add `ErrorAddressInUse` in P0-4.

Reuse the existing blocked `net.Pipe` timeout fixture and assert write timeout now has the envelope:

```go
err := c.Send(ctx, ogrenet.Bin([]byte("blocked")))
assertTransportError(t, err, OpWrite, ogrenet.SchemeTCP, ErrorTimeout)
var timeout *TimeoutError
if !errors.As(err, &timeout) || timeout.Kind != TimeoutWrite || !errors.Is(err, ErrTimeout) { t.Fatalf("timeout chain=%#v", err) }
```

- [ ] **Step 3: Write TLS RED tests**

1. Generate/use the existing local TLS fixture with an untrusted certificate; client `Dial` must return `OpHandshake/ErrorTLS`, `errors.Is(err, ErrTLS)`, and retain a typed x509/TLS verification cause.
2. Inject/reset the transport during TLS handshake; error must be `OpHandshake/ErrorReset`, not `ErrorTLS`.
3. Existing TLS `CloseWrite` timeout/failure must be `OpClose/ErrorTimeout` (specialized `TimeoutWrite` remains inside the chain).
4. Clean peer `close_notify` still closes `ReadClosed` and leaves `Session.Err()==nil`.

- [ ] **Step 4: Run TCP/TLS tests RED**

```bash
go test ./transport -run 'TestTransportErrorTCP|TestTransportErrorTLS' -count=1
```

Expected: failures because runtime paths still return raw/specialized/sentinel errors without the outer envelope.

- [ ] **Step 5: Wrap outbound Dial/Listen/Handshake boundaries**

In `dialTCP`, after `mapOperationTimeout`, preserve caller context first:

```go
if err != nil {
    mapped := mapOperationTimeout(ctx, dctx, TimeoutConnect, err)
    if cause := context.Cause(ctx); cause != nil { return nil, cause }
    return nil, classifyOperational(OpDial, endpoint.Scheme, nil, nil, mapped, hintNone)
}
```

In `listenTCP`, wrap bind/listen failures with `OpListen` after preserving caller context cancellation:

```go
raw, err := lc.Listen(ctx, "tcp", endpoint.Address())
if err != nil {
    if cause := context.Cause(ctx); cause != nil { return nil, cause }
    return nil, classifyOperational(OpListen, endpoint.Scheme, nil, nil, err, hintNone)
}
```

In `dialStream`, classify outbound admission `LimitError` as `OpDial/ErrorResourceExhausted`. For TLS handshake:

```go
err = e.cfg.handshakeClient(ctx, tlsConn)
handshake.release()
if err != nil {
    _ = tlsConn.Close()
    if cause := context.Cause(ctx); cause != nil { return nil, cause }
    return nil, classifyOperational(OpHandshake, endpoint.Scheme, raw.LocalAddr(), raw.RemoteAddr(), err, hintTLSHandshake)
}
```

Do not wrap `clientTLSConfig` configuration errors.

- [ ] **Step 6: Wrap stream Send/Write/Read/Decode failures at ownership points**

For local Send admission paths, return:

```go
return classifyOperational(OpSend, c.protocol, c.LocalAddr(), c.RemoteAddr(), err, hintNone)
```

only when `err` is operational (`ErrClosed`, `ErrWouldBlock`, too-large, queue-byte/resource sentinel, runtime plugin error). Caller context and local `Message.Validate()` remain direct.

In `writeFrame`, stop adding redundant human-only `fmt.Errorf("transport: write: %w")` before classification when the raw error should remain inspectable. Map timeout to `TimeoutError`, then call classifier in `handleOutbound` before ack/terminal ownership:

```go
rawErr := c.writeFrame(req.frame)
err := classifyOperational(OpWrite, c.protocol, c.LocalAddr(), c.RemoteAddr(), rawErr, hintNone)
```

Reader terminal errors classify before `initiateClose`:

```go
if normalized := normalizeConnError(readErr); normalized != nil {
    c.initiateClose(classifyOperational(OpRead, c.protocol, c.LocalAddr(), c.RemoteAddr(), normalized, hintNone))
}
```

Default-wire remote decode failure uses `hintWireDecode`; custom-framer plugin/invariant failures use `hintNone`/`ErrorUnknown`. Add one internal `wireFramer bool` field to `conn`, initialized from `e.cfg.framerFactory == nil`, solely to choose the correct hint. `ErrReadBufferFull` classifies as `OpRead/ErrorResourceExhausted` while retaining both `ErrResourceExhausted` and `ErrReadBufferFull`.

- [ ] **Step 7: Preserve first terminal typed error in stream lifecycle**

Change `conn.abort` so caller/explicit abort never stores a cause and operational failure causes are already classified before entering:

```go
func (c *conn) abort(reason abortReason, cause error) bool {
    cause = normalizeConnError(cause)
    if !c.life.abort(reason) { return false }
    c.closeOnce.Do(func() {
        if reason == abortFailure && cause != nil {
            c.errMu.Lock(); c.err = cause; c.errMu.Unlock()
        }
        c.gate.close()
        close(c.closing)
        ...
    })
    return true
}
```

The omitted physical-close lines are the existing unchanged P0-3 logic: close `c.physical` when non-nil, otherwise `c.raw`. Do not reclassify in `finalize`; derived `net.ErrClosed` after lifecycle abort stays suppressed.

- [ ] **Step 8: Classify stream close/half-close failures as OpClose**

`closeProtocolWrite` returns raw/specialized cause. The writer graceful path converts it before terminal ownership:

```go
if rawErr := c.closeProtocolWrite(); rawErr != nil {
    c.initiateClose(classifyOperational(OpClose, c.protocol, c.LocalAddr(), c.RemoteAddr(), rawErr, hintNone))
    return
}
```

For TLS `CloseWrite` timeout retain `TimeoutError{Kind: TimeoutWrite}` beneath `OpClose/ErrorTimeout`.

- [ ] **Step 9: Run TCP/TLS integration and focused race tests GREEN**

```bash
go test ./transport -run 'TestTransportErrorTCP|TestTransportErrorTLS' -count=1
go test -race ./transport -run 'TestTransportErrorTCP|TestTransportErrorTLS|TestGracefulRace' -count=1
```

Expected: PASS.

- [ ] **Step 10: Commit Task 3**

```bash
git add transport/tcp.go transport/tls.go transport/engine_stream_dial.go transport/conn.go transport/stream_graceful.go transport/error_tcp_integration_test.go transport/error_tls_integration_test.go
git commit -m "transport: type TCP and TLS operational errors"
```

---

### Task 4: WebSocket/WSS Operational Error Mapping

**Files:**
- Create: `transport/error_websocket_integration_test.go`
- Modify: `transport/websocket_client.go`
- Modify: `transport/websocket.go`
- Modify: `transport/websocket_graceful.go`
- Modify: `transport/websocket_server.go`
- Modify: `transport/websocket_server_admission.go`

**Interfaces:**
- Consumes classifier + typed envelope.
- Produces: WS/WSS `OpUpgrade`, `OpSend`, `OpWrite`, `OpRead`, and `OpClose` typed behavior; normal close remains error-free.

- [ ] **Step 1: Write WS/WSS RED tests**

Add deterministic tests:

1. HTTP server rejects upgrade with 403; `Engine.Dial(ws://...)` returns `OpUpgrade/ErrorProtocol`, `errors.Is(err, ErrProtocolViolation)`.
2. WSS server uses untrusted TLS certificate; `Dial(wss://...)` returns `OpHandshake/ErrorTLS` rather than generic upgrade protocol.
3. Peer sends close code `1002`; `Session.Err()` is `OpRead/ErrorProtocol` and `errors.Is(err, ErrProtocolViolation)`.
4. Peer sends close code `1009`; `Session.Err()` is `OpRead/ErrorTooLarge` and `errors.Is(err, ErrMessageTooLarge)`.
5. Peer sends normal 1000/1001 close; `Session.Err()==nil`.
6. Existing nonresponsive-close fixture with `CloseTimeout=50ms`; `Shutdown`/`Session.Err()` terminal error is `OpClose/ErrorTimeout`, and `TimeoutError.Kind==TimeoutClose`.
7. WS `TrySend` saturation returns `OpSend/ErrorBackpressure`; actual WS write timeout returns `OpWrite/ErrorTimeout` with `TimeoutWrite`.

- [ ] **Step 2: Run WS tests RED**

```bash
go test ./transport -run 'TestTransportError(WebSocket|WSS)' -count=1
```

Expected: raw coder/websocket errors or specialized errors without the typed envelope.

- [ ] **Step 3: Classify client upgrade failure by root boundary**

In `dialWebSocket`, caller context still bypasses. Before treating the outer websocket Dial error as `OpUpgrade`, inspect the already-wrapped underlying chain with `classifyOperational` using `hintWSUpgrade`; TLS/DNS/platform classifiers precede WS protocol classification by design.

Use `OpHandshake` when a TLS/x509 cause is discovered for WSS handshake. Implement a small internal helper that chooses final op after classification without double-wrapping:

```go
func classifyWebSocketDial(endpoint ogrenet.Endpoint, err error) error {
    first := classifyOperational(OpUpgrade, endpoint.Scheme, nil, nil, err, hintWSUpgrade)
    var te *Error
    if errors.As(first, &te) && endpoint.Scheme == ogrenet.SchemeWSS && te.Kind == ErrorTLS {
        return &Error{Op: OpHandshake, Protocol: endpoint.Scheme, Kind: te.Kind, Local: te.Local, Remote: te.Remote, Cause: te.Cause}
    }
    return first
}
```

Do not rewrite `TimeoutHandshake` semantics: it remains `ErrorTimeout`; `OpUpgrade` is acceptable for WS HTTP handshake timeout unless the failure is specifically TLS handshake-owned before HTTP upgrade.

- [ ] **Step 4: Classify WS send/write/read/decode**

Local admission and validation mirror stream semantics:

- queue/full/closed/too-large before physical write => `OpSend`.
- `s.ws.Write` timeout/reset/unknown => `OpWrite`.
- inbound abnormal close/protocol/decode/decrypt => `OpRead`.
- normal close => no terminal error.

When `websocket.CloseStatus(err)` is one of 1002/1003/1007/1008/1009/1011, pass the original error into the classifier so raw library cause remains available.

Inbound message-cipher authentication/decrypt failures use `hintMessageDecode` and become `ErrorProtocol`; local cipher `Seal` failure on otherwise valid input uses `OpSend/ErrorUnknown`.

- [ ] **Step 5: Classify close handshake failures at OpClose and preserve control ownership**

In `watchCloseTimeout`:

```go
raw := &TimeoutError{Kind: TimeoutClose, Cause: context.DeadlineExceeded}
err := classifyOperational(OpClose, s.protocol, s.LocalAddr(), s.RemoteAddr(), raw, hintNone)
s.abort(abortFailure, err)
```

`abort(abortCaller,nil)` and `abort(abortExplicit,nil)` never populate `s.err`. `finishLocalGraceful` only stores an error if its close handshake produced a classified operational failure.

- [ ] **Step 6: Keep inbound server upgrade rejection nonterminal to listener**

Resource/admission rejection of one inbound upgrade must close/release that child but must not set WS listener terminal `Err()`. Only fatal listener/HTTP-server operational failure owns listener terminal error.

- [ ] **Step 7: Run WS/WSS GREEN + race**

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

### Task 5: UDP, Listener, Admission, and Context-Owned Resource Semantics

**Files:**
- Create: `transport/error_udp_integration_test.go`
- Create: `transport/error_resource_lifecycle_test.go`
- Modify: `transport/packet.go`
- Modify: `transport/listener.go`

**Interfaces:**
- Produces: UDP operational errors use `OpSend`/`OpWrite`/`OpReceive`; fatal listener accept uses `OpAccept`; owner context cancellation leaves resource `Err()==nil`; outbound admission failures are enveloped, inbound rejection remains nonterminal.

- [ ] **Step 1: Write UDP RED tests**

Cover:

```go
func TestTransportErrorUDPDatagramTooLarge(t *testing.T) {
    // fixture creates a connected UDP PacketConn with max datagram smaller than payload
    err := p.Send(context.Background(), ogrenet.Packet{Data: make([]byte, max+1)})
    assertTransportError(t, err, OpSend, ogrenet.SchemeUDP, ErrorTooLarge)
    if !errors.Is(err, ErrDatagramTooLarge) { t.Fatalf("missing sentinel: %v", err) }
}
```

Add deterministic tests for:

- UDP write timeout => `OpWrite/ErrorTimeout` + `TimeoutWrite`.
- connected UDP read-idle => terminal `PacketConn.Err()` `OpReceive/ErrorTimeout` + `TimeoutReadIdle`.
- TrySend saturation => `OpSend/ErrorBackpressure` + `ErrWouldBlock`.
- explicit `Close()` => `PacketConn.Err()==nil`.

DNS is already proven with a typed `*net.DNSError` in Task 2. In `resolvePeer`, simply route any real resolver error through `classifyOperational(OpSend, ...)`; do not add a public-network-dependent `.invalid` integration test.

- [ ] **Step 2: Write resource owner-context RED tests**

For stream listener:

```go
ctx, cancel := context.WithCancel(context.Background())
ln, _ := e.Listen(ctx, endpoint, handler)
cancel()
waitClosed(t, ln.Done(), "listener")
if err := ln.Err(); err != nil { t.Fatalf("Listener.Err=%v want nil", err) }
```

For `ListenPacket`, cancel its owner context and assert `PacketConn.Err()==nil`.

Create a deterministic fatal listener wrapper whose `Accept()` returns `syscall.EIO`; assert listener terminal `Err()` is `OpAccept/ErrorUnknown` with raw cause preserved.

Create outbound limit test (`MaxConnections=1`) where a second `Dial` returns `OpDial/ErrorResourceExhausted`, `errors.As(*LimitError)`, `errors.Is(ErrResourceExhausted)`.

Keep inbound per-listener rejection test: rejected child must not populate `Listener.Err()`.

- [ ] **Step 3: Run Task 5 RED**

```bash
go test ./transport -run 'TestTransportErrorUDP|TestTransportErrorResource|TestTransportErrorListener' -count=1
```

Expected: failures from unwrapped operational errors and context cancellation still stored in resource `Err()`.

- [ ] **Step 4: Wrap UDP operational boundaries**

Rules in `packet.go`:

- local `ErrNotConnected`, `ErrPeerRequired`, `ErrPeerMismatch`, caller context remain direct validation/control errors.
- `resolvePeer` `net.ResolveUDPAddr` failure becomes `classifyOperational(OpSend, SchemeUDP, p.LocalAddr(), nil, err, hintNone)`.
- Send/TrySend closed/backpressure/too-large/queued-byte errors become `OpSend` envelopes.
- `writeDatagram` returns raw or `TimeoutError`; `handleOutbound` classifies as `OpWrite` before ack and terminal ownership.
- reader timeout/error classifies as `OpReceive`, using the datagram peer as `Remote` where available.

- [ ] **Step 5: Normalize packet/listener owner-context closure**

Do not call `initiateClose(ctx.Err())` from owner-context watchers. Use a control-close path that closes the resource without storing terminal operational error:

```go
case <-ctx.Done():
    p.initiateClose(nil)
```

For listener, preserve `l.cancel()`/socket close but leave `l.err=nil` when its owner context wins.

Fatal accept path classifies at `OpAccept` before `initiateClose`. `net.ErrClosed` caused by local close is normalized away.

- [ ] **Step 6: Envelope outbound admission failures only at public outbound boundary**

`LimitError` internals stay unchanged. `Engine.Dial`/`DialPacket`/outbound WS upgrade resource failures classify with their relevant public operation. Inbound accept/upgrade limit rejection only closes/rejects the child and increments existing counters; it does not terminate listener.

- [ ] **Step 7: Run Task 5 GREEN + full transport race**

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

### Task 6: First-Failure Races, Benchmarks, Documentation, FreeBSD Runtime CI, and Final Verification

**Files:**
- Create: `transport/error_race_test.go`
- Create: `transport/error_benchmark_test.go`
- Create: `docs/runtime-errors.md`
- Modify: `.github/workflows/netpoll-v2.yml`
- Modify existing tests only where direct sentinel equality on operational paths conflicts with the new documented `errors.Is` contract.

**Interfaces:**
- Final deliverable only; no new production API beyond Tasks 1-5.

- [ ] **Step 1: Add deterministic first-owner race tests**

Use existing blocking write/nonresponsive WS helpers and explicit release channels; do not use arbitrary sleeps as ordering mechanisms.

Required tests:

```text
TestTransportErrorRaceTimeoutVsDerivedClose
TestTransportErrorRaceResetVsExplicitClose
TestTransportErrorRaceShutdownDeadlineVsPhysicalClose
TestTransportErrorRaceWebSocketCloseTimeoutVsPhysicalClose
```

Each test must assert both convergence and exact terminal ownership. Example timeout case:

```go
waitClosed(t, c.Done(), "session")
err := c.Err()
assertTransportError(t, err, OpWrite, ogrenet.SchemeTCP, ErrorTimeout)
var timeout *TimeoutError
if !errors.As(err, &timeout) || timeout.Kind != TimeoutWrite { t.Fatalf("terminal error=%#v", err) }
```

Reset-vs-Close must prove both directions with explicit synchronization: reset ownership first preserves `ErrorReset`; explicit Close ownership first leaves `Session.Err()==nil` and suppresses derived reset/closed fallout.

- [ ] **Step 2: Run race tests repeatedly**

```bash
go test -race ./transport -run '^TestTransportErrorRace' -count=20
```

Expected: PASS. Any failure blocks progress and requires `systematic-debugging` before production changes.

- [ ] **Step 3: Add error-path microbenchmarks**

Create exact benchmarks:

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

- [ ] **Step 4: Run benchmark smoke and compare successful-path allocations**

```bash
go test ./transport -run '^$' -bench 'BenchmarkError(WrapKnown|WrapUnknown|ClassifyReset|ClassifyTimeout)' -benchmem -benchtime=1x
go test ./transport -run '^$' -bench 'BenchmarkGraceful(SendRunning|TrySendRunning)' -benchmem -benchtime=10x
```

Record `allocs/op` for the running-state graceful benchmarks and compare against `master` using the same Go version and command. Acceptance: no increase in allocations/op. Do not set a nanosecond threshold for error paths.

- [ ] **Step 5: Write `docs/runtime-errors.md`**

Include exactly these sections:

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
            // inspect TimeoutWrite, TimeoutHandshake, ...
        }
    }
}
```

Explicitly document that operational sentinel checks use `errors.Is`, not `==`, and that `Error()` strings are for humans only.

- [ ] **Step 6: Add error benchmark smoke to Linux CI**

After the graceful lifecycle benchmark smoke:

```yaml
      - name: Error taxonomy benchmark smoke
        run: >-
          go test ./transport -run '^$'
          -bench 'BenchmarkError(WrapKnown|WrapUnknown|ClassifyReset|ClassifyTimeout)'
          -benchmem -benchtime=1x
```

- [ ] **Step 7: Add pinned FreeBSD runtime classifier job**

Use verified `vmactions/freebsd-vm` v1.5.2 commit SHA `77ed28d336d03fe19a3f4f7266c1d2c4714dd79d` rather than a floating tag. Add:

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

If the VM action cannot install/run the repository's minimum Go version reliably, FreeBSD runtime evidence is a release blocker; do not delete the job or substitute cross-compile-only evidence.

- [ ] **Step 8: Commit Task 6 so CI can verify the exact candidate head**

```bash
git add transport/error_race_test.go transport/error_benchmark_test.go docs/runtime-errors.md .github/workflows/netpoll-v2.yml
git add transport/*_test.go  # only existing tests intentionally migrated from direct == to errors.Is
ngit commit -m "transport: harden and document typed errors"
```

The command above must be entered as `git commit`, not `ngit commit`; the leading `n` is not part of the command.

- [ ] **Step 9: Run complete final verification on the committed exact head**

Focused commands:

```bash
go test ./transport -run '^TestTransportError' -count=1
go test -race ./transport -run '^TestTransportError' -count=5
go test ./... -count=1
go test -race ./... -count=1
```

GitHub Actions exact-head matrix must finish with success for:

```text
Linux Go 1.25: format, module hygiene, vet, HTTP benchmark smoke, timeout benchmark smoke, graceful benchmark smoke, error benchmark smoke, full race
Linux Go 1.26: same
Windows Go 1.26: vet + full tests including Winsock classifier
macOS Go 1.26: vet + full tests including Unix classifier
FreeBSD 14.4 VM: focused runtime classifier/transport error tests
GmSSL: secure/gmssl + wire + transport
existing epoll/kqueue/IOCP cross-compile matrix
```

- [ ] **Step 10: Review final diff against the approved spec**

Check specifically:

```text
no config/programmer errors unintentionally wrapped
no caller context wrapped
no clean FIN/TLS EOF/normal WS close turned into terminal errors
no string-based classification
no duplicate Error envelopes
no child failure aggregation into Engine.Shutdown
no QUIC/HTTP scope creep
all new enums append-only and String() stable
all raw causes reachable
```

- [ ] **Step 11: Update Draft PR and mark Ready only after exact-head verification**

Update PR body with final exact head and CI run evidence. Then mark Ready for Review. Do not merge without explicit user authorization.

---

## Final Acceptance Map

- Public `Error`/`Op`/`ErrorKind`: Task 1.
- Stable category sentinels and cause-chain compatibility: Tasks 1-2.
- TimeoutError/LimitError composition: Task 2.
- DNS/Unix/Windows/TLS/WS/wire classification order: Task 2.
- `OpSend` vs `OpWrite`: Tasks 3-5.
- TCP Dial/Listen, TCP/TLS mapping, clean half-close, close-notify semantics: Task 3.
- WS/WSS upgrade, protocol close, normal close, CloseTimeout: Task 4.
- UDP, listener, admission, owner-context `Err()` semantics: Task 5.
- First-failure race ownership: Task 6.
- Linux/Windows/macOS/FreeBSD runtime evidence + GmSSL: Task 6.
- Hot-path allocation constraint and error microbenchmarks: Task 6.
- Public docs/migration from direct sentinel equality: Task 6.
- PR remains Draft until final exact-head verification; merge requires explicit user authorization.
