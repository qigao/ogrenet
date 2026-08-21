# P0-4 Typed Transport Error Model

Status: approved design, implementation not started  
Parent roadmap: #38  
Tracking issue: #52  
Base: `master` at `13c6c4878fad6512d00a4c1e17168f8352546f19`

## 1. Goal

P0-4 adds a stable typed taxonomy for **operational/runtime transport failures** across TCP, TLS, WS, WSS, and UDP. Callers must be able to classify failures with `errors.Is` / `errors.As` without parsing strings, while retaining the original OS, TLS, WebSocket, timeout, or resource-limit cause.

The model must also become the canonical terminal-failure representation used by `Session.Err()`, `PacketConn.Err()`, and `Listener.Err()` so that P0-5 observability can consume one deterministic failure source.

## 2. Scope

This phase covers operational failures that occur while the runtime is performing network lifecycle work:

- dial/connect
- listen/accept
- TLS handshake
- WebSocket HTTP upgrade
- stream/datagram read and receive
- application send admission/backpressure
- physical stream/datagram write
- protocol close/half-close
- graceful shutdown coordination when the failure is operational rather than caller control
- resource admission failures
- runtime timeouts
- protocol/wire violations

Configuration, validation, and programmer errors remain direct sentinel errors and are deliberately outside the typed operational envelope.

Examples that remain direct sentinels include:

- `ErrNilContext`
- `ErrInvalidQueueSize`
- `ErrInvalidBuffer`
- `ErrInvalidQueuedBytes`
- `ErrInvalidMessageSize`
- `ErrInvalidDatagramSize`
- `ErrInvalidTimeout`
- `ErrInvalidLimits`
- `ErrInvalidWebSocketConfig`
- `ErrProtocolMismatch`
- `ErrTLSConfigRequired`
- `ErrTLSVersion`
- `ErrTLSCertificateRequired`
- `ErrPeerRequired`
- `ErrPeerMismatch`
- other option/configuration validation failures

Caller context cancellation/deadline is also outside the typed operational envelope. It remains a control-plane result and is returned unchanged.

## 3. Public error envelope

Add a public error envelope in package `transport`:

```go
type Error struct {
    Op       Op
    Protocol ogrenet.Scheme
    Kind     ErrorKind
    Local    net.Addr
    Remote   net.Addr
    Cause    error
}
```

`Error` implements:

```go
func (e *Error) Error() string
func (e *Error) Unwrap() error
```

`Unwrap` returns `Cause`. `Error` does **not** implement category-matching magic in `Is`; category compatibility is carried by the cause chain.

An operational failure that cannot be classified reliably is still returned as `*Error` with `Kind == ErrorUnknown`, preserving operation/protocol/address context and the raw cause.

A runtime boundary must avoid re-wrapping an existing `*Error` for the same failure. Classification occurs at the earliest boundary that knows the real failing operation and owns the terminal cause.

## 4. Operation taxonomy

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
```

Values are append-only after publication. `Op.String()` provides stable human-readable names.

The taxonomy models caller-visible runtime boundaries, not implementation steps. Do not expose operations such as `SetDeadline`, `Encode`, `ConfigureSocket`, or `AcquireQuota`.

### 4.1 `OpSend` vs `OpWrite`

This distinction is intentional and mandatory.

`OpSend` is the local application-send boundary before physical network I/O, including:

- closed send admission
- queue/frame-slot backpressure
- global/per-session queued-byte admission
- message/datagram size validation

`OpWrite` is the actual physical stream/datagram write boundary after the request has been admitted.

Examples:

```text
TrySend queue full
=> OpSend / ErrorBackpressure / ErrWouldBlock

Send message too large
=> OpSend / ErrorTooLarge / ErrMessageTooLarge

Send admitted, TCP write reset
=> OpWrite / ErrorReset

UDP SendTo admitted, sendto timeout
=> OpWrite / ErrorTimeout / TimeoutWrite
```

### 4.2 Other operation boundaries

```text
TCP/UDP connection establishment       -> OpDial
listener bind                          -> OpListen
listener fatal accept error            -> OpAccept
TLS handshake                          -> OpHandshake
WebSocket HTTP upgrade                 -> OpUpgrade
TCP/TLS/WS application receive/decode  -> OpRead
UDP datagram receive                   -> OpReceive
protocol FIN/close_notify/WS Close I/O -> OpClose
engine/session graceful coordinator    -> OpShutdown only for coordinator-level operational failure
```

A TLS `CloseWrite` / close-notify failure and WebSocket close-handshake timeout are `OpClose`, not normal application `OpWrite`.

## 5. ErrorKind taxonomy

```go
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

Values are append-only after publication. `ErrorUnknown` is intentionally zero so the zero value is safe and conservative. `ErrorKind.String()` returns stable names.

`ErrorRefused` and `ErrorReset` remain separate because they have different retry/alert semantics: refused means connection establishment was actively rejected; reset means an established or in-progress transport was forcibly terminated.

## 6. Category sentinels and compatibility

Add only category sentinels that do not already have a stable equivalent:

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

Do not add generic `ErrBackpressure`, `ErrTooLarge`, or `ErrUnknown` sentinels.

Existing specific contracts remain authoritative:

- `ErrClosed`
- `ErrTimeout`
- `ErrResourceExhausted`
- `ErrWouldBlock`
- `ErrMessageTooLarge`
- `ErrDatagramTooLarge`
- `TimeoutError`
- `LimitError`

Therefore callers can classify at both levels:

```go
var e *transport.Error
if errors.As(err, &e) && e.Kind == transport.ErrorTimeout {
    var timeout *transport.TimeoutError
    if errors.As(err, &timeout) {
        // inspect TimeoutWrite, TimeoutReadIdle, ...
    }
}
```

## 7. Error-chain composition

The preferred chain is linear:

```text
*transport.Error
    ↓ Unwrap
specialized/category cause
    ↓ Unwrap
raw OS / TLS / WebSocket / decoder cause
```

### 7.1 Existing specialized errors

Timeouts keep their existing shape:

```text
Error{Kind: ErrorTimeout}
  -> TimeoutError{Kind: TimeoutWrite}
     -> raw timeout cause
```

Resource exhaustion keeps its existing shape:

```text
Error{Kind: ErrorResourceExhausted}
  -> LimitError{Kind: LimitConnections}
     -> ErrResourceExhausted
```

### 7.2 Internal categorized cause

For categories that need both a new stable sentinel and the raw root cause, use an unexported wrapper:

```go
type categorizedCause struct {
    category error
    cause    error
}

func (e *categorizedCause) Error() string
func (e *categorizedCause) Unwrap() error
func (e *categorizedCause) Is(target error) bool
```

`Is` matches only `category`. `Unwrap` returns the original cause.

Example:

```text
Error{Op: OpRead, Kind: ErrorReset}
  -> categorizedCause{category: ErrConnectionReset}
     -> *net.OpError
        -> *os.SyscallError
           -> syscall.ECONNRESET
```

This must satisfy all three levels simultaneously:

```go
errors.As(err, new(*transport.Error))
errors.Is(err, transport.ErrConnectionReset)
errors.Is(err, syscall.ECONNRESET) // on the platform where that cause applies
```

## 8. Classification architecture

Classification is centralized. TCP, TLS, WS/WSS, and UDP I/O code must not each grow their own errno/category tables.

Suggested files:

```text
transport/error_model.go
transport/error_classify.go
transport/error_classify_unix.go
transport/error_classify_windows.go
```

The protocol-neutral classifier accepts the operation and explicit context known by the failing boundary:

```go
func classifyOperational(
    op Op,
    protocol ogrenet.Scheme,
    local, remote net.Addr,
    cause error,
    hint classifyHint,
) error
```

`classifyHint` is internal and may include:

```go
type classifyHint uint8

const (
    hintNone classifyHint = iota
    hintTLSHandshake
    hintWSUpgrade
    hintWireDecode
    hintMessageDecode
)
```

Hints state what boundary produced the error; they are not inferred from strings.

## 9. Classification order

Order is fixed because one error chain may satisfy multiple classifiers:

```text
0. nil
1. caller context cause -> bypass, return unchanged
2. existing *Error -> do not double-wrap
3. TimeoutError / runtime timeout -> ErrorTimeout
4. LimitError / ErrResourceExhausted -> ErrorResourceExhausted
5. known runtime sentinel (ErrClosed / ErrWouldBlock / too-large)
6. *net.DNSError -> ErrorDNS
7. OS connection errno -> Refused / Reset / PeerClosed
8. TLS/x509 classification
9. WebSocket classification
10. wire/message protocol classification
11. fallback -> ErrorUnknown
```

OS connection classification precedes TLS/WS classification. A TCP reset during TLS handshake is a transport reset, not a certificate/TLS failure.

Examples:

```text
TLS handshake + ECONNRESET
=> OpHandshake / ErrorReset

TLS handshake + x509.UnknownAuthorityError
=> OpHandshake / ErrorTLS
```

## 10. Platform errno mapping

Platform mapping uses typed identity through `errors.Is` / `errors.As`, never `Error()` text.

### 10.1 Unix-family mapping

For established I/O boundaries where applicable:

```text
ECONNREFUSED -> ErrorRefused
ECONNRESET   -> ErrorReset
EPIPE        -> ErrorPeerClosed
ENOTCONN     -> ErrorPeerClosed only for established read/write/send/receive/close boundaries
```

Do not classify `ENOTCONN` as peer-closed in an unrelated setup/configuration path.

### 10.2 Windows mapping

Use Winsock errno identities through the wrapped error chain:

```text
WSAECONNREFUSED -> ErrorRefused
WSAECONNRESET   -> ErrorReset
WSAECONNABORTED -> ErrorReset
WSAESHUTDOWN    -> ErrorPeerClosed
WSAENOTCONN     -> ErrorPeerClosed for established I/O boundaries
```

The exact constants live in Windows-only code. Protocol code must consume only the normalized result.

## 11. DNS mapping

A `*net.DNSError` is `ErrorDNS` at the operation that triggered resolution, normally `OpDial`, `OpListen`, or UDP peer resolution when that resolution is operational.

The cause chain must retain `*net.DNSError` so callers can inspect fields such as name, timeout, and temporary metadata when meaningful.

Caller context cancellation that manifests through resolver work still bypasses the typed envelope if the caller context is the winning cause.

## 12. TLS / x509 mapping

Configuration errors such as `ErrTLSVersion`, `ErrTLSConfigRequired`, and `ErrTLSCertificateRequired` remain direct configuration sentinels.

Operational TLS handshake failures are classified after context/timeout/DNS/OS transport causes have been considered.

Recognize typed certificate failures such as:

- `x509.UnknownAuthorityError`
- `x509.HostnameError`
- `x509.CertificateInvalidError`
- `*tls.CertificateVerificationError`

These become:

```text
OpHandshake / ErrorTLS / ErrTLS / original typed cause
```

Remaining genuine TLS handshake/alert failures under `hintTLSHandshake` are also `ErrorTLS`.

Transport reset/refusal remains transport reset/refusal even when it happened during TLS handshake.

## 13. WebSocket mapping

Normal close statuses are lifecycle events, not terminal errors:

```text
1000 Normal Closure
1001 Going Away
```

They preserve `Session.Err() == nil` when they win through the normal graceful/peer-close path.

Protocol close failures map as follows:

```text
1002 Protocol Error   -> ErrorProtocol
1003 Unsupported Data -> ErrorProtocol
1007 Invalid Payload  -> ErrorProtocol
1008 Policy Violation -> ErrorProtocol
1009 Message Too Big  -> ErrorTooLarge / ErrMessageTooLarge
1011 Internal Error   -> ErrorUnknown unless a more precise underlying cause exists
```

Abnormal/no-close-frame termination first checks for a lower-level OS cause. If no reliable cause exists, classify conservatively as `ErrorPeerClosed` only when the boundary can prove peer termination; otherwise use `ErrorUnknown`.

WebSocket HTTP upgrade rejection is `OpUpgrade / ErrorProtocol` after lower-level DNS/TLS/OS causes are excluded. The HTTP/library cause remains reachable. HTTP status codes do not create new `ErrorKind` values.

## 14. Wire, message, and cipher failures

Remote malformed bytes are operational protocol failures:

- invalid stream frame decode
- invalid consumed length from a remote frame
- unsupported remote WebSocket message type
- malformed remote text/base64 payload
- inbound message-security authentication/decryption failure

These become `OpRead / ErrorProtocol / ErrProtocolViolation` (or `OpReceive` if a future datagram decode layer applies).

Do not misclassify local application mistakes as remote protocol violations:

- local `Message.Validate()` failure remains validation behavior
- local cipher `Seal` failure remains its original local failure unless a separate stable operational category is justified later

Inbound message-cipher authentication failure is `ErrorProtocol`, not `ErrorTLS`. TLS/WSS channel security and ogrenet message security are separate layers.

## 15. Clean lifecycle vs ErrorPeerClosed

`ErrorPeerClosed` does **not** mean every EOF.

P0-3 lifecycle semantics remain unchanged:

```text
clean TCP FIN        -> ReadClosed(), Session.Err()==nil
clean TLS EOF        -> ReadClosed(), Session.Err()==nil
normal WS close      -> Session.Err()==nil
successful Shutdown  -> nil terminal Err
explicit Close       -> nil terminal Err
```

`ErrorPeerClosed` is used only when peer termination causes an operation that was expected to continue to fail, for example a write observing a broken pipe after the peer has terminated the connection.

This distinction is required to preserve half-close semantics.

## 16. Public return rules

Operational failures returned by public transport runtime APIs use `*transport.Error` except for the following deliberate bypasses.

### 16.1 Caller context

Return caller context cause unchanged:

```text
context.Canceled
context.DeadlineExceeded
context.Cause(ctx)
```

Do not wrap it in `*Error`.

### 16.2 Configuration/programmer/validation failures

Return existing validation/configuration sentinels unchanged.

### 16.3 Lifecycle arbitration

P0-3 control-plane arbitration remains direct. For example, a graceful `Shutdown()` interrupted by explicit local `Close()` may return `ErrClosed` directly. This is not presented as a network failure.

All other runtime failures crossing public API boundaries receive the typed envelope.

## 17. Resource Err() contract

`Session.Err()`, `PacketConn.Err()`, and `Listener.Err()` record only the **first terminal operational failure** that caused abnormal resource termination.

They are `nil` for:

- clean peer FIN / TLS EOF
- normal WebSocket close
- successful graceful shutdown
- explicit local `Close()`
- owner/caller context cancellation that aborts a graceful phase
- listener owner context cancellation
- `ListenPacket` owner context cancellation

They contain `*transport.Error` for terminal runtime failure such as:

- read/write/receive timeout
- connection reset
- protocol violation
- fatal accept error
- TLS operational failure
- close-handshake timeout

`OnClose(..., err)` receives the same terminal error value/chain as `Resource.Err()`; it must not independently reclassify the failure.

### 17.1 Context-owned listener/packet lifetime

Current listener and packet-listener context watchers may write `context.Canceled` / deadline causes into resource `Err()`. P0-4 normalizes this behavior with Session semantics: owner-context expiry closes the resource as a control-plane action but leaves resource `Err()` nil.

The caller already owns the context and does not need its own cancellation re-reported as a transport failure.

## 18. First-failure precedence

Classification occurs at the failure ownership point, before hard close produces derivative errors.

Precedence follows established P0-2/P0-3 semantics:

```text
1. first real transport/protocol failure that wins terminal ownership
2. runtime timeout that wins terminal ownership
3. explicit local abort or caller cancellation (control-plane; no resource Err)
```

Examples:

```text
write reset wins, then Close()
=> Session.Err remains OpWrite/ErrorReset

timeout wins, closing socket causes net.ErrClosed in reader
=> Session.Err remains ErrorTimeout

caller Shutdown deadline wins
=> physical abort; method returns caller cause; Session.Err()==nil

explicit Close wins
=> derived reader/writer close errors do not populate Session.Err
```

Engine graceful shutdown continues not to aggregate child terminal errors. A failed child keeps its own typed `Session.Err()` while `Engine.Shutdown()` waits for convergence according to P0-3.

## 19. Address metadata

Addresses are snapshotted at error creation. Do not retain a mutable address object owned elsewhere when a copy can be made safely.

Rules:

- TCP/TLS/WS/WSS session errors: local and remote when known
- connected UDP: local and remote
- unconnected UDP receive: local socket address plus that datagram peer
- accept failure: listener local address; remote usually nil
- DNS/refused before a socket exists: local may be nil; remote remains nil if there is no actual `net.Addr`
- do not fabricate an address merely from endpoint text to fill the field

Copy standard `*net.TCPAddr` / `*net.UDPAddr` values and their IP slices when storing them.

## 20. Unknown operational errors

`ErrorUnknown` is a supported public result, not an implementation failure.

If the runtime knows that an operational boundary failed but cannot classify the cause reliably, return:

```go
&transport.Error{
    Op:       op,
    Protocol: protocol,
    Kind:     transport.ErrorUnknown,
    Local:    local,
    Remote:   remote,
    Cause:    raw,
}
```

Never guess reset/TLS/protocol from a human-readable string.

There is intentionally no `ErrUnknown` sentinel. Callers check `Error.Kind` and inspect `Cause`.

P0-5 may expose an unknown-error counter; a non-zero unknown rate can then justify future classifier additions based on evidence.

## 21. Testing strategy

### 21.1 Pure contract tests

Cover:

- `Error.Error` and `Unwrap`
- `Op.String`
- `ErrorKind.String`
- internal categorized-cause `Is` and `Unwrap`
- existing `TimeoutError` / `LimitError` composition through the envelope
- duplicate-wrap prevention
- address snapshot isolation
- unknown cause preservation

Each category test proves three levels when applicable:

1. `errors.As(err, *transport.Error)` and correct `Kind`
2. stable category/specific `errors.Is`
3. raw typed/errno cause still reachable

### 21.2 Protocol integration matrix

At minimum:

```text
TCP dial refused
TCP reset on read or write
TCP clean FIN remains error-free
TCP write timeout
TLS bad certificate / hostname verification
TLS reset during handshake classified Reset, not TLS
WS upgrade rejection
WS normal close remains error-free
WS protocol violation / message-too-big mapping
UDP write timeout
UDP receive/read-idle timeout for connected socket
UDP datagram too large
resource/admission exhaustion
TrySend queue saturation
caller context cancellation bypass
explicit Close leaves Err nil
```

### 21.3 First-owner race tests

Deterministically cover:

- timeout vs derived `net.ErrClosed`
- reset vs explicit Close
- caller Shutdown deadline vs physical-close fallout
- WebSocket CloseTimeout vs derived physical-close fallout

The terminal resource error may have only one owner.

### 21.4 No string-based classifier tests

Classifier correctness tests may use only:

- typed errors
- `syscall.Errno`
- `*os.SyscallError`
- `*net.OpError`
- `*net.DNSError`
- x509/tls typed errors
- coder/websocket typed close status
- deterministic wrappers

`strings.Contains(err.Error(), ...)` is forbidden for classification correctness.

## 22. Cross-platform verification

P0-4 must prove runtime mapping, not only compilation.

Required evidence:

```text
Linux:
  full existing race suite
  runtime errno classifier tests

Windows:
  runtime Winsock classifier tests
  full transport tests

macOS:
  runtime Unix errno classifier tests
  full transport tests

FreeBSD:
  focused runtime classifier + transport error tests
```

The current CI only cross-compiles kqueue for FreeBSD. That is insufficient to claim P0-4 OS mapping acceptance.

Implementation must add a pinned/reproducible FreeBSD runtime job using an approved VM/runner mechanism if GitHub-hosted runners do not provide FreeBSD directly. If a reliable runtime environment cannot be established, FreeBSD operational mapping remains an explicit release blocker rather than being inferred from compile success.

Existing GmSSL coverage remains required to prove that error-envelope changes do not regress tagged security/wire/transport tests.

## 23. Performance constraints

Successful steady-state paths must not enter the classifier or allocate the typed envelope.

Specifically:

```text
successful Send/TrySend:
  no *Error allocation
  no address snapshot
  no classifier

successful Read/Receive:
  no *Error allocation
  no errno classification
```

Error paths may allocate a small envelope and category wrapper because they are not the steady-state hot path.

Add microbenchmarks:

- `BenchmarkErrorWrapKnown`
- `BenchmarkErrorWrapUnknown`
- `BenchmarkErrorClassifyReset`
- `BenchmarkErrorClassifyTimeout`

Re-run existing running-state graceful benchmarks. Acceptance requirement:

> P0-4 must not increase running-state `BenchmarkGracefulSendRunning` or `BenchmarkGracefulTrySendRunning` allocations/op.

No nanosecond performance threshold is imposed on error handling itself.

## 24. Documentation

Create `docs/runtime-errors.md` with these sections:

1. Operational Errors vs Configuration Errors
2. Error / Op / ErrorKind
3. `errors.Is` and `errors.As`
4. TimeoutError and LimitError Composition
5. Closed vs PeerClosed vs Clean EOF
6. Context Cancellation
7. TCP/TLS Mapping
8. WebSocket Mapping
9. UDP Mapping
10. Resource Limits and Backpressure
11. Error Precedence and Resource `Err()`
12. Cross-Platform Mapping Guarantees
13. Unknown Errors

The documentation must explicitly warn users not to make logic decisions from `Error()` strings.

Recommended usage example:

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

## 25. Migration behavior

P0-4 is intentionally additive around established sentinel/specialized contracts where possible.

Existing checks such as:

```go
errors.Is(err, transport.ErrWouldBlock)
errors.Is(err, transport.ErrTimeout)
errors.As(err, &transport.TimeoutError{})
errors.Is(err, transport.ErrResourceExhausted)
errors.As(err, &transport.LimitError{})
```

must continue to work when the error is now wrapped by `*transport.Error`.

Code that compares sentinel errors with `==` on operational paths may stop working once those sentinels are wrapped. Documentation must explicitly require `errors.Is` rather than direct equality for runtime operational errors.

Configuration/programmer errors remain direct sentinels, so their existing direct behavior is unchanged.

## 26. Non-goals

P0-4 does not:

- wrap every configuration/programmer error in `*Error`
- change P0-3 lifecycle ownership or half-close semantics
- aggregate child errors into `Engine.Shutdown`
- change caller-context precedence from P0-2/P0-3
- add observer/statistics/metrics callbacks (P0-5)
- define retry policy or a business-level `Temporary()` contract
- unify QUIC or HTTP client errors into `transport.Error`
- introduce locale or error-string parsing
- add message-security-specific crypto error kinds
- guarantee identical raw errno across operating systems
- redesign wire/message security

The portable transport runtime guarantees stable `ErrorKind`, category sentinel behavior, operation metadata, and raw-cause preservation; raw OS causes remain platform-specific by design.

## 27. Acceptance checklist

P0-4 is complete when all of the following are true:

- operational runtime failures use the typed envelope consistently
- caller context and configuration/programmer failures follow the bypass rules
- `OpSend` vs `OpWrite` semantics are covered and documented
- clean FIN/TLS EOF/normal WS close do not create terminal errors
- `ErrorPeerClosed` is only used for failing operations, not clean lifecycle EOF
- TimeoutError and LimitError continue to work through `errors.As`
- category sentinels and raw causes remain reachable through `errors.Is/As`
- refused/reset/DNS/TLS/protocol/resource/backpressure/too-large/unknown are covered
- resource `Err()` stores only the first terminal operational failure
- context-owned listener/packet shutdown no longer pollutes resource `Err()`
- Linux/Windows/macOS/FreeBSD runtime classifier evidence exists
- GmSSL and existing cross-compiles remain green
- no classification logic parses strings
- successful hot paths do not classify or allocate `*Error`
- running-state Send/TrySend allocations/op do not regress
- `docs/runtime-errors.md` is complete

Refs #52
Refs #38
