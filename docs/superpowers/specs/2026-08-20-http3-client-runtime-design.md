# HTTP/3 Client Runtime Design

Date: 2026-08-20
Status: proposed for implementation after review
Parent roadmap: #41, #38
Stacked base: `feat/quic-client-runtime`

## Goal

Add a production-grade HTTP/3-only client transport to `ogrenet/client` using standard `net/http` request/response semantics while preserving the QUIC-native boundary established by `ogrenet/quic`.

The implementation must reuse ogrenet's QUIC security and resource policy without widening the public `ogrenet/quic` API to expose `quic-go` types or HTTP/3-specific stream machinery.

## Non-goals

This phase does not add:

- HTTP/3 fallback to HTTP/2 or HTTP/1.1
- a multi-protocol HTTP facade
- public QUIC listener/server APIs
- WebTransport or MASQUE
- 0-RTT
- active connection migration policy
- a new request/response abstraction
- public exposure of dependency-specific QUIC flow-control or stream-limit knobs

## Architecture

Use a shared internal QUIC policy package and keep HTTP/3 as a separate `client` transport.

```text
ogrenet
├── client
│   ├── http.go              HTTP/1.1 + HTTP/2
│   └── http3.go             HTTP/3-only RoundTripper/client
├── internal
│   └── quicpolicy           shared TLS/timeouts/windows/0-RTT policy
└── quic
    ├── client.go            QUIC-native public client runtime
    └── config.go            public QUIC config using quicpolicy
```

`internal/quicpolicy` may import `github.com/quic-go/quic-go` because it is module-internal. Neither `client.HTTP3Config` nor the public `quic` API exposes dependency concrete types.

The public `ogrenet/quic` contract remains unchanged by HTTP/3.

## Why HTTP/3 does not use the current public `quic.Conn`

HTTP/3 requires peer-initiated unidirectional QUIC streams for the HTTP/3 control stream and QPACK encoder/decoder streams. The current public QUIC client deliberately disables peer-initiated unidirectional streams because it does not expose a stable public uni-stream API.

Passing the current `quic.Conn` directly to HTTP/3 would therefore require broadening the public QUIC API for an HTTP/3 implementation detail.

Instead, public QUIC and HTTP/3 share the same internal policy but construct protocol-appropriate dependency configurations.

## Public API

Add to package `client`:

```go
type HTTP3Config struct {
    TLSConfig              *tls.Config
    HandshakeTimeout       time.Duration
    IdleTimeout            time.Duration
    MaxResponseHeaderBytes int64
    DisableCompression     bool
    EnableDatagrams        bool
}

type HTTP3Transport struct {
    // unexported implementation
}

func NewHTTP3Transport(cfg HTTP3Config) (*HTTP3Transport, error)
func NewHTTP3Client(cfg HTTP3Config) (*http.Client, error)
```

`HTTP3Transport` implements `http.RoundTripper` and `io.Closer`, and exposes:

```go
func (t *HTTP3Transport) RoundTrip(*http.Request) (*http.Response, error)
func (t *HTTP3Transport) Close() error
func (t *HTTP3Transport) CloseIdleConnections()
```

`NewHTTP3Client` leaves `http.Client.Timeout` unset, matching the existing HTTP/1.1 + HTTP/2 client. Request contexts govern request lifetime and streaming bodies.

### Stream-limit decision from design self-review

An earlier brainstorm considered exposing `MaxIncomingStreams` and `MaxIncomingUniStreams` in `HTTP3Config`. The final design rejects those public knobs.

Reasons:

- ordinary HTTP/3 clients must not accept server-initiated bidirectional request streams; exposing a positive peer-bidirectional limit would permit an invalid protocol state
- peer unidirectional streams are HTTP/3 control/QPACK implementation capacity, not an application-level feature in this phase
- WebTransport/Extended CONNECT, which would change the stream model, is explicitly out of scope

The H3 profile therefore owns these limits internally and tests them as policy invariants.

## Protocol semantics

HTTP/3 transport is HTTP/3-only.

- only `https://` requests are accepted
- ALPN is always `h3`
- TLS minimum is TLS 1.3
- no H3 -> H2/H1 fallback is attempted
- redirects remain standard `http.Client` policy
- request and response types remain `net/http` types
- HTTP status codes are normal responses, never transport errors
- compression behavior follows standard `net/http` expectations

The implementation uses `github.com/quic-go/quic-go/http3.Transport` internally.

## Shared QUIC policy

Create `internal/quicpolicy` containing policy defaults and builders used by public QUIC and HTTP/3.

The shared policy owns:

- TLS 1.3 minimum enforcement
- TLS config cloning
- ALPN pinning supplied by the internal profile
- handshake timeout default
- idle timeout default
- initial/max stream receive windows
- initial/max connection receive windows
- 0-RTT policy
- datagram enablement

### Public QUIC profile

The existing public QUIC profile remains behaviorally unchanged:

- peer bidirectional stream limit remains at the current bounded default
- peer unidirectional streams remain disabled
- 0-RTT remains disabled
- datagrams remain opt-in

The policy extraction must pass the existing QUIC tests unchanged before HTTP/3 code is added.

### HTTP/3 profile

The HTTP/3 profile uses:

- peer-initiated bidirectional streams: disabled (`-1` in the dependency config)
- peer-initiated unidirectional streams: fixed bounded default of 16
- the same bounded receive-window defaults as the public QUIC profile unless an H3 test proves a protocol-specific minimum is required
- 0-RTT: disabled
- datagrams: disabled by default and enabled only through `HTTP3Config.EnableDatagrams`

The uni-stream default of 16 provides room for the mandatory control/QPACK streams plus bounded extension headroom without inheriting the dependency's much broader default.

## 0-RTT enforcement and QUIC dial path

`quic-go/http3.Transport` normally uses an early-dial path. That is not acceptable for this phase because 0-RTT is explicitly disabled until replay semantics are designed.

`HTTP3Transport` therefore supplies a custom internal `Dial` function to the dependency transport and uses non-early `quic.Transport.Dial`, never `DialEarly` or `DialAddrEarly`.

To avoid one UDP socket per host and to retain context-aware connection setup, the wrapper owns a lazily initialized shared QUIC transport:

```text
HTTP3Transport
  ├── http3.Transport
  ├── net.UDPConn
  └── quic.Transport
```

The custom dial path:

1. parses the authority host and numeric port
2. resolves host addresses with `net.Resolver.LookupIPAddr(ctx, host)` so request/dial cancellation can interrupt DNS
3. chooses a resolved UDP address deterministically using the first usable address for this phase
4. calls the owned `quic.Transport.Dial(ctx, udpAddr, tlsCfg, quicCfg)`

Happy Eyeballs / address racing for QUIC is deferred to the broader resolver/dual-stack work in #38. The H3 implementation must not silently fall back to TCP when QUIC address establishment fails.

The wrapper closes the dependency HTTP/3 transport first, then the owned QUIC transport, then the UDP socket. Repeated close is idempotent at the ogrenet boundary.

## Resource governance

HTTP/3 inherits request multiplexing and host connection pooling from the underlying H3 transport, while ogrenet owns the QUIC policy passed into it.

Required bounds:

- bounded handshake timeout
- bounded idle timeout
- bounded stream receive windows
- bounded connection receive windows
- peer unidirectional stream limit = 16
- peer bidirectional streams disabled
- bounded response header bytes
- no 0-RTT path
- one lazily allocated shared UDP transport per `HTTP3Transport`

`MaxResponseHeaderBytes == 0` uses a bounded default consistent with the existing HTTP client policy. Negative values are rejected. Because the dependency uses `int`, a value larger than the platform `int` range is rejected rather than truncated.

Engine-wide/global QUIC connection admission remains separate runtime-governance work and is not required for this H3 RoundTripper phase.

## Datagram policy

`EnableDatagrams` is explicit and defaults to false.

When false, both HTTP/3 datagrams and QUIC datagrams are disabled.

When true, both layers are enabled together. The implementation does not permit an internally inconsistent H3-datagrams-on / QUIC-datagrams-off state.

This does not expose WebTransport or MASQUE APIs.

## TLS policy

Caller-supplied `TLSConfig` is cloned.

Rules:

- `MinVersion == 0` becomes TLS 1.3
- `MinVersion < TLS 1.3` is rejected
- `MaxVersion != 0 && MaxVersion < TLS 1.3` is rejected
- caller-supplied `NextProtos` is replaced
- `NextProtos` is pinned to `[]string{"h3"}`
- insecure certificate verification is never enabled implicitly

The caller remains responsible for roots, client certificates, server name policy, and normal `tls.Config` concerns.

## Error model

### Configuration errors

Expose stable package-level sentinels:

```go
var (
    ErrInvalidHTTP3Config   = errors.New("client: invalid HTTP/3 transport configuration")
    ErrHTTP3TLSVersion      = errors.New("client: HTTP/3 requires TLS 1.3 or newer")
    ErrHTTP3TransportClosed = errors.New("client: HTTP/3 transport is closed")
)
```

Wrapping may add context, but callers can use `errors.Is`.

### Runtime errors

The first API uses a deliberately small stable taxonomy:

```go
type HTTP3ErrorKind uint8

const (
    HTTP3ErrorUnknown HTTP3ErrorKind = iota
    HTTP3ErrorTransport
    HTTP3ErrorProtocol
    HTTP3ErrorClosed
)

type HTTP3Error struct {
    Kind  HTTP3ErrorKind
    Cause error
}
```

Mapping rules:

- request context cancellation/deadline is returned with `context.Canceled` / `context.DeadlineExceeded` preserved through `errors.Is`
- dependency `http3.Error` values map to `HTTP3ErrorProtocol`
- QUIC connection/handshake/timeout failures map to `HTTP3ErrorTransport`
- dependency transport-closed state maps to `ErrHTTP3TransportClosed` and `HTTP3ErrorClosed`
- unrelated request-validation errors that cannot be classified stably are returned with their original cause rather than forced into a guessed category

The original dependency error is always retained as the cause when wrapping.

The design does not expose separate header/body kinds. Body read errors can arise after `RoundTrip` returns and wrapping every response body solely to manufacture a taxonomy is unnecessary complexity. HTTP status codes are never converted into `HTTP3Error`.

## Lifecycle semantics

`HTTP3Transport` owns its H3 pool, QUIC transport, and UDP socket.

Requirements:

- concurrent `RoundTrip` calls are supported
- `Close` is idempotent and returns the first close result
- after `Close`, new requests fail with `ErrHTTP3TransportClosed`
- `CloseIdleConnections` is safe and delegates to the H3 pool without interrupting active multiplexed requests
- closing the transport closes pooled QUIC connections, the shared QUIC transport, and the UDP socket
- closing one response body does not close unrelated multiplexed requests

The underlying `quic-go/http3.Transport` in v0.61 provides both `Close` and `CloseIdleConnections`; the wrapper preserves those semantics while owning the additional non-early QUIC dial resources.

`NewHTTP3Client` does not hide ownership: documentation instructs long-lived callers to retain and close the concrete transport, or use the client's idle-connection hook when only idle cleanup is needed.

## Internal implementation shape

Preferred files:

```text
internal/quicpolicy/policy.go
internal/quicpolicy/policy_test.go
quic/config.go
client/http3.go
client/http3_test.go
client/http3_integration_test.go
client/http3_benchmark_test.go
docs/http3-client.md
```

Responsibilities:

- `internal/quicpolicy/policy.go`: shared TLS/QUIC normalization and protocol profiles
- `quic/config.go`: public QUIC validation delegating to the public-client profile
- `client/http3.go`: H3 config, shared-UDP non-early dial bridge, lifecycle, RoundTripper/client constructors, stable error mapping
- unit tests: policy/config/lifecycle/error invariants
- integration tests: deterministic local H3 behavior
- benchmark: multiplexed request throughput
- docs: ownership, no-fallback, TLS, datagrams, shutdown

## Test strategy

All protocol tests use deterministic local servers; no public-network dependency.

### Required unit tests

- default config normalization
- negative timeouts rejected
- TLS minimum below 1.3 rejected
- TLS maximum below 1.3 rejected
- caller TLS config cloned
- ALPN forced to `h3`
- bounded QUIC receive windows
- H3 peer uni-stream limit exactly 16
- H3 peer bidirectional streams disabled
- 0-RTT/early dial path not used
- datagram flag propagated consistently to H3 and QUIC
- oversized `MaxResponseHeaderBytes` rejected without integer truncation
- repeated `Close` safe
- new request after close returns stable closed error
- context and protocol/transport error mapping preserves causes

### Required integration tests

- HTTP/3 TLS loopback request/response
- response body streaming
- request body streaming through standard `http.Request`
- context cancellation during an active request
- cancellation during DNS/connection establishment through the custom dial path
- concurrent multiplexed requests
- connection reuse across sequential requests
- TLS/ALPN handshake failure
- malformed/failed QUIC connection releases resources
- transport close terminates relevant pending work deterministically
- `CloseIdleConnections` leaves active requests intact
- no H3 -> H2/H1 fallback

The no-fallback test uses a target that would succeed through H2/H1 but does not provide a valid H3 path and asserts failure rather than silent TCP fallback.

### Race and cross-platform gates

Required CI gates remain:

- `gofmt`
- `go mod tidy` hygiene
- `go vet ./...`
- `go test -race -count=1 ./...` on Linux Go 1.25 and 1.26
- full tests on macOS and Windows
- existing GmSSL and native backend cross-compile jobs remain green

## Benchmarks

Required benchmark after functional tests are stable:

- concurrent multiplexed requests over one H3 connection

Optional if inexpensive:

- sequential connection reuse latency
- streaming response throughput

Benchmarks are regression scaffolding, not merge thresholds in this phase.

## Stacked PR strategy

Branch:

```text
feat/http3-client-runtime
```

is based on:

```text
feat/quic-client-runtime
```

While #43 is open, the HTTP/3 Draft PR targets `feat/quic-client-runtime`. This keeps the Phase 3 diff limited to shared-policy extraction plus HTTP/3 work.

Once #43 merges, retarget or rebase the H3 branch to `master` and verify the resulting diff before merge.

## Implementation order

1. Extract `internal/quicpolicy` without changing public QUIC behavior.
2. Run existing QUIC/repository tests; no H3 code until parity is proven.
3. Add `HTTP3Config`, H3 profile, lifecycle, and error mapping with unit tests.
4. Add the lazily initialized shared UDP / non-early QUIC dial bridge.
5. Add local H3 loopback transport/client integration.
6. Add streaming, cancellation, multiplexing, reuse, close-idle, and no-fallback tests.
7. Add docs and benchmark scaffolding.
8. Run full CI and fix platform/race behavior before marking the PR ready.

## Acceptance criteria

Phase 3 is complete when:

- `NewHTTP3Transport` returns a concrete closeable HTTP/3-only transport
- `NewHTTP3Client` uses standard `net/http` semantics
- HTTPS uses TLS 1.3+ with ALPN pinned to `h3`
- no HTTP/2 or HTTP/1.1 fallback occurs
- no early/0-RTT dial path is used
- streaming request/response bodies and cancellation work
- DNS/connection establishment honors cancellation
- concurrent H3 requests multiplex correctly
- connection reuse is covered
- QUIC/H3 resources remain bounded by explicit defaults
- datagrams are explicit and internally consistent
- close and close-idle semantics are deterministic
- protocol/transport errors are distinguishable from HTTP status codes
- malformed/failed QUIC paths do not leak test-observable resources
- race and cross-platform CI pass
- no `quic-go` concrete type appears in ogrenet's public API

## Deferred decisions

Separate design work is required for:

- multi-protocol HTTP selection/fallback policy
- HTTP/3 0-RTT and replay semantics
- WebTransport
- MASQUE / CONNECT-UDP
- active QUIC migration
- public H3 stream-limit tuning
- Happy Eyeballs / QUIC address racing
- engine-wide/global QUIC connection admission and memory accounting
