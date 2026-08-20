# HTTP/3 Client Runtime Design

Date: 2026-08-20
Status: proposed for implementation after review
Parent roadmap: #41, #38
Stacked base: `feat/quic-client-runtime`

## Goal

Add a production-grade HTTP/3-only client transport to `ogrenet/client` using normal `net/http` request/response semantics while preserving the QUIC-native boundary established by `ogrenet/quic`.

The implementation must reuse ogrenet's QUIC security and resource policy without widening the public `ogrenet/quic` API to expose implementation-specific `quic-go` types or HTTP/3-specific stream semantics.

## Non-goals

This phase does not add:

- HTTP/3 fallback to HTTP/2 or HTTP/1.1
- a multi-protocol HTTP facade
- public QUIC listener/server APIs
- WebTransport or MASQUE
- 0-RTT
- active connection migration policy
- arbitrary public exposure of every `quic-go` tuning field
- a new request/response abstraction

## Architectural decision

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

`internal/quicpolicy` owns dependency-facing configuration construction. It may import `github.com/quic-go/quic-go` because it is module-internal. Neither `client.HTTP3Config` nor the public `quic` API exposes `quic-go` concrete types.

The public `ogrenet/quic` contract remains unchanged by HTTP/3.

## Why not route HTTP/3 through the current public `quic.Conn`

HTTP/3 requires peer-initiated unidirectional QUIC streams for the HTTP/3 control stream and QPACK encoder/decoder streams. The current public QUIC client deliberately disables peer-initiated unidirectional streams because it does not expose a stable public uni-stream API.

Therefore HTTP/3 cannot be implemented by passing the existing `quic.Conn` abstraction directly to an HTTP/3 implementation without broadening the public API for an implementation detail.

The internal policy layer solves this cleanly: ordinary public QUIC connections and HTTP/3 connections share security/resource policy, but each builds a protocol-appropriate `quic-go` configuration.

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

`HTTP3Transport` implements:

```go
var _ http.RoundTripper = (*HTTP3Transport)(nil)
var _ io.Closer = (*HTTP3Transport)(nil)
```

It also exposes:

```go
func (t *HTTP3Transport) Close() error
func (t *HTTP3Transport) CloseIdleConnections()
```

`NewHTTP3Client` returns an `http.Client` with no whole-request `Client.Timeout`, matching the existing HTTP/1.1 + HTTP/2 client behavior. Request lifetime and streaming bodies are governed by request contexts.

### Deliberate omissions from `HTTP3Config`

The first version does not expose ALPN, QUIC versions, flow-control windows, 0-RTT, migration controls, or dozens of dependency-specific knobs.

ALPN is fixed internally to `h3`. Receive windows and stream limits use bounded ogrenet defaults from `internal/quicpolicy`.

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

The implementation uses `github.com/quic-go/quic-go/http3.Transport` internally. Dependency types remain private implementation details.

## Shared QUIC policy

Create `internal/quicpolicy` containing policy defaults and builders used by both public QUIC and HTTP/3.

The shared policy covers:

- TLS 1.3 minimum enforcement
- TLS config cloning
- ALPN pinning by caller-supplied internal policy
- handshake timeout default
- idle timeout default
- initial/max stream receive windows
- initial/max connection receive windows
- 0-RTT disabled by default
- datagram enablement propagated explicitly

### Public QUIC profile

The existing public QUIC profile remains behaviorally unchanged:

- peer bidirectional stream limit remains bounded at the current default
- peer unidirectional streams remain disabled
- 0-RTT remains disabled
- datagrams remain opt-in

Refactoring into `internal/quicpolicy` must pass the existing QUIC test suite unchanged before HTTP/3 code is added.

### HTTP/3 profile

The HTTP/3 profile differs only where required by RFC 9114 / QPACK operation:

- peer-initiated bidirectional streams are disabled for ordinary HTTP/3 client use
- peer-initiated unidirectional streams are enabled with a small bounded default sufficient for control/QPACK streams and limited extension headroom
- 0-RTT remains disabled
- datagrams are disabled by default
- when HTTP/3 datagrams are enabled, the underlying QUIC datagram capability is enabled in the same configuration path

The first version uses a fixed internal default for peer unidirectional streams rather than exposing it publicly. This avoids an API knob whose useful range is currently an implementation concern.

## Resource governance

HTTP/3 inherits connection multiplexing and pooling from the underlying `http3.Transport`, but ogrenet still owns the resource-policy defaults passed to QUIC.

Required bounds:

- bounded handshake timeout
- bounded idle timeout
- bounded stream receive windows
- bounded connection receive windows
- bounded peer unidirectional stream count
- peer bidirectional streams disabled for the H3 client profile
- bounded response header bytes
- no 0-RTT resource path

The initial design does not add an ogrenet-wide global QUIC connection admission controller. That remains a separate runtime-governance concern and is not required to land an HTTP/3-only RoundTripper.

## Datagram policy

`EnableDatagrams` is explicit and defaults to false.

When false:

- HTTP/3 datagrams are disabled
- QUIC datagrams are disabled

When true:

- both HTTP/3 datagrams and QUIC datagrams are enabled together

The implementation must reject or prevent internally inconsistent states where HTTP/3 datagrams are enabled but QUIC datagrams are not.

This setting does not expose a WebTransport or MASQUE API in this phase.

## TLS policy

Caller-supplied `TLSConfig` is cloned.

Rules:

- `MinVersion == 0` becomes TLS 1.3
- `MinVersion < TLS 1.3` is rejected
- `MaxVersion != 0 && MaxVersion < TLS 1.3` is rejected
- `NextProtos` supplied by the caller is ignored/replaced
- `NextProtos` is pinned to `[]string{"h3"}`
- insecure verification is never enabled implicitly

The caller remains responsible for roots, client certificates, server name policy, and other normal `tls.Config` concerns.

## Error model

### Configuration errors

Configuration failures use stable package-level sentinel errors in `client`, including at least:

```go
var (
    ErrInvalidHTTP3Config   = errors.New("client: invalid HTTP/3 transport configuration")
    ErrHTTP3TLSVersion      = errors.New("client: HTTP/3 requires TLS 1.3 or newer")
    ErrHTTP3TransportClosed = errors.New("client: HTTP/3 transport is closed")
)
```

Exact wrapping may add context, but callers must be able to use `errors.Is`.

### Runtime errors

Request context cancellation must preserve `errors.Is(err, context.Canceled)` and `errors.Is(err, context.DeadlineExceeded)` behavior.

QUIC and HTTP/3 protocol failures must remain distinguishable from HTTP status responses. The implementation should preserve the original dependency error as the cause.

Introduce a small wrapper only if needed to stabilize the ogrenet-facing classification:

```go
type HTTP3ErrorKind uint8

const (
    HTTP3ErrorUnknown HTTP3ErrorKind = iota
    HTTP3ErrorTransport
    HTTP3ErrorProtocol
    HTTP3ErrorHeader
    HTTP3ErrorBody
    HTTP3ErrorClosed
)

type HTTP3Error struct {
    Kind  HTTP3ErrorKind
    Cause error
}
```

Do not wrap successful HTTP responses based on status code.

The implementation plan must validate whether all of these kinds are observable and stable enough to expose in v1 of this API. If a category cannot be mapped without brittle dependency inspection, prefer a smaller stable taxonomy over speculative classification.

## Lifecycle semantics

`HTTP3Transport` owns the underlying `http3.Transport` and its QUIC resources.

Requirements:

- concurrent `RoundTrip` calls are supported
- `Close` is idempotent at the ogrenet wrapper boundary
- after `Close`, new requests fail with `ErrHTTP3TransportClosed`
- `CloseIdleConnections` is safe to call repeatedly
- closing the transport closes pooled QUIC connections and does not leave test-observable goroutine/socket leaks
- closing one response body does not close unrelated multiplexed requests

`NewHTTP3Client` does not hide transport ownership. Callers that construct long-lived clients should retain the transport or call `client.CloseIdleConnections()` where appropriate. Documentation must explain lifetime ownership.

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

- `internal/quicpolicy/policy.go`: shared QUIC/TLS policy normalization and dependency config builders
- `quic/config.go`: public QUIC config validation and delegation to the public-client policy profile
- `client/http3.go`: public H3 config, wrapper lifecycle, RoundTripper/client constructors
- unit tests: config normalization, TLS/ALPN, close state, datagram consistency
- integration tests: deterministic local HTTP/3 server behavior
- benchmark file: multiplexed request throughput / connection reuse
- docs: ownership, no-fallback policy, TLS requirements, datagram policy

## Test strategy

All protocol tests use deterministic local servers; no public network dependency.

### Required unit tests

- default config normalization
- reject negative timeouts
- reject TLS minimum below 1.3
- reject TLS maximum below 1.3
- clone caller TLS config
- force ALPN to `h3`
- bounded QUIC receive windows
- H3 peer uni-stream bound is nonzero and finite
- H3 peer bidirectional streams disabled
- 0-RTT disabled
- datagram flag propagates consistently to H3 and QUIC
- repeated `Close` is safe
- new request after close returns stable closed error

### Required integration tests

- HTTP/3 TLS loopback request/response
- response body streaming
- request body streaming where supported by the standard `http.Request` model
- context cancellation during an active request
- concurrent multiplexed requests over H3
- connection reuse across sequential requests
- TLS/ALPN handshake failure
- malformed/failed QUIC connection releases resources
- transport close unblocks/terminates relevant pending work deterministically
- no H3 -> H2/H1 fallback

The no-fallback test must use a server arrangement that would succeed under H2/H1 but fail H3, and assert the request fails rather than silently succeeding through another protocol.

### Race and cross-platform gates

Required CI gates remain:

- `gofmt`
- `go mod tidy` hygiene
- `go vet ./...`
- `go test -race -count=1 ./...` on Linux Go 1.25 and 1.26
- full tests on macOS and Windows
- existing GmSSL and native backend cross-compile jobs remain green

## Benchmarks

Add at least one HTTP/3 benchmark after functional tests are stable:

- multiplexed concurrent requests over one H3 connection

Optional if inexpensive:

- sequential keep-alive/reuse latency
- streaming response throughput at representative payload sizes

Benchmarks are evidence and regression scaffolding, not acceptance thresholds in this phase.

## Stacked PR strategy

Create branch:

```text
feat/http3-client-runtime
```

from:

```text
feat/quic-client-runtime
```

The HTTP/3 PR targets `feat/quic-client-runtime` while #43 is open. This keeps Phase 3 diff reviewable and prevents the QUIC Phase 2 PR from absorbing HTTP semantics.

Once #43 merges, the HTTP/3 branch should be rebased or retargeted to `master`, with the resulting diff verified before merge.

The HTTP/3 PR remains Draft until implementation and CI are green.

## Implementation order

1. Extract `internal/quicpolicy` without changing public QUIC behavior.
2. Run existing QUIC and repository tests; no HTTP/3 code until parity is proven.
3. Add `HTTP3Config` validation and `HTTP3Transport` lifecycle with unit tests.
4. Add local H3 loopback transport/client integration.
5. Add streaming, cancellation, multiplexing, reuse, and no-fallback tests.
6. Add docs and benchmark scaffolding.
7. Run full CI and fix platform/race behavior before marking the PR ready.

## Acceptance criteria

Phase 3 is complete when:

- `NewHTTP3Transport` returns a concrete closeable HTTP/3-only transport
- `NewHTTP3Client` uses standard `net/http` semantics
- HTTPS uses TLS 1.3+ with ALPN pinned to `h3`
- no HTTP/2 or HTTP/1.1 fallback occurs
- streaming request/response bodies and request cancellation work
- concurrent H3 requests multiplex correctly
- connection reuse is covered
- QUIC/H3 resources remain bounded by explicit defaults
- 0-RTT remains disabled
- datagrams are explicit and internally consistent
- close semantics are deterministic
- protocol/transport failures remain distinguishable from HTTP status codes
- local deterministic tests cover malformed/failed QUIC paths without leaks
- race and cross-platform CI pass
- no `quic-go` concrete type appears in ogrenet's public API

## Deferred decisions

The following require separate design work if needed later:

- multi-protocol HTTP selection/fallback policy
- HTTP/3 0-RTT and replay semantics
- WebTransport
- MASQUE / CONNECT-UDP
- active QUIC migration
- public tuning of H3-specific stream limits
- engine-wide/global QUIC connection admission and memory accounting
