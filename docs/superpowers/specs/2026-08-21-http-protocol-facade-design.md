# HTTP Protocol Facade Design

## Status

Approved for implementation on `feat/http-protocol-facade`.

## Goal

Add an explicit, ordered client-side HTTP protocol facade across HTTP/3, HTTP/2, and HTTP/1.1 without weakening the existing single-protocol transports. The facade may fall back only when policy says the request is safe to replay and the previous attempt failed at a transport stage that is eligible for protocol fallback.

## Non-goals

- No Alt-Svc discovery or learning.
- No origin capability cache.
- No browser-style adaptive protocol selection.
- No h2c enablement.
- No implicit proxy bypass or HTTP/3 proxy/MASQUE support.
- No generic retry engine for status codes, response timeouts, or application/protocol failures.
- No automatic replay of POST, PUT, PATCH, or DELETE.
- No at-most-once or exactly-once delivery guarantee.

## Public API

Extend `HTTPProtocol` with `HTTP3` while preserving existing `HTTP1` and `HTTP2` values and strict single-transport behavior.

```go
type HTTPFallbackPolicy uint8

const (
    HTTPFallbackDisabled HTTPFallbackPolicy = iota
    HTTPFallbackSafeReplay
)

type HTTPClientConfig struct {
    Protocols []HTTPProtocol
    HTTP      HTTPConfig
    HTTP3     HTTP3Config
    Fallback  HTTPFallbackPolicy
}

type HTTPAttemptInfo struct {
    Protocol HTTPProtocol
    Index    int
}

type HTTPAttemptError struct {
    Protocol HTTPProtocol
    Err      error
}

type HTTPFallbackError struct {
    Attempts []HTTPAttemptError
}

func NewHTTPRoundTripper(HTTPClientConfig) (*HTTPClientTransport, error)
func NewClient(HTTPClientConfig) (*http.Client, error)
func HTTPAttemptFromContext(context.Context) (HTTPAttemptInfo, bool)
```

`HTTPClientTransport` implements `http.RoundTripper`, `CloseIdleConnections()`, and `Close() error`.

Existing `NewHTTPTransport`, `NewHTTPClient`, `NewHTTP3Transport`, and `NewHTTP3Client` keep their current semantics.

## Configuration rules

`HTTPClientConfig.Protocols` is the only protocol selection and ordering source for facade mode.

- Empty `Protocols` is `ErrInvalidHTTPClientConfig`.
- Duplicate protocols are rejected instead of silently deduplicated.
- `HTTP.Protocols` must be empty in facade mode; setting it is a configuration conflict.
- `HTTP3` is valid only in the facade/client protocol list, not in `HTTPConfig.Protocols` passed to `NewHTTPTransport`.
- If `HTTP3` is present and `HTTP.Proxy != nil`, construction fails with `ErrInvalidHTTPClientConfig`. HTTP/3 proxying is not implemented.
- Unsupported fallback policy values fail construction.

## Strict ordered protocol slots

The facade must use one long-lived transport slot per configured protocol so configured ordering is real, not an illusion produced by ALPN auto-negotiation.

```go
type protocolTransport struct {
    protocol HTTPProtocol
    rt       http.RoundTripper
}
```

Construction rules:

- HTTP/3 slot: `NewHTTP3Transport(cfg.HTTP3)`.
- HTTP/2 slot: clone `cfg.HTTP`, set `Protocols = []HTTPProtocol{HTTP2}`, then call `NewHTTPTransport`.
- HTTP/1.1 slot: clone `cfg.HTTP`, set `Protocols = []HTTPProtocol{HTTP1}`, then call `NewHTTPTransport`.

`{HTTP1, HTTP2}` must actually try H1 first even when the server supports H2. `{HTTP2, HTTP1}` must try H2 first. H1 and H2 therefore do not share one auto-negotiating `http.Transport` inside the facade.

Each slot retains its own connection pool for the lifetime of the facade. There is no cross-request origin capability cache; every request begins with the first applicable configured protocol.

## URL scheme applicability

- HTTP/3 applies only to `https://`.
- HTTP/2 applies to HTTPS according to the existing transport; Phase 4 does not enable h2c.
- HTTP/1.1 applies to existing `http://` and `https://` behavior.
- An inapplicable protocol is skipped before creating an attempt and is not recorded as a failure.
- If no configured protocol applies to the URL, return a stable request/configuration error without network I/O.

## Fallback policy

`HTTPFallbackDisabled` tries only the first applicable protocol.

`HTTPFallbackSafeReplay` can advance to a later applicable protocol only when both request replay policy and error classification allow it.

### Replayable methods

Automatic fallback is limited to safe methods:

- GET
- HEAD
- OPTIONS
- TRACE

POST, PUT, PATCH, and DELETE never automatically fall back, even when `GetBody` is present.

For a safe method:

- `Body == nil` is replayable.
- A non-nil body is replayable only when `GetBody != nil`.
- Attempt 0 may use the original body.
- Attempt 1+ must call the original request's `GetBody()` to obtain an independent body.
- Failure to obtain a replay body is terminal.

The facade makes multiple network attempts under safe replay; it does not promise at-most-once delivery. A safe request may have reached the origin before a pre-response connection failure becomes observable.

## Request cloning and tracing

Every protocol attempt receives an independent request clone created from the original request. The original request must not have URL, Header, Host, Trailer, GetBody, or Context mutated.

Each attempt context includes:

```go
HTTPAttemptInfo{Protocol: protocol, Index: attemptIndex}
```

`HTTPAttemptFromContext` exposes that metadata. Existing `httptrace.ClientTrace` hooks remain on the cloned context; no second tracing framework is introduced.

Mutable Header maps must not be shared between attempts.

## Error classification

Internal fallback classification is conservative:

```go
type fallbackClass uint8

const (
    fallbackNever fallbackClass = iota
    fallbackPreRequest
    fallbackAmbiguousAfterSend
)
```

Unknown errors always classify as `fallbackNever`.

### `fallbackPreRequest`

Examples that may be eligible for safe-method fallback:

- DNS/connect failure.
- TCP connection establishment failure.
- QUIC handshake or version negotiation failure.
- TLS ALPN failure that indicates the selected HTTP protocol is unavailable.
- H2-only negotiation failure.
- Explicit transport setup failure before HTTP request bytes are sent.

### `fallbackAmbiguousAfterSend`

Examples that may be eligible only for safe-method replay:

- EOF, connection reset, or broken pipe before response headers.
- QUIC stream/connection close before response headers when it is not an application/protocol rejection.
- Stale pooled connection failures before a response.

The origin may already have observed the request in this class.

### `fallbackNever`

Never fall back for:

- `context.Canceled` or `context.DeadlineExceeded`.
- TLS certificate, hostname, or trust-chain verification failures.
- Any returned `*http.Response`, including 4xx and 5xx.
- Response-header timeout.
- HTTP/2 or HTTP/3 protocol/application errors.
- Malformed URL, invalid request, or configuration errors.
- Facade or underlying transport closed errors.
- Proxy authentication/policy errors.
- Request clone/body replay failures.
- Unknown/unclassified errors.

Classifiers must use `errors.Is`, `errors.As`, standard-library typed errors, and existing ogrenet wrappers. String matching is prohibited.

The existing public `HTTP3Error` taxonomy is not broadened for the facade. The facade may inspect wrapped causes internally.

## Aggregate errors

If all eligible attempts fail before any response, return `*HTTPFallbackError` with ordered attempt records:

```go
type HTTPAttemptError struct {
    Protocol HTTPProtocol
    Err      error
}
```

The aggregate keeps original errors for observability. `errors.Is` / `errors.As` support must reach contained causes. Error text must identify protocols in attempt order.

If fallback is disabled or a failure is terminal before a second attempt, returning the original error directly is acceptable unless multiple attempts already occurred; once multiple attempts exist, return `HTTPFallbackError`.

## Lifecycle

`HTTPClientTransport` owns all constructed protocol slots.

- `RoundTrip`, `CloseIdleConnections`, and `Close` are concurrency-safe.
- `CloseIdleConnections` delegates to every slot that supports it and does not end active requests.
- `Close` marks the facade closed, closes the H3 transport and its owned QUIC/UDP resources, and calls `CloseIdleConnections` on H1/H2 transports.
- `Close` is idempotent.
- New requests after `Close` return `ErrHTTPClientTransportClosed`.
- `Close` does not promise to synchronously terminate active H1/H2 requests; those remain governed by request contexts and standard `net/http` ownership.

No global active-request wait group is added.

## Proxy rules

The facade never silently changes routing strategy.

- H1/H2 proxy behavior remains exactly the explicit `HTTPConfig.Proxy` behavior.
- If H3 appears in the facade protocol list and `HTTP.Proxy` is non-nil, construction fails.
- No H3 direct-then-proxy fallback is implemented.
- No CONNECT-UDP or MASQUE behavior is implemented.

## Test requirements

### Policy/configuration

- Protocol order is preserved.
- `{H3,H2,H1}`, `{H1,H2}`, and `{H2,H1}` build distinct ordered slots.
- Empty and duplicate protocol lists fail.
- Nested `HTTP.Protocols` fails.
- H3 + proxy fails.
- H3 is skipped for `http://`.
- `HTTP3` remains invalid for `NewHTTPTransport`.

### Replay

- GET nil body can fall back.
- Safe request body with `GetBody` can fall back.
- Safe request body without `GetBody` cannot fall back.
- POST/PUT/PATCH/DELETE never automatically fall back.
- Original request and mutable headers are unchanged.
- Attempt bodies are independent.
- Context cancellation stops the chain immediately.

### Real protocol behavior

- H3 unavailable -> H2 success.
- H3 unavailable -> H2 unavailable -> H1 success.
- `{H1,H2}` reaches H1 first on an H2-capable server.
- `{H2,H1}` reaches H2 first.
- TLS identity failure does not fall back.
- H3 protocol/application error does not fall back.
- H2 response-header timeout does not fall back.
- Eligible pre-header connection failure for GET can fall back.
- HTTP 404/500 never triggers fallback.
- Response-body streaming error never triggers fallback.

### Lifecycle

- `CloseIdleConnections` reaches every slot.
- `Close` is idempotent.
- `Close` racing with `RoundTrip` is race-free.
- New requests after close fail with the facade sentinel.

### Verification

- `gofmt` gate.
- `go mod tidy` hygiene gate.
- `go vet ./...`.
- `go test -race -count=1 ./...` on supported Linux Go versions.
- Existing Windows/macOS tests and cross-compile/GmSSL jobs remain green.

## Benchmarks

Add small benchmark scaffolding for:

- Single-protocol facade overhead versus direct transport.
- H3 -> H2 fallback.
- H2 -> H1 fallback.

Benchmarks are regression scaffolding, not performance promises.

## Delivery

Implement on `feat/http-protocol-facade` as one Draft PR to `master` with reviewable commits:

1. facade config and strict slot construction;
2. safe replay and fallback classification;
3. integration/lifecycle coverage and fixes;
4. documentation and benchmark scaffolding.
