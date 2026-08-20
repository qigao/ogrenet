# HTTP client transports

The `client` package provides production-oriented HTTP/1.1, HTTP/2, and explicit
HTTP/3 client transport composition while keeping standard `net/http` request and
response semantics.

HTTP is intentionally **not** represented as `transport.Session`: request/response
streaming, connection pooling, multiplexing, redirects, and protocol negotiation
are HTTP semantics.

HTTP/3 has a separate H3-only transport. The multi-protocol facade described
below composes the strict transports without changing their standalone behavior.
See [HTTP/3 client transport](http3-client.md) for its QUIC and TLS details.

## Strict HTTP/1.1 and HTTP/2 transport

`HTTPConfig.Protocols` accepts `client.HTTP1` and `client.HTTP2`.

- empty: enable HTTP/1.1 and HTTP/2;
- `[]HTTPProtocol{HTTP1}`: HTTP/1.x only;
- `[]HTTPProtocol{HTTP2}`: HTTP/2 over TLS only;
- both: explicit HTTP/1.1 + HTTP/2 negotiation.

The implementation uses `net/http.Transport.Protocols`, available in Go 1.24+.
An HTTP/2-only standalone transport does not silently fall back to HTTP/1.1.
`HTTP3` remains invalid in `HTTPConfig.Protocols`; use `HTTP3Config` or the
explicit facade for H3.

## Explicit ordered protocol facade

`HTTPClientConfig` composes independently pooled strict protocol transports in
exactly the order supplied by `Protocols`:

```go
transport, err := client.NewHTTPRoundTripper(client.HTTPClientConfig{
    Protocols: []client.HTTPProtocol{
        client.HTTP3,
        client.HTTP2,
        client.HTTP1,
    },
    Fallback: client.HTTPFallbackSafeReplay,
})
if err != nil {
    // handle configuration error
}
defer transport.Close()

httpClient := &http.Client{Transport: transport}
```

The order is real. H1 and H2 use distinct transports rather than one
H1/H2-auto-negotiating pool, so `{HTTP1, HTTP2}` really starts with HTTP/1.1 and
`{HTTP2, HTTP1}` really starts with HTTP/2.

Facade protocol configuration is deliberately explicit:

- `HTTPClientConfig.Protocols` is required;
- duplicate protocols are rejected;
- `HTTPClientConfig.HTTP.Protocols` must remain empty because the top-level list
  is the only facade ordering source;
- HTTP/3 is skipped for `http://` requests rather than recorded as a failed
  network attempt;
- no Alt-Svc learning, origin capability cache, or browser-style adaptive
  downgrade state is maintained.

### Fallback policy

`HTTPFallbackDisabled` sends the request using only the first applicable
configured protocol.

`HTTPFallbackSafeReplay` may advance to the next protocol only for the HTTP-safe
methods `GET`, `HEAD`, `OPTIONS`, and `TRACE`, and only after an eligible
transport failure before any HTTP response is available. A non-nil request body
must have `GetBody` so each later attempt receives a fresh body.

`POST`, `PUT`, `PATCH`, and `DELETE` are never automatically replayed, even when
`GetBody` is available.

Fallback is conservative. It does **not** occur for:

- request context cancellation or deadline expiry;
- TLS certificate, hostname, or trust validation failure;
- HTTP status responses, including 4xx and 5xx;
- response-header timeout;
- HTTP/2 or HTTP/3 protocol/application failure;
- closed transports, proxy policy errors, malformed requests, or unknown errors.

A strict HTTP/2 ALPN mismatch is detected before HTTP request bytes are sent and
is eligible for safe-method fallback. Connection loss before response headers
may be ambiguous: the origin might already have observed the request. Therefore
safe fallback can produce multiple network attempts and **does not provide an
at-most-once delivery guarantee**. Use `HTTPFallbackDisabled` whenever that
property is unacceptable.

### Attempt observability

Each network attempt receives an independent cloned request and a context value
that can be inspected without replacing standard `httptrace`:

```go
if info, ok := client.HTTPAttemptFromContext(req.Context()); ok {
    log.Printf("protocol=%s attempt=%d", info.Protocol, info.Index)
}
```

If multiple attempts fail, `*HTTPFallbackError` preserves the ordered
`HTTPAttemptError` list and exposes all underlying causes through
`errors.Is` / `errors.As`.

### Proxy compatibility

H1/H2 proxy behavior remains the explicit `HTTPConfig.Proxy` policy. The facade
rejects a configuration that combines HTTP/3 with a non-nil HTTP proxy because
HTTP/3 proxying, CONNECT-UDP, and MASQUE are not implemented. It never silently
tries H3 directly and then changes routing policy for a fallback attempt.

## Security

HTTPS requires TLS 1.3 or newer. A supplied `tls.Config` is cloned before use.
When its `MinVersion` is zero, the client raises it to TLS 1.3. Configurations
that explicitly permit an older minimum TLS version are rejected.

Certificate verification otherwise follows normal Go `crypto/tls` behavior.
There is no library-provided insecure verification mode. HTTP ALPN is controlled
by the selected protocol transport; caller-supplied `tls.Config.NextProtos`
values are not used to broaden protocol selection implicitly.

## Bounded defaults

Zero-valued numeric and duration settings use bounded defaults:

| Setting | Default |
| --- | ---: |
| connect timeout | 10s |
| TLS handshake timeout | 10s |
| response-header timeout | 30s |
| expect-continue timeout | 1s |
| idle connection timeout | 90s |
| TCP keepalive | 30s |
| max idle connections | 100 |
| max idle connections per host | 16 |
| max connections per host | 64 |
| max response header bytes | 1 MiB |

Negative values are invalid.

`http.Client.Timeout` is intentionally left at zero. Whole-request deadlines are
best expressed through the request context so streaming response bodies are not
cut off by a hidden client-wide timeout.

## Proxy behavior for the strict H1/H2 transport

The default is **direct connection only**. Environment variables such as
`HTTP_PROXY` and `HTTPS_PROXY` are not read implicitly.

To opt into the standard environment-proxy policy:

```go
client.NewHTTPClient(client.HTTPConfig{
    Proxy: http.ProxyFromEnvironment,
})
```

Any other `net/http.Transport.Proxy`-compatible function can be supplied
explicitly.

## Ownership

Standalone `*http.Transport` values are safe for concurrent use and should be
reused. Call `CloseIdleConnections` when their pools are no longer needed.

`HTTPClientTransport` is also reusable and concurrency-safe.
`CloseIdleConnections` broadcasts to every protocol slot without terminating
active requests. `Close` is idempotent, prevents new facade requests, closes
HTTP/3-owned QUIC/UDP resources, and clears idle H1/H2 pools. It does not promise
to synchronously terminate active H1/H2 requests; those remain governed by their
request contexts and standard `net/http` ownership.
