# HTTP client transports

The `client` package provides production-oriented HTTP/1.1 and HTTP/2 client
transport configuration while keeping standard `net/http` request and response
semantics.

HTTP is intentionally **not** represented as `transport.Session`: request/response
streaming, connection pooling, multiplexing, redirects, and protocol negotiation
are HTTP semantics.

HTTP/3 uses a separate H3-only transport so protocol fallback is never implicit.
See [HTTP/3 client transport](http3-client.md) for its QUIC, TLS, lifecycle, and
no-fallback policy.

## Protocol selection

`HTTPConfig.Protocols` accepts `client.HTTP1` and `client.HTTP2`.

- empty: enable HTTP/1.1 and HTTP/2;
- `[]HTTPProtocol{HTTP1}`: HTTP/1.x only;
- `[]HTTPProtocol{HTTP2}`: HTTP/2 over TLS only;
- both: explicit HTTP/1.1 + HTTP/2 negotiation.

The implementation uses `net/http.Transport.Protocols`, available in Go 1.24+.
An HTTP/2-only transport does not silently fall back to HTTP/1.1.

## Security

HTTPS requires TLS 1.3 or newer. A supplied `tls.Config` is cloned before use.
When its `MinVersion` is zero, the client raises it to TLS 1.3. Configurations
that explicitly permit an older minimum TLS version are rejected.

Certificate verification otherwise follows normal Go `crypto/tls` behavior.
There is no library-provided insecure verification mode. HTTP ALPN is controlled
by `HTTPConfig.Protocols`; caller-supplied `tls.Config.NextProtos` values are not
used to change protocol selection implicitly.

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

## Proxy behavior

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

The returned `*http.Transport` is safe for concurrent use and should be reused.
Call `CloseIdleConnections` when its pool is no longer needed. Active requests
are governed by their request contexts.
