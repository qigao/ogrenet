# HTTP/3 client transport

The `client` package provides a dedicated HTTP/3-only transport that preserves
standard `net/http` request and response semantics.

Use `client.NewHTTP3Transport` when transport lifetime must be controlled
explicitly, or `client.NewHTTP3Client` for a ready-to-use `*http.Client`.

## Protocol policy

The HTTP/3 transport accepts HTTPS requests only and negotiates ALPN `h3`.
It never silently falls back to HTTP/2 or HTTP/1.1. If QUIC or HTTP/3 cannot be
established, the request fails rather than switching to TCP HTTP.

Multi-protocol HTTP selection remains a separate policy decision. The existing
`HTTPConfig` continues to configure only HTTP/1.1 and HTTP/2.

## TLS and 0-RTT

HTTP/3 requires TLS 1.3 or newer. A caller-supplied `tls.Config` is cloned before
use. A zero `MinVersion` is raised to TLS 1.3; explicit minimum or maximum
versions below TLS 1.3 are rejected. Caller-supplied `NextProtos` is replaced by
`h3`.

Certificate verification otherwise follows normal Go `crypto/tls` behavior.
The library never enables insecure certificate verification implicitly.

The client deliberately uses non-early `quic.Transport.Dial`; 0-RTT is not used.
Replay-sensitive early-data policy is outside this phase.

## Bounded QUIC policy

Zero-valued timeout and header settings use bounded defaults. The shared QUIC
policy also bounds stream and connection receive windows.

For the HTTP/3 client profile:

- peer-initiated bidirectional streams are disabled;
- peer-initiated unidirectional streams are bounded to 16 for HTTP/3 control and
  QPACK streams plus limited extension headroom;
- 0-RTT is disabled;
- response headers are bounded to 1 MiB by default;
- QUIC and HTTP/3 datagrams are disabled unless `EnableDatagrams` is set.

`EnableDatagrams` enables both layers together. It does not expose WebTransport,
MASQUE, or CONNECT-UDP APIs.

## Cancellation and streaming

`NewHTTP3Client` leaves `http.Client.Timeout` at zero. Request contexts govern
request lifetime, including connection establishment, active requests, and
streaming request or response bodies.

Cancellation preserves `context.Canceled` and `context.DeadlineExceeded` through
`errors.Is`, even if the underlying HTTP/3 stream reports its own cancellation
error concurrently.

HTTP status codes remain ordinary `*http.Response` values and are never
converted into transport errors.

## Errors

Configuration errors use stable sentinels such as `ErrInvalidHTTP3Config`,
`ErrHTTP3TLSVersion`, and `ErrHTTP3TransportClosed`.

Runtime failures may be wrapped in `HTTP3Error`. Its stable kinds distinguish
transport failures, HTTP/3 protocol failures, and closed transports while
preserving the original cause for `errors.Is` / `errors.As`.

## Ownership and shutdown

`HTTP3Transport` owns:

- the underlying HTTP/3 connection pool;
- a shared QUIC transport;
- the UDP socket used for QUIC dialing.

The transport is safe for concurrent requests and should be reused. Call
`Close` when the transport is no longer needed; `Close` is idempotent and closes
pooled H3 connections plus the owned QUIC/UDP resources.

`CloseIdleConnections` closes only currently idle pooled H3 connections. It does
not interrupt active multiplexed requests and does not tear down the shared UDP
transport.
