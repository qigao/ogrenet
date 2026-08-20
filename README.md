# ogrenet

`ogrenet` is a production-oriented Go networking library with explicit protocol
semantics and native OS poller primitives. It is a new API: there is no legacy
compatibility layer and no automatic protocol downgrade.

## Architecture

```text
                         application
                             |
                 +-----------+-----------+
                 |                       |
              Session                 PacketConn
        TCP / TLS / WS / WSS              UDP
                 |                       |
        +--------+--------+               |
        |                 |               |
   byte stream       WS messages       datagrams
    TCP / TLS          WS / WSS            |
        |                 |               |
    wire.Codec       Text / Binary         |
        |                 |               |
        +-----------------+---------------+
                          |
              epoll / kqueue / IOCP
```

Native packages remain directly available:

| Package | Platform | Kernel model |
| --- | --- | --- |
| `epoll` | Linux | readiness |
| `iocp` | Windows | completion |
| `kqueue` | macOS / FreeBSD | readiness/filter |

The portable `transport` Engine currently uses Go's `net` package above this
boundary. Future native Engines can implement the same root contracts without
changing application code.

## Protocols

| Scheme | API | Boundary | Channel security | Default application framing |
| --- | --- | --- | --- | --- |
| `tcp://` | `Session` | byte stream | none | `wire.Codec` |
| `tls://` | `Session` | byte stream | TLS 1.3+ | `wire.Codec` |
| `udp://` | `PacketConn` | datagram | none | native datagram |
| `ws://` | `Session` | WebSocket message | none | native WS Text/Binary |
| `wss://` | `Session` | WebSocket message | TLS 1.3+ | native WS Text/Binary |

There is no fallback. A TLS failure does not become TCP, WSS does not become WS,
and a failed WebSocket upgrade does not expose a raw stream.

## Endpoints

Endpoints are parsed once and strongly typed:

```go
serverEP, err := ogrenet.ParseEndpoint("tls://127.0.0.1:9443")
wsEP, err := ogrenet.ParseEndpoint("wss://api.example.com/realtime")
udpEP, err := ogrenet.ParseEndpoint("udp://127.0.0.1:9000")
```

Supported schemes are `tcp`, `udp`, `tls`, `ws`, and `wss`. TCP/UDP require an
explicit port. WS defaults to port 80; TLS/WSS default to port 443. Port zero is
allowed for listeners and requests an OS-assigned ephemeral port; outbound Dial
requires a non-zero port and host.

## Session API: TCP / TLS / WS / WSS

```go
type Session interface {
    ID() uint64
    Protocol() Scheme
    Endpoint() Endpoint
    Send(context.Context, Message) error
    TrySend(Message) error
    LocalAddr() net.Addr
    RemoteAddr() net.Addr
    Done() <-chan struct{}
    Err() error
    Close() error
}
```

Messages are explicit Text or Binary values:

```go
ogrenet.Text("hello 世界")
ogrenet.Bin([]byte{0x01, 0x02, 0xff})
```

Text is always valid UTF-8. Handler callbacks for one Session are serialized:

```text
OnOpen -> OnMessage* -> OnClose
```

`Done` is a shutdown barrier. It closes only after internal I/O loops have
stopped and `OnClose` has returned; `Err` is stable after `Done` closes.

### TCP example

```go
engine, err := transport.New()
if err != nil {
    return err
}
defer engine.Close()

endpoint := ogrenet.Endpoint{
    Scheme: ogrenet.SchemeTCP,
    Host:   "127.0.0.1",
    Port:   9000,
}

listener, err := engine.Listen(ctx, endpoint, ogrenet.HandlerFuncs{
    Message: func(s ogrenet.Session, msg ogrenet.Message) {
        _ = s.Send(context.Background(), msg)
    },
})
```

TCP and TLS are byte streams and therefore use a stream framer. The default is
`wire.Codec`. Stateful custom stream protocols may use `WithFramerFactory`.
Custom stream framing and the Engine's message-cipher option are deliberately
mutually exclusive: a custom framer owns its entire wire format.

## TLS

TLS is channel security and is independent of optional message encryption.
TLS/TWSS enforce TLS 1.3 as the minimum version; there is no TLS 1.2 fallback.

A TLS/WSS server must provide explicit certificate configuration:

```go
engine, err := transport.New(
    transport.WithTLSServerConfig(&tls.Config{
        Certificates: []tls.Certificate{certificate},
        MinVersion:   tls.VersionTLS13,
    }),
)
```

Clients use normal certificate verification. If no client config is supplied,
Go's system trust roots are used and `ServerName` is derived from the Endpoint.
`WithTLSClientConfig` can provide private roots, mTLS certificates, or ALPN.

TLS handshake time is bounded by `WithTLSHandshakeTimeout`.

## WebSocket: WS / WSS

WS/WSS use WebSocket's native message boundary. They do **not** add
`wire.Codec` inside a WebSocket message:

```text
WebSocket text   -> ogrenet.Text
WebSocket binary -> ogrenet.Bin
```

The implementation uses `github.com/coder/websocket` with compression disabled.
Server origin verification remains enabled by default; cross-origin access must
be explicitly authorized with `WebSocketConfig.OriginPatterns`. Redirects are
not followed by the client.

Production liveness and handshake controls are configured with:

```go
type WebSocketConfig struct {
    OriginPatterns   []string
    Subprotocols     []string
    HandshakeTimeout time.Duration
    WriteTimeout     time.Duration
    PingInterval     time.Duration
    PongTimeout      time.Duration
}
```

TCP socket settings are applied before WS/WSS handshakes as well.

## UDP

UDP has separate datagram semantics and is intentionally not represented as a
Session.

```go
type PacketConn interface {
    Protocol() Scheme
    Endpoint() Endpoint
    LocalAddr() net.Addr
    RemoteAddr() net.Addr
    Send(context.Context, Packet) error
    TrySend(Packet) error
    SendTo(context.Context, net.Addr, Packet) error
    TrySendTo(net.Addr, Packet) error
    Done() <-chan struct{}
    Err() error
    Close() error
}
```

`DialPacket` creates a connected UDP socket and uses `Send/TrySend`.
`ListenPacket` creates an unconnected socket and uses `SendTo/TrySendTo` with the
peer passed to `PacketHandler`.

The maximum accepted datagram size defaults to 65507 bytes and is configurable
with `WithMaxDatagramBytes`. Inbound datagrams larger than the configured limit
are dropped as complete datagrams; truncated partial payloads are never passed
to the application.

## Backpressure and memory bounds

Every Session and PacketConn has bounded send admission:

- a frame/datagram count limit (`WithWriteQueue`, default 256 waiting items);
- a byte budget (`WithMaxQueuedBytes`, default 64 MiB including in-flight I/O);
- admission happens before expensive stream frame encoding;
- only one stream frame per Session may exist encoded outside the byte quota
  while waiting for quota admission.

`Send` waits for admission and then the actual transport write result.
`TrySend` does not wait for admission or network I/O; it returns
`transport.ErrWouldBlock` immediately under backpressure. Encoding/encryption is
still synchronous once non-blocking admission succeeds.

`WithMaxMessageBytes` defaults to 16 MiB and bounds plaintext application
messages plus transport wire payloads. `WithMaxBufferedRead` bounds incomplete
TCP/TLS frame accumulation.

## Message security

Message security is optional and separate from TLS.

```go
type Cipher interface {
    Algorithm() Algorithm
    Seal(dst, plaintext []byte) ([]byte, error)
    Open(dst, ciphertext []byte) ([]byte, error)
}

type AuthenticatedCipher interface {
    Cipher
    SealAAD(dst, plaintext, aad []byte) ([]byte, error)
    OpenAAD(dst, ciphertext, aad []byte) ([]byte, error)
}
```

Built-in AES-GCM implements `AuthenticatedCipher`. TCP/TLS `wire.Codec`
authenticates semantic frame fields as AEAD associated data. WS/WSS authenticate
protocol + Text/Binary type as associated data. `WithCipher` is for
concurrency-safe implementations; mutable per-session ciphers use
`WithCipherFactory`.

RSA-OAEP/SHA-512 is exposed only as a `KeyWrapper` and requires at least a
2048-bit RSA key.

## GmSSL

Current national-cryptography support is provided directly through GmSSL 3.2.0
and is opt-in with CGO + the `gmssl` build tag:

- SM4-GCM authenticated encryption;
- SM3 digest;
- SM2 session-key wrapping;
- GmSSL cryptographic random generation;
- raw SM2 key generation/import helpers.

There are no legacy GM modes or compatibility shims.

```bash
CGO_ENABLED=1 go test -tags gmssl ./secure/gmssl ./wire ./transport
```

If GmSSL is installed outside standard compiler paths, provide `CGO_CFLAGS` and
`CGO_LDFLAGS`.

## Engine shutdown

`Engine.Close()` initiates shutdown and is idempotent. `Engine.Done()` is the
global barrier and closes after:

- all in-flight Listen/Dial operations finish or clean up;
- all listeners stop;
- all Session reader/writer loops stop;
- all UDP sockets stop;
- all per-transport `OnClose` callbacks return.

`Engine.Shutdown(ctx)` performs Close plus a context-bounded wait for that
barrier. Do not call `Shutdown` synchronously inside an Engine callback because
that callback is itself part of the barrier.

## Requirements and validation

- Go 1.25+
- `golang.org/x/sys v0.47.0`
- `github.com/coder/websocket v1.8.15`
- optional GmSSL 3.2.0 + C toolchain for national cryptography

CI validates formatting, module hygiene, vet/race tests, Linux/Windows/macOS
behavior, native poller cross-compilation, TCP/TLS/UDP/WS/WSS loopback behavior,
and a dedicated GmSSL 3.2.0 integration job.
