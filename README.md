# ogrenet

`ogrenet` is a production-oriented Go networking library built in layers. Native
OS polling mechanisms remain available directly, while the root package defines
a platform-independent transport contract for applications that do not need to
manage readiness/completion details themselves.

## Layers

| Package | Purpose |
| --- | --- |
| `epoll` | Linux native readiness backend |
| `iocp` | Windows native completion backend |
| `kqueue` | macOS / FreeBSD native readiness backend |
| root `ogrenet` | `Engine`, `Conn`, `Listener`, `Handler`, `Message` contracts |
| `transport` | portable byte-stream Engine implementing the common contracts |
| `wire` | default text/binary stream framing |
| `secure` | security interfaces plus AES-GCM and RSA-OAEP |
| `secure/legacy` | non-GM compatibility transforms such as AES-CFB and legacy `CipherKey` AES-GCM |
| `secure/gmssl` | optional GmSSL-backed SM2/SM3/SM4 using the current security model only |

The high-level API does not replace the native packages. epoll, kqueue, and IOCP
keep their native semantics. The portable `transport` package currently uses
Go's `net` package; future native Engines can satisfy the same root interfaces
without changing application code.

## Unified message API

Application messages are explicitly text or binary:

```go
msg := ogrenet.Text("hello 世界")
raw := ogrenet.Bin([]byte{0x01, 0x02, 0xff})
```

`Text` payloads must be valid UTF-8. `Binary` payloads are opaque bytes. Handlers
always receive plaintext messages after framing and security processing.
Callbacks for one connection are serialized in lifecycle order:
`OnOpen -> OnMessage* -> OnClose`.

```go
type Handler interface {
    OnOpen(ogrenet.Conn)
    OnMessage(ogrenet.Conn, ogrenet.Message)
    OnClose(ogrenet.Conn, error)
}
```

`Conn.Send` waits for frame admission and the socket-write result. `Conn.TrySend`
does not wait for frame/byte admission or socket I/O and returns
`transport.ErrWouldBlock` under backpressure; framing and encryption themselves
still run synchronously once admission is available. Once `TrySend` has admitted
a frame to the writer queue it returns `nil`, even if close races immediately
afterward.

Each connection has both a frame-count limit and a byte budget. Defaults are 256
waiting frames plus one in-flight frame, and 64 MiB of encoded queued/in-flight
bytes. Admission happens before expensive frame encoding, and only one encoded
frame per connection may wait outside the byte budget while budget becomes
available. Use `transport.WithWriteQueue` and `transport.WithMaxQueuedBytes` to
tune these limits.

`Conn.Done()` is a shutdown barrier: it closes only after reader/writer work has
stopped, pending sends have been released, and `OnClose` has returned. `Conn.Err()`
is stable once `Done` is closed. `Listener.Done()` similarly waits for the
accept loop to stop.

`Engine.Close()` initiates shutdown and is idempotent. `Engine.Done()` is the
global barrier and closes only after all tracked listeners and connections have
reached their own Done barriers. `Engine.Shutdown(ctx)` combines Close with a
context-bounded wait for Engine.Done. Do not call Shutdown synchronously from a
Handler callback on the same Engine, because that callback is part of the
barrier being awaited.

## Portable stream Engine

The `transport` package supports byte-stream networks: `tcp`, `tcp4`, `tcp6`,
and `unix`. Datagram and seqpacket transports are intentionally separate because
they have different message-boundary and truncation semantics.

```go
cipher, err := secure.NewAESGCM(key)
if err != nil {
    return err
}

server, err := transport.New(transport.WithCipher(cipher))
if err != nil {
    return err
}
defer server.Close()

listener, err := server.Listen(ctx, "tcp", "127.0.0.1:9000", ogrenet.HandlerFuncs{
    Message: func(c ogrenet.Conn, msg ogrenet.Message) {
        _ = c.Send(context.Background(), msg)
    },
})
if err != nil {
    return err
}
defer listener.Close()
```

A new framer is created per connection. Custom stateful protocols can use
`transport.WithFramerFactory`; the default is `wire.Codec`.

`transport.WithCipher` shares one cipher instance across connections and is
appropriate for concurrency-safe ciphers such as the built-in AES-GCM and GmSSL
SM4-GCM implementations. Stateful custom ciphers should use
`transport.WithCipherFactory`, which creates one cipher per connection:

```go
engine, err := transport.New(transport.WithCipherFactory(func() (secure.Cipher, error) {
    return newSessionCipher()
}))
```

Both `CipherFactory` and `FramerFactory` may be invoked concurrently.

## Default wire format

`wire.Codec` implements a small v1 envelope suitable for stream transports:

```text
+--------+---------+-------+-----------+--------+
| magic  | version | flags | algorithm | length |
| uint16 | uint8   | uint8 | uint16    | uint32 |
+--------+---------+-------+-----------+--------+
| payload ...                                  |
+----------------------------------------------+
```

The fixed header is 10 bytes. `DecodeOne` is incremental: incomplete stream data
returns `wire.ErrNeedMore` instead of treating a read as a message boundary.
Unknown flag bits are rejected.

When encryption is enabled:

- binary payloads carry raw ciphertext;
- text payloads carry Base64-encoded ciphertext so the payload remains text-safe;
- the algorithm identifier must match the configured cipher during decoding;
- ciphers implementing `secure.AuthenticatedCipher` authenticate the semantic
  header fields (`magic`, `version`, flags, algorithm) as AEAD associated data.

Applications with an existing protocol can implement their own `wire.Framer`.

## Security model

Security primitives have distinct contracts:

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

type Digest interface {
    Algorithm() Algorithm
    Sum(dst, data []byte) []byte
    Verify(data, digest []byte) bool
}

type KeyWrapper interface {
    Algorithm() Algorithm
    Wrap(key []byte) ([]byte, error)
    Unwrap(wrapped []byte) ([]byte, error)
}
```

`Algorithm` values are explicit stable wire identifiers; existing numeric IDs
must never be renumbered. SM3 is a digest, not a reversible cipher. RSA-OAEP and
SM2 are intended to wrap session keys rather than bulk application messages.

### Standard backend

AES-GCM is available without CGO and implements `AuthenticatedCipher`:

```go
cipher, err := secure.NewAESGCM(key)
codec := wire.New(cipher)
```

RSA-OAEP uses SHA-512, preserving the primitive used by the pre-v2 code while
assigning it the correct key-wrapping role.

## GmSSL backend

National cryptography support uses the upstream GmSSL C library directly. The
adapter targets **GmSSL 3.2.0** and is compiled only when both CGO and the
`gmssl` build tag are enabled.

Build and install GmSSL first, then:

```bash
CGO_ENABLED=1 go test -tags gmssl ./secure/gmssl ./wire ./transport
```

If GmSSL is installed outside the compiler's default paths, set `CGO_CFLAGS`
and `CGO_LDFLAGS` accordingly.

The GmSSL backend provides only the new security model:

- SM4-GCM authenticated encryption with associated-data support;
- SM3 digest;
- SM2 session-key wrapping;
- GmSSL cryptographic random generation for SM4-GCM nonces;
- raw SM2 key generation/import helpers.

Legacy GM wire formats and old GM method semantics are intentionally not
supported. There is no legacy SM2 C1C2C3 transform, no SM3 `Encrypt/Decrypt`
compatibility shim, and no legacy SM4-CBC compatibility mode.

GmSSL is optional: native pollers plus the standard security/wire/transport
packages continue to build with `CGO_ENABLED=0`.

## Non-GM legacy crypto compatibility

The redesign does not restore the old connection-manager API, but selected
non-GM cryptographic methods remain available for protocol interoperability:

| Old method | New location | Notes |
| --- | --- | --- |
| raw | `secure/legacy.Raw` | no-op transform |
| AES-128-CFB | `secure/legacy.NewAES128CFB` | preserves old key/IV normalization |
| AES-192-CFB | `secure/legacy.NewAES192CFB` | preserves old key/IV normalization |
| AES-256-CFB | `secure/legacy.NewAES256CFB` | preserves old key/IV normalization |
| `CipherKey` AES-GCM | `secure/legacy.CipherKey` | preserves legacy key generation and nonce-prefixed AES-GCM behavior |

New encrypted sessions should use an authenticated cipher such as AES-GCM or
SM4-GCM.

## Native pollers

### Linux / epoll

`epoll.Open`, `Add`, `Mod`, `Del`, `Wait`, `Wake`, and `Close` expose Linux
readiness semantics with a 64-bit opaque registration value and an internal
`eventfd` wake mechanism.

### Windows / IOCP

`iocp.Open`, `Associate`, `Post`, `Get`, and `Close` expose completion semantics.
Multiple workers may block in `Get` concurrently.

### macOS / FreeBSD / kqueue

`kqueue.Open`, `Apply`, `Del`, `Wait`, `Wake`, and `Close` preserve native
`(ident, filter)` event identity and use `EVFILT_USER` for internal wakeups.

## Requirements and validation

- Go 1.25 or newer.
- `golang.org/x/sys` for native OS calls.
- Optional GmSSL 3.2.0 + a working C toolchain for `-tags gmssl`.

GitHub Actions validates formatting, module hygiene, vet/race tests, portable
transport behavior, native Linux/Windows/macOS behavior, cross-architecture
builds, and a dedicated GmSSL 3.2.0 security/wire/transport integration job.
