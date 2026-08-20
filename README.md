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
| root `ogrenet` | `Engine`, `Conn`, `Handler`, `Message` contracts |
| `wire` | default text/binary stream framing |
| `secure` | security interfaces plus AES-GCM and RSA-OAEP |
| `secure/legacy` | non-GM compatibility transforms such as AES-CFB and legacy `CipherKey` AES-GCM |
| `secure/gmssl` | optional GmSSL-backed SM2/SM3/SM4 using the current security model only |

The high-level API does not replace the native packages. epoll, kqueue, and IOCP
keep their native semantics; Engine implementations compose them above that
boundary.

## Unified message API

Application messages are explicitly text or binary:

```go
msg := ogrenet.Text("hello 世界")
raw := ogrenet.Bin([]byte{0x01, 0x02, 0xff})
```

`Text` payloads must be valid UTF-8. `Binary` payloads are opaque bytes.
Handlers always receive plaintext messages after framing and security processing.

```go
type Handler interface {
    OnOpen(ogrenet.Conn)
    OnMessage(ogrenet.Conn, ogrenet.Message)
    OnClose(ogrenet.Conn, error)
}
```

`Conn` exposes `Send`, `TrySend`, addresses, a stable ID, and idempotent close.
`Engine` exposes the common `Listen`/`Dial` lifecycle. Concrete platform engines
are implemented above the native poller packages rather than by pretending that
IOCP completion and epoll/kqueue readiness are the same kernel model.

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

The fixed header is 10 bytes. `DecodeOne` is incremental: incomplete TCP data
returns `wire.ErrNeedMore` instead of treating a read as a message boundary.

When encryption is enabled:

- binary payloads carry raw ciphertext;
- text payloads carry Base64-encoded ciphertext so the payload remains text-safe;
- the algorithm identifier is encoded in the header and must match the configured
  cipher during decoding.

Applications with an existing protocol can implement their own `wire.Framer`.

## Security model

Security primitives have distinct contracts:

```go
type Cipher interface {
    Algorithm() Algorithm
    Seal(dst, plaintext []byte) ([]byte, error)
    Open(dst, ciphertext []byte) ([]byte, error)
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

SM3 is therefore a digest, not a reversible cipher. RSA-OAEP and SM2 are
intended to wrap session keys rather than bulk application messages.

### Standard backend

AES-GCM is available without CGO:

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
CGO_ENABLED=1 go test -tags gmssl ./secure/gmssl ./wire
```

If GmSSL is installed outside the compiler's default paths, set `CGO_CFLAGS`
and `CGO_LDFLAGS` accordingly.

The GmSSL backend provides only the new security model:

- SM4-GCM authenticated encryption;
- SM3 digest;
- SM2 session-key wrapping;
- GmSSL cryptographic random generation for SM4-GCM nonces;
- raw SM2 key generation/import helpers.

Legacy GM wire formats and old GM method semantics are intentionally not
supported. In particular, there is no legacy SM2 C1C2C3 transform, no SM3
"encrypt/decrypt" compatibility shim, and no legacy SM4-CBC compatibility mode.

GmSSL is optional: the native poller packages and the standard security/wire
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
| `CipherKey` AES-GCM | `secure/legacy.CipherKey` | preserves legacy key generation and nonce-prefixed AES-GCM wire behavior |

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

## Ownership and lifecycle guarantees

- User sockets/files remain caller-owned by the native poller layer.
- Poller close is idempotent and wakes blocked waits.
- Descriptor/handle lifetime is synchronized against concurrent syscalls to
  avoid close/reuse races inside the wrappers.
- Internal wake events are never exposed as application messages.
- There is no hidden global goroutine pool or logger.

## Requirements

- Go 1.25 or newer.
- `golang.org/x/sys` for native OS calls.
- Optional GmSSL 3.2.0 + a working C toolchain for `-tags gmssl`.

GitHub Actions validates formatting, module hygiene, vet/race tests, native
Linux/Windows/macOS behavior, cross-architecture builds, and a dedicated GmSSL
3.2.0 integration job.
