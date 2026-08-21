# Runtime Transport Errors

The portable transport runtime exposes a stable typed error taxonomy for operational failures while keeping configuration, validation, and caller-control results distinct. Callers should classify runtime errors with `errors.Is` and `errors.As`; the text returned by `Error()` is for diagnostics only and is never a classification API.

## 1. Operational Errors vs Configuration Errors

Operational failures happen after valid input enters a runtime transport boundary such as dial, listen, handshake, read, write, send admission, receive, close, or shutdown. These failures use `*transport.Error`.

Configuration, validation, and programmer errors detected before that boundary remain their existing direct errors. Examples include `ErrNilContext`, invalid queue/buffer/timeout options, protocol mismatch, missing TLS configuration, invalid peers, and invalid application messages.

Caller-owned context cancellation and deadlines are control-plane results, not transport failures, and are returned unchanged.

## 2. Error, Op, and ErrorKind

`transport.Error` records the portable classification and transport context:

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

`Op` identifies the caller-visible failing boundary: dial, listen, accept, handshake, upgrade, read, write, send, receive, close, or shutdown. `ErrorKind` identifies the stable category: unknown, closed, peer-closed, timeout, refused, reset, DNS, TLS, protocol, resource-exhausted, backpressure, or too-large.

The numeric enum order is append-only after publication. Address fields are snapshots when the runtime can safely capture them.

## 3. errors.Is and errors.As

Use `errors.As` for the portable envelope and `errors.Is` for stable categories or existing sentinels:

```go
var te *transport.Error
if errors.As(err, &te) {
    switch te.Kind {
    case transport.ErrorRefused:
        // connection establishment was refused
    case transport.ErrorTimeout:
        var timeout *transport.TimeoutError
        if errors.As(err, &timeout) {
            // inspect TimeoutWrite, TimeoutHandshake, and other timeout domains
        }
    }
}
```

Runtime sentinel checks must use `errors.Is(err, sentinel)`, not `err == sentinel`, because operational sentinels can now appear inside the typed envelope.

The raw OS, TLS, WebSocket, decoder, or plugin cause remains reachable through the unwrap chain whenever one exists.

## 4. TimeoutError and LimitError Composition

The typed envelope adds context without replacing existing specialized errors.

A runtime timeout is shaped like:

```text
*transport.Error{Kind: ErrorTimeout}
  -> *transport.TimeoutError
     -> raw timeout cause
```

A resource-limit failure is shaped like:

```text
*transport.Error{Kind: ErrorResourceExhausted}
  -> *transport.LimitError
     -> ErrResourceExhausted
```

Existing checks such as `errors.Is(err, transport.ErrTimeout)`, `errors.As(err, &timeout)`, `errors.Is(err, transport.ErrResourceExhausted)`, and `errors.As(err, &limit)` continue to work.

## 5. Closed vs PeerClosed vs Clean EOF

`ErrorClosed` means an operation failed because the local transport/resource is already closed. `ErrorPeerClosed` means an operation that was expected to continue failed because the peer terminated the connection.

A clean lifecycle close is different and is not converted into a terminal error:

- clean TCP FIN: read half closes and `Session.Err()` stays nil
- clean TLS EOF/close-notify: read half closes and `Session.Err()` stays nil
- normal WebSocket close (`1000` or `1001`): `Session.Err()` stays nil
- successful graceful shutdown or explicit local `Close()`: resource `Err()` stays nil

## 6. Context Cancellation

Caller context cancellation/deadline has precedence over the transport envelope when the caller context is the winning cause. The runtime returns `context.Cause(ctx)` unchanged.

A caller-owned shutdown deadline may force physical teardown, but that teardown is a control action: it does not populate `Session.Err()`, `PacketConn.Err()`, or `Listener.Err()` with the caller's context error.

## 7. TCP and TLS Mapping

Typical portable mappings include:

```text
connection refused       -> ErrorRefused
connection reset         -> ErrorReset
broken pipe / peer gone  -> ErrorPeerClosed
runtime I/O timeout      -> ErrorTimeout
TLS/x509 verification    -> ErrorTLS
wire/message violation   -> ErrorProtocol
```

Transport causes have precedence over protocol-layer hints. For example, a TCP reset during TLS handshake remains `ErrorReset`; an x509 certificate verification failure is `ErrorTLS`.

TLS close-notify failures are classified at `OpClose`, while normal close-notify remains a lifecycle event.

## 8. WebSocket Mapping

WebSocket HTTP upgrade failure is classified at `OpUpgrade` after lower-level DNS/TLS/OS causes are considered.

Normal close statuses `1000` and `1001` are lifecycle events. Protocol-related statuses such as protocol error, unsupported data, invalid payload, and policy violation map to `ErrorProtocol`. Message-too-big maps to `ErrorTooLarge`. Unknown abnormal failures remain `ErrorUnknown` unless a lower-level cause proves a more precise category.

Physical connection-close fallout must not replace an already-owned write timeout or close-handshake timeout.

## 9. UDP Mapping

UDP dial/listen failures use `OpDial` or `OpListen`. Send admission and target resolution use `OpSend`; physical datagram writes use `OpWrite`; receive failures and connected read-idle timeouts use `OpReceive`.

Oversized datagrams remain matchable with `ErrDatagramTooLarge` and use `ErrorTooLarge`. Caller-owned packet-listener cancellation closes the resource without populating `PacketConn.Err()`.

## 10. Resource Limits and Backpressure

Resource exhaustion and temporary backpressure are intentionally distinct:

```text
resource/admission limit -> ErrorResourceExhausted
TrySend cannot admit now -> ErrorBackpressure
message/datagram too big -> ErrorTooLarge
```

Existing specific errors such as `ErrWouldBlock`, `ErrFrameExceedsQueueBudget`, `ErrReadBufferFull`, `ErrMessageTooLarge`, and `ErrDatagramTooLarge` remain reachable through `errors.Is`.

Inbound admission rejection does not poison a healthy listener's terminal error. The rejected child is closed while the listener continues serving.

## 11. Error Precedence and Resource Err()

`Session.Err()`, `PacketConn.Err()`, and `Listener.Err()` retain only the first terminal operational failure that wins ownership. `OnClose(..., err)` observes the same terminal error chain rather than reclassifying it independently.

Examples:

```text
write reset wins, then local Close
=> Session.Err remains OpWrite/ErrorReset

write timeout wins, then socket teardown produces net.ErrClosed
=> Session.Err remains the typed timeout

explicit Close wins, then reader/writer sees closed socket
=> Session.Err remains nil

caller Shutdown deadline wins
=> method returns caller cause; Session.Err remains nil
```

Engine graceful shutdown does not aggregate child terminal errors. Each failed child retains its own `Err()`.

## 12. Cross-Platform Mapping Guarantees

The portable contract guarantees stable `Op`, `ErrorKind`, category matching, and preservation of the raw cause. Raw errno values remain platform-specific.

Linux, macOS, and FreeBSD Unix-family mappings use typed errno identity. Windows uses typed Winsock error identity. Classification never parses human-readable error strings.

FreeBSD runtime mapping is verified in CI in addition to cross-compilation; compile success alone is not treated as operational mapping evidence.

## 13. Unknown Errors

`ErrorUnknown` is a supported conservative result. When the runtime knows an operational boundary failed but cannot classify the cause reliably, it returns a typed envelope with `Kind == ErrorUnknown` and preserves the original cause.

There is intentionally no generic `ErrUnknown` sentinel. Inspect `Error.Kind` and, when needed, use `errors.Is` / `errors.As` on the preserved cause. Never infer a category from `Error()` text.
