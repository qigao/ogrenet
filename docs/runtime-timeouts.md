# Runtime timeout and deadline policy

The portable `transport.Engine` applies explicit timeout policy to TCP, TLS, WS,
WSS, and UDP without exposing raw socket deadlines through `Session` or
`PacketConn`.

The policy separates three ownership domains:

- caller contexts bound one API call or wait;
- Connect, Handshake, and Write bound operations that can otherwise block;
- ReadIdle, ConnectionIdle, and MaxLifetime govern established resource
  lifetimes.

## Configuration

Use `WithTimeouts`:

```go
type Timeouts struct {
    Connect        time.Duration
    Handshake      time.Duration
    Write          time.Duration
    ReadIdle       time.Duration
    ConnectionIdle time.Duration
    MaxLifetime    time.Duration
}
```

The effective defaults are:

| Domain | Default | Zero value |
| --- | ---: | --- |
| Connect | 10s | production default |
| Handshake | 10s | production default |
| Write | 10s | production default |
| ReadIdle | disabled | disabled |
| ConnectionIdle | disabled | disabled |
| MaxLifetime | disabled | disabled |

Any negative duration is invalid and causes Engine creation to fail with
`ErrInvalidTimeout`.

Connect, Handshake, and Write intentionally cannot be disabled with a zero
value. They protect bounded operations from indefinite blocking. Idle and
lifetime policies can change legitimate long-lived connection behavior, so they
remain opt-in.

`Shutdown(ctx)` does not have a second Engine timeout. Its caller-provided
context remains the shutdown deadline.

## Protocol-specific overrides

Existing protocol-specific configuration remains supported:

- `WithTLSHandshakeTimeout` overrides the base Handshake timeout for TLS stages;
- `WebSocketConfig.HandshakeTimeout` overrides the base Handshake timeout for
  WebSocket HTTP upgrade stages;
- `WebSocketConfig.WriteTimeout` overrides the base Write timeout for WS/WSS
  message writes.

The precedence is fixed:

```text
protocol-specific explicit override > Timeouts base > production default
```

This precedence is independent of the order in which Options are supplied.

## Caller context precedence

Caller cancellation describes caller ownership; an Engine timeout describes
runtime policy. Caller cancellation wins deterministically when they race.

For example:

```text
caller deadline 5s + Connect 10s  -> context.DeadlineExceeded
caller deadline 30s + Connect 10s -> TimeoutConnect
caller cancellation                -> context.Canceled
```

The same rule applies to TLS and WebSocket handshake stages.

Engine-generated timeouts do not masquerade as `context.DeadlineExceeded`.

## Send ownership

`Send(ctx)` keeps its existing ownership contract. The caller context controls
admission and how long the caller waits, while an admitted frame/message or
datagram may already belong to the asynchronous writer.

```text
Send(ctx)
  admission / queue wait -> caller context
  actual network write   -> Engine Write timeout
```

If the caller context ends after queue admission, `Send` can return the caller
cause even if the writer later transmits the item or closes the resource with
`TimeoutWrite`. `Session.Err()` / `PacketConn.Err()` describes the final
transport failure, not necessarily the return value from one `Send` call.

## Timeout domains

### Connect

Connect starts when a Dial operation begins and ends when the underlying
connected socket exists. TCP and connected UDP Dial operations include resolver
and connect work performed by Go's dialer. Accepted inbound sockets do not have
a Connect timeout.

### Handshake

Handshake starts after the underlying TCP connection exists.

```text
TLS: TCP connect -> TLS handshake
WS:  TCP connect -> HTTP/WebSocket upgrade
WSS: TCP connect -> TLS handshake -> HTTP/WebSocket upgrade
```

For WSS, TLS and HTTP upgrade are separately bounded stages. Each gets its own
effective protocol-specific handshake budget; the two stages do not share one
decrementing total timer.

### Write

Write begins when the asynchronous writer starts one actual frame, WebSocket
message, or UDP datagram write. The timeout is a hard upper bound on that whole
write operation.

For TCP/TLS, partial socket-write progress refreshes ConnectionIdle activity but
does not extend the frame's Write deadline.

### ReadIdle

ReadIdle detects inbound silence.

For TCP/TLS, any successful raw read progress (`n > 0`) refreshes ReadIdle,
even when only part of a frame has arrived. This is not a slow-frame timeout.
The read deadline is cleared before frame decoding and user handler callbacks,
so time spent inside `OnMessage` does not count as ReadIdle.

For WS/WSS, one successful business-message read refreshes ReadIdle because the
WebSocket library does not expose raw partial read progress at this layer.

For connected UDP, one successfully received datagram refreshes ReadIdle.

### ConnectionIdle

ConnectionIdle detects absence of successful bidirectional business/network
activity:

- TCP/TLS: successful raw read or write progress;
- WS/WSS: successful business-message read or write;
- connected UDP: successful datagram read or write.

Automatic WebSocket ping/pong traffic does **not** refresh ConnectionIdle. A
heartbeat can prove protocol liveness, but it must not keep a connection with no
business traffic alive forever.

### MaxLifetime

MaxLifetime is measured from successful Session or connected PacketConn
establishment. Connect and handshake time do not consume the established
resource lifetime. Traffic never extends MaxLifetime.

If MaxLifetime and ConnectionIdle become due at the same instant, MaxLifetime
wins deterministically.

## Protocol matrix

| Policy | TCP | TLS | WS | WSS | DialPacket | ListenPacket |
| --- | --- | --- | --- | --- | --- | --- |
| Connect | yes | yes | yes | yes | yes | n/a after listen |
| Handshake | n/a | TLS | HTTP upgrade | TLS + HTTP upgrade | n/a | n/a |
| Write | frame | frame | message | message | datagram | datagram |
| ReadIdle | yes | yes | yes | yes | yes | no |
| ConnectionIdle | yes | yes | yes | yes | yes | no |
| MaxLifetime | yes | yes | yes | yes | yes | no |

A listening UDP socket is a server resource and is not automatically destroyed
because no packets arrive. It remains alive until its context, explicit Close,
or Engine shutdown ends it.

## Timeout errors

Runtime timeouts use a stable error taxonomy:

```go
var ErrTimeout = errors.New("transport: operation timed out")

type TimeoutKind uint8

const (
    TimeoutConnect TimeoutKind = iota + 1
    TimeoutHandshake
    TimeoutWrite
    TimeoutReadIdle
    TimeoutConnectionIdle
    TimeoutMaxLifetime
)

type TimeoutError struct {
    Kind  TimeoutKind
    Cause error
}
```

Use normal Go error inspection:

```go
if errors.Is(err, transport.ErrTimeout) {
    var te *transport.TimeoutError
    if errors.As(err, &te) {
        switch te.Kind {
        case transport.TimeoutReadIdle:
            // peer stopped making inbound progress
        case transport.TimeoutWrite:
            // one actual network write exceeded its hard deadline
        }
    }
}
```

`TimeoutError.Timeout()` returns true and `Temporary()` returns false. When the
underlying OS, TLS, WebSocket, or context operation provides a useful root cause,
`Unwrap` preserves it.

## Deadline implementation

TCP/TLS and UDP use independent `SetReadDeadline` and `SetWriteDeadline` calls.
The runtime never uses `SetDeadline`, so reader and writer timeout policy cannot
clobber each other.

ConnectionIdle and MaxLifetime share one optional watchdog per established
resource. When both policies are disabled, the resource has no watchdog and the
normal read/write hot path performs no activity atomic update.

When enabled, activity wakeups are coalesced and timer expiry is revalidated
before closure, preventing a stale timer tick from racing with new activity.

## Close and cleanup semantics

The first terminal cause wins. Examples:

```text
explicit Close first      -> Err() == nil
timeout first             -> Err() == *TimeoutError
peer/protocol failure     -> Err() == the original terminal failure
```

A later `net.ErrClosed` caused by shutdown never overwrites an earlier timeout.

All timeout paths use the existing transport shutdown machinery. Before
`Done()` closes, reader/writer/watchdog work has stopped, queue slots and local
plus Engine-wide queued-byte quota have been released, connection/handshake or
upgrade admission leases have been returned, and `OnClose` has finished.
