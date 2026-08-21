# Graceful Runtime Shutdown

This document describes the P0-3 lifecycle contract for ogrenet Sessions and Engine shutdown.

## 1. Close vs Shutdown

`Session.Close()` and `Engine.Close()` are immediate local abort operations. They stop new work, close the owned transport promptly, and do not promise that queued application data reaches the peer.

`Session.Shutdown(ctx)` and `Engine.Shutdown(ctx)` are graceful operations. They stop new admission, drain work that was already accepted, perform the protocol-specific graceful close, and wait for lifecycle completion subject to the owner context.

`Close()` does not promise a particular TCP RST packet. Its contract is prompt local abort with no graceful-delivery guarantee.

## 2. TCP/TLS Half-Close and ReadClosed

TCP and TLS Sessions implement `ogrenet.HalfCloseSession`:

```go
type HalfCloseSession interface {
    Session
    CloseWrite(context.Context) error
    ReadClosed() <-chan struct{}
}
```

For TCP, `CloseWrite` drains accepted writes and then closes the TCP write side. For TLS, it drains accepted application writes and performs the TLS write-side close (`close_notify`).

`ReadClosed()` closes when no more inbound application messages can arrive. A peer FIN or clean TLS EOF closes the read half but does not automatically close the write half, so the local application may still send a final response.

`Done()` remains the full-session barrier. It closes only after the Session has fully terminated.

## 3. WebSocket Full Close and CloseTimeout

WS and WSS Sessions do not implement `HalfCloseSession`; WebSocket has no application half-close equivalent.

Local `Shutdown(ctx)` drains already accepted business messages, then performs the WebSocket close handshake. `WebSocketConfig.CloseTimeout` bounds the runtime-owned close handshake and defaults to 10 seconds.

A runtime-owned close-handshake timeout is reported as `TimeoutClose` through `TimeoutError` and matches `ErrTimeout`.

Caller deadline or cancellation is different: if the owner `Shutdown` context expires first, ogrenet physically aborts the connection and returns `context.Cause(ctx)` without converting that caller-owned deadline into `TimeoutClose`.

WS/WSS retain ownership of the physical TCP connection so an explicit `Close()`, caller deadline, or runtime close timeout can interrupt a blocked WebSocket close handshake even through TLS wrapping.

## 4. Engine Graceful Shutdown

The first `Engine.Shutdown(ctx)` call transitions the Engine from Running to Draining and owns that shutdown phase.

The Engine then:

1. stops listeners first;
2. rejects new `Listen`, `Dial`, `ListenPacket`, and `DialPacket` operations;
3. rejects late adoption from operations that were already in flight;
4. requests graceful shutdown on tracked TCP/TLS/WS/WSS Sessions;
5. requests internal queue drain on tracked UDP PacketConns; and
6. waits for the existing Engine `Done()` tracking barrier.

The Engine does not create one waiter goroutine per child. Existing Session/PacketConn reader/writer loops advance their own graceful lifecycle.

If the owner context expires, the Engine upgrades to abort, closes remaining resources, returns `context.Cause(ctx)`, and still lets `Done()` remain the exact final cleanup barrier.

## 5. UDP Behavior

`PacketConn` does not gain a public `Shutdown` or half-close API.

During `Engine.Shutdown`, UDP PacketConns stop new send admission, let already entered send operations finish, drain already accepted datagrams, then close the UDP socket.

`PacketConn.Close()` remains immediate and does not guarantee delivery of queued datagrams.

## 6. Context Ownership and Concurrent Shutdown Calls

Graceful phases use owner/waiter semantics.

The first caller that starts a graceful phase is the owner. Its context bounds that phase and may upgrade it to abort.

Later callers are waiters. A waiter's shorter context only causes that waiter to return its own `context.Cause(ctx)`; it does not shorten or abort the owner's graceful operation.

An explicit `Close()` is always authoritative and immediately aborts an in-progress graceful lifecycle.

## 7. Error Precedence

For an individual Session, an existing transport or protocol failure remains the terminal cause. A runtime-owned WebSocket close timeout is `TimeoutClose`. Explicit abort is represented to graceful waiters as `ErrClosed` when no stronger transport failure exists.

Caller context cancellation/deadline is returned by the API call that owns or waits on the graceful phase, but caller context causes are not stored as `Session.Err()` transport failures.

`Engine.Shutdown` does not aggregate ordinary child Session failures. A failed child keeps its own `Session.Err()` while Engine shutdown continues draining the remaining children. Engine shutdown reports control-plane failure or its own caller context, not a potentially unbounded join of child connection errors.

## 8. Interaction with Runtime Timeouts

P0-2 runtime timeout policy remains active while a connection is draining:

- `WriteTimeout` still bounds each actual write;
- `ReadIdle` still applies while the read half is open;
- `ConnectionIdle` still tracks real network activity; and
- `MaxLifetime` is never extended by entering graceful shutdown.

A transport timeout that occurs during graceful drain remains the Session terminal error and may finish the Session before the graceful caller context expires.

## 9. Admission Accounting During Drain

A graceful connection follows the internal accounting lifecycle:

```text
Opening -> Active -> Draining -> Released
```

Draining connections continue to consume global, per-peer, and per-listener connection limits until the resource is fully released. This prevents graceful shutdown from prematurely freeing resource budget while file descriptors, buffers, and goroutines are still owned by the Engine.

`Engine.Done()` requires opening, active, and draining accounting to return to zero.

## 10. Migration from Pre-P0-3 Engine.Shutdown

Before P0-3, `Engine.Shutdown(ctx)` behaved like immediate `Close()` followed by a context-bounded wait.

After P0-3, use `Shutdown(ctx)` when graceful delivery and protocol close are required. Code that needs the old immediate-stop behavior should call `Close()` and, if necessary, wait on `Done()` separately.

Example Session shutdown:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if hs, ok := session.(ogrenet.HalfCloseSession); ok {
    if err := hs.CloseWrite(ctx); err != nil {
        log.Printf("close write: %v", err)
    }
    select {
    case <-hs.ReadClosed():
    case <-ctx.Done():
    }
}

if err := session.Shutdown(ctx); err != nil {
    log.Printf("shutdown: %v", err)
}
```
