# ogrenet

`ogrenet` v2 is a small Go library for native OS network-event mechanisms. It deliberately does **not** provide the old connection-manager API: codecs, encryption, load balancing, protocol framing, timers, and application callback orchestration belong above this layer.

## Packages

| Package | Platform | Kernel model | Core API |
| --- | --- | --- | --- |
| `epoll` | Linux | readiness | `Open`, `Add`, `Mod`, `Del`, `Wait`, `Wake`, `Close` |
| `iocp` | Windows | completion | `Open`, `Associate`, `Post`, `Get`, `Close` |
| `kqueue` | macOS, 64-bit FreeBSD | readiness/filter | `Open`, `Apply`, `Del`, `Wait`, `Wake`, `Close` |

There is intentionally no root-level networking framework. Applications compose these low-level packages with their own connection lifecycle, buffers, protocols, and scheduling policy.

## Design guarantees

- Native semantics are preserved instead of forcing epoll, kqueue, and IOCP behind a misleading common interface.
- User sockets/files remain caller-owned. Closing a poller never closes registered or associated application handles.
- `Close` is idempotent and wakes blocked waits.
- Native descriptor/handle lifetime is synchronized against concurrent control and wait operations, preventing close/reuse races inside the wrapper.
- Internal wake notifications are consumed by the wrapper and never exposed as application events.
- Hot-path wait buffers are reused by the poller after their first required allocation.
- No hidden goroutine pool, logger, codec, encryption stack, or load-balancing policy is installed.

## Requirements

The module targets Go 1.25 or newer and currently depends only on `golang.org/x/sys`.

## Linux / epoll

```go
p, err := epoll.Open()
if err != nil {
    return err
}
defer p.Close()

if err := p.Add(fd, epoll.Readable|epoll.PeerClosed|epoll.EdgeTriggered, 1); err != nil {
    return err
}

events := make([]epoll.Event, 256)
for {
    n, err := p.Wait(events, -1)
    if err != nil {
        return err
    }
    for _, event := range events[:n] {
        _ = event // drain non-blocking I/O to EAGAIN when using edge triggering
    }
}
```

The `Data` field is an opaque 64-bit registration value. `math.MaxUint64` is reserved for the internal eventfd wake event.

## Windows / IOCP

```go
port, err := iocp.Open(0)
if err != nil {
    return err
}
defer port.Close()

if err := port.Associate(handle, 42); err != nil {
    return err
}

completion, err := port.Get(30 * time.Second)
if err != nil {
    return err
}
_ = completion
```

IOCP remains a completion API. `Get` may be called by multiple worker goroutines concurrently. A failed overlapped operation can return both a populated `Completion` and a non-nil error.

## macOS / FreeBSD / kqueue

```go
p, err := kqueue.Open()
if err != nil {
    return err
}
defer p.Close()

err = p.Apply(kqueue.Change{
    Ident:  uint64(fd),
    Filter: kqueue.Read,
    Flags:  kqueue.Add | kqueue.Clear,
})
if err != nil {
    return err
}

events := make([]kqueue.Event, 256)
n, err := p.Wait(events, -1)
if err != nil {
    return err
}
_ = events[:n]
```

kqueue uses `(Ident, Filter)` as the native event identity. The wrapper does not turn `udata` into an arbitrary Go pointer/token because that would introduce unsafe GC and lifetime semantics into the public API.

## Validation

GitHub Actions runs formatting, module-hygiene checks, `go vet`, Linux race tests, native Windows tests, native macOS tests, and a FreeBSD kqueue cross-compile. Linux is tested on both Go 1.25 and the current Go 1.26 release line.
