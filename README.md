# ogrenet

`ogrenet` is being rebuilt as a thin, production-oriented Go wrapper around native OS event mechanisms.

The v2 design does **not** preserve the old connection-manager API. Protocol codecs, encryption, load balancing, timers, and application callback orchestration do not belong in the low-level polling layer and are being removed from the core design.

## Design rules

- Preserve the native kernel model instead of forcing unlike mechanisms behind a misleading abstraction.
- Keep descriptor/handle ownership explicit: wrappers never close user-owned sockets or files.
- Make shutdown, wakeup, timeout, and error semantics deterministic.
- Avoid hidden goroutine pools and application-level scheduling.
- Keep hot-path allocation under caller control where practical.
- Validate each backend on its native operating system in CI.

## Linux: epoll

```go
package main

import (
    "time"

    "github.com/qigao/ogrenet/epoll"
)

func run(fd int) error {
    p, err := epoll.Open()
    if err != nil {
        return err
    }
    defer p.Close()

    // The opaque data field should normally contain an application-owned
    // registration/generation identifier rather than the fd itself.
    if err := p.Add(fd, epoll.Readable|epoll.PeerClosed|epoll.EdgeTriggered, 1); err != nil {
        return err
    }

    events := make([]epoll.Event, 256)
    for {
        n, err := p.Wait(events, -1*time.Nanosecond)
        if err != nil {
            return err
        }
        for _, event := range events[:n] {
            _ = event // drain the non-blocking fd until EAGAIN
        }
    }
}
```

The epoll wrapper exposes the native 64-bit user-data field, supports `Add`, `Mod`, `Del`, `Wait`, `Wake`, and idempotent `Close`, and uses an internal `eventfd` for wakeups. Only one `Wait` call may be active per poller; control operations may run concurrently with it.

A negative timeout means wait indefinitely, zero means non-blocking poll, and positive durations are rounded up to epoll's millisecond precision.

## Windows: IOCP

```go
package main

import (
    "time"

    "github.com/qigao/ogrenet/iocp"
)

func worker() error {
    port, err := iocp.Open(0)
    if err != nil {
        return err
    }
    defer port.Close()

    completion, err := port.Get(30 * time.Second)
    if err != nil {
        return err
    }
    _ = completion
    return nil
}
```

IOCP is exposed as a completion API, not as an epoll-style readiness API. `Associate` binds Windows handles to the completion port, `Post` queues application-defined packets, and `Get` returns completion packets including failed overlapped operations together with their error.

## Repository status

The `refactor/netpoll-v2` branch currently contains the new low-level backends while the legacy implementation still exists in the repository. The next refactor stage is to remove the old connection-manager surface, trim dependencies, and add additional native backends such as kqueue.
