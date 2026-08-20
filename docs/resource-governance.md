# Resource governance

`transport.WithLimits` applies one Engine-wide admission policy to TCP, TLS,
UDP, WS, and WSS. Limits are hard bounds: a zero value means unlimited and a
negative value is invalid.

```go
engine, err := transport.New(transport.WithLimits(transport.Limits{
    MaxConnections:            100_000,
    MaxConnectionsPerPeer:     1_000,
    MaxConnectionsPerListener: 50_000,
    MaxConcurrentHandshakes:   512,
    MaxPendingUpgrades:        512,
    MaxQueuedBytesTotal:       2 << 30,
}))
```

The numbers above are examples, not defaults. Production values must be chosen
from the process file-descriptor budget, memory budget, expected connection
mix, TLS cost, and traffic profile.

## What is counted

`MaxConnections` counts both opening and active resources. A connection is
charged as soon as the transport has enough information to own it, before an
expensive TLS handshake or WebSocket upgrade can escape the global bound.
Capacity remains owned until the transport is fully released.

`MaxConnectionsPerPeer` uses the canonical remote IP when one is available.
`MaxConnectionsPerListener` is independent for each TCP/TLS/WS/WSS Listener.
For WS/WSS, accepted sockets are charged before HTTP headers are read, so slow
or incomplete HTTP clients cannot bypass connection admission.

`MaxConcurrentHandshakes` is shared by TLS and WSS. Slow handshakes do not
serialize the accept loop: excess handshakes are rejected rather than queued
without bound. `MaxPendingUpgrades` similarly bounds WS/WSS upgrade work.

`MaxQueuedBytesTotal` is the Engine-wide complement to the per-transport
`WithMaxQueuedBytes` budget. Send admission must satisfy both budgets. The
Engine-wide counter covers encoded queued and in-flight application payload
bytes across TCP/TLS, WS/WSS, and UDP.

## Overload behavior

Resource exhaustion is deterministic and never triggers a protocol downgrade.
Outbound operations return `*transport.LimitError`, which unwraps to
`transport.ErrResourceExhausted`; inspect `LimitError.Kind` to distinguish the
exhausted resource.

Inbound behavior is intentionally simple and bounded:

- TCP/TLS accepted sockets that exceed connection, peer, listener, or handshake
  capacity are closed promptly.
- WS/WSS sockets are connection-admitted before HTTP header parsing. If the
  request reaches the upgrade handler but upgrade capacity is exhausted, the
  server responds with HTTP 503 and `Connection: close`.
- UDP does not create per-peer connection state for unconnected listeners; the
  PacketConn itself is still covered by Engine connection and queued-byte
  accounting.

There is no public overload-policy callback in the core runtime. A future policy
hook can be added behind the admission boundary without placing user callbacks
on the I/O hot path.

## Shutdown and accounting

Admission ownership is exact-once across open, handshake, upgrade, active,
close, and failure paths. `Engine.Done()` does not close while opening
connections, handshakes, upgrades, Sessions, or PacketConns still own Engine
capacity. Cancellation and failed writes release both local and global byte
quota.

The implementation keeps observer-facing accounting internal for now. Public
Stats/OpenTelemetry APIs are a separate roadmap item so resource governance does
not prematurely freeze an observability surface.

## Performance

The uncontended queued-byte acquire/release path does not allocate. Wake-up
channels are allocated lazily only when quota contention creates waiters. The
repository contains admission and global-byte-quota benchmarks plus race/stress
tests for connection floods, slow handshakes, queue saturation, and shutdown
during overload.
