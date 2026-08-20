# HTTP/3 Client Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a production-grade HTTP/3-only `net/http` client transport with bounded QUIC policy, no H3→H2/H1 fallback, no 0-RTT client dial path, deterministic lifecycle semantics, and cross-platform race-tested coverage.

**Architecture:** Extract the existing QUIC TLS/timeouts/window policy into `internal/quicpolicy`, preserving the public `ogrenet/quic` API and behavior. Add `client.HTTP3Transport` as a small wrapper around `quic-go/http3.Transport`, but override its default early dial path with an ogrenet-owned shared UDP `quic.Transport` that always calls non-early `Dial`.

**Tech Stack:** Go 1.25+, `net/http`, `crypto/tls`, `net`, `github.com/quic-go/quic-go v0.61.0`, `github.com/quic-go/quic-go/http3`, existing GitHub Actions matrix.

**Spec:** `docs/superpowers/specs/2026-08-20-http3-client-runtime-design.md`

## Global Constraints

- Go module minimum remains `go 1.25.0`.
- Keep `github.com/quic-go/quic-go` at `v0.61.0` unless a concrete blocker requires a separately reviewed dependency change.
- HTTP/3 is `https://` + ALPN `h3` only; never fall back to HTTP/2 or HTTP/1.1.
- TLS minimum is 1.3. Clone caller TLS config; never enable insecure verification implicitly.
- The HTTP/3 client path must use `quic.Transport.Dial`, never `DialEarly` or `DialAddrEarly`.
- Public `ogrenet/quic` API behavior remains unchanged by the shared-policy extraction.
- No public `quic-go` concrete type may appear in `client` or `quic` exported signatures.
- Peer-initiated H3 bidirectional streams are disabled; peer unidirectional stream limit is fixed internally at 16.
- 0-RTT stays disabled; WebTransport, MASQUE, active migration, Happy Eyeballs and multi-protocol fallback remain out of scope.
- `EnableDatagrams` is opt-in and must enable/disable HTTP/3 and QUIC datagrams together.
- All tests use deterministic local endpoints; no public-network-dependent tests.
- Existing Linux Go 1.25/1.26 race, macOS, Windows, GmSSL and cross-compile CI gates must remain green.

---

### Task 1: Extract shared QUIC policy without behavior change

**Files:**
- Create: `internal/quicpolicy/policy.go`
- Create: `internal/quicpolicy/policy_test.go`
- Modify: `quic/config.go`
- Modify: `quic/config_test.go`

**Interfaces:**
- Consumes: existing `quic.Config` fields and current bounded defaults.
- Produces:
  - `quicpolicy.Config`
  - `quicpolicy.Build(Config) (*tls.Config, *quicgo.Config, error)`
  - exported internal constants for the shared timeout/window defaults
  - sentinel errors aliased by public `quic` to preserve `errors.Is` behavior.

- [ ] **Step 1: Add failing policy tests**

Create `internal/quicpolicy/policy_test.go` with focused tests for the extracted behavior:

```go
package quicpolicy

import (
    "crypto/tls"
    "errors"
    "testing"
    "time"
)

func TestBuildClonesTLSAndPinsALPN(t *testing.T) {
    original := &tls.Config{NextProtos: []string{"caller"}}
    tlsCfg, qcfg, err := Build(Config{
        TLSConfig:             original,
        ALPN:                  "ogrenet-test",
        MaxIncomingStreams:    32,
        MaxIncomingUniStreams: -1,
        EnableDatagrams:       true,
    })
    if err != nil {
        t.Fatal(err)
    }
    if tlsCfg == original {
        t.Fatal("TLS config was not cloned")
    }
    if tlsCfg.MinVersion != tls.VersionTLS13 {
        t.Fatalf("MinVersion = %d", tlsCfg.MinVersion)
    }
    if len(tlsCfg.NextProtos) != 1 || tlsCfg.NextProtos[0] != "ogrenet-test" {
        t.Fatalf("NextProtos = %v", tlsCfg.NextProtos)
    }
    if !qcfg.EnableDatagrams || qcfg.Allow0RTT {
        t.Fatalf("datagrams=%v Allow0RTT=%v", qcfg.EnableDatagrams, qcfg.Allow0RTT)
    }
    if qcfg.MaxIncomingStreams != 32 || qcfg.MaxIncomingUniStreams != -1 {
        t.Fatalf("stream limits = %d/%d", qcfg.MaxIncomingStreams, qcfg.MaxIncomingUniStreams)
    }
}

func TestBuildValidation(t *testing.T) {
    if _, _, err := Build(Config{}); !errors.Is(err, ErrALPNRequired) {
        t.Fatalf("empty ALPN: %v", err)
    }
    if _, _, err := Build(Config{ALPN: "x", HandshakeTimeout: -time.Second}); !errors.Is(err, ErrInvalidTimeout) {
        t.Fatalf("negative timeout: %v", err)
    }
    if _, _, err := Build(Config{ALPN: "x", TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}); !errors.Is(err, ErrTLSVersion) {
        t.Fatalf("TLS 1.2: %v", err)
    }
}
```

- [ ] **Step 2: Run the new test and verify red**

Run:

```bash
go test ./internal/quicpolicy
```

Expected: FAIL because package/functions do not exist yet.

- [ ] **Step 3: Move the policy into `internal/quicpolicy`**

Create `internal/quicpolicy/policy.go` with this shape:

```go
package quicpolicy

import (
    "crypto/tls"
    "errors"
    "time"

    quicgo "github.com/quic-go/quic-go"
)

const (
    DefaultHandshakeTimeout = 5 * time.Second
    DefaultIdleTimeout      = 30 * time.Second

    DefaultMaxIncomingStreams             int64  = 32
    DefaultInitialStreamReceiveWindow     uint64 = 512 << 10
    DefaultMaxStreamReceiveWindow         uint64 = 4 << 20
    DefaultInitialConnectionReceiveWindow uint64 = 1 << 20
    DefaultMaxConnectionReceiveWindow     uint64 = 8 << 20
)

var (
    ErrALPNRequired   = errors.New("quic: ALPN is required")
    ErrInvalidTimeout = errors.New("quic: timeout must not be negative")
    ErrTLSVersion     = errors.New("quic: TLS minimum version must be TLS 1.3 or newer")
)

type Config struct {
    TLSConfig             *tls.Config
    ALPN                  string
    HandshakeTimeout      time.Duration
    IdleTimeout           time.Duration
    EnableDatagrams       bool
    MaxIncomingStreams    int64
    MaxIncomingUniStreams int64
}

func Build(c Config) (*tls.Config, *quicgo.Config, error) {
    if c.ALPN == "" {
        return nil, nil, ErrALPNRequired
    }
    if c.HandshakeTimeout < 0 || c.IdleTimeout < 0 {
        return nil, nil, ErrInvalidTimeout
    }

    tlsCfg := &tls.Config{}
    if c.TLSConfig != nil {
        tlsCfg = c.TLSConfig.Clone()
    }
    if tlsCfg.MinVersion != 0 && tlsCfg.MinVersion < tls.VersionTLS13 {
        return nil, nil, ErrTLSVersion
    }
    if tlsCfg.MaxVersion != 0 && tlsCfg.MaxVersion < tls.VersionTLS13 {
        return nil, nil, ErrTLSVersion
    }
    if tlsCfg.MinVersion == 0 {
        tlsCfg.MinVersion = tls.VersionTLS13
    }
    tlsCfg.NextProtos = []string{c.ALPN}

    handshake := c.HandshakeTimeout
    if handshake == 0 {
        handshake = DefaultHandshakeTimeout
    }
    idle := c.IdleTimeout
    if idle == 0 {
        idle = DefaultIdleTimeout
    }

    return tlsCfg, &quicgo.Config{
        HandshakeIdleTimeout:           handshake,
        MaxIdleTimeout:                 idle,
        InitialStreamReceiveWindow:     DefaultInitialStreamReceiveWindow,
        MaxStreamReceiveWindow:         DefaultMaxStreamReceiveWindow,
        InitialConnectionReceiveWindow: DefaultInitialConnectionReceiveWindow,
        MaxConnectionReceiveWindow:     DefaultMaxConnectionReceiveWindow,
        MaxIncomingStreams:             c.MaxIncomingStreams,
        MaxIncomingUniStreams:          c.MaxIncomingUniStreams,
        Allow0RTT:                      false,
        EnableDatagrams:                c.EnableDatagrams,
    }, nil
}
```

Modify `quic/config.go` so the public errors remain aliases and `build` delegates exactly once:

```go
var (
    ErrALPNRequired   = quicpolicy.ErrALPNRequired
    ErrInvalidTimeout = quicpolicy.ErrInvalidTimeout
    ErrTLSVersion     = quicpolicy.ErrTLSVersion
)

func (c Config) build() (*tls.Config, *quicgo.Config, error) {
    return quicpolicy.Build(quicpolicy.Config{
        TLSConfig:             c.TLSConfig,
        ALPN:                  c.ALPN,
        HandshakeTimeout:      c.HandshakeTimeout,
        IdleTimeout:           c.IdleTimeout,
        EnableDatagrams:       c.EnableDatagrams,
        MaxIncomingStreams:    quicpolicy.DefaultMaxIncomingStreams,
        MaxIncomingUniStreams: -1,
    })
}
```

Update `quic/config_test.go` references from the removed local constants to `quicpolicy.Default...` constants.

- [ ] **Step 4: Verify policy and existing QUIC parity**

Run:

```bash
gofmt -w internal/quicpolicy quic/config.go quic/config_test.go
go test ./internal/quicpolicy ./quic
go test -race ./quic
```

Expected: PASS. Existing `quic` tests must not need semantic expectation changes.

- [ ] **Step 5: Commit Task 1**

```bash
git add internal/quicpolicy/policy.go internal/quicpolicy/policy_test.go quic/config.go quic/config_test.go
git commit -m "refactor: share QUIC client policy"
```

---

### Task 2: Add HTTP/3 configuration and stable error model

**Files:**
- Create: `client/http3.go`
- Create: `client/http3_test.go`
- Modify: `client/doc.go`

**Interfaces:**
- Consumes: `quicpolicy.Build` from Task 1.
- Produces:
  - `HTTP3Config`
  - `HTTP3ErrorKind`
  - `HTTP3Error`
  - `ErrInvalidHTTP3Config`, `ErrHTTP3TLSVersion`, `ErrHTTP3TransportClosed`
  - `normalizeHTTP3Config(HTTP3Config) (normalizedHTTP3Config, error)`.

- [ ] **Step 1: Write failing HTTP/3 config tests**

Add tests in `client/http3_test.go`:

```go
func TestHTTP3ConfigDefaultsAndPolicy(t *testing.T) {
    got, err := normalizeHTTP3Config(HTTP3Config{})
    if err != nil {
        t.Fatal(err)
    }
    if got.tlsConfig.MinVersion != tls.VersionTLS13 {
        t.Fatalf("TLS min = %d", got.tlsConfig.MinVersion)
    }
    if len(got.tlsConfig.NextProtos) != 1 || got.tlsConfig.NextProtos[0] != http3.NextProtoH3 {
        t.Fatalf("ALPN = %v", got.tlsConfig.NextProtos)
    }
    if got.quicConfig.MaxIncomingStreams != -1 {
        t.Fatalf("peer bidi streams = %d", got.quicConfig.MaxIncomingStreams)
    }
    if got.quicConfig.MaxIncomingUniStreams != 16 {
        t.Fatalf("peer uni streams = %d", got.quicConfig.MaxIncomingUniStreams)
    }
    if got.quicConfig.EnableDatagrams || got.quicConfig.Allow0RTT {
        t.Fatalf("datagrams=%v Allow0RTT=%v", got.quicConfig.EnableDatagrams, got.quicConfig.Allow0RTT)
    }
    if got.maxResponseHeaderBytes != 1<<20 {
        t.Fatalf("header bound = %d", got.maxResponseHeaderBytes)
    }
}

func TestHTTP3ConfigRejectsInvalidValues(t *testing.T) {
    cases := []HTTP3Config{
        {HandshakeTimeout: -time.Second},
        {IdleTimeout: -time.Second},
        {MaxResponseHeaderBytes: -1},
        {TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}},
        {TLSConfig: &tls.Config{MaxVersion: tls.VersionTLS12}},
    }
    for _, cfg := range cases {
        if _, err := normalizeHTTP3Config(cfg); err == nil {
            t.Fatalf("normalizeHTTP3Config(%+v) succeeded", cfg)
        }
    }
}

func TestHTTP3DatagramsAreEnabledAtBothLayers(t *testing.T) {
    got, err := normalizeHTTP3Config(HTTP3Config{EnableDatagrams: true})
    if err != nil {
        t.Fatal(err)
    }
    if !got.enableDatagrams || !got.quicConfig.EnableDatagrams {
        t.Fatalf("H3=%v QUIC=%v", got.enableDatagrams, got.quicConfig.EnableDatagrams)
    }
}
```

Add a platform-range helper test that constructs `MaxResponseHeaderBytes` larger than `int` when `strconv.IntSize == 32`, and on 64-bit directly test `math.MaxInt64` converts without overflow while negatives are rejected.

- [ ] **Step 2: Run the tests and verify red**

```bash
go test ./client -run 'TestHTTP3Config|TestHTTP3Datagrams'
```

Expected: FAIL with undefined HTTP/3 symbols.

- [ ] **Step 3: Implement configuration normalization**

In `client/http3.go`, define exactly:

```go
type HTTP3Config struct {
    TLSConfig              *tls.Config
    HandshakeTimeout       time.Duration
    IdleTimeout            time.Duration
    MaxResponseHeaderBytes int64
    DisableCompression     bool
    EnableDatagrams        bool
}

const (
    defaultHTTP3MaxResponseHeaderBytes int64 = 1 << 20
    http3MaxIncomingUniStreams         int64 = 16
)

type normalizedHTTP3Config struct {
    tlsConfig              *tls.Config
    quicConfig             *quicgo.Config
    maxResponseHeaderBytes int
    disableCompression     bool
    enableDatagrams        bool
}
```

Normalization must call:

```go
quicpolicy.Build(quicpolicy.Config{
    TLSConfig:             cfg.TLSConfig,
    ALPN:                  http3.NextProtoH3,
    HandshakeTimeout:      cfg.HandshakeTimeout,
    IdleTimeout:           cfg.IdleTimeout,
    EnableDatagrams:       cfg.EnableDatagrams,
    MaxIncomingStreams:    -1,
    MaxIncomingUniStreams: http3MaxIncomingUniStreams,
})
```

Map `quicpolicy.ErrTLSVersion` to `ErrHTTP3TLSVersion`; map negative timeout/header values to `ErrInvalidHTTP3Config`. Validate `MaxResponseHeaderBytes` against platform `int` width before converting.

- [ ] **Step 4: Implement stable HTTP/3 error types**

Add:

```go
type HTTP3ErrorKind uint8

const (
    HTTP3ErrorUnknown HTTP3ErrorKind = iota
    HTTP3ErrorTransport
    HTTP3ErrorProtocol
    HTTP3ErrorClosed
)

type HTTP3Error struct {
    Kind  HTTP3ErrorKind
    Cause error
}

func (e *HTTP3Error) Error() string {
    if e == nil {
        return "<nil>"
    }
    return fmt.Sprintf("client: HTTP/3 %d: %v", e.Kind, e.Cause)
}

func (e *HTTP3Error) Unwrap() error {
    if e == nil {
        return nil
    }
    return e.Cause
}
```

Add the sentinels from the spec. Do not add header/body error kinds.

- [ ] **Step 5: Verify Task 2**

```bash
gofmt -w client/http3.go client/http3_test.go client/doc.go
go test ./client ./internal/quicpolicy ./quic
go vet ./client ./internal/quicpolicy ./quic
```

Expected: PASS.

- [ ] **Step 6: Commit Task 2**

```bash
git add client/http3.go client/http3_test.go client/doc.go
git commit -m "client: add HTTP/3 configuration policy"
```

---

### Task 3: Implement non-early shared UDP dial bridge and transport lifecycle

**Files:**
- Create: `client/http3_dial.go`
- Modify: `client/http3.go`
- Modify: `client/http3_test.go`

**Interfaces:**
- Consumes: `normalizedHTTP3Config` from Task 2.
- Produces:
  - `HTTP3Transport`
  - `NewHTTP3Transport(HTTP3Config) (*HTTP3Transport, error)`
  - `NewHTTP3Client(HTTP3Config) (*http.Client, error)`
  - `(*HTTP3Transport).RoundTrip`, `Close`, `CloseIdleConnections`
  - internal `http3Dialer` that resolves with context and calls only `quic.Transport.Dial`.

- [ ] **Step 1: Write lifecycle and dial-path tests first**

Add tests:

```go
func TestHTTP3TransportCloseIsIdempotent(t *testing.T) {
    tr, err := NewHTTP3Transport(HTTP3Config{})
    if err != nil {
        t.Fatal(err)
    }
    if err := tr.Close(); err != nil {
        t.Fatal(err)
    }
    if err := tr.Close(); err != nil {
        t.Fatalf("second Close: %v", err)
    }

    req, _ := http.NewRequest(http.MethodGet, "https://127.0.0.1/", nil)
    _, err = tr.RoundTrip(req)
    if !errors.Is(err, ErrHTTP3TransportClosed) {
        t.Fatalf("RoundTrip after Close = %v", err)
    }
}

func TestHTTP3ClientHasNoWholeRequestTimeout(t *testing.T) {
    c, err := NewHTTP3Client(HTTP3Config{})
    if err != nil {
        t.Fatal(err)
    }
    if c.Timeout != 0 {
        t.Fatalf("Client.Timeout = %v", c.Timeout)
    }
    _ = c.Transport.(*HTTP3Transport).Close()
}
```

For the dial path, inject a private `dialQUIC` function into `http3Dialer`; the test should record that the production path reaches that function after resolver/UDP setup. The production default must be exactly:

```go
func(tr *quicgo.Transport, ctx context.Context, addr net.Addr, tlsCfg *tls.Config, qcfg *quicgo.Config) (*quicgo.Conn, error) {
    return tr.Dial(ctx, addr, tlsCfg, qcfg)
}
```

- [ ] **Step 2: Run and verify red**

```bash
go test ./client -run 'TestHTTP3TransportClose|TestHTTP3ClientHasNoWholeRequestTimeout|TestHTTP3Dial'
```

Expected: FAIL because constructors/dialer do not exist.

- [ ] **Step 3: Implement `http3Dialer`**

Use this private structure:

```go
type http3Resolver interface {
    LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
    LookupPort(context.Context, string, string) (int, error)
}

type http3Dialer struct {
    mu        sync.Mutex
    resolver  http3Resolver
    udp       *net.UDPConn
    transport *quicgo.Transport
    closed    bool
    listenUDP func(string, *net.UDPAddr) (*net.UDPConn, error)
    dialQUIC  func(*quicgo.Transport, context.Context, net.Addr, *tls.Config, *quicgo.Config) (*quicgo.Conn, error)
}
```

`Dial` must:

1. `net.SplitHostPort(addr)`.
2. resolve the port with `LookupPort(ctx, "udp", port)`.
3. resolve addresses with `LookupIPAddr(ctx, host)`.
4. fail if the returned address list is empty.
5. use the first returned IP deterministically.
6. lazily create one `udp4`/`udp6`-capable UDP socket using `net.ListenUDP("udp", nil)`.
7. lazily create one `quicgo.Transport{Conn: udp}`.
8. call only `dialQUIC`, whose production implementation calls `transport.Dial`.

`Close` must close the `quicgo.Transport` and then the UDP socket, and be safe if initialization never happened.

- [ ] **Step 4: Implement `HTTP3Transport`**

Use:

```go
type HTTP3Transport struct {
    raw       *http3.Transport
    dialer    *http3Dialer
    closed    atomic.Bool
    closeOnce sync.Once
    closeErr  error
}
```

`NewHTTP3Transport` must build `raw := &http3.Transport{...}` with:

```go
raw.TLSClientConfig = normalized.tlsConfig
raw.QUICConfig = normalized.quicConfig
raw.EnableDatagrams = normalized.enableDatagrams
raw.MaxResponseHeaderBytes = normalized.maxResponseHeaderBytes
raw.DisableCompression = normalized.disableCompression
raw.Dial = dialer.Dial
```

`RoundTrip` short-circuits when `closed` is set. `Close` order is `raw.Close()` then `dialer.Close()`, preserving the first non-nil error. `CloseIdleConnections` delegates to `raw.CloseIdleConnections()` and does not close the shared UDP socket.

- [ ] **Step 5: Implement runtime error mapping**

`mapHTTP3Error(err error)` must preserve `context.Canceled` and `context.DeadlineExceeded`; map `*http3.Error` to `HTTP3ErrorProtocol`; map QUIC transport/application/timeout/reset/version-negotiation errors to `HTTP3ErrorTransport`; and map `http3.ErrTransportClosed` to an `HTTP3Error{Kind: HTTP3ErrorClosed, Cause: errors.Join(ErrHTTP3TransportClosed, err)}`.

Do not classify ordinary request validation errors unless they match a stable type.

- [ ] **Step 6: Verify no early-dial reference exists in client implementation**

Run:

```bash
grep -R "DialEarly\|DialAddrEarly" client internal/quicpolicy || true
```

Expected: no production-code match. Test comments may mention the string only if clearly scoped.

Then run:

```bash
gofmt -w client/http3.go client/http3_dial.go client/http3_test.go
go test ./client ./internal/quicpolicy ./quic
go test -race ./client
```

Expected: PASS.

- [ ] **Step 7: Commit Task 3**

```bash
git add client/http3.go client/http3_dial.go client/http3_test.go
git commit -m "client: add HTTP/3 transport lifecycle"
```

---

### Task 4: Add deterministic HTTP/3 loopback, streaming, cancellation and multiplexing

**Files:**
- Create: `client/http3_integration_test.go`
- Modify: `client/http3_test.go` only if a shared test helper belongs there.

**Interfaces:**
- Consumes: public constructors and lifecycle from Task 3.
- Produces: deterministic local proof of standard `net/http` semantics over H3.

- [ ] **Step 1: Build a reusable local H3 test server helper**

In `client/http3_integration_test.go`, generate an Ed25519 self-signed certificate valid for `localhost`, build trusted client roots, then create a UDP socket and non-early QUIC listener:

```go
pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
if err != nil { t.Fatal(err) }
ln, err := quicgo.Listen(pc, http3.ConfigureTLSConfig(serverTLS), &quicgo.Config{MaxIdleTimeout: 5 * time.Second})
if err != nil { t.Fatal(err) }
server := &http3.Server{Handler: handler}
go func() { _ = server.ServeListener(ln) }()
```

The helper returns `https://<listener address>`, a client TLS config with `ServerName: "localhost"`, and a close function that closes server, listener and UDP socket.

- [ ] **Step 2: Write and run a failing H3 loopback test**

```go
func TestHTTP3Loopback(t *testing.T) {
    url, tlsCfg, closeServer := startHTTP3Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("X-Proto", r.Proto)
        _, _ = io.WriteString(w, "ok")
    }))
    defer closeServer()

    tr, err := NewHTTP3Transport(HTTP3Config{TLSConfig: tlsCfg})
    if err != nil { t.Fatal(err) }
    defer tr.Close()

    resp, err := (&http.Client{Transport: tr}).Get(url)
    if err != nil { t.Fatal(err) }
    defer resp.Body.Close()
    if resp.ProtoMajor != 3 {
        t.Fatalf("protocol = %s", resp.Proto)
    }
}
```

Run:

```bash
go test ./client -run TestHTTP3Loopback -count=1
```

Expected before all helper/wiring is correct: FAIL; after fixes: PASS.

- [ ] **Step 3: Add response and request streaming tests**

For response streaming, have the handler write/flush `first`, block on a channel, then write `second`; assert the client can read `first` before releasing the second write.

For request streaming, use `io.Pipe()` as `Request.Body`; write two chunks after `client.Do` starts and assert the handler receives the concatenated body without requiring the whole body up front.

Run:

```bash
go test ./client -run 'TestHTTP3(Response|Request)Streaming' -count=1
```

Expected: PASS.

- [ ] **Step 4: Add active-request cancellation test**

Use a handler that signals entry and waits on `r.Context().Done()`. Cancel the request context and assert:

```go
if !errors.Is(err, context.Canceled) {
    t.Fatalf("error = %v", err)
}
```

- [ ] **Step 5: Add connection reuse and concurrent multiplexing test**

Use `http3.Server.ConnContext` with an `atomic.Int32` counter. Send at least 16 concurrent requests to one origin and assert all complete while the connection count remains 1. Then send two sequential requests and assert the count still remains 1.

Run:

```bash
go test ./client -run 'TestHTTP3(ConnectionReuse|ConcurrentMultiplex)' -count=10
```

Expected: PASS for all repetitions.

- [ ] **Step 6: Verify Task 4 under race detector**

```bash
gofmt -w client/http3_integration_test.go
go test -race ./client -run 'TestHTTP3' -count=1
```

Expected: PASS with no race report.

- [ ] **Step 7: Commit Task 4**

```bash
git add client/http3_integration_test.go client/http3_test.go
git commit -m "test: cover HTTP/3 streaming and multiplexing"
```

---

### Task 5: Prove failure semantics, no fallback and close-idle behavior

**Files:**
- Modify: `client/http3_integration_test.go`
- Modify: `client/http3_test.go`

**Interfaces:**
- Consumes: completed H3 transport.
- Produces: acceptance coverage for no fallback, DNS/dial cancellation, handshake failure, resource cleanup and active-request close-idle behavior.

- [ ] **Step 1: Add no-fallback test**

Start an `httptest.NewTLSServer` and first prove its ordinary TCP client succeeds. Then create `NewHTTP3Transport` targeting the same `https://host:port` with a short `HandshakeTimeout` and the test server TLS config.

The H3 request must fail. The test must not inspect an HTTP response because any successful response would mean fallback occurred:

```go
resp, err := h3Client.Get(tcpServer.URL)
if err == nil {
    resp.Body.Close()
    t.Fatal("HTTP/3 client silently fell back to TCP HTTP")
}
```

- [ ] **Step 2: Add ALPN/TLS handshake failure test**

Run a local QUIC listener whose TLS `NextProtos` deliberately excludes `h3`; assert the client request fails with `HTTP3ErrorTransport` and preserves the underlying cause through `errors.As`/`errors.Unwrap`.

- [ ] **Step 3: Add DNS/connection cancellation test using the resolver seam**

Inject a test resolver whose `LookupIPAddr` blocks until `ctx.Done()` and returns `context.Cause(ctx)`. Cancel the request context and assert `errors.Is(err, context.Canceled)` and that no UDP transport was initialized before DNS completed.

- [ ] **Step 4: Add blackhole QUIC cleanup test**

Bind a local UDP socket that reads and discards packets. Issue a request with a bounded context. After the request returns, call `Close()` in a goroutine and require it to complete within one second. Call `Close()` a second time and require the same stable result.

This is the deterministic resource-cleanup assertion; do not use flaky global goroutine-count comparisons.

- [ ] **Step 5: Add `CloseIdleConnections` active-request test**

Start one blocking request, call `tr.CloseIdleConnections()`, release the handler, and assert the active request still succeeds. Then issue another request and allow either connection reuse or a new connection, but never failure caused by close-idle.

- [ ] **Step 6: Run failure suite repeatedly**

```bash
go test ./client -run 'TestHTTP3(NoFallback|ALPN|DNS|Blackhole|CloseIdle)' -count=20
go test -race ./client -run 'TestHTTP3(NoFallback|ALPN|DNS|Blackhole|CloseIdle)' -count=1
```

Expected: PASS without intermittent timeout/race failures.

- [ ] **Step 7: Commit Task 5**

```bash
git add client/http3_integration_test.go client/http3_test.go
git commit -m "test: enforce HTTP/3 failure semantics"
```

---

### Task 6: Documentation, benchmark and repository-wide verification

**Files:**
- Create: `client/http3_benchmark_test.go`
- Create: `docs/http3-client.md`
- Modify: `docs/http-client.md`
- Modify: `client/doc.go`
- Do not modify `.github/workflows/netpoll-v2.yml` unless a real H3 platform failure demonstrates a necessary CI change.

**Interfaces:**
- Consumes: complete Phase 3 API.
- Produces: user-facing ownership/security docs and regression benchmark.

- [ ] **Step 1: Add HTTP/3 multiplex benchmark**

Use the same local server helper and one long-lived transport:

```go
func BenchmarkHTTP3MultiplexedRequests(b *testing.B) {
    url, tlsCfg, closeServer := startHTTP3Server(b, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        _, _ = io.WriteString(w, "ok")
    }))
    defer closeServer()

    tr, err := NewHTTP3Transport(HTTP3Config{TLSConfig: tlsCfg})
    if err != nil { b.Fatal(err) }
    defer tr.Close()
    client := &http.Client{Transport: tr}

    b.ReportAllocs()
    b.ResetTimer()
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            resp, err := client.Get(url)
            if err != nil { b.Fatal(err) }
            _, _ = io.Copy(io.Discard, resp.Body)
            _ = resp.Body.Close()
        }
    })
}
```

If the helper currently accepts only `*testing.T`, refactor it to a small interface containing `Helper`, `Fatal`, `Cleanup` rather than duplicate server setup.

- [ ] **Step 2: Write `docs/http3-client.md`**

Document these exact points:

- `NewHTTP3Transport` is H3-only and accepts only HTTPS requests.
- no H3→H2/H1 fallback exists.
- TLS 1.3+ and ALPN `h3` are enforced.
- caller TLS config is cloned; certificate verification is ordinary Go TLS policy.
- 0-RTT is disabled because the transport uses non-early QUIC dial.
- datagrams are opt-in and do not imply WebTransport/MASQUE support.
- `HTTP3Transport` owns its H3 pool/shared QUIC transport/UDP socket and should be closed.
- `CloseIdleConnections` leaves active multiplexed requests alone.
- request contexts govern cancellation and streaming; `http.Client.Timeout` remains zero.

Add a short link from `docs/http-client.md` to the H3-specific document while keeping HTTP1/2 protocol-selection text unchanged.

- [ ] **Step 3: Run benchmark as a smoke check**

```bash
go test ./client -run '^$' -bench BenchmarkHTTP3MultiplexedRequests -benchtime=100x -count=1
```

Expected: benchmark completes without error. No numeric threshold is required.

- [ ] **Step 4: Run complete local verification**

```bash
test -z "$(gofmt -l .)"
go mod tidy
git diff --exit-code -- go.mod go.sum
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
```

Expected: every command exits 0; `go mod tidy` produces no module diff.

- [ ] **Step 5: Verify public API does not leak quic-go types**

Run:

```bash
go doc github.com/qigao/ogrenet/client
go doc github.com/qigao/ogrenet/quic
grep -R "github.com/quic-go/quic-go" client/*.go quic/*.go
```

The grep may show private implementation imports, but `go doc` exported signatures must not contain `quic-go` types.

- [ ] **Step 6: Commit Task 6**

```bash
git add client/http3_benchmark_test.go docs/http3-client.md docs/http-client.md client/doc.go
git commit -m "docs: document HTTP/3 client runtime"
```

- [ ] **Step 7: Review stacked diff before PR work**

```bash
git diff --stat feat/quic-client-runtime...HEAD
git diff feat/quic-client-runtime...HEAD -- client quic internal/quicpolicy docs go.mod go.sum
```

Expected: only Phase 3 shared-policy/H3/docs changes; no unrelated repository edits.

- [ ] **Step 8: Push and create/update the stacked Draft PR only after explicit authorization**

The PR base must be `feat/quic-client-runtime`, head `feat/http3-client-runtime`, and remain Draft. Its body must cite #41 and #38, state that #43 is the stacked prerequisite, state that H3 fallback and 0-RTT are absent, and report the actual GitHub Actions run rather than claiming CI before it finishes.

After #43 merges, retarget/rebase this PR to `master` and re-verify the resulting diff before merge.
