# HTTP/3 Client Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a production-grade HTTP/3-only `net/http` client transport with bounded QUIC policy, no H3→H2/H1 fallback, no 0-RTT client dial path, deterministic lifecycle semantics, and cross-platform race-tested coverage.

**Architecture:** Extract the existing QUIC TLS/timeouts/window policy into `internal/quicpolicy`, preserving the public `ogrenet/quic` API and behavior. Add `client.HTTP3Transport` as a wrapper around `quic-go/http3.Transport`, overriding its default early dial path with an ogrenet-owned shared UDP `quic.Transport` that always calls non-early `Dial`.

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
- Consumes: existing `quic.Config` and current bounded defaults.
- Produces: `quicpolicy.Config`, `quicpolicy.Build(Config) (*tls.Config, *quicgo.Config, error)`, shared default constants, and sentinel errors aliased by public `quic`.

- [ ] **Step 1: Write the failing policy tests**

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
    if err != nil { t.Fatal(err) }
    if tlsCfg == original { t.Fatal("TLS config was not cloned") }
    if tlsCfg.MinVersion != tls.VersionTLS13 { t.Fatalf("MinVersion = %d", tlsCfg.MinVersion) }
    if len(tlsCfg.NextProtos) != 1 || tlsCfg.NextProtos[0] != "ogrenet-test" { t.Fatalf("NextProtos = %v", tlsCfg.NextProtos) }
    if !qcfg.EnableDatagrams || qcfg.Allow0RTT { t.Fatalf("datagrams=%v Allow0RTT=%v", qcfg.EnableDatagrams, qcfg.Allow0RTT) }
    if qcfg.MaxIncomingStreams != 32 || qcfg.MaxIncomingUniStreams != -1 { t.Fatalf("limits = %d/%d", qcfg.MaxIncomingStreams, qcfg.MaxIncomingUniStreams) }
}

func TestBuildValidation(t *testing.T) {
    if _, _, err := Build(Config{}); !errors.Is(err, ErrALPNRequired) { t.Fatalf("empty ALPN: %v", err) }
    if _, _, err := Build(Config{ALPN: "x", HandshakeTimeout: -time.Second}); !errors.Is(err, ErrInvalidTimeout) { t.Fatalf("timeout: %v", err) }
    if _, _, err := Build(Config{ALPN: "x", TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}); !errors.Is(err, ErrTLSVersion) { t.Fatalf("TLS: %v", err) }
}
```

- [ ] **Step 2: Verify red**

```bash
go test ./internal/quicpolicy
```

Expected: FAIL because the package does not exist.

- [ ] **Step 3: Implement the shared policy**

Create `internal/quicpolicy/policy.go`:

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
    if c.ALPN == "" { return nil, nil, ErrALPNRequired }
    if c.HandshakeTimeout < 0 || c.IdleTimeout < 0 { return nil, nil, ErrInvalidTimeout }
    tlsCfg := &tls.Config{}
    if c.TLSConfig != nil { tlsCfg = c.TLSConfig.Clone() }
    if tlsCfg.MinVersion != 0 && tlsCfg.MinVersion < tls.VersionTLS13 { return nil, nil, ErrTLSVersion }
    if tlsCfg.MaxVersion != 0 && tlsCfg.MaxVersion < tls.VersionTLS13 { return nil, nil, ErrTLSVersion }
    if tlsCfg.MinVersion == 0 { tlsCfg.MinVersion = tls.VersionTLS13 }
    tlsCfg.NextProtos = []string{c.ALPN}
    handshake := c.HandshakeTimeout
    if handshake == 0 { handshake = DefaultHandshakeTimeout }
    idle := c.IdleTimeout
    if idle == 0 { idle = DefaultIdleTimeout }
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

Modify `quic/config.go` so public sentinels alias `quicpolicy` sentinels and `build` delegates with `MaxIncomingStreams: quicpolicy.DefaultMaxIncomingStreams` and `MaxIncomingUniStreams: -1`. Update `quic/config_test.go` to assert the same values via `quicpolicy.Default...` constants.

- [ ] **Step 4: Verify policy extraction parity**

```bash
gofmt -w internal/quicpolicy quic/config.go quic/config_test.go
go test ./internal/quicpolicy ./quic
go test -race ./quic
```

Expected: PASS with no changed public QUIC expectations.

- [ ] **Step 5: Commit**

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
- Consumes: `quicpolicy.Build`.
- Produces: `HTTP3Config`, `HTTP3ErrorKind`, `HTTP3Error`, `ErrInvalidHTTP3Config`, `ErrHTTP3TLSVersion`, `ErrHTTP3TransportClosed`, and `normalizeHTTP3Config`.

- [ ] **Step 1: Write failing config tests**

```go
func TestHTTP3ConfigDefaultsAndPolicy(t *testing.T) {
    got, err := normalizeHTTP3Config(HTTP3Config{})
    if err != nil { t.Fatal(err) }
    if got.tlsConfig.MinVersion != tls.VersionTLS13 { t.Fatalf("TLS min = %d", got.tlsConfig.MinVersion) }
    if len(got.tlsConfig.NextProtos) != 1 || got.tlsConfig.NextProtos[0] != http3.NextProtoH3 { t.Fatalf("ALPN = %v", got.tlsConfig.NextProtos) }
    if got.quicConfig.MaxIncomingStreams != -1 { t.Fatalf("peer bidi = %d", got.quicConfig.MaxIncomingStreams) }
    if got.quicConfig.MaxIncomingUniStreams != 16 { t.Fatalf("peer uni = %d", got.quicConfig.MaxIncomingUniStreams) }
    if got.quicConfig.EnableDatagrams || got.quicConfig.Allow0RTT { t.Fatalf("datagrams=%v Allow0RTT=%v", got.quicConfig.EnableDatagrams, got.quicConfig.Allow0RTT) }
    if got.maxResponseHeaderBytes != 1<<20 { t.Fatalf("header bound = %d", got.maxResponseHeaderBytes) }
}

func TestHTTP3ConfigValidation(t *testing.T) {
    cases := []HTTP3Config{
        {HandshakeTimeout: -time.Second},
        {IdleTimeout: -time.Second},
        {MaxResponseHeaderBytes: -1},
        {TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}},
        {TLSConfig: &tls.Config{MaxVersion: tls.VersionTLS12}},
    }
    for _, cfg := range cases {
        if _, err := normalizeHTTP3Config(cfg); err == nil { t.Fatalf("accepted %+v", cfg) }
    }
}

func TestHTTP3DatagramPolicy(t *testing.T) {
    got, err := normalizeHTTP3Config(HTTP3Config{EnableDatagrams: true})
    if err != nil { t.Fatal(err) }
    if !got.enableDatagrams || !got.quicConfig.EnableDatagrams { t.Fatalf("H3=%v QUIC=%v", got.enableDatagrams, got.quicConfig.EnableDatagrams) }
}
```

Also add `TestHTTP3MaxResponseHeaderBytesFitsInt`: calculate `maxInt := uint64(^uint(0) >> 1)` and reject any positive `MaxResponseHeaderBytes` whose `uint64` value exceeds it before conversion to `int`.

- [ ] **Step 2: Verify red**

```bash
go test ./client -run 'TestHTTP3(Config|Datagram|MaxResponse)'
```

Expected: FAIL with undefined HTTP/3 symbols.

- [ ] **Step 3: Implement normalization and API types**

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

Call `quicpolicy.Build` with `ALPN: http3.NextProtoH3`, `MaxIncomingStreams: -1`, `MaxIncomingUniStreams: 16`, and `EnableDatagrams: cfg.EnableDatagrams`. Map policy TLS failures to `ErrHTTP3TLSVersion`; map negative timeout/header and integer-overflow cases to `ErrInvalidHTTP3Config`.

- [ ] **Step 4: Implement the error taxonomy**

```go
type HTTP3ErrorKind uint8
const (
    HTTP3ErrorUnknown HTTP3ErrorKind = iota
    HTTP3ErrorTransport
    HTTP3ErrorProtocol
    HTTP3ErrorClosed
)

type HTTP3Error struct { Kind HTTP3ErrorKind; Cause error }
func (e *HTTP3Error) Error() string { if e == nil { return "<nil>" }; return fmt.Sprintf("client: HTTP/3 %d: %v", e.Kind, e.Cause) }
func (e *HTTP3Error) Unwrap() error { if e == nil { return nil }; return e.Cause }
```

Add the three sentinel errors from the spec. Do not add header/body categories.

- [ ] **Step 5: Verify and commit**

```bash
gofmt -w client/http3.go client/http3_test.go client/doc.go
go test ./client ./internal/quicpolicy ./quic
go vet ./client ./internal/quicpolicy ./quic
git add client/http3.go client/http3_test.go client/doc.go
git commit -m "client: add HTTP/3 configuration policy"
```

---

### Task 3: Implement non-early shared UDP dial bridge and lifecycle

**Files:**
- Create: `client/http3_dial.go`
- Modify: `client/http3.go`
- Modify: `client/http3_test.go`

**Interfaces:**
- Consumes: normalized config from Task 2.
- Produces: `HTTP3Transport`, `NewHTTP3Transport`, `NewHTTP3Client`, `RoundTrip`, `Close`, `CloseIdleConnections`, and private `http3Dialer`.

- [ ] **Step 1: Write lifecycle tests**

```go
func TestHTTP3TransportCloseIsIdempotent(t *testing.T) {
    tr, err := NewHTTP3Transport(HTTP3Config{})
    if err != nil { t.Fatal(err) }
    if err := tr.Close(); err != nil { t.Fatal(err) }
    if err := tr.Close(); err != nil { t.Fatalf("second Close: %v", err) }
    req, _ := http.NewRequest(http.MethodGet, "https://127.0.0.1/", nil)
    _, err = tr.RoundTrip(req)
    if !errors.Is(err, ErrHTTP3TransportClosed) { t.Fatalf("after close: %v", err) }
}

func TestHTTP3ClientHasNoWholeRequestTimeout(t *testing.T) {
    c, err := NewHTTP3Client(HTTP3Config{})
    if err != nil { t.Fatal(err) }
    if c.Timeout != 0 { t.Fatalf("Timeout = %v", c.Timeout) }
    _ = c.Transport.(*HTTP3Transport).Close()
}
```

- [ ] **Step 2: Write a dial-path seam test**

Define in production:

```go
type http3Resolver interface {
    LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
    LookupPort(context.Context, string, string) (int, error)
}
```

In the test, use a resolver that returns `127.0.0.1` and port `443`, and a `dialQUIC` closure that sets `called = true` and returns a sentinel error. Assert `http3Dialer.Dial` returns that sentinel and `called` is true. This proves the custom path is used without requiring a real QUIC connection.

- [ ] **Step 3: Verify red**

```bash
go test ./client -run 'TestHTTP3TransportClose|TestHTTP3ClientHasNoWholeRequestTimeout|TestHTTP3Dial'
```

Expected: FAIL because constructors/dialer do not exist.

- [ ] **Step 4: Implement `http3Dialer`**

```go
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

Production `dialQUIC` is exactly:

```go
func(tr *quicgo.Transport, ctx context.Context, addr net.Addr, tlsCfg *tls.Config, qcfg *quicgo.Config) (*quicgo.Conn, error) {
    return tr.Dial(ctx, addr, tlsCfg, qcfg)
}
```

`Dial` must split host/port, call `resolver.LookupPort(ctx, "udp", port)`, call `resolver.LookupIPAddr(ctx, host)`, reject an empty address list, use the first IP deterministically, lazily allocate `net.ListenUDP("udp", nil)`, lazily construct `quicgo.Transport{Conn: udp}`, then call `dialQUIC`. `Close` closes the QUIC transport then UDP socket and is safe before initialization.

- [ ] **Step 5: Implement `HTTP3Transport`**

```go
type HTTP3Transport struct {
    raw       *http3.Transport
    dialer    *http3Dialer
    closed    atomic.Bool
    closeOnce sync.Once
    closeErr  error
}
```

Constructor wiring:

```go
raw := &http3.Transport{
    TLSClientConfig:        n.tlsConfig,
    QUICConfig:             n.quicConfig,
    EnableDatagrams:        n.enableDatagrams,
    MaxResponseHeaderBytes: n.maxResponseHeaderBytes,
    DisableCompression:     n.disableCompression,
}
raw.Dial = dialer.Dial
```

`RoundTrip` checks `closed` before calling `raw.RoundTrip`. `Close` sets `closed`, calls `raw.Close`, then always calls `dialer.Close`, returning the first non-nil error. `CloseIdleConnections` delegates to `raw` and does not close the shared UDP socket.

- [ ] **Step 6: Implement runtime error mapping**

Preserve context cancellation/deadline. Map `*http3.Error` to protocol, QUIC handshake/transport/application/reset/version/timeout errors to transport, and `http3.ErrTransportClosed` to:

```go
&HTTP3Error{Kind: HTTP3ErrorClosed, Cause: errors.Join(ErrHTTP3TransportClosed, err)}
```

Leave unrecognized request-validation errors unwrapped.

- [ ] **Step 7: Verify no early dial and run race tests**

```bash
grep -R "DialEarly\|DialAddrEarly" client internal/quicpolicy || true
gofmt -w client/http3.go client/http3_dial.go client/http3_test.go
go test ./client ./internal/quicpolicy ./quic
go test -race ./client
```

Expected: no production early-dial match; all tests PASS.

- [ ] **Step 8: Commit**

```bash
git add client/http3.go client/http3_dial.go client/http3_test.go
git commit -m "client: add HTTP/3 transport lifecycle"
```

---

### Task 4: Add deterministic H3 loopback, streaming, cancellation and multiplexing

**Files:**
- Create: `client/http3_integration_test.go`

**Interfaces:**
- Consumes: Task 3 public constructors.
- Produces: local H3 integration helper usable by both tests and benchmarks.

- [ ] **Step 1: Implement a benchmark-compatible test helper**

```go
type http3TestTB interface {
    Helper()
    Fatal(args ...any)
    Cleanup(func())
}

func http3TLSConfigs(tb http3TestTB) (*tls.Config, *tls.Config) {
    tb.Helper()
    _, key, err := ed25519.GenerateKey(rand.Reader)
    if err != nil { tb.Fatal(err) }
    now := time.Now()
    tmpl := &x509.Certificate{
        SerialNumber: big.NewInt(1),
        Subject: pkix.Name{CommonName: "localhost"},
        DNSNames: []string{"localhost"},
        NotBefore: now.Add(-time.Minute),
        NotAfter: now.Add(time.Hour),
        KeyUsage: x509.KeyUsageDigitalSignature,
        ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
    }
    der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
    if err != nil { tb.Fatal(err) }
    cert, err := x509.ParseCertificate(der)
    if err != nil { tb.Fatal(err) }
    roots := x509.NewCertPool(); roots.AddCert(cert)
    server := &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS13}
    client := &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS13}
    return server, client
}

func startHTTP3Server(tb http3TestTB, handler http.Handler) (string, *tls.Config) {
    tb.Helper()
    serverTLS, clientTLS := http3TLSConfigs(tb)
    pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
    if err != nil { tb.Fatal(err) }
    ln, err := quicgo.Listen(pc, http3.ConfigureTLSConfig(serverTLS), &quicgo.Config{MaxIdleTimeout: 5 * time.Second})
    if err != nil { tb.Fatal(err) }
    server := &http3.Server{Handler: handler}
    go func() { _ = server.ServeListener(ln) }()
    tb.Cleanup(func() { _ = server.Close(); _ = ln.Close(); _ = pc.Close() })
    return "https://" + ln.Addr().String(), clientTLS
}
```

- [ ] **Step 2: Add loopback test**

```go
func TestHTTP3Loopback(t *testing.T) {
    url, tlsCfg := startHTTP3Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        _, _ = io.WriteString(w, "ok")
    }))
    tr, err := NewHTTP3Transport(HTTP3Config{TLSConfig: tlsCfg})
    if err != nil { t.Fatal(err) }
    defer tr.Close()
    resp, err := (&http.Client{Transport: tr}).Get(url)
    if err != nil { t.Fatal(err) }
    defer resp.Body.Close()
    if resp.ProtoMajor != 3 { t.Fatalf("protocol = %s", resp.Proto) }
}
```

Run `go test ./client -run TestHTTP3Loopback -count=1`; make it green before adding more integration cases.

- [ ] **Step 3: Add response streaming**

Use channels `firstWritten` and `releaseSecond`. Handler writes `first`, calls `Flush`, closes `firstWritten`, waits, then writes `second`. Client must read `first` before closing `releaseSecond`.

- [ ] **Step 4: Add request streaming**

Use `io.Pipe`; start `client.Do(req)` in a goroutine, write `first` and `second` to the pipe, close the writer, and assert the handler reads `firstsecond`.

- [ ] **Step 5: Add active cancellation**

Handler closes `entered` then waits on `r.Context().Done()`. Client cancels the request context after `<-entered`; assert `errors.Is(err, context.Canceled)`.

- [ ] **Step 6: Add reuse and multiplexing**

Set `server.ConnContext` in a second helper variant to increment `atomic.Int32`. Send 16 concurrent requests and require all 16 to succeed with connection count 1. Then send two sequential requests and require the count remain 1.

- [ ] **Step 7: Stress/race verify and commit**

```bash
gofmt -w client/http3_integration_test.go
go test ./client -run 'TestHTTP3(Loopback|ResponseStreaming|RequestStreaming|Cancellation|ConnectionReuse|ConcurrentMultiplex)' -count=10
go test -race ./client -run 'TestHTTP3' -count=1
git add client/http3_integration_test.go
git commit -m "test: cover HTTP/3 streaming and multiplexing"
```

---

### Task 5: Prove failure semantics, no fallback and close-idle behavior

**Files:**
- Modify: `client/http3_integration_test.go`
- Modify: `client/http3_test.go`

**Interfaces:**
- Consumes: completed H3 transport and private resolver seam.
- Produces: acceptance coverage for failure classification and cleanup.

- [ ] **Step 1: Add no-fallback test**

```go
func TestHTTP3NoFallbackToTCP(t *testing.T) {
    tcp := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
    defer tcp.Close()
    resp, err := tcp.Client().Get(tcp.URL)
    if err != nil { t.Fatal(err) }
    resp.Body.Close()

    tlsCfg := tcp.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
    tr, err := NewHTTP3Transport(HTTP3Config{TLSConfig: tlsCfg, HandshakeTimeout: 150 * time.Millisecond})
    if err != nil { t.Fatal(err) }
    defer tr.Close()
    resp, err = (&http.Client{Transport: tr}).Get(tcp.URL)
    if err == nil {
        resp.Body.Close()
        t.Fatal("HTTP/3 client silently fell back to TCP HTTP")
    }
}
```

- [ ] **Step 2: Add ALPN mismatch test**

Create a UDP socket and QUIC listener using a TLS config with `NextProtos: []string{"not-h3"}`. Start `ln.Accept` in a goroutine so the handshake is driven. Request the listener address with H3; require an error and `errors.As(err, &HTTP3Error{})` with kind `HTTP3ErrorTransport`.

- [ ] **Step 3: Add DNS cancellation test with exact fake resolver**

```go
type blockingHTTP3Resolver struct{ entered chan struct{} }
func (r *blockingHTTP3Resolver) LookupPort(context.Context, string, string) (int, error) { return 443, nil }
func (r *blockingHTTP3Resolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
    close(r.entered)
    <-ctx.Done()
    return nil, context.Cause(ctx)
}
```

Replace `tr.dialer.resolver` in the package-local test, start a request, wait for `entered`, cancel, assert `errors.Is(err, context.Canceled)`, then inspect `tr.dialer.udp` under its mutex and require it is still nil.

- [ ] **Step 4: Add blackhole cleanup test**

Bind `net.ListenUDP("udp4", 127.0.0.1:0)` and run a goroutine that repeatedly `ReadFromUDP` and discards bytes. Issue an H3 request to that port with a 250ms context timeout. After it fails, invoke `tr.Close()` in a buffered result channel and require completion before `time.After(time.Second)`. Call `Close` again and require the same stable result. Do not compare global goroutine counts.

- [ ] **Step 5: Add `CloseIdleConnections` active-request test**

Handler closes `entered`, waits on `release`, then writes 204. Start request, wait for `entered`, call `tr.CloseIdleConnections()`, close `release`, and assert the request succeeds. Issue one additional request and assert it also succeeds.

- [ ] **Step 6: Repeat failure suite under race**

```bash
go test ./client -run 'TestHTTP3(NoFallback|ALPN|DNS|Blackhole|CloseIdle)' -count=20
go test -race ./client -run 'TestHTTP3(NoFallback|ALPN|DNS|Blackhole|CloseIdle)' -count=1
```

Expected: PASS with no intermittent timeout/race failure.

- [ ] **Step 7: Commit**

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

**Interfaces:**
- Consumes: complete Phase 3 API and `startHTTP3Server(http3TestTB, http.Handler)` from Task 4.
- Produces: ownership/security docs and regression benchmark.

- [ ] **Step 1: Add multiplex benchmark**

```go
func BenchmarkHTTP3MultiplexedRequests(b *testing.B) {
    url, tlsCfg := startHTTP3Server(b, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
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

- [ ] **Step 2: Write `docs/http3-client.md`**

Document: H3-only HTTPS, no fallback, TLS 1.3/ALPN `h3`, cloned TLS config, non-early dial/0-RTT disabled, opt-in datagrams, transport ownership, `Close`/`CloseIdleConnections`, request-context cancellation/streaming, and zero `http.Client.Timeout`. Link it from `docs/http-client.md` without changing HTTP1/2 protocol semantics.

- [ ] **Step 3: Benchmark smoke check**

```bash
go test ./client -run '^$' -bench BenchmarkHTTP3MultiplexedRequests -benchtime=100x -count=1
```

- [ ] **Step 4: Run complete verification**

```bash
test -z "$(gofmt -l .)"
go mod tidy
git diff --exit-code -- go.mod go.sum
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
```

Every command must exit 0 before a completion claim.

- [ ] **Step 5: Verify exported API boundary**

```bash
go doc github.com/qigao/ogrenet/client
go doc github.com/qigao/ogrenet/quic
grep -R "github.com/quic-go/quic-go" client/*.go quic/*.go
```

Private imports are allowed. Exported `go doc` signatures must not expose dependency concrete types.

- [ ] **Step 6: Commit docs/benchmark**

```bash
git add client/http3_benchmark_test.go docs/http3-client.md docs/http-client.md client/doc.go
git commit -m "docs: document HTTP/3 client runtime"
```

- [ ] **Step 7: Review stacked diff**

```bash
git diff --stat feat/quic-client-runtime...HEAD
git diff feat/quic-client-runtime...HEAD -- client quic internal/quicpolicy docs go.mod go.sum
```

Expected: only Phase 3 shared-policy/H3/docs changes.

- [ ] **Step 8: Push and create/update stacked Draft PR only after explicit authorization**

Base: `feat/quic-client-runtime`. Head: `feat/http3-client-runtime`. Keep Draft. Cite #41/#38 and #43 as prerequisite. State explicitly that fallback and 0-RTT are absent. Report actual GitHub Actions results only after the run completes. After #43 merges, retarget/rebase to `master` and re-verify the diff before merge.
