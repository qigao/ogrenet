# HTTP Protocol Facade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an explicit ordered HTTP/3 -> HTTP/2 -> HTTP/1.1 facade with conservative safe-method-only fallback, strict lifecycle ownership, and no hidden protocol learning.

**Architecture:** Keep existing H1/H2 and H3 transports strict and independent. The new facade owns one long-lived transport slot per configured protocol, clones requests per attempt, replays bodies only when HTTP method semantics and `GetBody` allow it, classifies fallback errors conservatively, and aggregates multi-attempt failures without hiding causes.

**Tech Stack:** Go 1.25/1.26, `net/http`, `httptrace`, existing `client.HTTPConfig`, existing `client.HTTP3Config`, quic-go through the existing H3 transport only.

**Spec:** `docs/superpowers/specs/2026-08-21-http-protocol-facade-design.md`

## Global Constraints

- Existing `NewHTTPTransport`, `NewHTTPClient`, `NewHTTP3Transport`, and `NewHTTP3Client` semantics do not change.
- Top-level facade `Protocols` is mandatory, ordered, duplicate-free, and is the only facade protocol policy source.
- H1, H2, and H3 use distinct long-lived transport slots.
- Safe replay is limited to GET, HEAD, OPTIONS, and TRACE.
- POST, PUT, PATCH, and DELETE never automatically fall back.
- Unknown failures never fall back.
- TLS identity/certificate failures never fall back.
- HTTP responses, status codes, response-header timeouts, protocol/application errors, and context cancellation never fall back.
- HTTP3 plus a non-nil H1/H2 proxy is a construction-time error.
- No Alt-Svc, origin capability cache, h2c, MASQUE, CONNECT-UDP, or browser-style adaptive policy.
- No public quic-go types are introduced.
- Full existing cross-platform/race/GmSSL verification must remain green.

---

### Task 1: Facade config, protocol enum, strict ordered slots, lifecycle skeleton

**Files:**
- Modify: `client/http.go`
- Create: `client/facade.go`
- Create: `client/facade_test.go`

**Interfaces:**
- Produces: `HTTP3 HTTPProtocol`, `HTTPFallbackPolicy`, `HTTPClientConfig`, `HTTPClientTransport`, `NewHTTPRoundTripper`, `NewClient`, `HTTPAttemptInfo`, `HTTPAttemptFromContext`, `ErrInvalidHTTPClientConfig`, `ErrHTTPClientTransportClosed`.
- Consumes: `NewHTTPTransport(HTTPConfig)`, `NewHTTP3Transport(HTTP3Config)`.

- [ ] **Step 1: Write failing enum/config tests**

Add tests asserting:

```go
func TestHTTPProtocolStringIncludesHTTP3(t *testing.T) {
    if got := HTTP3.String(); got != "h3" { t.Fatalf("HTTP3.String() = %q", got) }
}

func TestHTTPClientConfigRequiresExplicitProtocols(t *testing.T) {
    _, err := NewHTTPRoundTripper(HTTPClientConfig{})
    if !errors.Is(err, ErrInvalidHTTPClientConfig) { t.Fatalf("err = %v", err) }
}

func TestHTTPClientConfigRejectsDuplicateProtocols(t *testing.T) {
    _, err := NewHTTPRoundTripper(HTTPClientConfig{Protocols: []HTTPProtocol{HTTP2, HTTP2}})
    if !errors.Is(err, ErrInvalidHTTPClientConfig) { t.Fatalf("err = %v", err) }
}

func TestHTTPClientConfigRejectsNestedHTTPProtocols(t *testing.T) {
    _, err := NewHTTPRoundTripper(HTTPClientConfig{
        Protocols: []HTTPProtocol{HTTP2},
        HTTP: HTTPConfig{Protocols: []HTTPProtocol{HTTP2}},
    })
    if !errors.Is(err, ErrInvalidHTTPClientConfig) { t.Fatalf("err = %v", err) }
}

func TestHTTPClientConfigRejectsHTTP3WithProxy(t *testing.T) {
    _, err := NewHTTPRoundTripper(HTTPClientConfig{
        Protocols: []HTTPProtocol{HTTP3, HTTP2},
        HTTP: HTTPConfig{Proxy: http.ProxyFromEnvironment},
    })
    if !errors.Is(err, ErrInvalidHTTPClientConfig) { t.Fatalf("err = %v", err) }
}
```

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
go test ./client -run 'TestHTTPProtocolStringIncludesHTTP3|TestHTTPClientConfig'
```

Expected: compile failure because facade types and HTTP3 enum are not defined.

- [ ] **Step 3: Extend `HTTPProtocol` without changing H1/H2 normalization**

In `client/http.go`, add `HTTP3` and return `"h3"` from `String()`. Do not add HTTP3 to `normalizeProtocols`; `NewHTTPTransport` must continue to reject it.

- [ ] **Step 4: Add facade configuration types and validation**

In `client/facade.go`, define:

```go
type HTTPFallbackPolicy uint8
const (
    HTTPFallbackDisabled HTTPFallbackPolicy = iota
    HTTPFallbackSafeReplay
)

type HTTPClientConfig struct {
    Protocols []HTTPProtocol
    HTTP HTTPConfig
    HTTP3 HTTP3Config
    Fallback HTTPFallbackPolicy
}
```

Validation must reject empty protocols, duplicates, unknown protocols, nested `HTTP.Protocols`, unsupported fallback policies, and H3 plus non-nil proxy.

- [ ] **Step 5: Add strict slot construction**

Define:

```go
type protocolTransport struct {
    protocol HTTPProtocol
    rt http.RoundTripper
}
```

Build one slot per configured protocol in the original order. For H1/H2, clone `HTTPConfig` and inject exactly one protocol before calling `NewHTTPTransport`. For H3 call `NewHTTP3Transport`.

- [ ] **Step 6: Add lifecycle skeleton and client constructor**

Define `HTTPClientTransport` with `[]protocolTransport`, closed state, `CloseIdleConnections`, idempotent `Close`, and closed-before-new-RoundTrip sentinel behavior. `NewClient` returns `&http.Client{Transport: facade}` with `Timeout == 0`.

- [ ] **Step 7: Add attempt metadata context helper**

Define `HTTPAttemptInfo` and an unexported context key. `HTTPAttemptFromContext` returns metadata without exposing mutable state.

- [ ] **Step 8: Run focused tests, full client tests, vet**

```bash
go test ./client -run 'TestHTTPProtocolStringIncludesHTTP3|TestHTTPClientConfig|TestHTTPClientTransport'
go test ./client
go vet ./client
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add client/http.go client/facade.go client/facade_test.go
git commit -m "client: add explicit HTTP protocol facade"
```

---

### Task 2: Request cloning, safe replay, fallback classifier, aggregate errors

**Files:**
- Modify: `client/facade.go`
- Create: `client/facade_fallback.go`
- Create: `client/facade_fallback_test.go`

**Interfaces:**
- Produces: `HTTPAttemptError`, `HTTPFallbackError`, internal `fallbackClass`, request preparation helpers, H1/H2 and H3 fallback classifiers.
- Consumes: existing `HTTP3Error` wrappers and standard library errors.

- [ ] **Step 1: Write failing replay-policy tests**

Cover the following table with stub RoundTrippers that record attempts:

```text
GET nil body                      => may fallback
GET body + GetBody               => may fallback
GET body without GetBody         => no second attempt
HEAD/OPTIONS/TRACE nil body       => may fallback
POST/PUT/PATCH/DELETE + GetBody   => no second attempt
canceled context                 => no second attempt
```

Assert that each attempt receives an independent request pointer and header map, the original request is unchanged, and attempt 1+ gets a fresh body from `GetBody`.

- [ ] **Step 2: Run replay tests and verify RED**

```bash
go test ./client -run 'TestFacadeReplay|TestFacadeRequestClone|TestFacadeCancellation'
```

Expected: failure because fallback loop/helpers are missing.

- [ ] **Step 3: Implement request applicability and safe replay helpers**

Implement helpers equivalent to:

```go
func protocolApplies(protocol HTTPProtocol, req *http.Request) bool
func isSafeReplayMethod(method string) bool
func requestCanReplay(req *http.Request) bool
func cloneAttemptRequest(req *http.Request, protocol HTTPProtocol, attemptIndex int) (*http.Request, error)
```

HTTP3 applies only to HTTPS. HTTP2 does not enable h2c, so skip it for plain HTTP. HTTP1 remains applicable to HTTP and HTTPS.

- [ ] **Step 4: Write failing classifier tests**

Use typed errors to verify:

```text
context canceled/deadline         => fallbackNever
x509 certificate errors           => fallbackNever
HTTP3 protocol/application error  => fallbackNever
response-header timeout           => fallbackNever
unknown error                     => fallbackNever
connection refused                => fallbackPreRequest
QUIC version/handshake failure    => fallbackPreRequest
EOF/reset before headers          => fallbackAmbiguousAfterSend
```

Do not use string matching in implementation or tests.

- [ ] **Step 5: Implement conservative classifiers**

Define:

```go
type fallbackClass uint8
const (
    fallbackNever fallbackClass = iota
    fallbackPreRequest
    fallbackAmbiguousAfterSend
)
```

Classifiers use `errors.Is`, `errors.As`, `net.OpError`, `url.Error`, `x509` typed errors, `net.Error`, existing `HTTP3Error`, and quic-go causes only behind existing internal implementation boundaries. Unknown is always `fallbackNever`.

- [ ] **Step 6: Implement ordered RoundTrip loop**

Rules:

1. Return closed sentinel before attempts when closed.
2. Filter inapplicable protocols without recording a failure.
3. `FallbackDisabled` executes only the first applicable slot.
4. `FallbackSafeReplay` proceeds only for safe replayable requests and eligible classifier results.
5. Any non-nil response is terminal success regardless of status.
6. Context cause is terminal.
7. Once a second attempt is needed, rebuild body from original `GetBody`.
8. If multiple attempts fail, return `*HTTPFallbackError` preserving ordered protocol/error pairs.

- [ ] **Step 7: Implement aggregate error behavior**

`HTTPFallbackError.Error()` names each protocol in order. `Unwrap() []error` returns contained errors so `errors.Is/As` can reach all causes. Copy attempt slices on construction or exposure so callers cannot mutate internal state.

- [ ] **Step 8: Run focused and full client tests**

```bash
go test ./client -run 'TestFacadeReplay|TestFacadeRequestClone|TestFacadeCancellation|TestFallbackClassifier|TestHTTPFallbackError'
go test ./client
go vet ./client
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add client/facade.go client/facade_fallback.go client/facade_fallback_test.go
git commit -m "client: add safe protocol fallback policy"
```

---

### Task 3: Real H1/H2/H3 ordering, failure semantics, lifecycle and race coverage

**Files:**
- Create: `client/facade_integration_test.go`
- Create: `client/facade_failure_integration_test.go`
- Modify: `client/facade.go` only for defects proven by integration tests
- Modify: `client/facade_fallback.go` only for defects proven by integration tests

**Interfaces:**
- Consumes the facade public API from Tasks 1-2.
- Produces no additional public API unless an integration failure proves a missing stable sentinel is necessary.

- [ ] **Step 1: Add strict H1/H2 ordering tests**

Create TLS test servers capable of H2. Verify `{HTTP1, HTTP2}` reaches a handler as HTTP/1.1 first and `{HTTP2, HTTP1}` reaches it as HTTP/2 first. The H1 and H2 attempts must use distinct strict transports.

- [ ] **Step 2: Add H3-to-H2 and H3-to-H2-to-H1 fallback tests**

Use deterministic localhost endpoints:

- first H3 endpoint unavailable while H2 server succeeds;
- H3 unavailable and H2 negotiation intentionally unavailable while H1 succeeds.

Assert `HTTPAttemptInfo` reports the actual network attempt order.

- [ ] **Step 3: Add terminal-failure tests**

Verify no fallback for:

- bad TLS certificate/hostname;
- HTTP 404 and 500 responses;
- H3 protocol/application failure;
- H2 response-header timeout;
- canceled request context;
- streaming body read failure after response headers.

- [ ] **Step 4: Add replay ambiguity test**

For GET, create a server/connection that closes before response headers after observing the request, then verify the next protocol may be attempted. Document in the test that duplicate safe network delivery is allowed by policy.

- [ ] **Step 5: Add lifecycle tests**

Verify `CloseIdleConnections` reaches all slots, `Close` is idempotent, new requests after close fail, and concurrent `Close` / `RoundTrip` does not race under `-race`.

- [ ] **Step 6: Run integration tests**

```bash
go test -count=1 ./client -run 'TestFacade.*Integration|TestFacadeStrictOrder|TestFacadeTerminal|TestFacadeLifecycle'
```

Expected: PASS.

- [ ] **Step 7: Run full race suite locally where available**

```bash
go test -race -count=1 ./...
```

Expected: PASS. If local environment cannot provide dependency/network prerequisites, use the repository PR CI matrix as the authoritative race gate and do not claim a local result.

- [ ] **Step 8: Commit**

```bash
git add client/facade*.go
git commit -m "client: verify HTTP protocol fallback integration"
```

---

### Task 4: Documentation, benchmarks, full repository verification and Draft PR

**Files:**
- Modify: `docs/client.md`
- Modify: `client/doc.go` if package documentation needs the facade named explicitly
- Create: `client/facade_benchmark_test.go`
- Modify: `.github/workflows/netpoll-v2.yml` only if verification diagnostics need improvement; do not weaken gates

**Interfaces:**
- Documents all public facade APIs and fallback safety semantics.

- [ ] **Step 1: Document the facade contract**

Update `docs/client.md` with examples of explicit protocol order, fallback disabled versus safe replay, proxy/H3 conflict, close semantics, and the warning that safe fallback can create multiple network attempts and is not at-most-once delivery.

- [ ] **Step 2: Add benchmark scaffolding**

Add benchmarks for:

```text
single protocol facade vs direct transport
H3 -> H2 fallback path
H2 -> H1 fallback path
```

Use local deterministic servers and `b.ReportAllocs()` where practical.

- [ ] **Step 3: Run formatting/module/vet/test checks**

```bash
gofmt -w client/*.go
git diff --exit-code
go mod tidy
git diff --exit-code -- go.mod go.sum
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
```

Expected: all PASS. If local dependency download is unavailable, record exactly which checks were not run and rely on fresh PR CI for those gates.

- [ ] **Step 4: Run benchmark smoke**

```bash
go test ./client -run '^$' -bench 'BenchmarkHTTPProtocolFacade' -benchtime=1x
```

Expected: benchmark functions execute successfully. Numbers are not acceptance thresholds.

- [ ] **Step 5: Commit docs/benchmarks**

```bash
git add docs/client.md client/doc.go client/facade_benchmark_test.go
git commit -m "docs: document explicit HTTP protocol fallback"
```

- [ ] **Step 6: Open Draft PR to master**

PR title:

```text
client: add explicit HTTP protocol facade
```

Body must summarize strict ordered slots, safe-method-only fallback, terminal downgrade conditions, no hidden learning, tests, and benchmark status. Reference #41 and #38.

- [ ] **Step 7: Require fresh CI on the final head**

Do not consider Phase 4 complete until the final-head PR run passes:

```text
Linux Go 1.25 format/module/vet/race
Linux Go 1.26 format/module/vet/race
Windows vet/full tests
macOS vet/full tests
GmSSL
Linux/Windows/Darwin/FreeBSD cross-compile matrix
```

- [ ] **Step 8: Final branch review**

Confirm:

```text
PR base is master
branch is not behind master
public API leaks no quic-go types
NewHTTPTransport remains H1/H2-only
NewHTTP3Transport remains H3-only
no Alt-Svc/origin cache exists
no string-based fallback classifier exists
unknown errors are terminal
PR remains Draft and unmerged
```
