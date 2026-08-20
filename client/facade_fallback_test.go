package client

import (
	"context"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"

	quicgo "github.com/quic-go/quic-go"
)

type stubRoundTripper struct {
	mu       sync.Mutex
	calls    int
	requests []*http.Request
	fn       func(*http.Request) (*http.Response, error)
}

func (s *stubRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	s.mu.Lock()
	s.calls++
	s.requests = append(s.requests, req)
	s.mu.Unlock()
	if s.fn != nil {
		return s.fn(req)
	}
	return nil, errors.New("stub: no response")
}

func (s *stubRoundTripper) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func retryableDialError() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
}

func response(status int) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("ok")),
	}
}

func facadeWithStubs(policy HTTPFallbackPolicy, protocols []HTTPProtocol, stubs ...*stubRoundTripper) *HTTPClientTransport {
	attempts := make([]protocolTransport, 0, len(protocols))
	for i, protocol := range protocols {
		attempts = append(attempts, protocolTransport{protocol: protocol, rt: stubs[i]})
	}
	return &HTTPClientTransport{attempts: attempts, fallback: policy}
}

func TestFacadeReplayGETNilBody(t *testing.T) {
	first := &stubRoundTripper{fn: func(*http.Request) (*http.Response, error) { return nil, retryableDialError() }}
	second := &stubRoundTripper{fn: func(*http.Request) (*http.Response, error) { return response(http.StatusOK), nil }}
	tr := facadeWithStubs(HTTPFallbackSafeReplay, []HTTPProtocol{HTTP3, HTTP2}, first, second)

	req, _ := http.NewRequest(http.MethodGet, "https://example.test/", nil)
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if first.callCount() != 1 || second.callCount() != 1 {
		t.Fatalf("calls = %d/%d, want 1/1", first.callCount(), second.callCount())
	}
}

func TestFacadeReplaySafeBodyUsesGetBodyAndIndependentHeaders(t *testing.T) {
	first := &stubRoundTripper{fn: func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		if string(body) != "payload" {
			t.Fatalf("first body = %q", body)
		}
		req.Header.Set("X-Test", "mutated")
		return nil, retryableDialError()
	}}
	second := &stubRoundTripper{fn: func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		if string(body) != "payload" {
			t.Fatalf("second body = %q", body)
		}
		if got := req.Header.Get("X-Test"); got != "original" {
			t.Fatalf("second header = %q, want original", got)
		}
		return response(http.StatusOK), nil
	}}
	tr := facadeWithStubs(HTTPFallbackSafeReplay, []HTTPProtocol{HTTP3, HTTP2}, first, second)

	req, _ := http.NewRequest(http.MethodGet, "https://example.test/", strings.NewReader("payload"))
	req.Header.Set("X-Test", "original")
	originalHeader := req.Header.Clone()
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := req.Header.Get("X-Test"); got != originalHeader.Get("X-Test") {
		t.Fatalf("original header mutated to %q", got)
	}
}

func TestFacadeReplaySafeBodyWithoutGetBodyStops(t *testing.T) {
	first := &stubRoundTripper{fn: func(*http.Request) (*http.Response, error) { return nil, retryableDialError() }}
	second := &stubRoundTripper{fn: func(*http.Request) (*http.Response, error) { return response(http.StatusOK), nil }}
	tr := facadeWithStubs(HTTPFallbackSafeReplay, []HTTPProtocol{HTTP3, HTTP2}, first, second)

	req, _ := http.NewRequest(http.MethodGet, "https://example.test/", nil)
	req.Body = io.NopCloser(strings.NewReader("stream"))
	req.GetBody = nil
	_, err := tr.RoundTrip(req)
	if err == nil {
		t.Fatal("RoundTrip error = nil")
	}
	if second.callCount() != 0 {
		t.Fatalf("second calls = %d, want 0", second.callCount())
	}
}

func TestFacadeReplayUnsafeMethodsNeverFallback(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			first := &stubRoundTripper{fn: func(*http.Request) (*http.Response, error) { return nil, retryableDialError() }}
			second := &stubRoundTripper{fn: func(*http.Request) (*http.Response, error) { return response(http.StatusOK), nil }}
			tr := facadeWithStubs(HTTPFallbackSafeReplay, []HTTPProtocol{HTTP3, HTTP2}, first, second)
			req, _ := http.NewRequest(method, "https://example.test/", strings.NewReader("payload"))
			_, _ = tr.RoundTrip(req)
			if second.callCount() != 0 {
				t.Fatalf("second calls = %d, want 0", second.callCount())
			}
		})
	}
}

func TestFacadeFallbackDisabledStopsAfterFirstAttempt(t *testing.T) {
	first := &stubRoundTripper{fn: func(*http.Request) (*http.Response, error) { return nil, retryableDialError() }}
	second := &stubRoundTripper{fn: func(*http.Request) (*http.Response, error) { return response(http.StatusOK), nil }}
	tr := facadeWithStubs(HTTPFallbackDisabled, []HTTPProtocol{HTTP3, HTTP2}, first, second)
	req, _ := http.NewRequest(http.MethodGet, "https://example.test/", nil)
	_, _ = tr.RoundTrip(req)
	if second.callCount() != 0 {
		t.Fatalf("second calls = %d, want 0", second.callCount())
	}
}

func TestFacadeCancellationStopsFallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	first := &stubRoundTripper{fn: func(*http.Request) (*http.Response, error) {
		cancel()
		return nil, context.Canceled
	}}
	second := &stubRoundTripper{fn: func(*http.Request) (*http.Response, error) { return response(http.StatusOK), nil }}
	tr := facadeWithStubs(HTTPFallbackSafeReplay, []HTTPProtocol{HTTP3, HTTP2}, first, second)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.test/", nil)
	_, err := tr.RoundTrip(req)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RoundTrip error = %v, want context.Canceled", err)
	}
	if second.callCount() != 0 {
		t.Fatalf("second calls = %d, want 0", second.callCount())
	}
}

func TestFacadeSkipsInapplicableProtocolsWithoutAttempt(t *testing.T) {
	h3 := &stubRoundTripper{fn: func(*http.Request) (*http.Response, error) { return nil, errors.New("must not run") }}
	h1 := &stubRoundTripper{fn: func(req *http.Request) (*http.Response, error) {
		info, ok := HTTPAttemptFromContext(req.Context())
		if !ok || info.Protocol != HTTP1 || info.Index != 0 {
			t.Fatalf("attempt info = %+v, %v", info, ok)
		}
		return response(http.StatusOK), nil
	}}
	tr := facadeWithStubs(HTTPFallbackSafeReplay, []HTTPProtocol{HTTP3, HTTP1}, h3, h1)
	req, _ := http.NewRequest(http.MethodGet, "http://example.test/", nil)
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if h3.callCount() != 0 || h1.callCount() != 1 {
		t.Fatalf("calls = %d/%d, want 0/1", h3.callCount(), h1.callCount())
	}
}

func TestFacadeAnyResponseIsTerminal(t *testing.T) {
	first := &stubRoundTripper{fn: func(*http.Request) (*http.Response, error) { return response(http.StatusServiceUnavailable), nil }}
	second := &stubRoundTripper{fn: func(*http.Request) (*http.Response, error) { return response(http.StatusOK), nil }}
	tr := facadeWithStubs(HTTPFallbackSafeReplay, []HTTPProtocol{HTTP3, HTTP2}, first, second)
	req, _ := http.NewRequest(http.MethodGet, "https://example.test/", nil)
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || second.callCount() != 0 {
		t.Fatalf("status/calls = %d/%d", resp.StatusCode, second.callCount())
	}
}

func TestFallbackClassifier(t *testing.T) {
	cert := &x509.Certificate{Subject: pkix.Name{CommonName: "example.test"}}
	tests := []struct {
		name string
		err  error
		want fallbackClass
	}{
		{"context canceled", context.Canceled, fallbackNever},
		{"deadline", context.DeadlineExceeded, fallbackNever},
		{"certificate", x509.UnknownAuthorityError{Cert: cert}, fallbackNever},
		{"unknown", errors.New("opaque"), fallbackNever},
		{"dial", retryableDialError(), fallbackPreRequest},
		{"dns", &net.DNSError{Name: "example.test", Err: "not found"}, fallbackPreRequest},
		{"eof", io.EOF, fallbackAmbiguousAfterSend},
		{"read failure", &net.OpError{Op: "read", Net: "tcp", Err: io.ErrUnexpectedEOF}, fallbackAmbiguousAfterSend},
		{"quic version", &HTTP3Error{Kind: HTTP3ErrorTransport, Cause: &quicgo.VersionNegotiationError{}}, fallbackPreRequest},
		{"quic handshake", &HTTP3Error{Kind: HTTP3ErrorTransport, Cause: &quicgo.HandshakeTimeoutError{}}, fallbackPreRequest},
		{"h3 protocol", &HTTP3Error{Kind: HTTP3ErrorProtocol, Cause: errors.New("protocol")}, fallbackNever},
		{"h3 application", &HTTP3Error{Kind: HTTP3ErrorTransport, Cause: &quicgo.ApplicationError{}}, fallbackNever},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyFallback(tt.err); got != tt.want {
				t.Fatalf("classifyFallback(%T) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestHTTPFallbackErrorPreservesAttemptOrderAndCauses(t *testing.T) {
	errH3 := retryableDialError()
	errH2 := io.EOF
	errH1 := io.ErrUnexpectedEOF
	err := newHTTPFallbackError([]HTTPAttemptError{
		{Protocol: HTTP3, Err: errH3},
		{Protocol: HTTP2, Err: errH2},
		{Protocol: HTTP1, Err: errH1},
	})
	var fallback *HTTPFallbackError
	if !errors.As(err, &fallback) {
		t.Fatalf("error type = %T", err)
	}
	if len(fallback.Attempts) != 3 {
		t.Fatalf("attempts = %d", len(fallback.Attempts))
	}
	for i, want := range []HTTPProtocol{HTTP3, HTTP2, HTTP1} {
		if fallback.Attempts[i].Protocol != want {
			t.Fatalf("attempt[%d] = %v, want %v", i, fallback.Attempts[i].Protocol, want)
		}
	}
	if !errors.Is(err, errH2) || !errors.Is(err, errH1) {
		t.Fatalf("aggregate does not expose causes: %v", err)
	}
}
