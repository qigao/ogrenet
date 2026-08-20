package client

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type attemptSequence struct {
	mu        sync.Mutex
	protocols []HTTPProtocol
}

func (s *attemptSequence) add(protocol HTTPProtocol) {
	s.mu.Lock()
	s.protocols = append(s.protocols, protocol)
	s.mu.Unlock()
}

func (s *attemptSequence) snapshot() []HTTPProtocol {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]HTTPProtocol(nil), s.protocols...)
}

type observingRoundTripper struct {
	protocol HTTPProtocol
	next     http.RoundTripper
	sequence *attemptSequence
}

func (r *observingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	info, ok := HTTPAttemptFromContext(req.Context())
	if !ok || info.Protocol != r.protocol {
		return nil, errors.New("client test: missing or incorrect attempt metadata")
	}
	r.sequence.add(r.protocol)
	return r.next.RoundTrip(req)
}

func (r *observingRoundTripper) CloseIdleConnections() {
	if closer, ok := r.next.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

func (r *observingRoundTripper) Close() error {
	if closer, ok := r.next.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func observeFacadeAttempts(tr *HTTPClientTransport, sequence *attemptSequence) {
	for i := range tr.attempts {
		original := tr.attempts[i].rt
		tr.attempts[i].rt = &observingRoundTripper{
			protocol: tr.attempts[i].protocol,
			next:     original,
			sequence: sequence,
		}
	}
}

func startHTTP12TLSServer(t *testing.T, enableHTTP2 bool, handler http.Handler) (*httptest.Server, *tls.Config) {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = enableHTTP2
	server.StartTLS()
	t.Cleanup(server.Close)

	base := server.Client().Transport.(*http.Transport).TLSClientConfig
	return server, base.Clone()
}

func TestFacadeStrictOrderHTTP1BeforeHTTP2(t *testing.T) {
	server, tlsCfg := startHTTP12TLSServer(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Proto", r.Proto)
		w.WriteHeader(http.StatusNoContent)
	}))
	tr, err := NewHTTPRoundTripper(HTTPClientConfig{
		Protocols: []HTTPProtocol{HTTP1, HTTP2},
		HTTP:      HTTPConfig{TLSConfig: tlsCfg},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	resp, err := (&http.Client{Transport: tr}).Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 1 {
		t.Fatalf("protocol = %s, want HTTP/1.x", resp.Proto)
	}
}

func TestFacadeStrictOrderHTTP2BeforeHTTP1(t *testing.T) {
	server, tlsCfg := startHTTP12TLSServer(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	tr, err := NewHTTPRoundTripper(HTTPClientConfig{
		Protocols: []HTTPProtocol{HTTP2, HTTP1},
		HTTP:      HTTPConfig{TLSConfig: tlsCfg},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	resp, err := (&http.Client{Transport: tr}).Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("protocol = %s, want HTTP/2", resp.Proto)
	}
}

func TestFacadeHTTP3UnavailableFallsBackToHTTP2(t *testing.T) {
	server, tlsCfg := startHTTP12TLSServer(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	tr, err := NewHTTPRoundTripper(HTTPClientConfig{
		Protocols: []HTTPProtocol{HTTP3, HTTP2},
		Fallback:  HTTPFallbackSafeReplay,
		HTTP:      HTTPConfig{TLSConfig: tlsCfg},
		HTTP3: HTTP3Config{
			TLSConfig:        tlsCfg,
			HandshakeTimeout: 150 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	sequence := &attemptSequence{}
	observeFacadeAttempts(tr, sequence)

	resp, err := (&http.Client{Transport: tr}).Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("protocol = %s, want HTTP/2", resp.Proto)
	}
	want := []HTTPProtocol{HTTP3, HTTP2}
	if got := sequence.snapshot(); !equalProtocols(got, want) {
		t.Fatalf("attempts = %v, want %v", got, want)
	}
}

func TestFacadeHTTP3ThenHTTP2UnavailableFallsBackToHTTP1(t *testing.T) {
	server, tlsCfg := startHTTP12TLSServer(t, false, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	tr, err := NewHTTPRoundTripper(HTTPClientConfig{
		Protocols: []HTTPProtocol{HTTP3, HTTP2, HTTP1},
		Fallback:  HTTPFallbackSafeReplay,
		HTTP:      HTTPConfig{TLSConfig: tlsCfg},
		HTTP3: HTTP3Config{
			TLSConfig:        tlsCfg,
			HandshakeTimeout: 150 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	sequence := &attemptSequence{}
	observeFacadeAttempts(tr, sequence)

	resp, err := (&http.Client{Transport: tr}).Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 1 {
		t.Fatalf("protocol = %s, want HTTP/1.x", resp.Proto)
	}
	want := []HTTPProtocol{HTTP3, HTTP2, HTTP1}
	if got := sequence.snapshot(); !equalProtocols(got, want) {
		t.Fatalf("attempts = %v, want %v", got, want)
	}
}

func TestFacadeTLSIdentityFailureNeverFallsBack(t *testing.T) {
	server, _ := startHTTP12TLSServer(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	tr, err := NewHTTPRoundTripper(HTTPClientConfig{
		Protocols: []HTTPProtocol{HTTP2, HTTP1},
		Fallback:  HTTPFallbackSafeReplay,
		HTTP: HTTPConfig{TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS13,
			ServerName: "localhost",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	sequence := &attemptSequence{}
	observeFacadeAttempts(tr, sequence)

	_, err = (&http.Client{Transport: tr}).Get(server.URL)
	if err == nil {
		t.Fatal("request unexpectedly succeeded")
	}
	if got := sequence.snapshot(); !equalProtocols(got, []HTTPProtocol{HTTP2}) {
		t.Fatalf("attempts = %v, want [h2]", got)
	}
}

func TestFacadeResponseHeaderTimeoutNeverFallsBack(t *testing.T) {
	server, tlsCfg := startHTTP12TLSServer(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	}))
	tr, err := NewHTTPRoundTripper(HTTPClientConfig{
		Protocols: []HTTPProtocol{HTTP1, HTTP2},
		Fallback:  HTTPFallbackSafeReplay,
		HTTP: HTTPConfig{
			TLSConfig:             tlsCfg,
			ResponseHeaderTimeout: 25 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	sequence := &attemptSequence{}
	observeFacadeAttempts(tr, sequence)

	_, err = (&http.Client{Transport: tr}).Get(server.URL)
	if err == nil {
		t.Fatal("request unexpectedly succeeded")
	}
	if got := sequence.snapshot(); !equalProtocols(got, []HTTPProtocol{HTTP1}) {
		t.Fatalf("attempts = %v, want [http/1.1]", got)
	}
}

func TestFacadeHTTPStatusNeverFallsBack(t *testing.T) {
	server, tlsCfg := startHTTP12TLSServer(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	tr, err := NewHTTPRoundTripper(HTTPClientConfig{
		Protocols: []HTTPProtocol{HTTP2, HTTP1},
		Fallback:  HTTPFallbackSafeReplay,
		HTTP:      HTTPConfig{TLSConfig: tlsCfg},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	sequence := &attemptSequence{}
	observeFacadeAttempts(tr, sequence)

	resp, err := (&http.Client{Transport: tr}).Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got := sequence.snapshot(); !equalProtocols(got, []HTTPProtocol{HTTP2}) {
		t.Fatalf("attempts = %v, want [h2]", got)
	}
}

func equalProtocols(got, want []HTTPProtocol) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

type lifecycleRoundTripper struct {
	idleCalls  atomic.Int32
	closeCalls atomic.Int32
}

func (r *lifecycleRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	select {
	case <-req.Context().Done():
		return nil, req.Context().Err()
	default:
		return response(http.StatusNoContent), nil
	}
}

func (r *lifecycleRoundTripper) CloseIdleConnections() { r.idleCalls.Add(1) }
func (r *lifecycleRoundTripper) Close() error {
	r.closeCalls.Add(1)
	return nil
}

func TestFacadeLifecycleBroadcastAndCloseRace(t *testing.T) {
	first := &lifecycleRoundTripper{}
	second := &lifecycleRoundTripper{}
	tr := &HTTPClientTransport{
		attempts: []protocolTransport{
			{protocol: HTTP2, rt: first},
			{protocol: HTTP1, rt: second},
		},
		fallback: HTTPFallbackSafeReplay,
	}

	tr.CloseIdleConnections()
	if first.idleCalls.Load() != 1 || second.idleCalls.Load() != 1 {
		t.Fatalf("idle calls = %d/%d", first.idleCalls.Load(), second.idleCalls.Load())
	}

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.test/", nil)
			resp, _ := tr.RoundTrip(req)
			if resp != nil {
				_ = resp.Body.Close()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = tr.Close()
	}()
	wg.Wait()
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if first.closeCalls.Load() != 1 || second.closeCalls.Load() != 1 {
		t.Fatalf("close calls = %d/%d, want 1/1", first.closeCalls.Load(), second.closeCalls.Load())
	}
}
