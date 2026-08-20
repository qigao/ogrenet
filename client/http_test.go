package client

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPConfigDefaultsAreBounded(t *testing.T) {
	transport, err := NewHTTPTransport(HTTPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.CloseIdleConnections()

	if transport.Proxy != nil {
		t.Fatal("default transport must not inherit environment proxy behavior")
	}
	if transport.MaxConnsPerHost <= 0 || transport.MaxIdleConns <= 0 || transport.MaxIdleConnsPerHost <= 0 {
		t.Fatalf("connection pool is not bounded: %+v", transport)
	}
	if transport.TLSHandshakeTimeout <= 0 || transport.ResponseHeaderTimeout <= 0 || transport.IdleConnTimeout <= 0 {
		t.Fatal("timeouts must have non-zero defaults")
	}
	if transport.MaxResponseHeaderBytes <= 0 {
		t.Fatal("response headers must be bounded")
	}
	if transport.TLSClientConfig == nil || transport.TLSClientConfig.MinVersion != tls.VersionTLS13 {
		t.Fatalf("TLS min version = %#v, want TLS 1.3", transport.TLSClientConfig)
	}
}

func TestHTTPConfigValidation(t *testing.T) {
	cases := []HTTPConfig{
		{ConnectTimeout: -time.Second},
		{TLSHandshakeTimeout: -time.Second},
		{ResponseHeaderTimeout: -time.Second},
		{ExpectContinueTimeout: -time.Second},
		{IdleConnTimeout: -time.Second},
		{KeepAlive: -time.Second},
		{MaxIdleConns: -1},
		{MaxIdleConnsPerHost: -1},
		{MaxConnsPerHost: -1},
		{MaxResponseHeaderBytes: -1},
	}
	for _, cfg := range cases {
		if _, err := NewHTTPTransport(cfg); !errors.Is(err, ErrInvalidHTTPConfig) {
			t.Fatalf("NewHTTPTransport(%+v) = %v, want ErrInvalidHTTPConfig", cfg, err)
		}
	}
	if _, err := NewHTTPTransport(HTTPConfig{Protocols: []HTTPProtocol{99}}); !errors.Is(err, ErrInvalidHTTPProtocol) {
		t.Fatalf("invalid protocol = %v, want ErrInvalidHTTPProtocol", err)
	}
	if _, err := NewHTTPTransport(HTTPConfig{TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}); !errors.Is(err, ErrHTTPSTLSVersion) {
		t.Fatalf("TLS 1.2 config = %v, want ErrHTTPSTLSVersion", err)
	}
}

func TestHTTP1Loopback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Protocol", r.Proto)
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()

	client, err := NewHTTPClient(HTTPConfig{Protocols: []HTTPProtocol{HTTP1}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Transport.(*http.Transport).CloseIdleConnections()

	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 1 {
		t.Fatalf("protocol = %s, want HTTP/1.x", resp.Proto)
	}
}

func TestRequestContextCancellation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()

	client, err := NewHTTPClient(HTTPConfig{Protocols: []HTTPProtocol{HTTP1}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Transport.(*http.Transport).CloseIdleConnections()

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := client.Do(req)
		result <- err
	}()
	<-started
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("request error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("request did not cancel promptly")
	}
}

func TestResponseHeaderTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewHTTPClient(HTTPConfig{
		Protocols:             []HTTPProtocol{HTTP1},
		ResponseHeaderTimeout: 30 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Transport.(*http.Transport).CloseIdleConnections()

	_, err = client.Get(server.URL)
	if err == nil {
		t.Fatal("request unexpectedly succeeded")
	}
	var netErr interface{ Timeout() bool }
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("request error = %v, want timeout", err)
	}
}

func TestMaxConnsPerHostBoundsConcurrency(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewHTTPClient(HTTPConfig{
		Protocols:       []HTTPProtocol{HTTP1},
		MaxConnsPerHost: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Transport.(*http.Transport).CloseIdleConnections()

	var completed atomic.Int32
	do := func() {
		resp, err := client.Get(server.URL)
		if err == nil {
			resp.Body.Close()
			completed.Add(1)
		}
	}
	go do()
	<-entered
	go do()
	select {
	case <-entered:
		t.Fatal("second request reached server while MaxConnsPerHost=1")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for completed.Load() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := completed.Load(); got != 2 {
		t.Fatalf("completed requests = %d, want 2", got)
	}
}

func TestExplicitProxyOnly(t *testing.T) {
	transport, err := NewHTTPTransport(HTTPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if transport.Proxy != nil {
		t.Fatal("nil HTTPConfig.Proxy must produce a direct transport")
	}

	transport, err = NewHTTPTransport(HTTPConfig{Proxy: http.ProxyFromEnvironment})
	if err != nil {
		t.Fatal(err)
	}
	if transport.Proxy == nil {
		t.Fatal("explicit proxy function was not preserved")
	}
}
