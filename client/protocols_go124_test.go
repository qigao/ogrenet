//go:build go1.24

package client

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTP2OnlyTLSLoopback(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Protocol", r.Proto)
		w.WriteHeader(http.StatusNoContent)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()

	serverTLS := server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	serverTLS.MinVersion = tls.VersionTLS13
	client, err := NewHTTPClient(HTTPConfig{
		Protocols: []HTTPProtocol{HTTP2},
		TLSConfig: serverTLS,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Transport.(*http.Transport).CloseIdleConnections()

	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("protocol = %s, want HTTP/2", resp.Proto)
	}
	if resp.TLS == nil || resp.TLS.Version < tls.VersionTLS13 {
		t.Fatalf("TLS state = %#v, want TLS 1.3+", resp.TLS)
	}
}

func TestHTTP2OnlyDoesNotFallbackToHTTP1(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.EnableHTTP2 = false
	server.StartTLS()
	defer server.Close()

	serverTLS := server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	serverTLS.MinVersion = tls.VersionTLS13
	client, err := NewHTTPClient(HTTPConfig{
		Protocols: []HTTPProtocol{HTTP2},
		TLSConfig: serverTLS,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Transport.(*http.Transport).CloseIdleConnections()

	resp, err := client.Get(server.URL)
	if resp != nil {
		resp.Body.Close()
	}
	if err == nil {
		t.Fatal("HTTP/2-only client silently fell back to HTTP/1")
	}
}

func TestProtocolSetIsExplicit(t *testing.T) {
	transport, err := NewHTTPTransport(HTTPConfig{Protocols: []HTTPProtocol{HTTP1, HTTP2, HTTP1}})
	if err != nil {
		t.Fatal(err)
	}
	if transport.Protocols == nil || !transport.Protocols.HTTP1() || !transport.Protocols.HTTP2() {
		t.Fatalf("protocol set = %#v, want HTTP/1 + HTTP/2", transport.Protocols)
	}
}
