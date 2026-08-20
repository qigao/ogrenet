package client

import (
	"errors"
	"net/http"
	"testing"
)

func TestHTTPProtocolStringIncludesHTTP3(t *testing.T) {
	if got := HTTP3.String(); got != "h3" {
		t.Fatalf("HTTP3.String() = %q, want h3", got)
	}
}

func TestHTTPTransportStillRejectsHTTP3(t *testing.T) {
	if _, err := NewHTTPTransport(HTTPConfig{Protocols: []HTTPProtocol{HTTP3}}); !errors.Is(err, ErrInvalidHTTPProtocol) {
		t.Fatalf("NewHTTPTransport(HTTP3) error = %v, want ErrInvalidHTTPProtocol", err)
	}
}

func TestHTTPClientConfigRequiresExplicitProtocols(t *testing.T) {
	if _, err := NewHTTPRoundTripper(HTTPClientConfig{}); !errors.Is(err, ErrInvalidHTTPClientConfig) {
		t.Fatalf("NewHTTPRoundTripper error = %v, want ErrInvalidHTTPClientConfig", err)
	}
}

func TestHTTPClientConfigRejectsDuplicateProtocols(t *testing.T) {
	if _, err := NewHTTPRoundTripper(HTTPClientConfig{Protocols: []HTTPProtocol{HTTP2, HTTP2}}); !errors.Is(err, ErrInvalidHTTPClientConfig) {
		t.Fatalf("NewHTTPRoundTripper error = %v, want ErrInvalidHTTPClientConfig", err)
	}
}

func TestHTTPClientConfigRejectsNestedHTTPProtocols(t *testing.T) {
	_, err := NewHTTPRoundTripper(HTTPClientConfig{
		Protocols: []HTTPProtocol{HTTP2},
		HTTP:      HTTPConfig{Protocols: []HTTPProtocol{HTTP2}},
	})
	if !errors.Is(err, ErrInvalidHTTPClientConfig) {
		t.Fatalf("NewHTTPRoundTripper error = %v, want ErrInvalidHTTPClientConfig", err)
	}
}

func TestHTTPClientConfigRejectsHTTP3WithProxy(t *testing.T) {
	_, err := NewHTTPRoundTripper(HTTPClientConfig{
		Protocols: []HTTPProtocol{HTTP3, HTTP2},
		HTTP:      HTTPConfig{Proxy: http.ProxyFromEnvironment},
	})
	if !errors.Is(err, ErrInvalidHTTPClientConfig) {
		t.Fatalf("NewHTTPRoundTripper error = %v, want ErrInvalidHTTPClientConfig", err)
	}
}

func TestHTTPClientConfigRejectsUnknownFallbackPolicy(t *testing.T) {
	_, err := NewHTTPRoundTripper(HTTPClientConfig{
		Protocols: []HTTPProtocol{HTTP1},
		Fallback:  HTTPFallbackPolicy(255),
	})
	if !errors.Is(err, ErrInvalidHTTPClientConfig) {
		t.Fatalf("NewHTTPRoundTripper error = %v, want ErrInvalidHTTPClientConfig", err)
	}
}

func TestHTTPClientTransportBuildsStrictOrderedSlots(t *testing.T) {
	tr, err := NewHTTPRoundTripper(HTTPClientConfig{
		Protocols: []HTTPProtocol{HTTP3, HTTP2, HTTP1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	if len(tr.attempts) != 3 {
		t.Fatalf("attempt count = %d, want 3", len(tr.attempts))
	}
	for i, want := range []HTTPProtocol{HTTP3, HTTP2, HTTP1} {
		if got := tr.attempts[i].protocol; got != want {
			t.Fatalf("attempt[%d].protocol = %v, want %v", i, got, want)
		}
	}
}

func TestNewClientLeavesWholeRequestTimeoutUnset(t *testing.T) {
	c, err := NewClient(HTTPClientConfig{Protocols: []HTTPProtocol{HTTP1}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Transport.(*HTTPClientTransport).Close()
	if c.Timeout != 0 {
		t.Fatalf("Client.Timeout = %v, want 0", c.Timeout)
	}
}

func TestHTTPClientTransportCloseIsIdempotentAndRejectsNewRequests(t *testing.T) {
	tr, err := NewHTTPRoundTripper(HTTPClientConfig{Protocols: []HTTPProtocol{HTTP1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}

	req, err := http.NewRequest(http.MethodGet, "https://example.test/", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.RoundTrip(req); !errors.Is(err, ErrHTTPClientTransportClosed) {
		t.Fatalf("RoundTrip after Close error = %v, want ErrHTTPClientTransportClosed", err)
	}
}
