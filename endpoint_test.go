package ogrenet

import (
	"errors"
	"testing"
)

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		raw  string
		want Endpoint
	}{
		{"tcp://127.0.0.1:9000", Endpoint{Scheme: SchemeTCP, Host: "127.0.0.1", Port: 9000}},
		{"udp://:9001", Endpoint{Scheme: SchemeUDP, Port: 9001}},
		{"tls://example.com", Endpoint{Scheme: SchemeTLS, Host: "example.com", Port: 443}},
		{"ws://example.com/chat", Endpoint{Scheme: SchemeWS, Host: "example.com", Port: 80, Path: "/chat"}},
		{"wss://example.com/realtime?token=x", Endpoint{Scheme: SchemeWSS, Host: "example.com", Port: 443, Path: "/realtime", RawQuery: "token=x"}},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := ParseEndpoint(tt.raw)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestEndpointProductionValidation(t *testing.T) {
	if _, err := ParseEndpoint("tcp://127.0.0.1"); !errors.Is(err, ErrMissingPort) {
		t.Fatalf("tcp without port = %v, want ErrMissingPort", err)
	}
	if _, err := ParseEndpoint("tcp://127.0.0.1:1/path"); !errors.Is(err, ErrUnexpectedPath) {
		t.Fatalf("tcp path = %v, want ErrUnexpectedPath", err)
	}
	if _, err := ParseEndpoint("ftp://example.com:21"); !errors.Is(err, ErrUnsupportedScheme) {
		t.Fatalf("unsupported scheme = %v, want ErrUnsupportedScheme", err)
	}
	if err := (Endpoint{Scheme: SchemeTCP, Host: "127.0.0.1", Port: 0}).ValidateDial(); !errors.Is(err, ErrMissingPort) {
		t.Fatalf("dial port zero = %v, want ErrMissingPort", err)
	}
	if err := (Endpoint{Scheme: SchemeUDP, Port: 9000}).ValidateDial(); !errors.Is(err, ErrMissingHost) {
		t.Fatalf("dial wildcard host = %v, want ErrMissingHost", err)
	}
}

func TestSchemeIDsStable(t *testing.T) {
	want := map[Scheme]uint8{
		SchemeTCP: 0x01,
		SchemeUDP: 0x02,
		SchemeTLS: 0x03,
		SchemeWS:  0x04,
		SchemeWSS: 0x05,
	}
	for scheme, id := range want {
		if got := uint8(scheme); got != id {
			t.Fatalf("scheme %s = %#x, want %#x", scheme, got, id)
		}
	}
}
