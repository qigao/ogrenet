package client

import (
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
)

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
		t.Fatalf("peer bidi = %d", got.quicConfig.MaxIncomingStreams)
	}
	if got.quicConfig.MaxIncomingUniStreams != 16 {
		t.Fatalf("peer uni = %d", got.quicConfig.MaxIncomingUniStreams)
	}
	if got.quicConfig.EnableDatagrams || got.quicConfig.Allow0RTT {
		t.Fatalf("datagrams=%v Allow0RTT=%v", got.quicConfig.EnableDatagrams, got.quicConfig.Allow0RTT)
	}
	if got.maxResponseHeaderBytes != 1<<20 {
		t.Fatalf("header bound = %d", got.maxResponseHeaderBytes)
	}
}

func TestHTTP3ConfigValidation(t *testing.T) {
	cases := []struct {
		cfg  HTTP3Config
		want error
	}{
		{HTTP3Config{HandshakeTimeout: -time.Second}, ErrInvalidHTTP3Config},
		{HTTP3Config{IdleTimeout: -time.Second}, ErrInvalidHTTP3Config},
		{HTTP3Config{MaxResponseHeaderBytes: -1}, ErrInvalidHTTP3Config},
		{HTTP3Config{TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}, ErrHTTP3TLSVersion},
		{HTTP3Config{TLSConfig: &tls.Config{MaxVersion: tls.VersionTLS12}}, ErrHTTP3TLSVersion},
	}
	for _, tc := range cases {
		if _, err := normalizeHTTP3Config(tc.cfg); !errors.Is(err, tc.want) {
			t.Fatalf("normalize(%+v)=%v, want %v", tc.cfg, err, tc.want)
		}
	}
}

func TestHTTP3DatagramPolicy(t *testing.T) {
	got, err := normalizeHTTP3Config(HTTP3Config{EnableDatagrams: true})
	if err != nil {
		t.Fatal(err)
	}
	if !got.enableDatagrams || !got.quicConfig.EnableDatagrams {
		t.Fatalf("H3=%v QUIC=%v", got.enableDatagrams, got.quicConfig.EnableDatagrams)
	}
}

func TestHTTP3TLSConfigCloned(t *testing.T) {
	original := &tls.Config{NextProtos: []string{"caller"}}
	got, err := normalizeHTTP3Config(HTTP3Config{TLSConfig: original})
	if err != nil {
		t.Fatal(err)
	}
	if got.tlsConfig == original {
		t.Fatal("TLS config was not cloned")
	}
	if len(original.NextProtos) != 1 || original.NextProtos[0] != "caller" {
		t.Fatalf("caller TLS mutated: %v", original.NextProtos)
	}
}

func TestHTTP3MaxResponseHeaderBytesFitsInt(t *testing.T) {
	maxInt := uint64(^uint(0) >> 1)
	if maxInt < uint64(^uint32(0)>>1) {
		t.Fatalf("unexpected int width")
	}
	if maxInt < uint64(^uint64(0)>>1) {
		cfg := HTTP3Config{MaxResponseHeaderBytes: int64(maxInt + 1)}
		if _, err := normalizeHTTP3Config(cfg); !errors.Is(err, ErrInvalidHTTP3Config) {
			t.Fatalf("overflow = %v", err)
		}
	}
}
