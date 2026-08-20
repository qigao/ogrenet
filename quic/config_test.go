package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"
)

func TestConfigValidation(t *testing.T) {
	if _, _, err := (Config{}).build(); !errors.Is(err, ErrALPNRequired) {
		t.Fatalf("empty ALPN: %v", err)
	}
	if _, _, err := (Config{ALPN: "echo", HandshakeTimeout: -time.Second}).build(); !errors.Is(err, ErrInvalidTimeout) {
		t.Fatalf("negative timeout: %v", err)
	}
	if _, _, err := (Config{ALPN: "echo", TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}).build(); !errors.Is(err, ErrTLSVersion) {
		t.Fatalf("TLS 1.2 minimum: %v", err)
	}
}

func TestConfigBuildClonesTLSAndPinsALPN(t *testing.T) {
	original := &tls.Config{NextProtos: []string{"caller-value"}}
	tlsConfig, quicConfig, err := (Config{ALPN: "ogrenet-echo", TLSConfig: original}).build()
	if err != nil {
		t.Fatal(err)
	}
	if tlsConfig == original {
		t.Fatal("TLS config was not cloned")
	}
	if got := tlsConfig.NextProtos; len(got) != 1 || got[0] != "ogrenet-echo" {
		t.Fatalf("NextProtos = %v", got)
	}
	if tlsConfig.MinVersion != tls.VersionTLS13 {
		t.Fatalf("MinVersion = %d", tlsConfig.MinVersion)
	}
	if quicConfig.EnableDatagrams {
		t.Fatal("datagrams must be disabled in the first minimal API")
	}
	if quicConfig.HandshakeIdleTimeout != defaultHandshakeTimeout {
		t.Fatalf("HandshakeIdleTimeout = %v", quicConfig.HandshakeIdleTimeout)
	}
	if quicConfig.MaxIdleTimeout != defaultIdleTimeout {
		t.Fatalf("MaxIdleTimeout = %v", quicConfig.MaxIdleTimeout)
	}
}

func TestDialRejectsInvalidInputBeforeNetwork(t *testing.T) {
	if _, err := Dial(nil, "127.0.0.1:443", Config{ALPN: "echo"}); !errors.Is(err, ErrNilContext) {
		t.Fatalf("nil context: %v", err)
	}
	if _, err := Dial(context.Background(), "", Config{ALPN: "echo"}); !errors.Is(err, ErrEmptyAddress) {
		t.Fatalf("empty address: %v", err)
	}
}
