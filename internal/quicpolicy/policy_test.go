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
	if err != nil {
		t.Fatal(err)
	}
	if tlsCfg == original {
		t.Fatal("TLS config was not cloned")
	}
	if tlsCfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("MinVersion = %d", tlsCfg.MinVersion)
	}
	if len(tlsCfg.NextProtos) != 1 || tlsCfg.NextProtos[0] != "ogrenet-test" {
		t.Fatalf("NextProtos = %v", tlsCfg.NextProtos)
	}
	if !qcfg.EnableDatagrams || qcfg.Allow0RTT {
		t.Fatalf("datagrams=%v Allow0RTT=%v", qcfg.EnableDatagrams, qcfg.Allow0RTT)
	}
	if qcfg.MaxIncomingStreams != 32 || qcfg.MaxIncomingUniStreams != -1 {
		t.Fatalf("limits = %d/%d", qcfg.MaxIncomingStreams, qcfg.MaxIncomingUniStreams)
	}
}

func TestBuildValidation(t *testing.T) {
	if _, _, err := Build(Config{}); !errors.Is(err, ErrALPNRequired) {
		t.Fatalf("empty ALPN: %v", err)
	}
	if _, _, err := Build(Config{ALPN: "x", HandshakeTimeout: -time.Second}); !errors.Is(err, ErrInvalidTimeout) {
		t.Fatalf("timeout: %v", err)
	}
	if _, _, err := Build(Config{ALPN: "x", TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}); !errors.Is(err, ErrTLSVersion) {
		t.Fatalf("TLS: %v", err)
	}
}
