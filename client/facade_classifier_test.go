package client

import (
	"testing"

	quicgo "github.com/quic-go/quic-go"
)

func TestFallbackClassifierRejectsQUICTransportProtocolError(t *testing.T) {
	err := &HTTP3Error{Kind: HTTP3ErrorTransport, Cause: &quicgo.TransportError{}}
	if got := classifyFallback(err); got != fallbackNever {
		t.Fatalf("classifyFallback(QUIC TransportError) = %v, want fallbackNever", got)
	}
}
