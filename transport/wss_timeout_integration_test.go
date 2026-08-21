package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestWSSSlowTLSHandshakeIsTyped(t *testing.T) {
	endpoint := startStalledWSEndpoint(t)
	endpoint.Scheme = ogrenet.SchemeWSS
	_, clientTLS := testTLSConfigs(t)
	client, err := New(
		WithTLSClientConfig(clientTLS),
		WithTimeouts(Timeouts{Handshake: 40 * time.Millisecond}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	_, err = client.Dial(ctx, endpoint, ogrenet.HandlerFuncs{})
	var te *TimeoutError
	if !errors.As(err, &te) || te.Kind != TimeoutHandshake {
		t.Fatalf("WSS Dial error = %#v, want TimeoutHandshake", err)
	}
}
