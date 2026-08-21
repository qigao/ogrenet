package transport_test

import (
	"context"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/transport"
)

type contractProfile struct {
	TCP bool
	UDP bool
}

type engineFactory struct {
	name    string
	profile contractProfile
	new     func(t *testing.T, opts ...transport.Option) ogrenet.Engine
}

func portableFactory() engineFactory {
	return engineFactory{
		name:    "portable",
		profile: contractProfile{TCP: true, UDP: true},
		new: func(t *testing.T, opts ...transport.Option) ogrenet.Engine {
			t.Helper()
			e, err := transport.New(opts...)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			return e
		},
	}
}

func runEngineContracts(t *testing.T, f engineFactory) {
	if f.profile.TCP {
		t.Run(f.name+"/tcp", func(t *testing.T) { runTCPContract(t, f) })
	}
	if f.profile.UDP {
		t.Run(f.name+"/udp", func(t *testing.T) { runUDPContract(t, f) })
	}
}

func TestEnginePublicContracts(t *testing.T) {
	runEngineContracts(t, portableFactory())
}

func contractContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func waitContractDone(t *testing.T, ctx context.Context, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", what, context.Cause(ctx))
	}
}

func recvContract[T any](t *testing.T, ctx context.Context, ch <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", what, context.Cause(ctx))
		var zero T
		return zero
	}
}
