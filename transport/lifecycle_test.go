package transport

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestUnsupportedDatagramNetwork(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	if _, err := e.Listen(context.Background(), "udp", "127.0.0.1:0", nil); !errors.Is(err, ErrUnsupportedNet) {
		t.Fatalf("got %v, want ErrUnsupportedNet", err)
	}
}

func TestListenerContextCancellation(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	listener, err := e.Listen(ctx, "tcp", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	cancel()

	select {
	case <-listener.Done():
	case <-time.After(time.Second):
		t.Fatal("listener did not close after context cancellation")
	}
	if !errors.Is(listener.Err(), context.Canceled) {
		t.Fatalf("got listener error %v, want context.Canceled", listener.Err())
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	listener, err := e.Listen(context.Background(), "tcp", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
}
