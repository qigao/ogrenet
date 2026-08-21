package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestTCPWriteTimeoutClosesSession(t *testing.T) {
	e, err := New(WithTimeouts(Timeouts{Write: 40 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	left, right := net.Pipe()
	defer right.Close()
	c, err := e.adoptStream(left, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	err = c.Send(ctx, ogrenet.Bin([]byte("blocked")))
	var te *TimeoutError
	if !errors.As(err, &te) || te.Kind != TimeoutWrite {
		t.Fatalf("Send error = %#v, want TimeoutWrite", err)
	}
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("session did not close after write timeout")
	}
	if !errors.As(c.Err(), &te) || te.Kind != TimeoutWrite {
		t.Fatalf("Session.Err = %#v, want TimeoutWrite", c.Err())
	}
}

func TestTCPReadIdleClosesSession(t *testing.T) {
	e, err := New(WithTimeouts(Timeouts{ReadIdle: 40 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	left, right := net.Pipe()
	defer right.Close()
	c, err := e.adoptStream(left, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("session did not close after read idle timeout")
	}
	var te *TimeoutError
	if !errors.As(c.Err(), &te) || te.Kind != TimeoutReadIdle {
		t.Fatalf("Session.Err = %#v, want TimeoutReadIdle", c.Err())
	}
}

func TestTCPMaxLifetimeClosesSession(t *testing.T) {
	e, err := New(WithTimeouts(Timeouts{MaxLifetime: 50 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	left, right := net.Pipe()
	defer right.Close()
	c, err := e.adoptStream(left, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("session did not close at max lifetime")
	}
	var te *TimeoutError
	if !errors.As(c.Err(), &te) || te.Kind != TimeoutMaxLifetime {
		t.Fatalf("Session.Err = %#v, want TimeoutMaxLifetime", c.Err())
	}
}

func TestTLSHandshakeTimeoutIsTyped(t *testing.T) {
	cfg := defaultConfig()
	cfg.timeouts.Handshake = 35 * time.Millisecond
	cfg.tlsHandshakeTimeout = 35 * time.Millisecond

	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	conn := tls.Client(left, &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}) // test-only stalled peer
	err := cfg.handshakeClient(context.Background(), conn)
	var te *TimeoutError
	if !errors.As(err, &te) || te.Kind != TimeoutHandshake {
		t.Fatalf("handshake error = %#v, want TimeoutHandshake", err)
	}
}

func TestTLSHandshakeCallerDeadlineWins(t *testing.T) {
	cfg := defaultConfig()
	cfg.timeouts.Handshake = time.Second
	cfg.tlsHandshakeTimeout = time.Second

	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	conn := tls.Client(left, &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}) // test-only stalled peer
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err := cfg.handshakeClient(ctx, conn)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("handshake error = %#v, want context.DeadlineExceeded", err)
	}
	var te *TimeoutError
	if errors.As(err, &te) {
		t.Fatalf("caller timeout was wrapped as runtime timeout: %#v", te)
	}
}
