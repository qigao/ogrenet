package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestWSHandshakeTimeoutIsTyped(t *testing.T) {
	endpoint := startStalledWSEndpoint(t)
	client, err := New(WithTimeouts(Timeouts{Handshake: 40 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	_, err = client.Dial(ctx, endpoint, ogrenet.HandlerFuncs{})
	var te *TimeoutError
	if !errors.As(err, &te) || te.Kind != TimeoutHandshake {
		t.Fatalf("WS Dial error = %#v, want TimeoutHandshake", err)
	}
}

func TestWSHandshakeCallerDeadlineWins(t *testing.T) {
	endpoint := startStalledWSEndpoint(t)
	client, err := New(WithTimeouts(Timeouts{Handshake: time.Second}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = client.Dial(ctx, endpoint, ogrenet.HandlerFuncs{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WS Dial error = %#v, want context.DeadlineExceeded", err)
	}
	var te *TimeoutError
	if errors.As(err, &te) {
		t.Fatalf("caller timeout was wrapped as runtime timeout: %#v", te)
	}
}

func TestWSReadIdleClosesSession(t *testing.T) {
	listener, closeServer := startTimeoutWSServer(t, ogrenet.SchemeWS, false)
	defer closeServer()

	client, err := New(WithTimeouts(Timeouts{ReadIdle: 50 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := client.Dial(ctx, listener.Endpoint(), ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	waitSessionTimeoutKind(t, s, TimeoutReadIdle)
}

func TestWSSReadIdleClosesSession(t *testing.T) {
	serverTLS, clientTLS := testTLSConfigs(t)
	server, err := New(WithTLSServerConfig(serverTLS))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	listener, err := server.Listen(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeWSS, Host: "127.0.0.1", Port: 0, Path: "/"}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	client, err := New(WithTLSClientConfig(clientTLS), WithTimeouts(Timeouts{ReadIdle: 50 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := client.Dial(ctx, listener.Endpoint(), ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	waitSessionTimeoutKind(t, s, TimeoutReadIdle)
}

func TestWSConnectionIdleIgnoresPingPong(t *testing.T) {
	listener, closeServer := startTimeoutWSServer(t, ogrenet.SchemeWS, false)
	defer closeServer()

	client, err := New(
		WithTimeouts(Timeouts{ConnectionIdle: 100 * time.Millisecond}),
		WithWebSocketConfig(WebSocketConfig{
			HandshakeTimeout: time.Second,
			WriteTimeout:     time.Second,
			PingInterval:     20 * time.Millisecond,
			PongTimeout:      80 * time.Millisecond,
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := client.Dial(ctx, listener.Endpoint(), ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	waitSessionTimeoutKind(t, s, TimeoutConnectionIdle)
}

func TestWSMaxLifetimeCannotBeExtendedByTraffic(t *testing.T) {
	listener, closeServer := startTimeoutWSServer(t, ogrenet.SchemeWS, true)
	defer closeServer()

	client, err := New(WithTimeouts(Timeouts{MaxLifetime: 120 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := client.Dial(ctx, listener.Endpoint(), ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.Done():
			var te *TimeoutError
			if !errors.As(s.Err(), &te) || te.Kind != TimeoutMaxLifetime {
				t.Fatalf("Session.Err = %#v, want TimeoutMaxLifetime", s.Err())
			}
			return
		case <-ticker.C:
			sendCtx, sendCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			err := s.Send(sendCtx, ogrenet.Text("keepalive"))
			sendCancel()
			if err != nil && !errors.Is(err, ErrClosed) {
				var te *TimeoutError
				if !errors.As(err, &te) || te.Kind != TimeoutMaxLifetime {
					t.Fatalf("Send error before lifetime close = %v", err)
				}
			}
		case <-ctx.Done():
			t.Fatal("session lifetime was extended by traffic")
		}
	}
}

func startStalledWSEndpoint(t *testing.T) ogrenet.Endpoint {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		<-stop
	}()
	t.Cleanup(func() {
		close(stop)
		_ = ln.Close()
		<-done
	})
	addr := ln.Addr().(*net.TCPAddr)
	return ogrenet.Endpoint{Scheme: ogrenet.SchemeWS, Host: "127.0.0.1", Port: uint16(addr.Port), Path: "/"}
}

func startTimeoutWSServer(t *testing.T, scheme ogrenet.Scheme, echo bool) (ogrenet.Listener, func()) {
	t.Helper()
	server, err := New()
	if err != nil {
		t.Fatal(err)
	}
	h := ogrenet.HandlerFuncs{}
	if echo {
		h.Message = func(s ogrenet.Session, msg ogrenet.Message) {
			_ = s.Send(context.Background(), msg)
		}
	}
	listener, err := server.Listen(context.Background(), ogrenet.Endpoint{Scheme: scheme, Host: "127.0.0.1", Port: 0, Path: "/"}, h)
	if err != nil {
		_ = server.Close()
		t.Fatal(err)
	}
	return listener, func() {
		_ = listener.Close()
		_ = server.Close()
	}
}

func waitSessionTimeoutKind(t *testing.T, s ogrenet.Session, kind TimeoutKind) {
	t.Helper()
	select {
	case <-s.Done():
	case <-time.After(time.Second):
		t.Fatalf("session did not close for %s timeout", kind)
	}
	var te *TimeoutError
	if !errors.As(s.Err(), &te) || te.Kind != kind {
		t.Fatalf("Session.Err = %#v, want %s timeout", s.Err(), kind)
	}
}
