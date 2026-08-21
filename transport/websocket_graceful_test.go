package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/qigao/ogrenet"
)

func TestWebSocketShutdownDrainsAcceptedMessagesBeforeClose(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeWS)
	defer p.close()

	graceful := requireGracefulShutdown(t, p.client)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	accepted := make([]string, 0, 32)
	for i := 0; i < 32; i++ {
		text := fmt.Sprintf("ws-%02d", i)
		err := p.client.TrySend(ogrenet.Text(text))
		switch {
		case err == nil:
			accepted = append(accepted, text)
		case errors.Is(err, ErrWouldBlock):
			continue
		default:
			t.Fatalf("TrySend(%d): %v", i, err)
		}
	}
	if len(accepted) == 0 {
		t.Fatal("no WebSocket messages accepted")
	}

	if err := graceful.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := p.client.Send(ctx, ogrenet.Text("late")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Send after Shutdown = %v, want ErrClosed", err)
	}

	got := make([]string, 0, len(accepted))
	for len(got) < len(accepted) {
		select {
		case msg := <-p.serverMsgs:
			got = append(got, string(msg.Data))
		case <-ctx.Done():
			t.Fatalf("received %d/%d messages: %v", len(got), len(accepted), ctx.Err())
		}
	}
	if !reflect.DeepEqual(got, accepted) {
		t.Fatalf("drain order = %v, want %v", got, accepted)
	}
	waitClosed(t, p.client.Done(), "client WebSocket session")
	if err := p.client.Err(); err != nil {
		t.Fatalf("client Err = %v, want nil", err)
	}
}

func TestWebSocketCloseTimeoutIsTyped(t *testing.T) {
	endpoint, waitAccepted := startNonResponsiveWebSocketServer(t)
	client, err := New(WithWebSocketConfig(testWebSocketCloseConfig(50 * time.Millisecond)))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	s, err := client.Dial(context.Background(), endpoint, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	waitAccepted()

	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	err = requireGracefulShutdown(t, s).Shutdown(ctx)
	var te *TimeoutError
	if !errors.As(err, &te) || te.Kind != TimeoutClose {
		t.Fatalf("Shutdown = %#v, want TimeoutClose", err)
	}
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("Shutdown = %v, want ErrTimeout match", err)
	}
	var sessionTE *TimeoutError
	if !errors.As(s.Err(), &sessionTE) || sessionTE.Kind != TimeoutClose {
		t.Fatalf("Session.Err = %#v, want TimeoutClose", s.Err())
	}
}

func TestWebSocketShutdownCallerDeadlineWinsCloseTimeout(t *testing.T) {
	endpoint, waitAccepted := startNonResponsiveWebSocketServer(t)
	client, err := New(WithWebSocketConfig(testWebSocketCloseConfig(2 * time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	s, err := client.Dial(context.Background(), endpoint, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	waitAccepted()

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- requireGracefulShutdown(t, s).Shutdown(ctx) }()

	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown = %v, want caller deadline", err)
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatal("Shutdown did not honor caller deadline promptly")
	}
	waitClosed(t, s.Done(), "caller-aborted WebSocket session")
	if err := s.Err(); err != nil {
		t.Fatalf("Session.Err = %v, want nil after caller-owned abort", err)
	}
}

func testWebSocketCloseConfig(closeTimeout time.Duration) WebSocketConfig {
	return WebSocketConfig{
		HandshakeTimeout: time.Second,
		WriteTimeout:     time.Second,
		CloseTimeout:     closeTimeout,
		PingInterval:     0,
		PongTimeout:      time.Second,
	}
}

func startNonResponsiveWebSocketServer(t *testing.T) (ogrenet.Endpoint, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan *websocket.Conn, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		accepted <- ws
		<-release
		_ = ws.CloseNow()
	})
	server := &http.Server{Handler: mux}
	serverDone := make(chan struct{})
	go func() {
		_ = server.Serve(ln)
		close(serverDone)
	}()

	var serverWS *websocket.Conn
	var waitOnce sync.Once
	waitAccepted := func() {
		t.Helper()
		waitOnce.Do(func() {
			select {
			case serverWS = <-accepted:
			case <-time.After(2 * time.Second):
				t.Fatal("WebSocket server did not accept upgrade")
			}
		})
	}

	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		if serverWS != nil {
			_ = serverWS.CloseNow()
		}
		_ = server.Close()
		_ = ln.Close()
		select {
		case <-serverDone:
		case <-time.After(2 * time.Second):
			t.Error("non-responsive WebSocket server did not stop")
		}
	})

	addr := ln.Addr().(*net.TCPAddr)
	return ogrenet.Endpoint{Scheme: ogrenet.SchemeWS, Host: "127.0.0.1", Port: uint16(addr.Port), Path: "/"}, waitAccepted
}
