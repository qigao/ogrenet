package transport

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/qigao/ogrenet"
)

func TestMapOperationTimeoutConnectRuntimeTimeout(t *testing.T) {
	parent := context.Background()
	op, cancel := context.WithTimeout(parent, time.Nanosecond)
	defer cancel()
	<-op.Done()

	root := errors.New("dial failed")
	err := mapOperationTimeout(parent, op, TimeoutConnect, root)
	var te *TimeoutError
	if !errors.As(err, &te) || te.Kind != TimeoutConnect {
		t.Fatalf("mapOperationTimeout = %#v, want TimeoutConnect", err)
	}
	if !errors.Is(err, root) {
		t.Fatalf("mapped timeout lost root cause: %v", err)
	}
}

func TestMapOperationTimeoutConnectCallerCancelWins(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	op, opCancel := context.WithCancel(parent)
	defer opCancel()

	err := mapOperationTimeout(parent, op, TimeoutConnect, errors.New("dial failed"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("mapOperationTimeout = %#v, want context.Canceled", err)
	}
	var te *TimeoutError
	if errors.As(err, &te) {
		t.Fatalf("caller cancellation was wrapped as runtime timeout: %#v", te)
	}
}

func TestWebSocketWriteTimeoutIsTyped(t *testing.T) {
	endpoint := startNonReadingWebSocketEndpoint(t)
	client, err := New(WithTimeouts(Timeouts{Write: 30 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := client.Dial(ctx, endpoint, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 4<<20)
	sendCtx, sendCancel := context.WithTimeout(context.Background(), 2*time.Second)
	err = s.Send(sendCtx, ogrenet.Bin(payload))
	sendCancel()
	var te *TimeoutError
	if !errors.As(err, &te) || te.Kind != TimeoutWrite {
		t.Fatalf("Send error = %#v, want TimeoutWrite", err)
	}
	waitSessionTimeoutKind(t, s, TimeoutWrite)
}

type smallReadBufferListener struct {
	net.Listener
}

func (l smallReadBufferListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		if err := tcp.SetReadBuffer(1024); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

func startNonReadingWebSocketEndpoint(t *testing.T) ogrenet.Endpoint {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		defer ws.CloseNow()
		<-stop
	})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(smallReadBufferListener{Listener: ln})
	}()
	t.Cleanup(func() {
		close(stop)
		_ = server.Close()
		_ = ln.Close()
		<-done
	})
	addr := ln.Addr().(*net.TCPAddr)
	return ogrenet.Endpoint{Scheme: ogrenet.SchemeWS, Host: "127.0.0.1", Port: uint16(addr.Port), Path: "/"}
}
