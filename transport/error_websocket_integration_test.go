package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/qigao/ogrenet"
)

func TestTransportErrorWebSocketUpgradeRejectionUsesOpUpgrade(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	})}
	go server.Serve(ln)
	t.Cleanup(func() {
		_ = server.Close()
		_ = ln.Close()
	})

	addr := ln.Addr().(*net.TCPAddr)
	endpoint := ogrenet.Endpoint{Scheme: ogrenet.SchemeWS, Host: "127.0.0.1", Port: uint16(addr.Port), Path: "/"}
	client, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, err = client.Dial(context.Background(), endpoint, ogrenet.HandlerFuncs{})
	assertTransportError(t, err, OpUpgrade, ogrenet.SchemeWS, ErrorProtocol)
	if !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("upgrade rejection does not match ErrProtocolViolation: %v", err)
	}
}

func TestTransportErrorWSSCertificateFailureUsesOpHandshake(t *testing.T) {
	serverTLS, _ := testTLSConfigs(t)
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

	client, err := New(WithTLSClientConfig(&tls.Config{MinVersion: tls.VersionTLS13}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = client.Dial(ctx, listener.Endpoint(), ogrenet.HandlerFuncs{})
	assertTransportError(t, err, OpHandshake, ogrenet.SchemeWSS, ErrorTLS)
	if !errors.Is(err, ErrTLS) {
		t.Fatalf("WSS certificate failure does not match ErrTLS: %v", err)
	}
}

func TestTransportErrorWebSocketPeerProtocolCloseUsesOpRead(t *testing.T) {
	endpoint := startStatusClosingWebSocketServer(t, websocket.StatusProtocolError)
	client, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	s, err := client.Dial(context.Background(), endpoint, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	waitClosed(t, s.Done(), "protocol-closed WebSocket session")
	assertTransportError(t, s.Err(), OpRead, ogrenet.SchemeWS, ErrorProtocol)
	if !errors.Is(s.Err(), ErrProtocolViolation) {
		t.Fatalf("protocol close does not match ErrProtocolViolation: %v", s.Err())
	}
}

func TestTransportErrorWebSocketPeerTooLargeCloseUsesOpRead(t *testing.T) {
	endpoint := startStatusClosingWebSocketServer(t, websocket.StatusMessageTooBig)
	client, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	s, err := client.Dial(context.Background(), endpoint, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	waitClosed(t, s.Done(), "too-large-closed WebSocket session")
	assertTransportError(t, s.Err(), OpRead, ogrenet.SchemeWS, ErrorTooLarge)
	if !errors.Is(s.Err(), ErrMessageTooLarge) {
		t.Fatalf("message-too-big close does not match ErrMessageTooLarge: %v", s.Err())
	}
}

func TestTransportErrorWebSocketNormalPeerCloseRemainsErrorFree(t *testing.T) {
	for _, status := range []websocket.StatusCode{websocket.StatusNormalClosure, websocket.StatusGoingAway} {
		t.Run(status.String(), func(t *testing.T) {
			endpoint := startStatusClosingWebSocketServer(t, status)
			client, err := New()
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			s, err := client.Dial(context.Background(), endpoint, ogrenet.HandlerFuncs{})
			if err != nil {
				t.Fatal(err)
			}
			waitClosed(t, s.Done(), "normal peer-closed WebSocket session")
			if err := s.Err(); err != nil {
				t.Fatalf("normal close populated Session.Err: %v", err)
			}
		})
	}
}

func TestTransportErrorWebSocketCloseTimeoutUsesOpClose(t *testing.T) {
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
	err = s.Shutdown(ctx)
	assertTransportError(t, err, OpClose, ogrenet.SchemeWS, ErrorTimeout)
	var timeout *TimeoutError
	if !errors.As(err, &timeout) || timeout.Kind != TimeoutClose || !errors.Is(err, ErrTimeout) {
		t.Fatalf("close timeout chain = %#v", err)
	}
	waitClosed(t, s.Done(), "timeout-closed WebSocket session")
	if s.Err() != err {
		t.Fatalf("Session.Err identity mismatch: got %v want %v", s.Err(), err)
	}
}

func TestTransportErrorWebSocketTrySendBackpressureUsesOpSend(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeWS)
	defer p.close()
	s := p.client.(*wsSession)

	for i := 0; i < cap(s.frameSlots); i++ {
		s.frameSlots <- struct{}{}
	}
	t.Cleanup(func() {
		for len(s.frameSlots) > 0 {
			<-s.frameSlots
		}
	})

	err := s.TrySend(ogrenet.Text("blocked"))
	assertTransportError(t, err, OpSend, ogrenet.SchemeWS, ErrorBackpressure)
	if !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("TrySend backpressure does not match ErrWouldBlock: %v", err)
	}
}

func TestTransportErrorWebSocketWriteTimeoutUsesOpWrite(t *testing.T) {
	endpoint := startNonReadingWebSocketEndpoint(t)
	client, err := New(WithTimeouts(Timeouts{Write: 30 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	s, err := client.Dial(context.Background(), endpoint, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = s.Send(ctx, ogrenet.Bin(make([]byte, 4<<20)))
	assertTransportError(t, err, OpWrite, ogrenet.SchemeWS, ErrorTimeout)
	var timeout *TimeoutError
	if !errors.As(err, &timeout) || timeout.Kind != TimeoutWrite || !errors.Is(err, ErrTimeout) {
		t.Fatalf("write timeout chain = %#v", err)
	}
	waitClosed(t, s.Done(), "write-timeout WebSocket session")
	if s.Err() != err {
		t.Fatalf("Session.Err identity mismatch: got %v want %v", s.Err(), err)
	}
}

func startStatusClosingWebSocketServer(t *testing.T, status websocket.StatusCode) ogrenet.Endpoint {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverDone := make(chan struct{})
	accepted := make(chan *websocket.Conn, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		accepted <- ws
		_ = ws.Close(status, "test close")
	})}
	go func() {
		_ = server.Serve(ln)
		close(serverDone)
	}()

	var peer *websocket.Conn
	var once sync.Once
	t.Cleanup(func() {
		once.Do(func() {
			select {
			case peer = <-accepted:
			default:
			}
		})
		if peer != nil {
			_ = peer.CloseNow()
		}
		_ = server.Close()
		_ = ln.Close()
		select {
		case <-serverDone:
		case <-time.After(2 * time.Second):
			t.Error("status-close WebSocket server did not stop")
		}
	})

	addr := ln.Addr().(*net.TCPAddr)
	return ogrenet.Endpoint{Scheme: ogrenet.SchemeWS, Host: "127.0.0.1", Port: uint16(addr.Port), Path: "/"}
}
