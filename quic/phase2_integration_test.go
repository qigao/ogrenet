package quic

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"
)

func TestAcceptPeerStream(t *testing.T) {
	serverTLS, clientTLS := echoTLSConfigs(t)
	listener, err := quicgo.ListenAddr("127.0.0.1:0", serverTLS, &quicgo.Config{
		HandshakeIdleTimeout: 3 * time.Second,
		MaxIdleTimeout:       10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	clientDone := make(chan struct{})
	go func() {
		conn, err := listener.Accept(context.Background())
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.CloseWithError(0, "")
		stream, err := conn.OpenStreamSync(context.Background())
		if err != nil {
			serverErr <- err
			return
		}
		if _, err := stream.Write([]byte("server initiated")); err != nil {
			serverErr <- err
			return
		}
		if err := stream.Close(); err != nil {
			serverErr <- err
			return
		}
		serverErr <- nil
		<-clientDone
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Dial(ctx, listener.Addr().String(), Config{TLSConfig: clientTLS, ALPN: echoALPN})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(stream)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "server initiated" {
		t.Fatalf("payload = %q", payload)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
	close(clientDone)
}

func TestDatagramLoopback(t *testing.T) {
	serverTLS, clientTLS := echoTLSConfigs(t)
	listener, err := quicgo.ListenAddr("127.0.0.1:0", serverTLS, &quicgo.Config{
		HandshakeIdleTimeout: 3 * time.Second,
		MaxIdleTimeout:       10 * time.Second,
		EnableDatagrams:      true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	clientDone := make(chan struct{})
	go func() {
		conn, err := listener.Accept(context.Background())
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.CloseWithError(0, "")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		payload, err := conn.ReceiveDatagram(ctx)
		if err != nil {
			serverErr <- err
			return
		}
		if string(payload) != "ping" {
			serverErr <- errors.New("unexpected datagram payload")
			return
		}
		if err := conn.SendDatagram([]byte("pong")); err != nil {
			serverErr <- err
			return
		}
		serverErr <- nil
		<-clientDone
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Dial(ctx, listener.Addr().String(), Config{
		TLSConfig:       clientTLS,
		ALPN:            echoALPN,
		EnableDatagrams: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.SendDatagram([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	payload, err := conn.ReceiveDatagram(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "pong" {
		t.Fatalf("payload = %q", payload)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
	close(clientDone)
}

func TestDatagramsRequireExplicitOptIn(t *testing.T) {
	conn := &Conn{}
	if err := conn.SendDatagram([]byte("x")); !errors.Is(err, ErrDatagramsDisabled) {
		t.Fatalf("SendDatagram error = %v", err)
	}
	if _, err := conn.ReceiveDatagram(context.Background()); !errors.Is(err, ErrDatagramsDisabled) {
		t.Fatalf("ReceiveDatagram error = %v", err)
	}
}

func TestDoneAndErrReportPeerClose(t *testing.T) {
	serverTLS, clientTLS := echoTLSConfigs(t)
	listener, err := quicgo.ListenAddr("127.0.0.1:0", serverTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	accepted := make(chan struct{})
	closeNow := make(chan struct{})
	go func() {
		conn, err := listener.Accept(context.Background())
		if err != nil {
			serverErr <- err
			return
		}
		close(accepted)
		<-closeNow
		serverErr <- conn.CloseWithError(77, "peer shutdown")
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Dial(ctx, listener.Addr().String(), Config{TLSConfig: clientTLS, ALPN: echoALPN})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-accepted:
	case <-ctx.Done():
		t.Fatal("server did not accept connection")
	}
	close(closeNow)

	select {
	case <-conn.Done():
	case <-ctx.Done():
		t.Fatal("connection Done did not close")
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
	terminal := conn.Err()
	if terminal == nil {
		t.Fatal("Err = nil after peer close")
	}
	var qerr *Error
	if !errors.As(terminal, &qerr) {
		t.Fatalf("Err type = %T", terminal)
	}
	if qerr.Kind != ErrorClosed {
		t.Fatalf("Err kind = %v, want %v", qerr.Kind, ErrorClosed)
	}
	var appErr *quicgo.ApplicationError
	if !errors.As(terminal, &appErr) {
		t.Fatalf("terminal error did not preserve ApplicationError: %v", terminal)
	}
}
