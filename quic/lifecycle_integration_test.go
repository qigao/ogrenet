package quic

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"
)

func TestDialCancellation(t *testing.T) {
	blackhole, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer blackhole.Close()

	_, clientTLS := echoTLSConfigs(t)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		conn, err := Dial(ctx, blackhole.LocalAddr().String(), Config{
			TLSConfig:        clientTLS,
			ALPN:             echoALPN,
			HandshakeTimeout: 5 * time.Second,
		})
		if conn != nil {
			_ = conn.Close()
		}
		result <- err
	}()

	time.Sleep(25 * time.Millisecond)
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Dial error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Dial did not unblock after context cancellation")
	}
}

func TestOpenStreamCancellation(t *testing.T) {
	serverTLS, clientTLS := echoTLSConfigs(t)
	listener, err := quicgo.ListenAddr("127.0.0.1:0", serverTLS, &quicgo.Config{
		HandshakeIdleTimeout: 3 * time.Second,
		MaxIdleTimeout:       10 * time.Second,
		MaxIncomingStreams:   -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	accepted := make(chan *quicgo.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept(context.Background())
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Dial(ctx, listener.Addr().String(), Config{TLSConfig: clientTLS, ALPN: echoALPN})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var serverConn *quicgo.Conn
	select {
	case serverConn = <-accepted:
		defer serverConn.CloseWithError(0, "")
	case err := <-acceptErr:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	streamCtx, streamCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer streamCancel()
	if _, err := conn.OpenStream(streamCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("OpenStream error = %v, want context.DeadlineExceeded", err)
	}
}

func TestPeerCloseUnblocksStreamRead(t *testing.T) {
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
	go func() {
		conn, err := listener.Accept(context.Background())
		if err != nil {
			serverErr <- err
			return
		}
		stream, err := conn.AcceptStream(context.Background())
		if err != nil {
			serverErr <- err
			return
		}
		_ = stream
		serverErr <- conn.CloseWithError(42, "peer close test")
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Dial(ctx, listener.Addr().String(), Config{TLSConfig: clientTLS, ALPN: echoALPN})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stream, err := conn.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}

	readErr := make(chan error, 1)
	go func() {
		var b [1]byte
		_, err := stream.Read(b[:])
		readErr <- err
	}()
	select {
	case err := <-readErr:
		if err == nil || errors.Is(err, io.EOF) {
			t.Fatalf("stream Read error = %v, want peer-close error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream Read did not unblock after peer close")
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestConnCloseIsIdempotent(t *testing.T) {
	serverTLS, clientTLS := echoTLSConfigs(t)
	listener, err := quicgo.ListenAddr("127.0.0.1:0", serverTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	accepted := make(chan *quicgo.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept(context.Background())
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Dial(ctx, listener.Addr().String(), Config{TLSConfig: clientTLS, ALPN: echoALPN})
	if err != nil {
		t.Fatal(err)
	}
	var serverConn *quicgo.Conn
	select {
	case serverConn = <-accepted:
	case err := <-acceptErr:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	defer serverConn.CloseWithError(0, "")

	if err := conn.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
