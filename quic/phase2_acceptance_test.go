package quic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"
)

func TestConcurrentStreams(t *testing.T) {
	const streamCount = 16
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

		var wg sync.WaitGroup
		handlerErr := make(chan error, streamCount)
		for i := 0; i < streamCount; i++ {
			stream, err := conn.AcceptStream(context.Background())
			if err != nil {
				serverErr <- err
				return
			}
			wg.Add(1)
			go func(stream *quicgo.Stream) {
				defer wg.Done()
				if _, err := io.Copy(stream, stream); err != nil {
					handlerErr <- err
					return
				}
				handlerErr <- stream.Close()
			}(stream)
		}
		wg.Wait()
		close(handlerErr)
		for err := range handlerErr {
			if err != nil {
				serverErr <- err
				return
			}
		}
		serverErr <- nil
		<-clientDone
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := Dial(ctx, listener.Addr().String(), Config{TLSConfig: clientTLS, ALPN: echoALPN})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	clientErr := make(chan error, streamCount)
	var wg sync.WaitGroup
	for i := 0; i < streamCount; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream, err := conn.OpenStream(ctx)
			if err != nil {
				clientErr <- err
				return
			}
			payload := []byte(fmt.Sprintf("stream-%02d", i))
			if _, err := stream.Write(payload); err != nil {
				clientErr <- err
				return
			}
			if err := stream.CloseWrite(); err != nil {
				clientErr <- err
				return
			}
			echoed, err := io.ReadAll(stream)
			if err != nil {
				clientErr <- err
				return
			}
			if string(echoed) != string(payload) {
				clientErr <- fmt.Errorf("stream %d echo = %q, want %q", i, echoed, payload)
				return
			}
			clientErr <- nil
		}()
	}
	wg.Wait()
	close(clientErr)
	for err := range clientErr {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
	close(clientDone)
}

func TestALPNMismatchIsProtocolError(t *testing.T) {
	serverTLS, clientTLS := echoTLSConfigs(t)
	listener, err := quicgo.ListenAddr("127.0.0.1:0", serverTLS, &quicgo.Config{
		HandshakeIdleTimeout: time.Second,
		MaxIdleTimeout:       3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := Dial(ctx, listener.Addr().String(), Config{
		TLSConfig:        clientTLS,
		ALPN:             "ogrenet-quic-wrong-alpn",
		HandshakeTimeout: time.Second,
	})
	if conn != nil {
		_ = conn.Close()
		t.Fatal("Dial succeeded with mismatched ALPN")
	}
	if err == nil {
		t.Fatal("Dial error = nil")
	}
	var qerr *Error
	if !errors.As(err, &qerr) {
		t.Fatalf("Dial error type = %T", err)
	}
	if qerr.Kind != ErrorProtocol {
		t.Fatalf("Dial error kind = %v, want %v: %v", qerr.Kind, ErrorProtocol, err)
	}
}

func TestIdleTimeoutIsTyped(t *testing.T) {
	const idleTimeout = 150 * time.Millisecond
	serverTLS, clientTLS := echoTLSConfigs(t)
	listener, err := quicgo.ListenAddr("127.0.0.1:0", serverTLS, &quicgo.Config{
		HandshakeIdleTimeout: time.Second,
		MaxIdleTimeout:       idleTimeout,
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
		<-conn.Context().Done()
		serverErr <- nil
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := Dial(ctx, listener.Addr().String(), Config{
		TLSConfig:   clientTLS,
		ALPN:        echoALPN,
		IdleTimeout: idleTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-conn.Done():
	case <-ctx.Done():
		t.Fatal("connection did not reach idle timeout")
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
	if err := conn.Err(); !errors.Is(err, ErrTimeout) {
		t.Fatalf("Err = %v, want ErrTimeout", err)
	}
}

func TestPeerInitiatedStreamLimitIsBounded(t *testing.T) {
	serverTLS, clientTLS := echoTLSConfigs(t)
	listener, err := quicgo.ListenAddr("127.0.0.1:0", serverTLS, &quicgo.Config{
		HandshakeIdleTimeout: time.Second,
		MaxIdleTimeout:       5 * time.Second,
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
		defer conn.CloseWithError(0, "")

		streams := make([]*quicgo.Stream, 0, defaultMaxIncomingStreams)
		for i := 0; i < defaultMaxIncomingStreams; i++ {
			stream, err := conn.OpenStreamSync(context.Background())
			if err != nil {
				serverErr <- fmt.Errorf("open stream %d: %w", i, err)
				return
			}
			streams = append(streams, stream)
		}
		defer func() {
			for _, stream := range streams {
				stream.CancelRead(0)
				stream.CancelWrite(0)
			}
		}()

		extraCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		if stream, err := conn.OpenStreamSync(extraCtx); err == nil {
			stream.CancelRead(0)
			stream.CancelWrite(0)
			serverErr <- errors.New("peer opened stream beyond configured limit")
			return
		} else if !errors.Is(err, context.DeadlineExceeded) {
			serverErr <- fmt.Errorf("extra stream error = %w", err)
			return
		}
		serverErr <- nil
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := Dial(ctx, listener.Addr().String(), Config{TLSConfig: clientTLS, ALPN: echoALPN})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	select {
	case err := <-serverErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("stream-limit check timed out")
	}
}
