package quic

import (
	"context"
	"io"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"
)

func BenchmarkDial(b *testing.B) {
	serverTLS, clientTLS := echoTLSConfigs(b)
	listener, err := quicgo.ListenAddr("127.0.0.1:0", serverTLS, &quicgo.Config{
		HandshakeIdleTimeout: 3 * time.Second,
		MaxIdleTimeout:       10 * time.Second,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serverErr := make(chan error, 1)
	go func() {
		for i := 0; i < b.N; i++ {
			conn, err := listener.Accept(ctx)
			if err != nil {
				serverErr <- err
				return
			}
			<-conn.Context().Done()
		}
		serverErr <- nil
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := Dial(ctx, listener.Addr().String(), Config{TLSConfig: clientTLS, ALPN: echoALPN})
		if err != nil {
			b.Fatal(err)
		}
		if err := conn.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := <-serverErr; err != nil {
		b.Fatal(err)
	}
}

func BenchmarkStreamThroughput64KiB(b *testing.B) {
	serverTLS, clientTLS := echoTLSConfigs(b)
	listener, err := quicgo.ListenAddr("127.0.0.1:0", serverTLS, &quicgo.Config{
		HandshakeIdleTimeout: 3 * time.Second,
		MaxIdleTimeout:       10 * time.Second,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer listener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept(ctx)
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.CloseWithError(0, "")
		buf := make([]byte, 64<<10)
		for i := 0; i < b.N; i++ {
			stream, err := conn.AcceptStream(ctx)
			if err != nil {
				serverErr <- err
				return
			}
			if _, err := io.ReadFull(stream, buf); err != nil {
				serverErr <- err
				return
			}
			if _, err := stream.Write(buf); err != nil {
				serverErr <- err
				return
			}
			if err := stream.Close(); err != nil {
				serverErr <- err
				return
			}
		}
		serverErr <- nil
	}()

	conn, err := Dial(ctx, listener.Addr().String(), Config{TLSConfig: clientTLS, ALPN: echoALPN})
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	payload := make([]byte, 64<<10)
	response := make([]byte, len(payload))
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := conn.OpenStream(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := stream.Write(payload); err != nil {
			b.Fatal(err)
		}
		if err := stream.CloseWrite(); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(stream, response); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := <-serverErr; err != nil {
		b.Fatal(err)
	}
}
