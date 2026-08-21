package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func BenchmarkGracefulSendRunning(b *testing.B) {
	e, c, peer := newGracefulBenchmarkConn(b)
	defer e.Close()
	defer c.Close()
	defer peer.Close()

	payload := ogrenet.Bin(make([]byte, 128))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Send(context.Background(), payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGracefulTrySendRunning(b *testing.B) {
	e, c, peer := newGracefulBenchmarkConn(b)
	defer e.Close()
	defer c.Close()
	defer peer.Close()

	payload := ogrenet.Bin(make([]byte, 128))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for {
			err := c.TrySend(payload)
			switch {
			case err == nil:
				goto accepted
			case errors.Is(err, ErrWouldBlock):
				runtime.Gosched()
			default:
				b.Fatal(err)
			}
		}
	accepted:
	}
}

func BenchmarkGracefulDrainOneFrame(b *testing.B) {
	benchmarkGracefulDrainFrames(b, 1)
}

func BenchmarkGracefulDrain256Frames(b *testing.B) {
	benchmarkGracefulDrainFrames(b, 256)
}

func BenchmarkEngineGracefulDrain100(b *testing.B) {
	benchmarkEngineGracefulDrain(b, 100)
}

func BenchmarkEngineGracefulDrain1000(b *testing.B) {
	benchmarkEngineGracefulDrain(b, 1000)
}

func benchmarkGracefulDrainFrames(b *testing.B, frames int) {
	payload := ogrenet.Bin(make([]byte, 128))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		e, c, peer := newGracefulBenchmarkConn(b)
		for n := 0; n < frames; n++ {
			for {
				err := c.TrySend(payload)
				switch {
				case err == nil:
					goto accepted
				case errors.Is(err, ErrWouldBlock):
					runtime.Gosched()
				default:
					b.Fatal(err)
				}
			}
		accepted:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		b.StartTimer()
		if err := c.CloseWrite(ctx); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		cancel()
		_ = c.Close()
		_ = peer.Close()
		_ = e.Close()
	}
}

func benchmarkEngineGracefulDrain(b *testing.B, sessions int) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		server, err := New()
		if err != nil {
			b.Fatal(err)
		}
		accepted := make(chan ogrenet.Session, sessions)
		listener, err := server.Listen(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, ogrenet.HandlerFuncs{
			Open: func(s ogrenet.Session) { accepted <- s },
		})
		if err != nil {
			_ = server.Close()
			b.Fatal(err)
		}
		client, err := New()
		if err != nil {
			_ = server.Close()
			b.Fatal(err)
		}
		clientSessions := make([]ogrenet.Session, 0, sessions)
		for n := 0; n < sessions; n++ {
			s, err := client.Dial(context.Background(), listener.Endpoint(), ogrenet.HandlerFuncs{})
			if err != nil {
				_ = client.Close()
				_ = server.Close()
				b.Fatal(err)
			}
			clientSessions = append(clientSessions, s)
		}
		_ = clientSessions
		serverSessions := make([]ogrenet.Session, 0, sessions)
		for n := 0; n < sessions; n++ {
			select {
			case s := <-accepted:
				serverSessions = append(serverSessions, s)
			case <-time.After(10 * time.Second):
				_ = client.Close()
				_ = server.Close()
				b.Fatal("accept timeout")
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		peerDone := make(chan error, 1)
		go func() {
			for _, s := range serverSessions {
				hs, ok := s.(ogrenet.HalfCloseSession)
				if !ok {
					peerDone <- ErrClosed
					return
				}
				select {
				case <-hs.ReadClosed():
				case <-ctx.Done():
					peerDone <- ctx.Err()
					return
				}
				if err := hs.CloseWrite(ctx); err != nil {
					peerDone <- err
					return
				}
			}
			peerDone <- nil
		}()

		b.StartTimer()
		err = client.Shutdown(ctx)
		b.StopTimer()
		if err != nil {
			cancel()
			_ = server.Close()
			b.Fatal(err)
		}
		if err := <-peerDone; err != nil {
			cancel()
			_ = server.Close()
			b.Fatal(err)
		}
		cancel()
		_ = server.Close()
	}
}

type gracefulBenchmarkConn struct {
	net.Conn
}

func (c *gracefulBenchmarkConn) CloseWrite() error { return nil }

func newGracefulBenchmarkConn(b *testing.B) (*Engine, *conn, net.Conn) {
	b.Helper()
	e, err := New(WithWriteQueue(512))
	if err != nil {
		b.Fatal(err)
	}
	left, right := net.Pipe()
	wrapped := &gracefulBenchmarkConn{Conn: left}
	c, err := e.adoptStream(wrapped, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		_ = e.Close()
		_ = right.Close()
		b.Fatal(err)
	}
	go func() { _, _ = io.Copy(io.Discard, right) }()
	return e, c, right
}
