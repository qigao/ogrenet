package transport

import (
	"context"
	"errors"
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

	// TrySend returns after admission, while the writer processes the accepted
	// frame asynchronously. Benchmark only the caller-side admission path and
	// drain the writer with the timer stopped so process-wide allocation counters
	// do not depend on scheduler timing.
	previousProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousProcs)

	payload := ogrenet.Bin(make([]byte, 128))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.TrySend(payload); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		for len(c.frameSlots) != 0 {
			runtime.Gosched()
		}
		b.StartTimer()
	}
	b.StopTimer()
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
	readerReady := make(chan struct{}, 1)
	c, err := e.adoptStream(wrapped, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{
		Message: func(ogrenet.Session, ogrenet.Message) {
			select {
			case readerReady <- struct{}{}:
			default:
			}
		},
	})
	if err != nil {
		_ = e.Close()
		_ = right.Close()
		b.Fatal(err)
	}

	// Keep asynchronous peer-drain setup outside the timed/allocation sample.
	// With -benchtime=1x, starting io.Copy immediately before ResetTimer made its
	// first buffer/goroutine allocations race with the single measured Send.
	drainBuf := make([]byte, 32<<10)
	drainReady := make(chan struct{})
	go func() {
		first := true
		for {
			_, readErr := right.Read(drainBuf)
			if first {
				close(drainReady)
				first = false
			}
			if readErr != nil {
				return
			}
		}
	}()
	if _, err := left.Write([]byte{0}); err != nil {
		_ = e.Close()
		_ = right.Close()
		b.Fatal(err)
	}
	<-drainReady

	// Also force the connection reader loop through buffer allocation, one
	// successful read/decode, and the application callback before the benchmark
	// timer starts. Otherwise its two 64 KiB initial buffers can race with a
	// one-iteration Send sample and appear as 64/128 KiB of false allocations.
	warmFrame, err := c.encode(ogrenet.Bin([]byte{0}))
	if err != nil {
		_ = e.Close()
		_ = right.Close()
		b.Fatal(err)
	}
	if _, err := right.Write(warmFrame); err != nil {
		_ = e.Close()
		_ = right.Close()
		b.Fatal(err)
	}
	<-readerReady

	return e, c, right
}
