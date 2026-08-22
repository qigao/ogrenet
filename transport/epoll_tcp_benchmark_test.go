//go:build linux

package transport

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

type tcpBenchmarkBackend struct {
	name string
	new  func(*testing.B) ogrenet.Engine
}

func tcpBenchmarkBackends() []tcpBenchmarkBackend {
	return []tcpBenchmarkBackend{
		{
			name: "portable",
			new: func(b *testing.B) ogrenet.Engine {
				b.Helper()
				e, err := New()
				if err != nil {
					b.Fatal(err)
				}
				return e
			},
		},
		{
			name: "epoll",
			new: func(b *testing.B) ogrenet.Engine {
				b.Helper()
				e, err := NewEpoll(EpollConfig{
					Pollers:         1,
					CallbackWorkers: 4,
					CallbackQueue:   256,
				})
				if err != nil {
					b.Fatal(err)
				}
				return e
			},
		},
	}
}

type tcpBenchmarkEcho struct {
	engine ogrenet.Engine
	ln     ogrenet.Listener
	client ogrenet.Session
	echoed chan ogrenet.Message
	errs   chan error
}

func newTCPBenchmarkEcho(b *testing.B, backend tcpBenchmarkBackend) *tcpBenchmarkEcho {
	b.Helper()
	e := backend.new(b)
	serverOpen := make(chan struct{}, 1)
	clientOpen := make(chan struct{}, 1)
	echoed := make(chan ogrenet.Message, 1)
	errs := make(chan error, 1)

	ln, err := e.Listen(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeTCP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.HandlerFuncs{
		Open: func(ogrenet.Session) {
			serverOpen <- struct{}{}
		},
		Message: func(s ogrenet.Session, msg ogrenet.Message) {
			if err := s.Send(context.Background(), msg); err != nil {
				select {
				case errs <- err:
				default:
				}
			}
		},
	})
	if err != nil {
		_ = e.Close()
		benchmarkWaitDone(b, e.Done(), "benchmark Engine after Listen failure")
		b.Fatal(err)
	}

	client, err := e.Dial(context.Background(), ln.Endpoint(), ogrenet.HandlerFuncs{
		Open: func(ogrenet.Session) {
			clientOpen <- struct{}{}
		},
		Message: func(_ ogrenet.Session, msg ogrenet.Message) {
			echoed <- msg
		},
	})
	if err != nil {
		_ = ln.Close()
		_ = e.Close()
		benchmarkWaitDone(b, e.Done(), "benchmark Engine after Dial failure")
		b.Fatal(err)
	}
	benchmarkWaitSignal(b, serverOpen, "benchmark server OnOpen")
	benchmarkWaitSignal(b, clientOpen, "benchmark client OnOpen")

	return &tcpBenchmarkEcho{engine: e, ln: ln, client: client, echoed: echoed, errs: errs}
}

func (x *tcpBenchmarkEcho) close(b *testing.B) {
	b.Helper()
	if x == nil {
		return
	}
	if x.client != nil {
		_ = x.client.Close()
	}
	if x.ln != nil {
		_ = x.ln.Close()
	}
	if x.engine != nil {
		_ = x.engine.Close()
		benchmarkWaitDone(b, x.engine.Done(), "benchmark Engine.Done")
		if native, ok := x.engine.(*epollEngine); ok {
			benchmarkAssertEpollZeroInvariants(b, native)
		}
	}
}

func benchmarkWaitSignal[T any](b *testing.B, ch <-chan T, what string) T {
	b.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		b.Fatalf("timeout waiting for %s", what)
		var zero T
		return zero
	}
}

func benchmarkWaitDone(b *testing.B, done <-chan struct{}, what string) {
	b.Helper()
	benchmarkWaitSignal(b, done, what)
}

func benchmarkAssertEpollZeroInvariants(b *testing.B, e *epollEngine) {
	b.Helper()
	got := snapshotEpollEngineInvariants(e)
	want := epollEngineInvariantSnapshot{}
	if got != want {
		b.Fatalf("epoll benchmark teardown invariants=%+v, want %+v", got, want)
	}
}

func benchmarkReportLatency(b *testing.B, samples []time.Duration) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	slices.Sort(samples)
	percentile := func(p int) time.Duration {
		index := (len(samples)*p + 99) / 100
		if index <= 0 {
			index = 1
		}
		if index > len(samples) {
			index = len(samples)
		}
		return samples[index-1]
	}
	b.ReportMetric(float64(percentile(50).Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(percentile(95).Nanoseconds()), "p95-ns")
	b.ReportMetric(float64(percentile(99).Nanoseconds()), "p99-ns")
}

func BenchmarkTCPBackendEcho(b *testing.B) {
	for _, backend := range tcpBenchmarkBackends() {
		backend := backend
		for _, size := range []int{1 << 10, 4 << 10, 64 << 10} {
			size := size
			b.Run(fmt.Sprintf("%s/%dKiB", backend.name, size>>10), func(b *testing.B) {
				x := newTCPBenchmarkEcho(b, backend)
				defer x.close(b)
				payload := ogrenet.Bin(make([]byte, size))
				samples := make([]time.Duration, b.N)
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					started := time.Now()
					if err := x.client.Send(context.Background(), payload); err != nil {
						b.Fatal(err)
					}
					select {
					case got := <-x.echoed:
						if len(got.Data) != size {
							b.Fatalf("echo bytes=%d, want %d", len(got.Data), size)
						}
					case err := <-x.errs:
						b.Fatal(err)
					}
					samples[i] = time.Since(started)
				}
				b.StopTimer()
				benchmarkReportLatency(b, samples)
			})
		}
	}
}

func BenchmarkTCPBackendConnect(b *testing.B) {
	for _, backend := range tcpBenchmarkBackends() {
		backend := backend
		b.Run(backend.name, func(b *testing.B) {
			e := backend.new(b)
			accepted := make(chan ogrenet.Session, 1)
			ln, err := e.Listen(context.Background(), ogrenet.Endpoint{
				Scheme: ogrenet.SchemeTCP,
				Host:   "127.0.0.1",
				Port:   0,
			}, ogrenet.HandlerFuncs{
				Open: func(s ogrenet.Session) { accepted <- s },
			})
			if err != nil {
				_ = e.Close()
				benchmarkWaitDone(b, e.Done(), "connect benchmark Engine")
				b.Fatal(err)
			}
			defer func() {
				_ = ln.Close()
				_ = e.Close()
				benchmarkWaitDone(b, e.Done(), "connect benchmark Engine.Done")
				if native, ok := e.(*epollEngine); ok {
					benchmarkAssertEpollZeroInvariants(b, native)
				}
			}()

			samples := make([]time.Duration, b.N)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				started := time.Now()
				client, err := e.Dial(context.Background(), ln.Endpoint(), ogrenet.HandlerFuncs{})
				samples[i] = time.Since(started)
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				peer := benchmarkWaitSignal(b, accepted, "connect benchmark accepted Session")
				_ = client.Close()
				_ = peer.Close()
				benchmarkWaitDone(b, client.Done(), "connect benchmark client Done")
				benchmarkWaitDone(b, peer.Done(), "connect benchmark peer Done")
				b.StartTimer()
			}
			b.StopTimer()
			benchmarkReportLatency(b, samples)
		})
	}
}

func newEpollOneWayBenchmark(b *testing.B) (*epollEngine, ogrenet.Listener, ogrenet.Session, <-chan struct{}) {
	b.Helper()
	raw, err := NewEpoll(EpollConfig{Pollers: 1, CallbackWorkers: 4, CallbackQueue: 256})
	if err != nil {
		b.Fatal(err)
	}
	e := raw.(*epollEngine)
	received := make(chan struct{}, 1)
	serverOpen := make(chan struct{}, 1)
	clientOpen := make(chan struct{}, 1)
	ln, err := e.Listen(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeTCP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.HandlerFuncs{
		Open: func(ogrenet.Session) { serverOpen <- struct{}{} },
		Message: func(ogrenet.Session, ogrenet.Message) {
			received <- struct{}{}
		},
	})
	if err != nil {
		_ = e.Close()
		benchmarkWaitDone(b, e.Done(), "one-way benchmark Engine")
		b.Fatal(err)
	}
	client, err := e.Dial(context.Background(), ln.Endpoint(), ogrenet.HandlerFuncs{
		Open: func(ogrenet.Session) { clientOpen <- struct{}{} },
	})
	if err != nil {
		_ = ln.Close()
		_ = e.Close()
		benchmarkWaitDone(b, e.Done(), "one-way benchmark Engine")
		b.Fatal(err)
	}
	benchmarkWaitSignal(b, serverOpen, "one-way server OnOpen")
	benchmarkWaitSignal(b, clientOpen, "one-way client OnOpen")
	return e, ln, client, received
}

func closeEpollOneWayBenchmark(b *testing.B, e *epollEngine, ln ogrenet.Listener, client ogrenet.Session) {
	b.Helper()
	_ = client.Close()
	_ = ln.Close()
	_ = e.Close()
	benchmarkWaitDone(b, e.Done(), "one-way benchmark Engine.Done")
	benchmarkAssertEpollZeroInvariants(b, e)
}

func BenchmarkEpollSend(b *testing.B) {
	e, ln, client, received := newEpollOneWayBenchmark(b)
	defer closeEpollOneWayBenchmark(b, e, ln, client)
	payload := ogrenet.Bin(make([]byte, 4<<10))
	samples := make([]time.Duration, b.N)
	b.SetBytes(int64(len(payload.Data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		started := time.Now()
		if err := client.Send(context.Background(), payload); err != nil {
			b.Fatal(err)
		}
		samples[i] = time.Since(started)
		b.StopTimer()
		benchmarkWaitSignal(b, received, "EpollSend peer receive")
		b.StartTimer()
	}
	b.StopTimer()
	benchmarkReportLatency(b, samples)
}

func BenchmarkEpollTrySend(b *testing.B) {
	e, ln, client, received := newEpollOneWayBenchmark(b)
	defer closeEpollOneWayBenchmark(b, e, ln, client)
	payload := ogrenet.Bin(make([]byte, 4<<10))
	samples := make([]time.Duration, b.N)
	b.SetBytes(int64(len(payload.Data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		started := time.Now()
		if err := client.TrySend(payload); err != nil {
			b.Fatal(err)
		}
		samples[i] = time.Since(started)
		b.StopTimer()
		benchmarkWaitSignal(b, received, "EpollTrySend peer receive")
		b.StartTimer()
	}
	b.StopTimer()
	benchmarkReportLatency(b, samples)
}

func BenchmarkEpollEngineShutdownFanout(b *testing.B) {
	const connections = 64 // 128 native Sessions: dialing + accepted.
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		raw, err := NewEpoll(EpollConfig{Pollers: 4, CallbackWorkers: 16, CallbackQueue: 512})
		if err != nil {
			b.Fatal(err)
		}
		e := raw.(*epollEngine)
		serverOpen := make(chan ogrenet.Session, connections)
		clientOpen := make(chan struct{}, connections)
		ln, err := e.Listen(context.Background(), ogrenet.Endpoint{
			Scheme: ogrenet.SchemeTCP,
			Host:   "127.0.0.1",
			Port:   0,
		}, ogrenet.HandlerFuncs{
			Open: func(s ogrenet.Session) { serverOpen <- s },
		})
		if err != nil {
			_ = e.Close()
			benchmarkWaitDone(b, e.Done(), "shutdown fanout Engine")
			b.Fatal(err)
		}
		clients := make([]ogrenet.Session, 0, connections)
		for n := 0; n < connections; n++ {
			client, err := e.Dial(context.Background(), ln.Endpoint(), ogrenet.HandlerFuncs{
				Open: func(ogrenet.Session) { clientOpen <- struct{}{} },
			})
			if err != nil {
				_ = e.Close()
				benchmarkWaitDone(b, e.Done(), "shutdown fanout Engine")
				b.Fatal(err)
			}
			clients = append(clients, client)
		}
		for n := 0; n < connections; n++ {
			benchmarkWaitSignal(b, clientOpen, "shutdown fanout client OnOpen")
			benchmarkWaitSignal(b, serverOpen, "shutdown fanout server OnOpen")
		}
		_ = clients
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		b.StartTimer()
		err = e.Shutdown(ctx)
		b.StopTimer()
		cancel()
		if err != nil {
			_ = e.Close()
			benchmarkWaitDone(b, e.Done(), "shutdown fanout failed Engine")
			b.Fatal(err)
		}
		benchmarkWaitDone(b, e.Done(), "shutdown fanout Engine.Done")
		benchmarkAssertEpollZeroInvariants(b, e)
	}
}

func BenchmarkEpollSessionStatsSnapshot(b *testing.B) {
	e, ln, client, _ := newEpollOneWayBenchmark(b)
	defer closeEpollOneWayBenchmark(b, e, ln, client)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = client.Stats()
	}
}

func BenchmarkEpollObserverDisabledEmitPath(b *testing.B) {
	e, ln, client, _ := newEpollOneWayBenchmark(b)
	defer closeEpollOneWayBenchmark(b, e, ln, client)
	session, ok := client.(*epollSession)
	if !ok {
		b.Fatalf("client=%T, want *epollSession", client)
	}
	if e.observer != nil {
		b.Fatal("benchmark requires disabled Observer")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		session.observeNative(ogrenet.EventRead, 4096, nil)
	}
}
