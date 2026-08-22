//go:build linux

package transport

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

type udpBenchmarkBackend struct {
	name string
	new  func(*testing.B) ogrenet.Engine
}

func udpBenchmarkBackends() []udpBenchmarkBackend {
	return []udpBenchmarkBackend{
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
				e, err := NewEpoll(EpollConfig{Pollers: 1, CallbackWorkers: 4, CallbackQueue: 256})
				if err != nil {
					b.Fatal(err)
				}
				return e
			},
		},
	}
}

type udpBenchmarkEcho struct {
	engine ogrenet.Engine
	server ogrenet.PacketConn
	client ogrenet.PacketConn
	echoed chan int
	errs   chan error
}

func newUDPBenchmarkConnectedEcho(b *testing.B, backend udpBenchmarkBackend) *udpBenchmarkEcho {
	b.Helper()
	e := backend.new(b)
	echoed := make(chan int, 1)
	errs := make(chan error, 1)
	server, err := e.ListenPacket(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.PacketHandlerFuncs{
		Packet: func(pc ogrenet.PacketConn, peer net.Addr, packet ogrenet.Packet) {
			if err := pc.SendTo(context.Background(), peer, packet); err != nil {
				select {
				case errs <- err:
				default:
				}
			}
		},
	})
	if err != nil {
		_ = e.Close()
		benchmarkWaitDone(b, e.Done(), "UDP connected benchmark Engine after ListenPacket failure")
		b.Fatal(err)
	}
	client, err := e.DialPacket(context.Background(), server.Endpoint(), ogrenet.PacketHandlerFuncs{
		Packet: func(_ ogrenet.PacketConn, _ net.Addr, packet ogrenet.Packet) {
			echoed <- len(packet.Data)
		},
	})
	if err != nil {
		_ = server.Close()
		_ = e.Close()
		benchmarkWaitDone(b, e.Done(), "UDP connected benchmark Engine after DialPacket failure")
		b.Fatal(err)
	}
	return &udpBenchmarkEcho{engine: e, server: server, client: client, echoed: echoed, errs: errs}
}

func newUDPBenchmarkUnconnectedEcho(b *testing.B, backend udpBenchmarkBackend) *udpBenchmarkEcho {
	b.Helper()
	e := backend.new(b)
	echoed := make(chan int, 1)
	errs := make(chan error, 1)
	server, err := e.ListenPacket(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.PacketHandlerFuncs{
		Packet: func(pc ogrenet.PacketConn, peer net.Addr, packet ogrenet.Packet) {
			if err := pc.SendTo(context.Background(), peer, packet); err != nil {
				select {
				case errs <- err:
				default:
				}
			}
		},
	})
	if err != nil {
		_ = e.Close()
		benchmarkWaitDone(b, e.Done(), "UDP unconnected benchmark Engine after server failure")
		b.Fatal(err)
	}
	client, err := e.ListenPacket(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.PacketHandlerFuncs{
		Packet: func(_ ogrenet.PacketConn, _ net.Addr, packet ogrenet.Packet) {
			echoed <- len(packet.Data)
		},
	})
	if err != nil {
		_ = server.Close()
		_ = e.Close()
		benchmarkWaitDone(b, e.Done(), "UDP unconnected benchmark Engine after client failure")
		b.Fatal(err)
	}
	return &udpBenchmarkEcho{engine: e, server: server, client: client, echoed: echoed, errs: errs}
}

func (x *udpBenchmarkEcho) close(b *testing.B) {
	b.Helper()
	if x == nil {
		return
	}
	if x.client != nil {
		_ = x.client.Close()
	}
	if x.server != nil {
		_ = x.server.Close()
	}
	if x.engine != nil {
		_ = x.engine.Close()
		benchmarkWaitDone(b, x.engine.Done(), "UDP benchmark Engine.Done")
		if native, ok := x.engine.(*epollEngine); ok {
			benchmarkAssertEpollZeroInvariants(b, native)
		}
	}
}

func BenchmarkUDPBackendConnectedRoundTrip(b *testing.B) {
	for _, backend := range udpBenchmarkBackends() {
		backend := backend
		for _, size := range []int{64, 1200} {
			size := size
			b.Run(fmt.Sprintf("%s/%dB", backend.name, size), func(b *testing.B) {
				x := newUDPBenchmarkConnectedEcho(b, backend)
				defer x.close(b)
				payload := ogrenet.Packet{Data: make([]byte, size)}
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
						if got != size {
							b.Fatalf("UDP connected echo bytes=%d, want %d", got, size)
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

func BenchmarkUDPBackendUnconnectedRoundTrip(b *testing.B) {
	for _, backend := range udpBenchmarkBackends() {
		backend := backend
		for _, size := range []int{64, 1200} {
			size := size
			b.Run(fmt.Sprintf("%s/%dB", backend.name, size), func(b *testing.B) {
				x := newUDPBenchmarkUnconnectedEcho(b, backend)
				defer x.close(b)
				payload := ogrenet.Packet{Data: make([]byte, size)}
				samples := make([]time.Duration, b.N)
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					started := time.Now()
					if err := x.client.SendTo(context.Background(), x.server.LocalAddr(), payload); err != nil {
						b.Fatal(err)
					}
					select {
					case got := <-x.echoed:
						if got != size {
							b.Fatalf("UDP unconnected echo bytes=%d, want %d", got, size)
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

func BenchmarkUDPBackendConnectedSetup(b *testing.B) {
	raw, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		b.Fatal(err)
	}
	defer raw.Close()
	endpoint := udpBenchmarkEndpoint(raw.LocalAddr())
	for _, backend := range udpBenchmarkBackends() {
		backend := backend
		b.Run(backend.name, func(b *testing.B) {
			e := backend.new(b)
			defer func() {
				_ = e.Close()
				benchmarkWaitDone(b, e.Done(), "UDP setup benchmark Engine.Done")
				if native, ok := e.(*epollEngine); ok {
					benchmarkAssertEpollZeroInvariants(b, native)
				}
			}()
			samples := make([]time.Duration, b.N)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				started := time.Now()
				pc, err := e.DialPacket(context.Background(), endpoint, ogrenet.PacketHandlerFuncs{})
				samples[i] = time.Since(started)
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				_ = pc.Close()
				benchmarkWaitDone(b, pc.Done(), "UDP setup PacketConn.Done")
				b.StartTimer()
			}
			b.StopTimer()
			benchmarkReportLatency(b, samples)
		})
	}
}

type epollUDPWriteBenchmark struct {
	engine *epollEngine
	pc     ogrenet.PacketConn
	peer   *net.UDPConn
}

func newEpollUDPWriteBenchmark(b *testing.B, connected bool) *epollUDPWriteBenchmark {
	b.Helper()
	peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		b.Fatal(err)
	}
	raw, err := NewEpoll(EpollConfig{Pollers: 1, CallbackWorkers: 4, CallbackQueue: 256})
	if err != nil {
		_ = peer.Close()
		b.Fatal(err)
	}
	e := raw.(*epollEngine)
	var pc ogrenet.PacketConn
	if connected {
		pc, err = e.DialPacket(context.Background(), udpBenchmarkEndpoint(peer.LocalAddr()), ogrenet.PacketHandlerFuncs{})
	} else {
		pc, err = e.ListenPacket(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 0}, ogrenet.PacketHandlerFuncs{})
	}
	if err != nil {
		_ = peer.Close()
		_ = e.Close()
		benchmarkWaitDone(b, e.Done(), "Epoll UDP write benchmark Engine")
		b.Fatal(err)
	}
	if err := peer.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		b.Fatal(err)
	}
	return &epollUDPWriteBenchmark{engine: e, pc: pc, peer: peer}
}

func (x *epollUDPWriteBenchmark) close(b *testing.B) {
	b.Helper()
	if x == nil {
		return
	}
	if x.pc != nil {
		_ = x.pc.Close()
	}
	if x.peer != nil {
		_ = x.peer.Close()
	}
	if x.engine != nil {
		_ = x.engine.Close()
		benchmarkWaitDone(b, x.engine.Done(), "Epoll UDP write benchmark Engine.Done")
		benchmarkAssertEpollZeroInvariants(b, x.engine)
	}
}

func benchmarkEpollUDPWrite(b *testing.B, connected, try bool) {
	b.Helper()
	x := newEpollUDPWriteBenchmark(b, connected)
	defer x.close(b)
	payload := ogrenet.Packet{Data: make([]byte, 1200)}
	buf := make([]byte, 2048)
	samples := make([]time.Duration, b.N)
	b.SetBytes(int64(len(payload.Data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		started := time.Now()
		var err error
		switch {
		case connected && try:
			err = x.pc.TrySend(payload)
		case connected:
			err = x.pc.Send(context.Background(), payload)
		case try:
			err = x.pc.TrySendTo(x.peer.LocalAddr(), payload)
		default:
			err = x.pc.SendTo(context.Background(), x.peer.LocalAddr(), payload)
		}
		samples[i] = time.Since(started)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		n, _, err := x.peer.ReadFromUDP(buf)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(payload.Data) {
			b.Fatalf("UDP write bytes=%d, want %d", n, len(payload.Data))
		}
		b.StartTimer()
	}
	b.StopTimer()
	benchmarkReportLatency(b, samples)
}

func BenchmarkEpollUDPSend(b *testing.B)       { benchmarkEpollUDPWrite(b, true, false) }
func BenchmarkEpollUDPTrySend(b *testing.B)    { benchmarkEpollUDPWrite(b, true, true) }
func BenchmarkEpollUDPSendTo(b *testing.B)     { benchmarkEpollUDPWrite(b, false, false) }
func BenchmarkEpollUDPTrySendTo(b *testing.B)  { benchmarkEpollUDPWrite(b, false, true) }

func BenchmarkEpollUDPEngineShutdownFanout(b *testing.B) {
	const connections = 64
	peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		b.Fatal(err)
	}
	defer peer.Close()
	endpoint := udpBenchmarkEndpoint(peer.LocalAddr())
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		raw, err := NewEpoll(EpollConfig{Pollers: 4, CallbackWorkers: 16, CallbackQueue: 256})
		if err != nil {
			b.Fatal(err)
		}
		e := raw.(*epollEngine)
		clients := make([]ogrenet.PacketConn, 0, connections)
		for n := 0; n < connections; n++ {
			pc, err := e.DialPacket(context.Background(), endpoint, ogrenet.PacketHandlerFuncs{})
			if err != nil {
				_ = e.Close()
				benchmarkWaitDone(b, e.Done(), "UDP shutdown fanout failed Engine")
				b.Fatal(err)
			}
			clients = append(clients, pc)
		}
		_ = clients
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		b.StartTimer()
		err = e.Shutdown(ctx)
		b.StopTimer()
		cancel()
		if err != nil {
			_ = e.Close()
			benchmarkWaitDone(b, e.Done(), "UDP shutdown fanout failed Engine")
			b.Fatal(err)
		}
		benchmarkWaitDone(b, e.Done(), "UDP shutdown fanout Engine.Done")
		benchmarkAssertEpollZeroInvariants(b, e)
	}
}

func BenchmarkEpollUDPPacketStatsSnapshot(b *testing.B) {
	x := newEpollUDPWriteBenchmark(b, true)
	defer x.close(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = x.pc.Stats()
	}
}

func BenchmarkEpollUDPObserverDisabledEmitPath(b *testing.B) {
	x := newEpollUDPWriteBenchmark(b, true)
	defer x.close(b)
	pc, ok := x.pc.(*epollPacketConn)
	if !ok {
		b.Fatalf("PacketConn=%T, want *epollPacketConn", x.pc)
	}
	if x.engine.observer != nil {
		b.Fatal("benchmark requires disabled Observer")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pc.observeNativePacket(ogrenet.EventRead, 1200, nil, nil)
	}
}

func udpBenchmarkEndpoint(addr net.Addr) ogrenet.Endpoint {
	udp := addr.(*net.UDPAddr)
	return ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: uint16(udp.Port)}
}
