package transport

import (
	"net"
	"sync"
	"testing"

	"github.com/qigao/ogrenet"
)

var (
	benchmarkObserverBoolSink bool
	benchmarkEngineStatsSink ogrenet.EngineStats
	benchmarkSessionStatsSink ogrenet.SessionStats
	benchmarkPacketStatsSink ogrenet.PacketConnStats
)

func BenchmarkObserverDisabledEmitPath(b *testing.B) {
	e, err := New()
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if e.observer != nil {
			benchmarkObserverBoolSink = e.observer.emit(ogrenet.Event{
				Kind:     ogrenet.EventRead,
				Resource: ogrenet.ResourceSession,
			})
		}
	}
}

func BenchmarkObserverEnabledNoop(b *testing.B) {
	d := newObserverDispatcher(ogrenet.ObserverFunc(func(ogrenet.Event) {}), defaultObserverBuffer)
	defer d.stop()
	event := ogrenet.Event{Kind: ogrenet.EventRead, Resource: ogrenet.ResourceSession}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkObserverBoolSink = d.emit(event)
	}
}

func BenchmarkObserverSaturatedProducer(b *testing.B) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	d := newObserverDispatcher(ogrenet.ObserverFunc(func(ogrenet.Event) {
		once.Do(func() { close(entered) })
		<-release
	}), 1)
	defer func() {
		close(release)
		d.stop()
	}()

	event := ogrenet.Event{Kind: ogrenet.EventRead, Resource: ogrenet.ResourceSession}
	if !d.emit(event) {
		b.Fatal("failed to enqueue observer warmup event")
	}
	<-entered
	if !d.emit(event) {
		b.Fatal("failed to fill observer queue")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkObserverBoolSink = d.emit(event)
	}
}

func BenchmarkSessionStatsSnapshot(b *testing.B) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	c := &conn{
		id:         1,
		protocol:   ogrenet.SchemeTCP,
		raw:        left,
		stats:      newSessionCounters(),
		frameSlots: make(chan struct{}, 1),
		quota:      newByteQuota(1024),
	}
	c.stats.bytesRX.Store(64)
	c.stats.bytesTX.Store(128)
	c.stats.messagesRX.Store(1)
	c.stats.messagesTX.Store(2)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkSessionStatsSink = c.Stats()
	}
}

func BenchmarkPacketStatsSnapshot(b *testing.B) {
	raw, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		b.Fatal(err)
	}
	defer raw.Close()

	p := &packetConn{
		id:    1,
		conn:  raw,
		stats: newPacketCounters(),
		slots: make(chan struct{}, 1),
		quota: newByteQuota(1024),
	}
	p.stats.bytesRX.Store(64)
	p.stats.bytesTX.Store(128)
	p.stats.packetsRX.Store(1)
	p.stats.packetsTX.Store(2)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkPacketStatsSink = p.Stats()
	}
}

func BenchmarkEngineStatsSnapshot(b *testing.B) {
	e, err := New()
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkEngineStatsSink = e.Stats()
	}
}
