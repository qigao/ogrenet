//go:build linux

package transport

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func newEpollPacketCallbackTestEngine(t *testing.T, cfg EpollConfig, opts ...Option) *epollEngine {
	t.Helper()
	raw, err := NewEpoll(cfg, opts...)
	if err != nil {
		t.Fatal(err)
	}
	e := raw.(*epollEngine)
	t.Cleanup(func() {
		_ = e.Close()
		waitEpollEngineDone(t, e.Done())
	})
	return e
}

func listenEpollPacketCallbackTestServer(t *testing.T, e *epollEngine, h ogrenet.PacketHandler) *epollPacketConn {
	t.Helper()
	p, err := e.listenNativeUDP(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   0,
	}, h)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func newEpollPacketRawPeer(t *testing.T) *net.UDPConn {
	t.Helper()
	p, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func writeEpollPacketRaw(t *testing.T, p *net.UDPConn, target net.Addr, data []byte) {
	t.Helper()
	addr, ok := target.(*net.UDPAddr)
	if !ok {
		t.Fatalf("target type=%T, want *net.UDPAddr", target)
	}
	if _, err := p.WriteToUDP(data, addr); err != nil {
		t.Fatal(err)
	}
}

type epollPacketRetainHandler struct {
	count    int
	retained ogrenet.Packet
	done     chan struct{}
}

func (h *epollPacketRetainHandler) OnPacket(_ ogrenet.PacketConn, _ net.Addr, packet ogrenet.Packet) {
	h.count++
	if h.count == 1 {
		h.retained = packet
	}
	if h.count == 32 {
		close(h.done)
	}
}
func (*epollPacketRetainHandler) OnClose(ogrenet.PacketConn, error) {}

func TestEpollNativePacketETDrainRetainsPayloadAcrossLaterReceives(t *testing.T) {
	e := newEpollPacketCallbackTestEngine(t, EpollConfig{Pollers: 1, CallbackWorkers: 1, CallbackQueue: 4})
	h := &epollPacketRetainHandler{done: make(chan struct{})}
	server := listenEpollPacketCallbackTestServer(t, e, h)
	peer := newEpollPacketRawPeer(t)

	first := []byte("retained-first-datagram")
	writeEpollPacketRaw(t, peer, server.LocalAddr(), first)
	for i := 1; i < 32; i++ {
		writeEpollPacketRaw(t, peer, server.LocalAddr(), bytes.Repeat([]byte{byte(i)}, 64+i))
	}
	waitNativeSendSignal(t, h.done, "32 UDP callbacks from one ET readiness stream")
	if h.count != 32 {
		t.Fatalf("OnPacket count=%d, want 32", h.count)
	}
	if !bytes.Equal(h.retained.Data, first) {
		t.Fatalf("retained packet mutated: got=%q want=%q", h.retained.Data, first)
	}
	stats := server.Stats()
	if stats.PacketsRX != 32 {
		t.Fatalf("PacketsRX=%d, want 32", stats.PacketsRX)
	}
}

type epollPacketSerialHandler struct {
	inFlight atomic.Int32
	count    atomic.Int32
	err      chan error
	done     chan struct{}
	once     sync.Once
}

func (h *epollPacketSerialHandler) OnPacket(ogrenet.PacketConn, net.Addr, ogrenet.Packet) {
	if got := h.inFlight.Add(1); got != 1 {
		select {
		case h.err <- fmt.Errorf("concurrent OnPacket callbacks=%d", got):
		default:
		}
	}
	time.Sleep(time.Millisecond)
	h.inFlight.Add(-1)
	if h.count.Add(1) == 50 {
		h.once.Do(func() { close(h.done) })
	}
}
func (*epollPacketSerialHandler) OnClose(ogrenet.PacketConn, error) {}

func TestEpollNativePacketCallbacksNeverOverlapPerConn(t *testing.T) {
	e := newEpollPacketCallbackTestEngine(t, EpollConfig{Pollers: 1, CallbackWorkers: 4, CallbackQueue: 16})
	h := &epollPacketSerialHandler{err: make(chan error, 1), done: make(chan struct{})}
	server := listenEpollPacketCallbackTestServer(t, e, h)
	peer := newEpollPacketRawPeer(t)
	for i := 0; i < 50; i++ {
		writeEpollPacketRaw(t, peer, server.LocalAddr(), []byte{byte(i)})
	}
	waitNativeSendSignal(t, h.done, "serialized UDP callbacks")
	select {
	case err := <-h.err:
		t.Fatal(err)
	default:
	}
	if got := h.count.Load(); got != 50 {
		t.Fatalf("OnPacket count=%d, want 50", got)
	}
}

type epollPacketBlockingHandler struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *epollPacketBlockingHandler) OnPacket(ogrenet.PacketConn, net.Addr, ogrenet.Packet) {
	h.once.Do(func() { close(h.entered) })
	<-h.release
}
func (*epollPacketBlockingHandler) OnClose(ogrenet.PacketConn, error) {}

func TestEpollNativePacketBlockedHandlerDoesNotBlockSameReactorConn(t *testing.T) {
	e := newEpollPacketCallbackTestEngine(t, EpollConfig{Pollers: 1, CallbackWorkers: 2, CallbackQueue: 4})
	blocked := &epollPacketBlockingHandler{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-blocked.release:
		default:
			close(blocked.release)
		}
	})
	serverA := listenEpollPacketCallbackTestServer(t, e, blocked)
	progress := make(chan ogrenet.Packet, 1)
	serverB := listenEpollPacketCallbackTestServer(t, e, ogrenet.PacketHandlerFuncs{
		Packet: func(_ ogrenet.PacketConn, _ net.Addr, packet ogrenet.Packet) { progress <- packet },
	})
	peer := newEpollPacketRawPeer(t)

	writeEpollPacketRaw(t, peer, serverA.LocalAddr(), []byte("block-A"))
	waitNativeSendSignal(t, blocked.entered, "blocking UDP handler A")
	writeEpollPacketRaw(t, peer, serverB.LocalAddr(), []byte("progress-B"))
	select {
	case packet := <-progress:
		if string(packet.Data) != "progress-B" {
			t.Fatalf("PacketConn B payload=%q", packet.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked PacketConn A handler stalled PacketConn B")
	}
	close(blocked.release)
}

type epollPacketBlockingWorkerTask struct {
	entered chan struct{}
	release chan struct{}
}

func (t *epollPacketBlockingWorkerTask) runEpollWorkerTask() {
	close(t.entered)
	<-t.release
}

func TestEpollNativePacketCallbackCapacityReleaseRetriesReadableConn(t *testing.T) {
	e := newEpollPacketCallbackTestEngine(t, EpollConfig{Pollers: 1, CallbackWorkers: 1, CallbackQueue: 1})
	packets := make(chan ogrenet.Packet, 1)
	server := listenEpollPacketCallbackTestServer(t, e, ogrenet.PacketHandlerFuncs{
		Packet: func(_ ogrenet.PacketConn, _ net.Addr, packet ogrenet.Packet) { packets <- packet },
	})

	first := &epollPacketBlockingWorkerTask{entered: make(chan struct{}), release: make(chan struct{})}
	second := &epollPacketBlockingWorkerTask{entered: make(chan struct{}), release: make(chan struct{})}
	if !e.callbacks.tryReserve() {
		t.Fatal("reserve running UDP worker task")
	}
	e.callbacks.submitReserved(first)
	waitNativeSendSignal(t, first.entered, "running UDP worker task")
	if !e.callbacks.tryReserve() {
		t.Fatal("reserve queued UDP worker task")
	}
	e.callbacks.submitReserved(second)

	peer := newEpollPacketRawPeer(t)
	writeEpollPacketRaw(t, peer, server.LocalAddr(), []byte("retry-after-capacity"))
	deadline := time.Now().Add(2 * time.Second)
	for !server.reactor.hasWorkerBlocked.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !server.reactor.hasWorkerBlocked.Load() {
		close(first.release)
		close(second.release)
		t.Fatal("readable PacketConn never entered workerBlocked")
	}
	close(first.release)
	waitNativeSendSignal(t, second.entered, "queued UDP worker task promotion")
	close(second.release)

	select {
	case packet := <-packets:
		if string(packet.Data) != "retry-after-capacity" {
			t.Fatalf("retried UDP payload=%q", packet.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker capacity release did not retry readable PacketConn")
	}
}

func TestEpollNativePacketOversizeDropUsesActualDatagramSize(t *testing.T) {
	events := make(chan ogrenet.Event, 16)
	delivered := make(chan struct{}, 1)
	e := newEpollPacketCallbackTestEngine(t,
		EpollConfig{Pollers: 1, CallbackWorkers: 1, CallbackQueue: 4},
		WithMaxDatagramBytes(4),
		WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) {
			if event.Resource == ogrenet.ResourcePacketConn {
				events <- event
			}
		})),
	)
	server := listenEpollPacketCallbackTestServer(t, e, ogrenet.PacketHandlerFuncs{
		Packet: func(ogrenet.PacketConn, net.Addr, ogrenet.Packet) { delivered <- struct{}{} },
	})
	peer := newEpollPacketRawPeer(t)
	writeEpollPacketRaw(t, peer, server.LocalAddr(), []byte("12345678"))

	var drop ogrenet.Event
	deadline := time.After(2 * time.Second)
	for drop.Kind != ogrenet.EventDrop {
		select {
		case event := <-events:
			if event.Kind == ogrenet.EventDrop && event.ResourceID == server.id {
				drop = event
			}
		case <-deadline:
			t.Fatal("missing UDP EventDrop")
		}
	}
	if drop.Bytes != 8 || drop.Err != nil || drop.Protocol != ogrenet.SchemeUDP {
		t.Fatalf("EventDrop=%+v, want actual size 8", drop)
	}
	stats := server.Stats()
	if stats.DroppedDatagrams != 1 || stats.PacketsRX != 0 || stats.BytesRX != 0 {
		t.Fatalf("oversize stats=%+v", stats)
	}
	select {
	case <-delivered:
		t.Fatal("oversize UDP datagram reached Handler")
	case <-time.After(25 * time.Millisecond):
	}
}

type epollPacketCloseBarrierHandler struct {
	entered chan struct{}
	release chan struct{}
	err     chan error
	once    sync.Once
}

func (*epollPacketCloseBarrierHandler) OnPacket(ogrenet.PacketConn, net.Addr, ogrenet.Packet) {}
func (h *epollPacketCloseBarrierHandler) OnClose(_ ogrenet.PacketConn, err error) {
	h.once.Do(func() { close(h.entered) })
	h.err <- err
	<-h.release
}

func TestEpollNativePacketDoneWaitsForOnClose(t *testing.T) {
	h := &epollPacketCloseBarrierHandler{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		err:     make(chan error, 1),
	}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(h.release) }) })
	e := newEpollPacketCallbackTestEngine(t, EpollConfig{Pollers: 1, CallbackWorkers: 1, CallbackQueue: 4})
	server := listenEpollPacketCallbackTestServer(t, e, h)

	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	waitNativeSendSignal(t, h.entered, "UDP OnClose entry")
	select {
	case <-server.Done():
		t.Fatal("PacketConn.Done closed before OnClose returned")
	case <-time.After(25 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(h.release) })
	waitEpollPacketDone(t, server.Done(), "UDP Done after OnClose")
	select {
	case err := <-h.err:
		if err != nil {
			t.Fatalf("clean UDP OnClose error=%v", err)
		}
	default:
		t.Fatal("missing UDP OnClose error publication")
	}
}
