//go:build linux

package transport

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

type epollPacketCloseOrderHandler struct {
	packetEntered chan struct{}
	packetRelease chan struct{}
	closeEntered  chan struct{}
	overlap       chan struct{}
	inFlight      atomic.Int32
	packetOnce    sync.Once
	closeOnce     sync.Once
}

func (h *epollPacketCloseOrderHandler) OnPacket(ogrenet.PacketConn, net.Addr, ogrenet.Packet) {
	h.inFlight.Add(1)
	h.packetOnce.Do(func() { close(h.packetEntered) })
	<-h.packetRelease
	h.inFlight.Add(-1)
}

func (h *epollPacketCloseOrderHandler) OnClose(ogrenet.PacketConn, error) {
	if h.inFlight.Load() != 0 {
		select {
		case h.overlap <- struct{}{}:
		default:
		}
	}
	h.closeOnce.Do(func() { close(h.closeEntered) })
}

func TestEpollNativePacketOnCloseWaitsForEarlierPacketCallback(t *testing.T) {
	closeEvents := make(chan ogrenet.Event, 4)
	h := &epollPacketCloseOrderHandler{
		packetEntered: make(chan struct{}),
		packetRelease: make(chan struct{}),
		closeEntered:  make(chan struct{}),
		overlap:       make(chan struct{}, 1),
	}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(h.packetRelease) }) })

	e := newEpollPacketCallbackTestEngine(t,
		EpollConfig{Pollers: 1, CallbackWorkers: 2, CallbackQueue: 4},
		WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) {
			if event.Kind == ogrenet.EventClose && event.Resource == ogrenet.ResourcePacketConn {
				select {
				case closeEvents <- event:
				default:
				}
			}
		})),
	)
	server := listenEpollPacketCallbackTestServer(t, e, h)
	peer := newEpollPacketRawPeer(t)
	writeEpollPacketRaw(t, peer, server.LocalAddr(), []byte("packet-before-close"))
	waitNativeSendSignal(t, h.packetEntered, "blocking UDP OnPacket")

	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	var closeEvent ogrenet.Event
	select {
	case closeEvent = <-closeEvents:
	case <-time.After(2 * time.Second):
		t.Fatal("missing EventClose while earlier OnPacket was blocked")
	}
	if closeEvent.ResourceID != server.id || closeEvent.Err != nil {
		t.Fatalf("EventClose=%+v", closeEvent)
	}
	ageAtClose := server.Stats().Age
	if ageAtClose <= 0 {
		t.Fatalf("frozen age at EventClose=%v", ageAtClose)
	}

	select {
	case <-h.closeEntered:
		t.Fatal("OnClose entered while earlier OnPacket was still running")
	case <-time.After(25 * time.Millisecond):
	}
	select {
	case <-server.Done():
		t.Fatal("PacketConn.Done closed while earlier OnPacket was still running")
	case <-time.After(25 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(h.packetRelease) })
	waitNativeSendSignal(t, h.closeEntered, "serialized UDP OnClose")
	select {
	case <-h.overlap:
		t.Fatal("OnPacket and OnClose overlapped")
	default:
	}
	waitEpollPacketDone(t, server.Done(), "UDP Done after serialized OnClose")
	if got := server.Stats().Age; got != ageAtClose {
		t.Fatalf("PacketConn age changed after EventClose: got=%v want=%v", got, ageAtClose)
	}
}
