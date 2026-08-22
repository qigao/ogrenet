//go:build linux

package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

type epollPacketReceiveSnapshot struct {
	peer  net.Addr
	data  []byte
	stats ogrenet.PacketConnStats
}

func TestEpollNativePacketReceiveDeliversPeerAfterRXStats(t *testing.T) {
	received := make(chan epollPacketReceiveSnapshot, 1)
	e := newEpollPacketTestEngine(t, 1)
	server, err := e.listenNativeUDP(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.PacketHandlerFuncs{
		Packet: func(c ogrenet.PacketConn, peer net.Addr, packet ogrenet.Packet) {
			received <- epollPacketReceiveSnapshot{
				peer:  peer,
				data:  packet.Data,
				stats: c.Stats(),
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	payload := []byte("native-udp-receive")
	if _, err := peer.WriteToUDP(payload, server.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-received:
		if got.peer == nil || got.peer.String() != peer.LocalAddr().String() {
			t.Fatalf("OnPacket peer=%v, want %v", got.peer, peer.LocalAddr())
		}
		if string(got.data) != string(payload) {
			t.Fatalf("OnPacket payload=%q, want %q", got.data, payload)
		}
		if got.stats.PacketsRX != 1 || got.stats.BytesRX != uint64(len(payload)) {
			t.Fatalf("RX stats at OnPacket entry=%+v, want 1 packet/%d bytes", got.stats, len(payload))
		}
	case <-time.After(time.Second):
		t.Fatal("native UDP readable edge did not deliver OnPacket")
	}
}
