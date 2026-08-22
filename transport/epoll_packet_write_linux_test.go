//go:build linux

package transport

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func newRawUDP4Peer(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func readRawUDPPacket(t *testing.T, conn *net.UDPConn, want []byte) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len(want)+32)
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("ReadFromUDP: %v", err)
	}
	if !bytes.Equal(buf[:n], want) {
		t.Fatalf("UDP payload=%q, want=%q", buf[:n], want)
	}
}

func TestEpollNativePacketConnectedSendWritesDatagram(t *testing.T) {
	peer := newRawUDP4Peer(t)
	addr := peer.LocalAddr().(*net.UDPAddr)
	e := newEpollPacketTestEngine(t, 1, WithWriteQueue(4), WithMaxQueuedBytes(1<<20))
	client, err := e.dialNativeUDP(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   uint16(addr.Port),
	}, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	payload := []byte("connected-write")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Send(ctx, ogrenet.Packet{Data: payload}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	readRawUDPPacket(t, peer, payload)

	stats := client.Stats()
	if stats.PacketsTX != 1 || stats.BytesTX != uint64(len(payload)) {
		t.Fatalf("TX stats=%+v, want 1 packet/%d bytes", stats, len(payload))
	}
	if stats.QueuedPackets != 0 || stats.QueuedBytes != 0 {
		t.Fatalf("write completion retained queue ownership: %+v", stats)
	}
}

func TestEpollNativePacketUnconnectedSendToWritesDatagram(t *testing.T) {
	peer := newRawUDP4Peer(t)
	e := newEpollPacketTestEngine(t, 1, WithWriteQueue(4), WithMaxQueuedBytes(1<<20))
	server, err := e.listenNativeUDP(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	payload := []byte("sendto-write")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.SendTo(ctx, peer.LocalAddr(), ogrenet.Packet{Data: payload}); err != nil {
		t.Fatalf("SendTo: %v", err)
	}
	readRawUDPPacket(t, peer, payload)

	stats := server.Stats()
	if stats.PacketsTX != 1 || stats.BytesTX != uint64(len(payload)) {
		t.Fatalf("TX stats=%+v, want 1 packet/%d bytes", stats, len(payload))
	}
	if stats.QueuedPackets != 0 || stats.QueuedBytes != 0 {
		t.Fatalf("SendTo completion retained queue ownership: %+v", stats)
	}
}
