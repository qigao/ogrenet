package transport_test

import (
	"context"
	"net"
	"testing"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/transport"
)

func runUDPObserverContract(t *testing.T, f engineFactory) {
	t.Helper()
	ctx, cancel := contractContext(t)
	defer cancel()

	events := make(chan ogrenet.Event, 64)
	e := f.new(t,
		transport.WithMaxDatagramBytes(4),
		transport.WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) {
			select {
			case events <- event:
			default:
			}
		})),
	)

	serverPackets := make(chan ogrenet.Packet, 2)
	server, err := e.ListenPacket(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 0}, ogrenet.PacketHandlerFuncs{
		Packet: func(_ ogrenet.PacketConn, _ net.Addr, packet ogrenet.Packet) {
			serverPackets <- ogrenet.Packet{Data: append([]byte(nil), packet.Data...)}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := e.DialPacket(ctx, server.Endpoint(), ogrenet.PacketHandlerFuncs{})
	if err != nil {
		_ = server.Close()
		t.Fatal(err)
	}

	payload := []byte("abc")
	if err := client.Send(ctx, ogrenet.Packet{Data: payload}); err != nil {
		t.Fatal(err)
	}
	packet := recvContract(t, ctx, serverPackets, "observer UDP receive")
	if string(packet.Data) != string(payload) {
		t.Fatalf("received payload=%q, want %q", packet.Data, payload)
	}

	clientStats := client.Stats()
	serverStats := server.Stats()
	if clientStats.ResourceID == 0 || serverStats.ResourceID == 0 || clientStats.ResourceID == serverStats.ResourceID {
		t.Fatalf("invalid packet resource IDs client=%d server=%d", clientStats.ResourceID, serverStats.ResourceID)
	}
	if clientStats.BytesTX != uint64(len(payload)) || clientStats.PacketsTX != 1 {
		t.Fatalf("client stats before observer write=%+v", clientStats)
	}
	if serverStats.BytesRX != uint64(len(payload)) || serverStats.PacketsRX != 1 {
		t.Fatalf("server stats before observer read=%+v", serverStats)
	}

	seenWrite := false
	seenRead := false
	for !seenWrite || !seenRead {
		select {
		case event := <-events:
			switch {
			case event.Kind == ogrenet.EventWrite && event.ResourceID == clientStats.ResourceID:
				assertUDPContractEvent(t, event, ogrenet.EventWrite, clientStats.ResourceID, uint64(len(payload)))
				seenWrite = true
			case event.Kind == ogrenet.EventRead && event.ResourceID == serverStats.ResourceID:
				assertUDPContractEvent(t, event, ogrenet.EventRead, serverStats.ResourceID, uint64(len(payload)))
				seenRead = true
			}
		case <-ctx.Done():
			t.Fatalf("waiting for UDP write/read events: %v", context.Cause(ctx))
		}
	}

	udpAddr, ok := server.LocalAddr().(*net.UDPAddr)
	if !ok || udpAddr == nil {
		t.Fatalf("server local address type=%T", server.LocalAddr())
	}
	raw, err := net.DialUDP("udp4", nil, cloneUDPContractAddr(udpAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Write([]byte("12345678")); err != nil {
		t.Fatal(err)
	}

	drop := waitUDPContractEvent(t, ctx, events, "server EventDrop", func(event ogrenet.Event) bool {
		return event.Kind == ogrenet.EventDrop && event.ResourceID == serverStats.ResourceID
	})
	assertUDPContractEvent(t, drop, ogrenet.EventDrop, serverStats.ResourceID, 8)
	serverStats = server.Stats()
	if serverStats.DroppedDatagrams != 1 || serverStats.BytesRX != uint64(len(payload)) || serverStats.PacketsRX != 1 {
		t.Fatalf("oversize receive stats=%+v", serverStats)
	}
	select {
	case extra := <-serverPackets:
		t.Fatalf("oversize datagram reached handler: %q", extra.Data)
	default:
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	waitContractDone(t, ctx, client.Done(), "observer client close")
	waitContractDone(t, ctx, server.Done(), "observer server close")

	seenClientClose := false
	seenServerClose := false
	for !seenClientClose || !seenServerClose {
		select {
		case event := <-events:
			if event.Kind != ogrenet.EventClose {
				continue
			}
			switch event.ResourceID {
			case clientStats.ResourceID:
				assertUDPContractEvent(t, event, ogrenet.EventClose, clientStats.ResourceID, 0)
				seenClientClose = true
			case serverStats.ResourceID:
				assertUDPContractEvent(t, event, ogrenet.EventClose, serverStats.ResourceID, 0)
				seenServerClose = true
			default:
				t.Fatalf("close event leaked from unrelated resource: %+v", event)
			}
		case <-ctx.Done():
			t.Fatalf("waiting for UDP close events: %v", context.Cause(ctx))
		}
	}
}

func waitUDPContractEvent(t *testing.T, ctx context.Context, events <-chan ogrenet.Event, what string, match func(ogrenet.Event) bool) ogrenet.Event {
	t.Helper()
	for {
		select {
		case event := <-events:
			if match(event) {
				return event
			}
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", what, context.Cause(ctx))
			return ogrenet.Event{}
		}
	}
}

func assertUDPContractEvent(t *testing.T, event ogrenet.Event, kind ogrenet.EventKind, resourceID, bytes uint64) {
	t.Helper()
	if event.Kind != kind || event.Resource != ogrenet.ResourcePacketConn || event.ResourceID != resourceID || event.Protocol != ogrenet.SchemeUDP || event.Bytes != bytes || event.Err != nil {
		t.Fatalf("event=%+v, want kind=%v packet=%d bytes=%d clean", event, kind, resourceID, bytes)
	}
}
