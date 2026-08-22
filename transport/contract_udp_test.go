package transport_test

import (
	"context"
	"net"
	"testing"

	"github.com/qigao/ogrenet"
)

func runUDPContract(t *testing.T, f engineFactory) {
	t.Helper()
	ctx, cancel := contractContext(t)
	defer cancel()

	e := f.new(t)
	serverSendErr := make(chan error, 1)
	clientPackets := make(chan ogrenet.Packet, 1)
	serverClosed := make(chan error, 1)
	clientClosed := make(chan error, 1)

	server, err := e.ListenPacket(ctx, ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.PacketHandlerFuncs{
		Packet: func(c ogrenet.PacketConn, peerAddr net.Addr, packet ogrenet.Packet) {
			serverSendErr <- c.SendTo(context.Background(), peerAddr, packet)
		},
		Close: func(_ ogrenet.PacketConn, err error) {
			serverClosed <- err
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	client, err := e.DialPacket(ctx, server.Endpoint(), ogrenet.PacketHandlerFuncs{
		Packet: func(_ ogrenet.PacketConn, _ net.Addr, packet ogrenet.Packet) {
			clientPackets <- packet
		},
		Close: func(_ ogrenet.PacketConn, err error) {
			clientClosed <- err
		},
	})
	if err != nil {
		_ = server.Close()
		t.Fatal(err)
	}

	payload := []byte("contract-packet")
	if err := client.Send(ctx, ogrenet.Packet{Data: payload}); err != nil {
		t.Fatal(err)
	}
	if err := recvContract(t, ctx, serverSendErr, "server udp echo write"); err != nil {
		t.Fatal(err)
	}
	packet := recvContract(t, ctx, clientPackets, "client udp echo packet")
	if string(packet.Data) != string(payload) {
		t.Fatalf("payload=%q", packet.Data)
	}

	clientStats := client.Stats()
	if clientStats.BytesTX != uint64(len(payload)) || clientStats.PacketsTX != 1 {
		t.Fatalf("client tx stats=%+v", clientStats)
	}
	if clientStats.BytesRX != uint64(len(payload)) || clientStats.PacketsRX != 1 {
		t.Fatalf("client rx stats=%+v", clientStats)
	}
	serverStats := server.Stats()
	if serverStats.BytesRX != uint64(len(payload)) || serverStats.PacketsRX != 1 {
		t.Fatalf("server rx stats=%+v", serverStats)
	}
	if serverStats.BytesTX != uint64(len(payload)) || serverStats.PacketsTX != 1 {
		t.Fatalf("server tx stats=%+v", serverStats)
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	waitContractDone(t, ctx, client.Done(), "client udp close")
	waitContractDone(t, ctx, server.Done(), "server udp close")
	if err := recvContract(t, ctx, clientClosed, "client udp OnClose"); err != nil {
		t.Fatalf("client close callback err=%v", err)
	}
	if err := recvContract(t, ctx, serverClosed, "server udp OnClose"); err != nil {
		t.Fatalf("server close callback err=%v", err)
	}
	if err := client.Err(); err != nil {
		t.Fatalf("client terminal err=%v", err)
	}
	if err := server.Err(); err != nil {
		t.Fatalf("server terminal err=%v", err)
	}

	t.Run("method-legality", func(t *testing.T) { runUDPMethodLegalityContract(t, f) })
	t.Run("limits", func(t *testing.T) { runUDPQuotaContract(t, f) })
	t.Run("observer", func(t *testing.T) { runUDPObserverContract(t, f) })
	t.Run("timeout-error", func(t *testing.T) { runUDPTimeoutErrorContract(t, f) })
	t.Run("graceful", func(t *testing.T) { runUDPGracefulContract(t, f) })
}
