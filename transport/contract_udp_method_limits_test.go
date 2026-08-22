package transport_test

import (
	"errors"
	"net"
	"testing"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/transport"
)

func runUDPMethodLegalityContract(t *testing.T, f engineFactory) {
	t.Helper()
	ctx, cancel := contractContext(t)
	defer cancel()

	e := f.new(t)
	server, err := e.ListenPacket(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 0}, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := e.DialPacket(ctx, server.Endpoint(), ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	packet := ogrenet.Packet{Data: []byte("method-legality")}
	if err := server.Send(ctx, packet); !errors.Is(err, transport.ErrNotConnected) {
		t.Fatalf("unconnected Send=%v, want ErrNotConnected", err)
	}
	if err := server.TrySend(packet); !errors.Is(err, transport.ErrNotConnected) {
		t.Fatalf("unconnected TrySend=%v, want ErrNotConnected", err)
	}
	if err := server.SendTo(ctx, nil, packet); !errors.Is(err, transport.ErrPeerRequired) {
		t.Fatalf("SendTo(nil)=%v, want ErrPeerRequired", err)
	}
	if err := server.TrySendTo(nil, packet); !errors.Is(err, transport.ErrPeerRequired) {
		t.Fatalf("TrySendTo(nil)=%v, want ErrPeerRequired", err)
	}

	remote, ok := client.RemoteAddr().(*net.UDPAddr)
	if !ok || remote == nil {
		t.Fatalf("connected remote type=%T", client.RemoteAddr())
	}
	wrong := cloneUDPContractAddr(remote)
	if wrong.Port == 65535 {
		wrong.Port--
	} else {
		wrong.Port++
	}
	if err := client.SendTo(ctx, wrong, packet); !errors.Is(err, transport.ErrPeerMismatch) {
		t.Fatalf("connected SendTo(wrong peer)=%v, want ErrPeerMismatch", err)
	}
	if err := client.TrySendTo(wrong, packet); !errors.Is(err, transport.ErrPeerMismatch) {
		t.Fatalf("connected TrySendTo(wrong peer)=%v, want ErrPeerMismatch", err)
	}

	udp := server.Endpoint()
	if _, err := e.Listen(ctx, udp, nil); !errors.Is(err, transport.ErrProtocolMismatch) {
		t.Fatalf("Listen(udp)=%v, want ErrProtocolMismatch", err)
	}
	if _, err := e.Dial(ctx, udp, nil); !errors.Is(err, transport.ErrProtocolMismatch) {
		t.Fatalf("Dial(udp)=%v, want ErrProtocolMismatch", err)
	}
	tcp := ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 1}
	if _, err := e.ListenPacket(ctx, tcp, nil); !errors.Is(err, transport.ErrProtocolMismatch) {
		t.Fatalf("ListenPacket(tcp)=%v, want ErrProtocolMismatch", err)
	}
	if _, err := e.DialPacket(ctx, tcp, nil); !errors.Is(err, transport.ErrProtocolMismatch) {
		t.Fatalf("DialPacket(tcp)=%v, want ErrProtocolMismatch", err)
	}
}

func runUDPQuotaContract(t *testing.T, f engineFactory) {
	t.Helper()
	ctx, cancel := contractContext(t)
	defer cancel()
	peer := openUDPContractPeer(t)
	endpoint := udpContractEndpoint(peer.LocalAddr())

	t.Run("oversize-send", func(t *testing.T) {
		e := f.new(t, transport.WithMaxDatagramBytes(4))
		pc, err := e.DialPacket(ctx, endpoint, ogrenet.PacketHandlerFuncs{})
		if err != nil {
			t.Fatal(err)
		}
		defer pc.Close()

		err = pc.TrySend(ogrenet.Packet{Data: []byte("12345")})
		if !errors.Is(err, transport.ErrDatagramTooLarge) {
			t.Fatalf("TrySend oversize=%v, want ErrDatagramTooLarge", err)
		}
		assertUDPContractTransportError(t, err, transport.OpSend, transport.ErrorTooLarge)
		stats := pc.Stats()
		if stats.QueuedPackets != 0 || stats.QueuedBytes != 0 || stats.PacketsTX != 0 || stats.BytesTX != 0 || stats.DroppedDatagrams != 0 {
			t.Fatalf("oversize send changed ownership/stats: %+v", stats)
		}
	})

	t.Run("local-queued-bytes", func(t *testing.T) {
		e := f.new(t, transport.WithMaxQueuedBytes(4))
		pc, err := e.DialPacket(ctx, endpoint, ogrenet.PacketHandlerFuncs{})
		if err != nil {
			t.Fatal(err)
		}
		defer pc.Close()

		err = pc.TrySend(ogrenet.Packet{Data: []byte("12345")})
		if !errors.Is(err, transport.ErrFrameExceedsQueueBudget) || !errors.Is(err, transport.ErrResourceExhausted) {
			t.Fatalf("TrySend local quota=%v, want queue-budget + resource-exhausted", err)
		}
		assertUDPContractTransportError(t, err, transport.OpSend, transport.ErrorResourceExhausted)
		stats := pc.Stats()
		if stats.QueuedPackets != 0 || stats.QueuedBytes != 0 || e.Stats().GlobalQueuedBytes != 0 {
			t.Fatalf("local quota rejection leaked ownership: packet=%+v engine=%+v", stats, e.Stats())
		}
	})

	t.Run("engine-global-queued-bytes", func(t *testing.T) {
		e := f.new(t, transport.WithLimits(transport.Limits{MaxQueuedBytesTotal: 4}))
		pc, err := e.DialPacket(ctx, endpoint, ogrenet.PacketHandlerFuncs{})
		if err != nil {
			t.Fatal(err)
		}
		defer pc.Close()

		err = pc.TrySend(ogrenet.Packet{Data: []byte("12345")})
		if !errors.Is(err, transport.ErrResourceExhausted) {
			t.Fatalf("TrySend global quota=%v, want ErrResourceExhausted", err)
		}
		var limitErr *transport.LimitError
		if !errors.As(err, &limitErr) || limitErr.Kind != transport.LimitQueuedBytes {
			t.Fatalf("global quota error=%#v, want LimitQueuedBytes", err)
		}
		assertUDPContractTransportError(t, err, transport.OpSend, transport.ErrorResourceExhausted)
		stats := pc.Stats()
		engineStats := e.Stats()
		if stats.QueuedPackets != 0 || stats.QueuedBytes != 0 || engineStats.GlobalQueuedBytes != 0 {
			t.Fatalf("global quota rejection leaked ownership: packet=%+v engine=%+v", stats, engineStats)
		}
		if engineStats.RejectedQueuedBytes != 1 {
			t.Fatalf("RejectedQueuedBytes=%d, want 1", engineStats.RejectedQueuedBytes)
		}
	})
}

func assertUDPContractTransportError(t *testing.T, err error, op transport.Op, kind transport.ErrorKind) {
	t.Helper()
	var opErr *transport.Error
	if !errors.As(err, &opErr) {
		t.Fatalf("error=%#v, want *transport.Error", err)
	}
	if opErr.Op != op || opErr.Protocol != ogrenet.SchemeUDP || opErr.Kind != kind {
		t.Fatalf("operational error=%+v, want op=%s protocol=udp kind=%s", opErr, op, kind)
	}
}

func openUDPContractPeer(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func udpContractEndpoint(addr net.Addr) ogrenet.Endpoint {
	udp := addr.(*net.UDPAddr)
	return ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: uint16(udp.Port)}
}

func cloneUDPContractAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	out := *addr
	out.IP = append(net.IP(nil), addr.IP...)
	return &out
}
