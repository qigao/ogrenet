package transport_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/transport"
)

func runUDPGracefulContract(t *testing.T, f engineFactory) {
	t.Helper()

	t.Run("engine-shutdown-drains-admitted", func(t *testing.T) {
		ctx, cancel := contractContext(t)
		defer cancel()
		peer := openUDPContractPeer(t)
		if err := peer.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}

		e := f.new(t, transport.WithWriteQueue(2), transport.WithMaxQueuedBytes(128))
		pc, err := e.DialPacket(ctx, udpContractEndpoint(peer.LocalAddr()), ogrenet.PacketHandlerFuncs{})
		if err != nil {
			t.Fatal(err)
		}

		payload := []byte("admitted-before-engine-drain")
		if err := pc.TrySend(ogrenet.Packet{Data: payload}); err != nil {
			t.Fatalf("TrySend before Shutdown: %v", err)
		}
		if err := e.Shutdown(ctx); err != nil {
			t.Fatalf("Engine.Shutdown: %v", err)
		}

		buf := make([]byte, 128)
		n, _, err := peer.ReadFromUDP(buf)
		if err != nil {
			t.Fatalf("read admitted datagram after Shutdown: %v", err)
		}
		if !bytes.Equal(buf[:n], payload) {
			t.Fatalf("drained payload=%q, want %q", buf[:n], payload)
		}
		waitContractDone(t, ctx, pc.Done(), "UDP PacketConn after Engine.Shutdown")
		waitContractDone(t, ctx, e.Done(), "Engine.Done after UDP drain")
		if err := pc.Err(); err != nil {
			t.Fatalf("gracefully drained PacketConn.Err=%v, want nil", err)
		}
		packetStats := pc.Stats()
		if packetStats.QueuedPackets != 0 || packetStats.QueuedBytes != 0 {
			t.Fatalf("final packet ownership=%+v", packetStats)
		}
		engineStats := e.Stats()
		if engineStats.GlobalQueuedBytes != 0 || engineStats.ActiveConnections != 0 || engineStats.DrainingConnections != 0 {
			t.Fatalf("final engine ownership=%+v", engineStats)
		}
		if err := pc.TrySend(ogrenet.Packet{Data: []byte("after-shutdown")}); !errors.Is(err, transport.ErrClosed) {
			t.Fatalf("TrySend after Engine.Shutdown=%v, want ErrClosed", err)
		}
	})

	t.Run("engine-close-aborts", func(t *testing.T) {
		ctx, cancel := contractContext(t)
		defer cancel()
		peer := openUDPContractPeer(t)
		e := f.new(t)
		pc, err := e.DialPacket(ctx, udpContractEndpoint(peer.LocalAddr()), ogrenet.PacketHandlerFuncs{})
		if err != nil {
			t.Fatal(err)
		}

		if err := e.Close(); err != nil {
			t.Fatalf("Engine.Close: %v", err)
		}
		waitContractDone(t, ctx, pc.Done(), "UDP PacketConn after Engine.Close")
		waitContractDone(t, ctx, e.Done(), "Engine.Done after Close")
		if err := pc.Err(); err != nil {
			t.Fatalf("explicit Engine.Close PacketConn.Err=%v, want nil", err)
		}
		if err := pc.TrySend(ogrenet.Packet{Data: []byte("after-close")}); !errors.Is(err, transport.ErrClosed) {
			t.Fatalf("TrySend after Engine.Close=%v, want ErrClosed", err)
		}
	})
}
