package transport_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/transport"
)

func runUDPTimeoutErrorContract(t *testing.T, f engineFactory) {
	t.Helper()

	t.Run("connected-read-idle", func(t *testing.T) {
		ctx, cancel := contractContext(t)
		defer cancel()
		peer := openUDPContractPeer(t)
		closeErr := make(chan error, 1)
		e := f.new(t, transport.WithTimeouts(transport.Timeouts{ReadIdle: 40 * time.Millisecond}))
		pc, err := e.DialPacket(ctx, udpContractEndpoint(peer.LocalAddr()), ogrenet.PacketHandlerFuncs{
			Close: func(_ ogrenet.PacketConn, err error) { closeErr <- err },
		})
		if err != nil {
			t.Fatal(err)
		}

		waitContractDone(t, ctx, pc.Done(), "connected UDP ReadIdle")
		terminal := pc.Err()
		assertUDPContractTransportError(t, terminal, transport.OpReceive, transport.ErrorTimeout)
		var timeout *transport.TimeoutError
		if !errors.As(terminal, &timeout) || timeout.Kind != transport.TimeoutReadIdle || !errors.Is(terminal, transport.ErrTimeout) {
			t.Fatalf("terminal error=%#v, want TimeoutReadIdle", terminal)
		}
		if got := recvContract(t, ctx, closeErr, "connected UDP timeout OnClose"); got != terminal {
			t.Fatalf("OnClose error=%v, PacketConn.Err=%v; want same terminal owner", got, terminal)
		}
	})

	t.Run("unconnected-timeout-immunity", func(t *testing.T) {
		ctx, cancel := contractContext(t)
		defer cancel()
		e := f.new(t, transport.WithTimeouts(transport.Timeouts{
			ReadIdle:       30 * time.Millisecond,
			ConnectionIdle: 30 * time.Millisecond,
			MaxLifetime:    30 * time.Millisecond,
		}))
		pc, err := e.ListenPacket(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 0}, ogrenet.PacketHandlerFuncs{})
		if err != nil {
			t.Fatal(err)
		}

		timer := time.NewTimer(120 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-pc.Done():
			t.Fatalf("unconnected ListenPacket auto-closed: %v", pc.Err())
		case <-timer.C:
		}
		if err := pc.Close(); err != nil {
			t.Fatal(err)
		}
		waitContractDone(t, ctx, pc.Done(), "unconnected UDP explicit close")
		if err := pc.Err(); err != nil {
			t.Fatalf("unconnected explicit close Err=%v, want nil", err)
		}
	})

	t.Run("exact-caller-cancellation", func(t *testing.T) {
		ctx, cancel := contractContext(t)
		defer cancel()
		peer := openUDPContractPeer(t)
		e := f.new(t)
		pc, err := e.DialPacket(ctx, udpContractEndpoint(peer.LocalAddr()), ogrenet.PacketHandlerFuncs{})
		if err != nil {
			t.Fatal(err)
		}
		defer pc.Close()

		cause := errors.New("udp contract caller cancellation")
		cctx, ccancel := context.WithCancelCause(context.Background())
		ccancel(cause)
		if err := pc.Send(cctx, ogrenet.Packet{Data: []byte("not-admitted")}); err != cause {
			t.Fatalf("Send canceled error=%v, want exact cause %v", err, cause)
		}
		stats := pc.Stats()
		if stats.PacketsTX != 0 || stats.BytesTX != 0 || stats.QueuedPackets != 0 || stats.QueuedBytes != 0 {
			t.Fatalf("pre-admission cancellation changed packet ownership/stats: %+v", stats)
		}
	})
}
