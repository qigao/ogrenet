//go:build linux

package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func newEpollPacketPair(t *testing.T, opts ...Option) (*epollEngine, *epollPacketConn, *epollPacketConn) {
	t.Helper()
	e := newEpollPacketTestEngine(t, 1, opts...)
	server, err := e.listenNativeUDP(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	client, err := e.dialNativeUDP(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   server.Endpoint().Port,
	}, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	return e, server, client
}

func TestEpollNativePacketSendMethodLegalityAndOversize(t *testing.T) {
	_, server, client := newEpollPacketPair(t, WithMaxDatagramBytes(4))
	packet := ogrenet.Packet{Data: []byte("ping")}

	if err := server.Send(context.Background(), packet); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("unconnected Send error=%v, want ErrNotConnected", err)
	}
	if err := server.TrySend(packet); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("unconnected TrySend error=%v, want ErrNotConnected", err)
	}
	if err := server.SendTo(context.Background(), nil, packet); !errors.Is(err, ErrPeerRequired) {
		t.Fatalf("SendTo nil peer error=%v, want ErrPeerRequired", err)
	}
	if err := server.TrySendTo(nil, packet); !errors.Is(err, ErrPeerRequired) {
		t.Fatalf("TrySendTo nil peer error=%v, want ErrPeerRequired", err)
	}

	wrongPeer := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(server.Endpoint().Port) + 1}
	if err := client.TrySendTo(wrongPeer, packet); !errors.Is(err, ErrPeerMismatch) {
		t.Fatalf("connected TrySendTo wrong peer error=%v, want ErrPeerMismatch", err)
	}

	oversize := ogrenet.Packet{Data: []byte("12345")}
	err := client.TrySend(oversize)
	if !errors.Is(err, ErrDatagramTooLarge) {
		t.Fatalf("oversize TrySend error=%v, want ErrDatagramTooLarge", err)
	}
	var opErr *Error
	if !errors.As(err, &opErr) || opErr.Op != OpSend || opErr.Protocol != ogrenet.SchemeUDP || opErr.Kind != ErrorTooLarge {
		t.Fatalf("oversize operational error=%#v", opErr)
	}
	stats := client.Stats()
	if stats.QueuedPackets != 0 || stats.QueuedBytes != 0 {
		t.Fatalf("oversize acquired queue ownership: %+v", stats)
	}

	if err := client.TrySendTo(client.RemoteAddr(), packet); err != nil {
		t.Fatalf("connected TrySendTo same peer: %v", err)
	}
}

func TestEpollNativePacketTrySendAdmissionAndBackpressure(t *testing.T) {
	backpressure := make(chan ogrenet.Event, 4)
	_, _, client := newEpollPacketPair(t,
		WithWriteQueue(1),
		WithMaxQueuedBytes(64),
		WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) {
			if event.Kind == ogrenet.EventBackpressure && event.Resource == ogrenet.ResourcePacketConn {
				select {
				case backpressure <- event:
				default:
				}
			}
		})),
	)

	packet := ogrenet.Packet{Data: []byte("first")}
	if err := client.TrySend(packet); err != nil {
		t.Fatalf("first TrySend: %v", err)
	}
	stats := client.Stats()
	if stats.QueuedPackets != 1 || stats.QueuedBytes != uint64(len(packet.Data)) {
		t.Fatalf("retained queue stats=%+v, want 1 packet/%d bytes", stats, len(packet.Data))
	}
	if got := client.engine.Stats().GlobalQueuedBytes; got != uint64(len(packet.Data)) {
		t.Fatalf("Engine GlobalQueuedBytes=%d, want %d", got, len(packet.Data))
	}

	err := client.TrySend(ogrenet.Packet{Data: []byte("second")})
	if !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("second TrySend error=%v, want ErrWouldBlock", err)
	}
	var opErr *Error
	if !errors.As(err, &opErr) || opErr.Op != OpSend || opErr.Kind != ErrorBackpressure {
		t.Fatalf("backpressure operational error=%#v", opErr)
	}
	if got := client.Stats().Backpressure; got != 1 {
		t.Fatalf("Backpressure=%d, want 1", got)
	}

	select {
	case event := <-backpressure:
		if event.ResourceID != client.id || event.Protocol != ogrenet.SchemeUDP || !errors.Is(event.Err, ErrWouldBlock) {
			t.Fatalf("EventBackpressure=%+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("missing EventBackpressure")
	}
	select {
	case extra := <-backpressure:
		t.Fatalf("duplicate EventBackpressure=%+v", extra)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestEpollNativePacketSendCancellationAfterPublicationRetainsOwnership(t *testing.T) {
	_, _, client := newEpollPacketPair(t, WithWriteQueue(2), WithMaxQueuedBytes(64))
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	packet := ogrenet.Packet{Data: []byte("owned")}
	go func() {
		result <- client.Send(ctx, packet)
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats := client.Stats()
		if stats.QueuedPackets == 1 && stats.QueuedBytes == uint64(len(packet.Data)) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	stats := client.Stats()
	if stats.QueuedPackets != 1 || stats.QueuedBytes != uint64(len(packet.Data)) {
		cancel()
		t.Fatalf("Send was not published before deadline: %+v", stats)
	}

	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Send after cancellation error=%v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Send did not return caller cancellation")
	}

	stats = client.Stats()
	if stats.QueuedPackets != 1 || stats.QueuedBytes != uint64(len(packet.Data)) {
		t.Fatalf("caller cancellation revoked admitted datagram: %+v", stats)
	}
}
