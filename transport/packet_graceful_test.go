package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestPacketConnRequestDrainDeliversAcceptedDatagrams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	received := make(chan byte, 64)
	serverSocket, err := server.ListenPacket(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 0}, ogrenet.PacketHandlerFuncs{
		Packet: func(_ ogrenet.PacketConn, _ net.Addr, packet ogrenet.Packet) {
			if len(packet.Data) == 1 {
				received <- packet.Data[0]
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer serverSocket.Close()

	client, err := New(WithWriteQueue(32))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	public, err := client.DialPacket(ctx, serverSocket.Endpoint(), ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	p := public.(*packetConn)

	accepted := make([]byte, 0, 32)
	for i := 0; i < 32; i++ {
		value := byte(i)
		err := public.TrySend(ogrenet.Packet{Data: []byte{value}})
		switch {
		case err == nil:
			accepted = append(accepted, value)
		case errors.Is(err, ErrWouldBlock):
			continue
		default:
			t.Fatalf("TrySend(%d): %v", i, err)
		}
	}
	if len(accepted) == 0 {
		t.Fatal("no UDP datagrams accepted")
	}

	p.requestDrain()
	if err := public.TrySend(ogrenet.Packet{Data: []byte{0xff}}); !errors.Is(err, ErrClosed) {
		t.Fatalf("TrySend after requestDrain = %v, want ErrClosed", err)
	}

	got := make(map[byte]bool, len(accepted))
	for len(got) < len(accepted) {
		select {
		case value := <-received:
			got[value] = true
		case <-ctx.Done():
			t.Fatalf("received %d/%d accepted datagrams: %v", len(got), len(accepted), ctx.Err())
		}
	}
	for _, want := range accepted {
		if !got[want] {
			t.Fatalf("accepted datagram %d was not delivered", want)
		}
	}

	select {
	case <-public.Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := public.Err(); err != nil {
		t.Fatalf("PacketConn.Err = %v, want nil", err)
	}
	if snap := client.admissionSnapshot(); snap.GlobalQueuedBytes != 0 {
		t.Fatalf("GlobalQueuedBytes after drain = %d, want 0", snap.GlobalQueuedBytes)
	}
}

func TestPacketConnCloseRemainsImmediateAbort(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	serverSocket, err := server.ListenPacket(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 0}, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer serverSocket.Close()

	client, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	public, err := client.DialPacket(ctx, serverSocket.Endpoint(), ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	if err := public.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-public.Done():
	case <-time.After(time.Second):
		t.Fatal("PacketConn.Close did not converge promptly")
	}
	if err := public.Err(); err != nil {
		t.Fatalf("PacketConn.Err after explicit Close = %v, want nil", err)
	}
}
