package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestPacketStatsCountPayloadPacketsAndEvents(t *testing.T) {
	serverEvents := make(chan ogrenet.Event, 32)
	clientEvents := make(chan ogrenet.Event, 32)
	server, err := New(WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) { serverEvents <- event })))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := New(WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) { clientEvents <- event })))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	packets := make(chan ogrenet.Packet, 1)
	serverPacket, err := server.ListenPacket(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 0}, ogrenet.PacketHandlerFuncs{
		Packet: func(_ ogrenet.PacketConn, _ net.Addr, packet ogrenet.Packet) { packets <- packet },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer serverPacket.Close()
	clientPacket, err := client.DialPacket(context.Background(), serverPacket.Endpoint(), ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer clientPacket.Close()

	payload := []byte("udp-application-payload")
	if err := clientPacket.Send(context.Background(), ogrenet.Packet{Data: payload}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case got := <-packets:
		if string(got.Data) != string(payload) {
			t.Fatalf("payload=%q want %q", got.Data, payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("packet receive timeout")
	}

	clientStats := clientPacket.Stats()
	serverStats := serverPacket.Stats()
	if clientStats.ResourceID == 0 || serverStats.ResourceID == 0 {
		t.Fatalf("resource IDs client=%d server=%d", clientStats.ResourceID, serverStats.ResourceID)
	}
	if clientStats.BytesTX != uint64(len(payload)) || clientStats.PacketsTX != 1 {
		t.Fatalf("client stats=%+v", clientStats)
	}
	if serverStats.BytesRX != uint64(len(payload)) || serverStats.PacketsRX != 1 {
		t.Fatalf("server stats=%+v", serverStats)
	}
	if clientStats.Age <= 0 || serverStats.Age <= 0 {
		t.Fatalf("packet ages client=%v server=%v", clientStats.Age, serverStats.Age)
	}

	writeEvent := waitObservedEvent(t, clientEvents, func(event ogrenet.Event) bool {
		return event.Kind == ogrenet.EventWrite && event.Resource == ogrenet.ResourcePacketConn && event.ResourceID == clientStats.ResourceID
	})
	if writeEvent.Bytes != uint64(len(payload)) || writeEvent.Protocol != ogrenet.SchemeUDP || writeEvent.Err != nil {
		t.Fatalf("write event=%+v", writeEvent)
	}
	readEvent := waitObservedEvent(t, serverEvents, func(event ogrenet.Event) bool {
		return event.Kind == ogrenet.EventRead && event.Resource == ogrenet.ResourcePacketConn && event.ResourceID == serverStats.ResourceID
	})
	if readEvent.Bytes != uint64(len(payload)) || readEvent.Protocol != ogrenet.SchemeUDP || readEvent.Err != nil {
		t.Fatalf("read event=%+v", readEvent)
	}
}

func TestPacketStatsUseQueueOwnershipSources(t *testing.T) {
	server, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	serverPacket, err := server.ListenPacket(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 0}, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer serverPacket.Close()
	client, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	clientPacket, err := client.DialPacket(context.Background(), serverPacket.Endpoint(), ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer clientPacket.Close()
	p := clientPacket.(*packetConn)

	p.slots <- struct{}{}
	if err := p.quota.tryAcquire(17); err != nil {
		t.Fatal(err)
	}
	stats := p.Stats()
	if stats.QueuedPackets != 1 || stats.QueuedBytes != 17 {
		t.Fatalf("queue stats=%+v want packets=1 bytes=17", stats)
	}
	p.quota.release(17)
	p.releaseSlot()
}

func TestPacketTrySendBackpressureCountsOnce(t *testing.T) {
	server, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	serverPacket, err := server.ListenPacket(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 0}, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer serverPacket.Close()
	client, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	clientPacket, err := client.DialPacket(context.Background(), serverPacket.Endpoint(), ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer clientPacket.Close()
	p := clientPacket.(*packetConn)

	for i := 0; i < cap(p.slots); i++ {
		p.slots <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(p.slots); i++ {
			<-p.slots
		}
	}()
	err = p.TrySend(ogrenet.Packet{Data: []byte("blocked")})
	if !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("TrySend=%v want ErrWouldBlock", err)
	}
	if got := p.Stats().Backpressure; got != 1 {
		t.Fatalf("backpressure=%d want 1", got)
	}
}

func TestPacketOversizeReceiveCountsDropNotRX(t *testing.T) {
	events := make(chan ogrenet.Event, 32)
	delivered := make(chan struct{}, 1)
	server, err := New(
		WithMaxDatagramBytes(4),
		WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) { events <- event })),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	packetConn, err := server.ListenPacket(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 0}, ogrenet.PacketHandlerFuncs{
		Packet: func(ogrenet.PacketConn, net.Addr, ogrenet.Packet) { delivered <- struct{}{} },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer packetConn.Close()

	udpAddr, ok := packetConn.LocalAddr().(*net.UDPAddr)
	if !ok {
		t.Fatalf("local addr type=%T", packetConn.LocalAddr())
	}
	raw, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Write([]byte("12345678")); err != nil {
		t.Fatal(err)
	}

	resourceID := packetConn.Stats().ResourceID
	drop := waitObservedEvent(t, events, func(event ogrenet.Event) bool {
		return event.Kind == ogrenet.EventDrop && event.Resource == ogrenet.ResourcePacketConn && event.ResourceID == resourceID
	})
	if drop.Bytes != 8 || drop.Protocol != ogrenet.SchemeUDP || drop.Err != nil {
		t.Fatalf("drop event=%+v", drop)
	}
	stats := packetConn.Stats()
	if stats.DroppedDatagrams != 1 || stats.BytesRX != 0 || stats.PacketsRX != 0 {
		t.Fatalf("drop stats=%+v", stats)
	}
	select {
	case <-delivered:
		t.Fatal("oversize datagram was delivered")
	default:
	}
}

func TestPacketCloseEventSeesFrozenFinalStats(t *testing.T) {
	events := make(chan ogrenet.Event, 16)
	engine, err := New(WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) { events <- event })))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	packetConn, err := engine.ListenPacket(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 0}, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	resourceID := packetConn.Stats().ResourceID
	if err := packetConn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-packetConn.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("packet close timeout")
	}
	age := packetConn.Stats().Age
	if age <= 0 {
		t.Fatalf("final age=%v", age)
	}
	time.Sleep(time.Millisecond)
	if got := packetConn.Stats().Age; got != age {
		t.Fatalf("age changed after Done got=%v want=%v", got, age)
	}
	closeEvent := waitObservedEvent(t, events, func(event ogrenet.Event) bool {
		return event.Kind == ogrenet.EventClose && event.Resource == ogrenet.ResourcePacketConn && event.ResourceID == resourceID
	})
	if closeEvent.Err != nil {
		t.Fatalf("clean close event error=%v", closeEvent.Err)
	}
}
