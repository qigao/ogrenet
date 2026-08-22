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

type udpBackpressureFactory struct {
	name string
	new  func(*testing.T, ...Option) ogrenet.Engine
}

func TestUDPBackpressureParity(t *testing.T) {
	factories := []udpBackpressureFactory{
		{name: "portable", new: func(t *testing.T, opts ...Option) ogrenet.Engine {
			t.Helper()
			e, err := New(opts...)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			return e
		}},
		{name: "epoll", new: func(t *testing.T, opts ...Option) ogrenet.Engine {
			t.Helper()
			e, err := NewEpoll(EpollConfig{Pollers: 1}, opts...)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			return e
		}},
	}

	for _, factory := range factories {
		factory := factory
		t.Run(factory.name+"/TrySend", func(t *testing.T) {
			runUDPBackpressureContract(t, factory, true)
		})
		t.Run(factory.name+"/TrySendTo", func(t *testing.T) {
			runUDPBackpressureContract(t, factory, false)
		})
	}
}

func runUDPBackpressureContract(t *testing.T, factory udpBackpressureFactory, connected bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	raw, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	events := make(chan ogrenet.Event, 8)
	e := factory.new(t, WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) {
		select {
		case events <- event:
		default:
		}
	})))

	var pc ogrenet.PacketConn
	if connected {
		pc, err = e.DialPacket(ctx, udpBackpressureEndpoint(raw.LocalAddr()), ogrenet.PacketHandlerFuncs{})
	} else {
		pc, err = e.ListenPacket(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 0}, ogrenet.PacketHandlerFuncs{})
	}
	if err != nil {
		t.Fatal(err)
	}

	slots, release := saturateUDPParitySlots(t, pc)
	packet := ogrenet.Packet{Data: []byte("blocked")}
	if connected {
		err = pc.TrySend(packet)
	} else {
		err = pc.TrySendTo(raw.LocalAddr(), packet)
	}
	if !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("backpressure error=%v, want ErrWouldBlock", err)
	}
	var opErr *Error
	if !errors.As(err, &opErr) || opErr.Op != OpSend || opErr.Protocol != ogrenet.SchemeUDP || opErr.Kind != ErrorBackpressure {
		t.Fatalf("backpressure operational error=%#v", opErr)
	}

	stats := pc.Stats()
	if stats.Backpressure != 1 || stats.QueuedPackets != uint64(slots) || stats.QueuedBytes != 0 {
		t.Fatalf("backpressure stats=%+v, slots=%d", stats, slots)
	}
	if got := e.Stats().GlobalQueuedBytes; got != 0 {
		t.Fatalf("backpressure leaked Engine GlobalQueuedBytes=%d", got)
	}

	var event ogrenet.Event
	for {
		select {
		case event = <-events:
			if event.Kind == ogrenet.EventBackpressure && event.ResourceID == stats.ResourceID {
				goto observed
			}
		case <-ctx.Done():
			t.Fatalf("waiting for EventBackpressure: %v", context.Cause(ctx))
		}
	}

observed:
	if event.Resource != ogrenet.ResourcePacketConn || event.Protocol != ogrenet.SchemeUDP || event.Bytes != 0 || !errors.Is(event.Err, ErrWouldBlock) {
		t.Fatalf("backpressure event=%+v", event)
	}
	var eventErr *Error
	if !errors.As(event.Err, &eventErr) || eventErr.Op != OpSend || eventErr.Kind != ErrorBackpressure {
		t.Fatalf("backpressure event error=%#v", eventErr)
	}

	release()
	if stats := pc.Stats(); stats.QueuedPackets != 0 || stats.QueuedBytes != 0 {
		t.Fatalf("released saturation ownership=%+v", stats)
	}
	if err := pc.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-pc.Done():
	case <-ctx.Done():
		t.Fatalf("waiting for PacketConn close: %v", context.Cause(ctx))
	}
}

func saturateUDPParitySlots(t *testing.T, pc ogrenet.PacketConn) (int, func()) {
	t.Helper()
	var slots chan struct{}
	switch p := pc.(type) {
	case *packetConn:
		slots = p.slots
	case *epollPacketConn:
		slots = p.slots
	default:
		t.Fatalf("unsupported PacketConn type %T", pc)
	}
	n := cap(slots)
	if n == 0 {
		t.Fatal("packet slot capacity is zero")
	}
	for i := 0; i < n; i++ {
		slots <- struct{}{}
	}
	return n, func() {
		for i := 0; i < n; i++ {
			<-slots
		}
	}
}

func udpBackpressureEndpoint(addr net.Addr) ogrenet.Endpoint {
	udp := addr.(*net.UDPAddr)
	return ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: uint16(udp.Port)}
}
