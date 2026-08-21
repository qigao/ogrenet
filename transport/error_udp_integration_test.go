package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestTransportErrorUDPDatagramTooLargeUsesOpSend(t *testing.T) {
	peer := startUDPBlackhole(t)
	e, err := New(WithMaxDatagramBytes(4))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	p, err := e.DialPacket(context.Background(), udpEndpoint(peer), ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	err = p.TrySend(ogrenet.Packet{Data: []byte("12345")})
	assertTransportError(t, err, OpSend, ogrenet.SchemeUDP, ErrorTooLarge)
	if !errors.Is(err, ErrDatagramTooLarge) {
		t.Fatalf("too-large datagram does not match ErrDatagramTooLarge: %v", err)
	}
}

func TestTransportErrorUDPReadIdleUsesOpReceive(t *testing.T) {
	peer := startUDPBlackhole(t)
	e, err := New(WithTimeouts(Timeouts{ReadIdle: 40 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	p, err := e.DialPacket(context.Background(), udpEndpoint(peer), ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	waitClosed(t, p.Done(), "UDP read-idle socket")
	assertTransportError(t, p.Err(), OpReceive, ogrenet.SchemeUDP, ErrorTimeout)
	var timeout *TimeoutError
	if !errors.As(p.Err(), &timeout) || timeout.Kind != TimeoutReadIdle || !errors.Is(p.Err(), ErrTimeout) {
		t.Fatalf("UDP read-idle chain = %#v", p.Err())
	}
}

func TestTransportErrorUDPTrySendBackpressureUsesOpSend(t *testing.T) {
	peer := startUDPBlackhole(t)
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	pc, err := e.DialPacket(context.Background(), udpEndpoint(peer), ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	p := pc.(*packetConn)
	for i := 0; i < cap(p.slots); i++ {
		p.slots <- struct{}{}
	}
	t.Cleanup(func() {
		for len(p.slots) > 0 {
			<-p.slots
		}
	})

	err = pc.TrySend(ogrenet.Packet{Data: []byte("x")})
	assertTransportError(t, err, OpSend, ogrenet.SchemeUDP, ErrorBackpressure)
	if !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("TrySend backpressure does not match ErrWouldBlock: %v", err)
	}
}

func TestTransportErrorUDPExplicitCloseLeavesErrNil(t *testing.T) {
	peer := startUDPBlackhole(t)
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	p, err := e.DialPacket(context.Background(), udpEndpoint(peer), ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, p.Done(), "explicitly closed UDP socket")
	if err := p.Err(); err != nil {
		t.Fatalf("explicit Close populated PacketConn.Err: %v", err)
	}
}
