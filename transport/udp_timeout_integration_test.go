package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestDialPacketReadIdleClosesConnectedSocket(t *testing.T) {
	peer := startUDPBlackhole(t)
	e, err := New(WithTimeouts(Timeouts{ReadIdle: 50 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	p, err := e.DialPacket(context.Background(), udpEndpoint(peer), ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	waitPacketTimeoutKind(t, p, TimeoutReadIdle)
}

func TestDialPacketConnectionIdleClosesConnectedSocket(t *testing.T) {
	peer := startUDPBlackhole(t)
	e, err := New(WithTimeouts(Timeouts{ConnectionIdle: 70 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	p, err := e.DialPacket(context.Background(), udpEndpoint(peer), ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	waitPacketTimeoutKind(t, p, TimeoutConnectionIdle)
}

func TestDialPacketMaxLifetimeCannotBeExtendedByTraffic(t *testing.T) {
	peer := startUDPEcho(t)
	e, err := New(WithTimeouts(Timeouts{MaxLifetime: 120 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	p, err := e.DialPacket(context.Background(), udpEndpoint(peer), ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-p.Done():
			var te *TimeoutError
			if !errors.As(p.Err(), &te) || te.Kind != TimeoutMaxLifetime {
				t.Fatalf("PacketConn.Err = %#v, want TimeoutMaxLifetime", p.Err())
			}
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			err := p.Send(ctx, ogrenet.Packet{Data: []byte("keepalive")})
			cancel()
			if err != nil && !errors.Is(err, ErrClosed) {
				t.Fatalf("Send = %v", err)
			}
		case <-deadline.C:
			t.Fatal("UDP max lifetime was extended by traffic")
		}
	}
}

func TestListenPacketDoesNotIdleClose(t *testing.T) {
	e, err := New(WithTimeouts(Timeouts{
		ReadIdle:       40 * time.Millisecond,
		ConnectionIdle: 40 * time.Millisecond,
		MaxLifetime:    40 * time.Millisecond,
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	p, err := e.ListenPacket(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 0}, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.Done():
		t.Fatalf("ListenPacket closed for inactivity: %v", p.Err())
	case <-time.After(140 * time.Millisecond):
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func startUDPBlackhole(t *testing.T) *net.UDPAddr {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn.LocalAddr().(*net.UDPAddr)
}

func startUDPEcho(t *testing.T) *net.UDPAddr {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 2048)
		for {
			n, peer, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if _, err := conn.WriteToUDP(buf[:n], peer); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		_ = conn.Close()
		<-done
	})
	return conn.LocalAddr().(*net.UDPAddr)
}

func udpEndpoint(addr *net.UDPAddr) ogrenet.Endpoint {
	return ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: uint16(addr.Port)}
}

func waitPacketTimeoutKind(t *testing.T, p ogrenet.PacketConn, kind TimeoutKind) {
	t.Helper()
	select {
	case <-p.Done():
	case <-time.After(time.Second):
		t.Fatalf("PacketConn did not close for %s timeout", kind)
	}
	var te *TimeoutError
	if !errors.As(p.Err(), &te) || te.Kind != kind {
		t.Fatalf("PacketConn.Err = %#v, want %s timeout", p.Err(), kind)
	}
}
