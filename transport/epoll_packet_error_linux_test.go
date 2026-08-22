//go:build linux

package transport

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"golang.org/x/sys/unix"
)

func TestEpollNativePacketConnectedICMPErrorPreservesRawErrno(t *testing.T) {
	probe, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	closedPort := probe.LocalAddr().(*net.UDPAddr).Port
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}

	e := newEpollPacketTestEngine(t, 1)
	closeErr := make(chan error, 1)
	client, err := e.dialNativeUDP(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   uint16(closedPort),
	}, ogrenet.PacketHandlerFuncs{Close: func(_ ogrenet.PacketConn, err error) {
		closeErr <- err
	}})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	packet := ogrenet.Packet{Data: []byte("icmp-port-unreachable")}
	for time.Now().Before(deadline) {
		if err := client.Send(context.Background(), packet); err != nil {
			break
		}
		select {
		case <-client.Done():
			break
		case <-time.After(10 * time.Millisecond):
		}
		if client.Err() != nil {
			break
		}
	}
	waitEpollPacketDone(t, client.Done(), "connected UDP ICMP terminal error")

	terminal := client.Err()
	if terminal == nil {
		t.Fatal("connected UDP port-unreachable did not become a terminal error")
	}
	if !errors.Is(terminal, unix.ECONNREFUSED) {
		t.Fatalf("terminal error=%v, want raw ECONNREFUSED discoverable", terminal)
	}
	if !errors.Is(terminal, ErrConnectionRefused) {
		t.Fatalf("terminal error=%v, want ErrConnectionRefused category", terminal)
	}
	var errno syscall.Errno
	if !errors.As(terminal, &errno) || errno != syscall.ECONNREFUSED {
		t.Fatalf("terminal errno=%v, want syscall.ECONNREFUSED", errno)
	}
	var opErr *Error
	if !errors.As(terminal, &opErr) || opErr.Protocol != ogrenet.SchemeUDP || opErr.Kind != ErrorRefused {
		t.Fatalf("terminal operational error=%#v", opErr)
	}
	if opErr.Op != OpWrite && opErr.Op != OpReceive {
		t.Fatalf("terminal operation=%v, want OpWrite or OpReceive", opErr.Op)
	}

	select {
	case got := <-closeErr:
		if got != terminal {
			t.Fatalf("OnClose error=%v, PacketConn.Err=%v; want same first terminal owner", got, terminal)
		}
	case <-time.After(time.Second):
		t.Fatal("missing OnClose for connected UDP ICMP error")
	}
}
