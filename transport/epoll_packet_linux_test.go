//go:build linux

package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"golang.org/x/sys/unix"
)

func newEpollPacketTestEngine(t *testing.T, pollers int, opts ...Option) *epollEngine {
	t.Helper()
	if pollers <= 0 {
		pollers = 1
	}
	raw, err := NewEpoll(EpollConfig{Pollers: pollers, CallbackWorkers: 2, CallbackQueue: 8}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	e := raw.(*epollEngine)
	t.Cleanup(func() {
		_ = e.Close()
		waitEpollEngineDone(t, e.Done())
	})
	return e
}

func waitEpollPacketDone(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("waiting for %s", what)
	}
}

func TestEpollNativePacketBindDialAndAffinity(t *testing.T) {
	e := newEpollPacketTestEngine(t, 2)
	ctx := context.Background()

	server, err := e.listenNativeUDP(ctx, ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatalf("listenNativeUDP: %v", err)
	}
	local, ok := server.LocalAddr().(*net.UDPAddr)
	if !ok || local.Port == 0 {
		t.Fatalf("server LocalAddr=%T %v, want bound UDP port", server.LocalAddr(), server.LocalAddr())
	}
	if server.Endpoint().Port != uint16(local.Port) {
		t.Fatalf("server Endpoint=%+v local=%v", server.Endpoint(), local)
	}
	if server.RemoteAddr() != nil {
		t.Fatalf("unconnected RemoteAddr=%v, want nil", server.RemoteAddr())
	}
	if server.reactor == nil || server.reactor.resources[server.id] != server {
		t.Fatal("server packet resource not registered on owning reactor")
	}

	endpoint := ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: uint16(local.Port)}
	client, err := e.dialNativeUDP(ctx, endpoint, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatalf("dialNativeUDP: %v", err)
	}
	if client.RemoteAddr() == nil {
		t.Fatal("connected RemoteAddr=nil")
	}
	if got := client.RemoteAddr().String(); got != local.String() {
		t.Fatalf("client RemoteAddr=%s, want %s", got, local)
	}
	if client.LocalAddr() == nil {
		t.Fatal("connected LocalAddr=nil")
	}
	owner := client.reactor
	if owner == nil || owner.resources[client.id] != client {
		t.Fatal("client packet resource not registered on owning reactor")
	}
	owner.signal(client)
	time.Sleep(10 * time.Millisecond)
	if client.reactor != owner {
		t.Fatal("packet reactor affinity changed")
	}
}

func TestEpollNativePacketCloseIsReactorOwned(t *testing.T) {
	e := newEpollPacketTestEngine(t, 1)
	server, err := e.listenNativeUDP(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	fd := server.fd
	entered := make(chan struct{})
	release := make(chan struct{})
	server.testBeforePhysicalClose = func(*epollPacketConn) {
		close(entered)
		<-release
	}

	if err := server.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitEpollPacketDone(t, entered, "reactor physical-close barrier")
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil {
		t.Fatalf("caller closed reactor-owned fd before barrier release: %v", err)
	}
	close(release)
	waitEpollPacketDone(t, server.Done(), "packet Done")
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err == nil {
		t.Fatal("reactor-owned UDP fd remains open after Done")
	}
	if err := server.Err(); err != nil {
		t.Fatalf("clean Close Err=%v, want nil", err)
	}
}

var _ ogrenet.PacketConn = (*epollPacketConn)(nil)
