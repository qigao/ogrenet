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

type testNativeIPResolver struct {
	called int
	addrs  []net.IPAddr
	err    error
}

func (r *testNativeIPResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	r.called++
	return append([]net.IPAddr(nil), r.addrs...), r.err
}

func TestNativeTCPSockaddrRoundTripIPv4(t *testing.T) {
	want := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 43210}
	sa, family, err := nativeTCPAddrToSockaddr(want)
	if err != nil {
		t.Fatal(err)
	}
	if family != unix.AF_INET {
		t.Fatalf("family=%d, want AF_INET", family)
	}
	got, err := nativeSockaddrToTCPAddr(sa)
	if err != nil {
		t.Fatal(err)
	}
	if got.Port != want.Port || !got.IP.Equal(want.IP) {
		t.Fatalf("round trip=%v, want %v", got, want)
	}
}

func TestNativeTCPSockaddrRoundTripIPv6(t *testing.T) {
	want := &net.TCPAddr{IP: net.ParseIP("::1"), Port: 43211, Zone: ""}
	sa, family, err := nativeTCPAddrToSockaddr(want)
	if err != nil {
		t.Fatal(err)
	}
	if family != unix.AF_INET6 {
		t.Fatalf("family=%d, want AF_INET6", family)
	}
	got, err := nativeSockaddrToTCPAddr(sa)
	if err != nil {
		t.Fatal(err)
	}
	if got.Port != want.Port || !got.IP.Equal(want.IP) {
		t.Fatalf("round trip=%v, want %v", got, want)
	}
}

func TestNativeTCPResolveListenLiteralSkipsResolver(t *testing.T) {
	resolver := &testNativeIPResolver{err: context.DeadlineExceeded}
	endpoint := ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 12345}
	got, err := resolveNativeListenTCP(context.Background(), endpoint, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if resolver.called != 0 {
		t.Fatalf("resolver called %d times for literal address", resolver.called)
	}
	if got.Port != int(endpoint.Port) || !got.IP.Equal(net.ParseIP(endpoint.Host)) {
		t.Fatalf("resolved=%v", got)
	}
}

func TestNativeTCPResolveListenHostnameUsesResolver(t *testing.T) {
	resolver := &testNativeIPResolver{addrs: []net.IPAddr{{IP: net.ParseIP("127.0.0.2")}}}
	endpoint := ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "example.test", Port: 23456}
	got, err := resolveNativeListenTCP(context.Background(), endpoint, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if resolver.called != 1 {
		t.Fatalf("resolver called %d times, want 1", resolver.called)
	}
	if got.Port != int(endpoint.Port) || !got.IP.Equal(net.ParseIP("127.0.0.2")) {
		t.Fatalf("resolved=%v", got)
	}
}

func TestNativeTCPConfigObservable(t *testing.T) {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)

	cfg := TCPConfig{
		NoDelay:          true,
		KeepAlive:        true,
		KeepAlivePeriod:  15 * time.Second,
		ReadBufferBytes:  4096,
		WriteBufferBytes: 8192,
	}
	if err := configureNativeTCP(fd, cfg); err != nil {
		t.Fatal(err)
	}

	noDelay, err := unix.GetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY)
	if err != nil {
		t.Fatal(err)
	}
	if noDelay != 1 {
		t.Fatalf("TCP_NODELAY=%d, want 1", noDelay)
	}
	keepAlive, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_KEEPALIVE)
	if err != nil {
		t.Fatal(err)
	}
	if keepAlive != 1 {
		t.Fatalf("SO_KEEPALIVE=%d, want 1", keepAlive)
	}
	rcv, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF)
	if err != nil {
		t.Fatal(err)
	}
	if rcv < cfg.ReadBufferBytes {
		t.Fatalf("SO_RCVBUF=%d, want >= %d", rcv, cfg.ReadBufferBytes)
	}
	snd, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_SNDBUF)
	if err != nil {
		t.Fatal(err)
	}
	if snd < cfg.WriteBufferBytes {
		t.Fatalf("SO_SNDBUF=%d, want >= %d", snd, cfg.WriteBufferBytes)
	}
}

func TestNativeTCPSocketAddrReadsBoundLocalAddress(t *testing.T) {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if err := unix.Bind(fd, &unix.SockaddrInet4{Port: 0, Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	got, err := nativeSocketAddr(fd, false)
	if err != nil {
		t.Fatal(err)
	}
	if got.Port == 0 || !got.IP.Equal(net.ParseIP("127.0.0.1")) {
		t.Fatalf("local=%v", got)
	}
}
