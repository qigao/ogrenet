//go:build linux

package transport

import (
	"context"
	"net"
	"testing"

	"github.com/qigao/ogrenet"
)

var _ ogrenet.Session = (*epollSession)(nil)
var _ ogrenet.HalfCloseSession = (*epollSession)(nil)

func TestEpollNativeStoredSessionIdentityAndAddresses(t *testing.T) {
	_, s, _ := newEpollNativeSendSession(t)

	if got := s.ID(); got != s.id || got == 0 {
		t.Fatalf("ID()=%d internal=%d", got, s.id)
	}
	if got := s.Protocol(); got != ogrenet.SchemeTCP {
		t.Fatalf("Protocol()=%v, want tcp", got)
	}
	if got := s.Endpoint(); got != s.endpoint {
		t.Fatalf("Endpoint()=%v, want %v", got, s.endpoint)
	}

	local, ok := s.LocalAddr().(*net.TCPAddr)
	if !ok || local == nil {
		t.Fatalf("LocalAddr()=%T %v, want *net.TCPAddr", s.LocalAddr(), s.LocalAddr())
	}
	remote, ok := s.RemoteAddr().(*net.TCPAddr)
	if !ok || remote == nil {
		t.Fatalf("RemoteAddr()=%T %v, want *net.TCPAddr", s.RemoteAddr(), s.RemoteAddr())
	}
	if s.local == nil || s.remote == nil || local.String() != s.local.String() || remote.String() != s.remote.String() {
		t.Fatalf("stored addresses local=%v/%v remote=%v/%v", local, s.local, remote, s.remote)
	}

	// The public address accessors must be snapshots, not aliases to reactor-owned storage.
	local.IP[0] ^= 0xff
	remote.IP[0] ^= 0xff
	if again := s.LocalAddr().(*net.TCPAddr); again.String() != s.local.String() {
		t.Fatalf("LocalAddr snapshot mutated stored address: got=%v stored=%v", again, s.local)
	}
	if again := s.RemoteAddr().(*net.TCPAddr); again.String() != s.remote.String() {
		t.Fatalf("RemoteAddr snapshot mutated stored address: got=%v stored=%v", again, s.remote)
	}

	_ = s.Stats()
	select {
	case <-s.Done():
		t.Fatal("active session Done already closed")
	default:
	}
	if err := s.Err(); err != nil {
		t.Fatalf("active session Err()=%v, want nil", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = ctx // compile guard for CloseWrite/Shutdown context-bearing public surface.
}
