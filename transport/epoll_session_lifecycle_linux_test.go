//go:build linux

package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

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

	local.IP[0] ^= 0xff
	remote.IP[0] ^= 0xff
	if again := s.LocalAddr().(*net.TCPAddr); again.String() != s.local.String() {
		t.Fatalf("LocalAddr snapshot mutated stored address: got=%v stored=%v", again, s.local)
	}
	if again := s.RemoteAddr().(*net.TCPAddr); again.String() != s.remote.String() {
		t.Fatalf("RemoteAddr snapshot mutated stored address: got=%v stored=%v", again, s.remote)
	}

	stats := s.Stats()
	if stats.ResourceID != s.id || stats.Protocol != ogrenet.SchemeTCP {
		t.Fatalf("Stats identity=%+v", stats)
	}
	select {
	case <-s.Done():
		t.Fatal("active session Done already closed")
	default:
	}
	if err := s.Err(); err != nil {
		t.Fatalf("active session Err()=%v, want nil", err)
	}
}

func TestEpollNativeCloseDefersPhysicalCloseToReactor(t *testing.T) {
	_, s, _ := newEpollNativeSendSession(t)

	entered := make(chan struct{})
	release := make(chan struct{})
	blocker := newTestInboxItem(func(*epollReactor) {
		close(entered)
		<-release
	})
	s.reactor.signal(blocker)
	waitNativeSendSignal(t, entered, "reactor close blocker")

	physical := make(chan struct{}, 1)
	s.testBeforePhysicalClose = func(*epollSession) { physical <- struct{}{} }
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-physical:
		t.Fatal("physical close ran while owning reactor was blocked")
	default:
	}

	close(release)
	waitNativeSendSignal(t, physical, "reactor physical close")
	waitNativeSendSignal(t, s.Done(), "native session Done after Close")
	if err := s.Err(); err != nil {
		t.Fatalf("explicit Close Err()=%v, want nil", err)
	}
}

func TestEpollNativeCloseWriteDrainsAdmittedFrameBeforeFIN(t *testing.T) {
	_, s, peer := newEpollNativeSendSession(t)

	entered := make(chan struct{})
	release := make(chan struct{})
	blocker := newTestInboxItem(func(*epollReactor) {
		close(entered)
		<-release
	})
	s.reactor.signal(blocker)
	waitNativeSendSignal(t, entered, "reactor write-close blocker")

	msg := epollNativeMessage(bytes.Repeat([]byte("drain-before-fin"), 256))
	if err := s.TrySend(msg); err != nil {
		t.Fatalf("TrySend: %v", err)
	}
	result := make(chan error, 1)
	go func() { result <- s.CloseWrite(context.Background()) }()

	close(release)
	readNativeFrame(t, peer, encodedNativeFrame(t, msg))
	if err := waitNativeSendResult(t, result, "CloseWrite"); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	if err := peer.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	one := make([]byte, 1)
	if n, err := peer.Read(one); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("peer after CloseWrite: n=%d err=%v, want EOF", n, err)
	}
}

func TestEpollNativeShutdownWaitsForPeerFIN(t *testing.T) {
	_, s, peer := newEpollNativeSendSession(t)
	tcpPeer, ok := peer.(*net.TCPConn)
	if !ok {
		t.Fatalf("peer=%T, want *net.TCPConn", peer)
	}

	result := make(chan error, 1)
	go func() { result <- s.Shutdown(context.Background()) }()

	if err := peer.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	one := make([]byte, 1)
	if n, err := peer.Read(one); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("peer waiting for local FIN: n=%d err=%v", n, err)
	}
	select {
	case err := <-result:
		t.Fatalf("Shutdown returned before peer FIN: %v", err)
	default:
	}
	if err := tcpPeer.CloseWrite(); err != nil {
		t.Fatalf("peer CloseWrite: %v", err)
	}
	if err := waitNativeSendResult(t, result, "Shutdown after peer FIN"); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	waitNativeSendSignal(t, s.Done(), "Done after graceful Shutdown")
}

func TestEpollNativePeerFINClosesReadHalfAndKeepsWriteUsable(t *testing.T) {
	_, s, peer := newEpollNativeSendSession(t)
	tcpPeer, ok := peer.(*net.TCPConn)
	if !ok {
		t.Fatalf("peer=%T, want *net.TCPConn", peer)
	}
	if err := tcpPeer.CloseWrite(); err != nil {
		t.Fatalf("peer CloseWrite: %v", err)
	}
	waitNativeSendSignal(t, s.ReadClosed(), "ReadClosed after peer FIN")
	if err := s.Err(); err != nil {
		t.Fatalf("Err after clean peer FIN=%v, want nil", err)
	}

	msg := epollNativeMessage([]byte("write-after-peer-fin"))
	if err := s.TrySend(msg); err != nil {
		t.Fatalf("TrySend after peer FIN: %v", err)
	}
	readNativeFrame(t, peer, encodedNativeFrame(t, msg))
}

func TestEpollNativeLifecycleExplicitCloseOwnsLaterTerminalFailure(t *testing.T) {
	_, s, _ := newEpollNativeSendSession(t)

	entered := make(chan struct{})
	release := make(chan struct{})
	s.reactor.signal(newTestInboxItem(func(*epollReactor) {
		close(entered)
		<-release
	}))
	waitNativeSendSignal(t, entered, "lifecycle arbitration blocker")

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	synthetic := errors.New("derived terminal failure after explicit close")
	s.reactor.signal(newTestInboxItem(func(r *epollReactor) {
		s.failNativeLifecycle(r, synthetic)
	}))
	close(release)
	waitNativeSendSignal(t, s.Done(), "Done after lifecycle arbitration")
	if err := s.Err(); err != nil {
		t.Fatalf("Err=%v, want explicit Close owner to preserve nil terminal error", err)
	}
}
