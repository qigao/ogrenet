//go:build linux

package transport

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestEpollNativeLifecycleReplaysPendingEngineAbortAfterEstablishment(t *testing.T) {
	_, session, _ := newEpollNativeSendSession(t)

	// Reproduce the bootstrap -> established publication race residue: an
	// engine abort was published while the caller still observed established
	// as false, but the reactor observes the session only after establishment.
	session.engineAbort.Store(uint32(abortExplicit))
	session.reactor.signal(session)

	waitNativeSendSignal(t, session.Done(), "pending engine abort")
	if err := session.Err(); err != nil {
		t.Fatalf("explicit engine abort Err() = %v, want nil", err)
	}
}

func TestEpollNativeLifecycleReplaysPendingEngineShutdownAfterEstablishment(t *testing.T) {
	_, session, peer := newEpollNativeSendSession(t)
	tcpPeer, ok := peer.(*net.TCPConn)
	if !ok {
		t.Fatalf("peer=%T, want *net.TCPConn", peer)
	}

	// Reproduce the equivalent shutdown publication residue. The established
	// reactor must consume it instead of permanently ignoring the old atomic.
	session.shutdownRequested.Store(true)
	session.reactor.signal(session)

	if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	var one [1]byte
	n, err := peer.Read(one[:])
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("peer read after pending shutdown = (%d, %v), want (0, EOF)", n, err)
	}

	if err := tcpPeer.CloseWrite(); err != nil {
		t.Fatalf("peer CloseWrite: %v", err)
	}
	waitNativeSendSignal(t, session.Done(), "pending engine shutdown completion")
}
