//go:build linux

package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestEpollEngineShutdownRacesAcceptedHandoff(t *testing.T) {
	raw, err := NewEpoll(EpollConfig{Pollers: 2, CallbackWorkers: 2, CallbackQueue: 8})
	if err != nil {
		t.Fatal(err)
	}
	e := raw.(*epollEngine)
	t.Cleanup(func() {
		_ = e.Close()
		waitEpollEngineDone(t, e.Done())
	})

	ln, err := e.Listen(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeTCP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	// Listener creation consumes reactor 0; the first accepted Session is
	// delegated to reactor 1. Hold that reactor before it can adopt the fd.
	target := e.reactors[1]
	blocked := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	target.signal(newTestInboxItem(func(*epollReactor) {
		close(blocked)
		<-release
	}))
	waitEpollEngineSignal(t, blocked, "accepted handoff target blocker")

	addr := ln.Addr().(*net.TCPAddr)
	peer, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	waitEpollEngineCondition(t, "opening accepted Session before handoff", func() bool {
		return e.Stats().OpeningConnections == 1
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	shutdown := make(chan error, 1)
	go func() { shutdown <- e.Shutdown(ctx) }()
	waitEpollEngineSignal(t, ln.Done(), "listener close during accepted handoff Shutdown")

	releaseOnce.Do(func() { close(release) })
	waitPeerClosed(t, peer)
	select {
	case err := <-shutdown:
		if err != nil {
			t.Fatalf("Shutdown during accepted handoff=%v", err)
		}
	case <-ctx.Done():
		t.Fatalf("Shutdown during accepted handoff did not finish: %v", context.Cause(ctx))
	}
	waitEpollEngineSignal(t, e.Done(), "Engine.Done after accepted handoff Shutdown")
	assertEpollEngineZeroInvariants(t, e)
}

func TestEpollListenerCloseRacesAcceptFlood(t *testing.T) {
	raw, err := NewEpoll(EpollConfig{Pollers: 2, CallbackWorkers: 2, CallbackQueue: 16})
	if err != nil {
		t.Fatal(err)
	}
	e := raw.(*epollEngine)
	t.Cleanup(func() {
		_ = e.Close()
		waitEpollEngineDone(t, e.Done())
	})

	lnRaw, err := e.Listen(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeTCP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	ln := lnRaw.(*epollListener)

	blocked := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	ln.reactor.signal(newTestInboxItem(func(*epollReactor) {
		close(blocked)
		<-release
	}))
	waitEpollEngineSignal(t, blocked, "listener reactor accept-flood blocker")

	const attempts = 128
	peers := make(chan net.Conn, attempts)
	var connected atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			conn, err := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
			if err != nil {
				return
			}
			connected.Add(1)
			peers <- conn
		}()
	}
	close(start)
	waitEpollEngineCondition(t, "connections queued in listener backlog", func() bool {
		return connected.Load() >= 8
	})

	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(release) })
	waitEpollEngineSignal(t, ln.Done(), "listener Done after accept flood")
	wg.Wait()
	close(peers)
	for peer := range peers {
		_ = peer.Close()
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	waitEpollEngineSignal(t, e.Done(), "Engine.Done after accept flood")
	assertEpollEngineZeroInvariants(t, e)

	if _, err := net.DialTimeout("tcp", ln.Addr().String(), 100*time.Millisecond); err == nil {
		t.Fatal("closed listener accepted a post-Done connection")
	} else {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatalf("closed listener dial timed out instead of failing promptly: %v", err)
		}
	}
}
