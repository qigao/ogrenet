//go:build linux

package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

type epollEngineShutdownHandler struct {
	inner  ogrenet.Handler
	opened chan *epollSession
}

func (h *epollEngineShutdownHandler) OnOpen(s ogrenet.Session) {
	native, _ := s.(*epollSession)
	if native != nil {
		h.opened <- native
	}
	if h.inner != nil {
		h.inner.OnOpen(s)
	}
}

func (h *epollEngineShutdownHandler) OnMessage(s ogrenet.Session, msg ogrenet.Message) {
	if h.inner != nil {
		h.inner.OnMessage(s, msg)
	}
}

func (h *epollEngineShutdownHandler) OnClose(s ogrenet.Session, err error) {
	if h.inner != nil {
		h.inner.OnClose(s, err)
	}
}

func newEpollEngineShutdownPeer(t *testing.T, handler ogrenet.Handler, opts ...Option) (*epollEngine, ogrenet.Listener, *epollSession, *net.TCPConn) {
	t.Helper()
	raw, err := NewEpoll(EpollConfig{Pollers: 1, CallbackWorkers: 2, CallbackQueue: 4}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	e := raw.(*epollEngine)
	t.Cleanup(func() {
		_ = e.Close()
		waitEpollEngineDone(t, e.Done())
	})

	capture := &epollEngineShutdownHandler{inner: handler, opened: make(chan *epollSession, 1)}
	ln, err := e.Listen(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeTCP,
		Host:   "127.0.0.1",
		Port:   0,
	}, capture)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr=%T, want *net.TCPAddr", ln.Addr())
	}
	peer, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })

	var session *epollSession
	select {
	case session = <-capture.opened:
	case <-time.After(2 * time.Second):
		t.Fatal("accepted epoll Session did not reach OnOpen")
	}
	return e, ln, session, peer
}

func waitEpollEngineSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("waiting for %s", what)
	}
}

func waitEpollEngineCondition(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func epollEngineLifecycleState(e *epollEngine) (engineState, abortReason) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state, e.shutdownReason
}

func TestEpollEngineShutdownDrainsBeforePeerFIN(t *testing.T) {
	e, ln, session, peer := newEpollEngineShutdownPeer(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- e.Shutdown(ctx) }()

	waitEpollEngineSignal(t, ln.Done(), "listener close during graceful Shutdown")
	state, reason := epollEngineLifecycleState(e)
	if state != engineDraining || reason != abortNone {
		t.Fatalf("Shutdown state=(%v,%v), want (draining, abortNone)", state, reason)
	}
	waitEpollEngineCondition(t, "active lease to enter Draining", func() bool {
		stats := e.Stats()
		return stats.ActiveConnections == 0 && stats.DrainingConnections == 1
	})

	if _, err := e.Dial(context.Background(), ln.Endpoint(), ogrenet.HandlerFuncs{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Dial during Engine Shutdown=%v, want ErrClosed", err)
	}
	select {
	case <-session.Done():
		t.Fatal("Session reached Done before peer FIN")
	default:
	}

	if err := peer.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	n, err := peer.Read(one[:])
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("peer read after Engine Shutdown=(%d,%v), want (0,EOF)", n, err)
	}
	if err := peer.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Shutdown=%v", err)
		}
	case <-ctx.Done():
		t.Fatalf("waiting for graceful Shutdown completion: %v", context.Cause(ctx))
	}
	waitEpollEngineSignal(t, e.Done(), "Engine.Done after peer FIN")
	stats := e.Stats()
	if stats.OpeningConnections != 0 || stats.ActiveConnections != 0 || stats.DrainingConnections != 0 || stats.GlobalQueuedBytes != 0 {
		t.Fatalf("final Engine stats=%+v", stats)
	}
}

func TestEpollEngineShutdownOwnerCancellationBecomesAbortCaller(t *testing.T) {
	closeEntered := make(chan struct{})
	releaseClose := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseClose) }) })
	h := ogrenet.HandlerFuncs{Close: func(ogrenet.Session, error) {
		close(closeEntered)
		<-releaseClose
	}}
	e, ln, _, _ := newEpollEngineShutdownPeer(t, h)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- e.Shutdown(ctx) }()
	waitEpollEngineSignal(t, ln.Done(), "listener close before owner cancellation")
	waitEpollEngineCondition(t, "Engine draining before owner cancellation", func() bool {
		state, _ := epollEngineLifecycleState(e)
		return state == engineDraining
	})
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("owner Shutdown=%v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("owner Shutdown did not return after cancellation")
	}
	waitEpollEngineSignal(t, closeEntered, "OnClose after owner cancellation")
	state, reason := epollEngineLifecycleState(e)
	if state != engineAborting || reason != abortCaller {
		t.Fatalf("owner cancellation state=(%v,%v), want (aborting, abortCaller)", state, reason)
	}
	select {
	case <-e.Done():
		t.Fatal("Engine.Done closed while OnClose blocked")
	default:
	}
	releaseOnce.Do(func() { close(releaseClose) })
	waitEpollEngineSignal(t, e.Done(), "Engine.Done after canceled owner OnClose")
}

func TestEpollEngineShutdownNonOwnerCancellationCannotSteal(t *testing.T) {
	e, ln, _, peer := newEpollEngineShutdownPeer(t, nil)
	ownerCtx, ownerCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer ownerCancel()
	ownerResult := make(chan error, 1)
	go func() { ownerResult <- e.Shutdown(ownerCtx) }()
	waitEpollEngineSignal(t, ln.Done(), "listener close for owner Shutdown")
	waitEpollEngineCondition(t, "Engine draining for owner Shutdown", func() bool {
		state, reason := epollEngineLifecycleState(e)
		return state == engineDraining && reason == abortNone
	})

	nonOwnerCtx, nonOwnerCancel := context.WithCancel(context.Background())
	nonOwnerResult := make(chan error, 1)
	go func() { nonOwnerResult <- e.Shutdown(nonOwnerCtx) }()
	nonOwnerCancel()
	select {
	case err := <-nonOwnerResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("non-owner Shutdown=%v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("non-owner Shutdown did not return after cancellation")
	}
	state, reason := epollEngineLifecycleState(e)
	if state != engineDraining || reason != abortNone {
		t.Fatalf("non-owner cancellation stole owner: state=(%v,%v)", state, reason)
	}

	if err := peer.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-ownerResult:
		if err != nil {
			t.Fatalf("owner Shutdown=%v", err)
		}
	case <-ownerCtx.Done():
		t.Fatalf("owner Shutdown did not finish: %v", context.Cause(ownerCtx))
	}
}

func TestEpollEngineCloseReturnsBeforePhysicalCloseAndOnClose(t *testing.T) {
	closeEntered := make(chan struct{})
	releaseClose := make(chan struct{})
	physicalEntered := make(chan struct{})
	releasePhysical := make(chan struct{})
	var releaseCloseOnce sync.Once
	var releasePhysicalOnce sync.Once
	t.Cleanup(func() {
		releaseCloseOnce.Do(func() { close(releaseClose) })
		releasePhysicalOnce.Do(func() { close(releasePhysical) })
	})
	h := ogrenet.HandlerFuncs{Close: func(ogrenet.Session, error) {
		close(closeEntered)
		<-releaseClose
	}}
	e, _, session, _ := newEpollEngineShutdownPeer(t, h)
	session.testBeforePhysicalClose = func(*epollSession) {
		close(physicalEntered)
		<-releasePhysical
	}

	returned := make(chan error, 1)
	go func() { returned <- e.Close() }()
	select {
	case err := <-returned:
		if err != nil {
			t.Fatalf("Close=%v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Engine.Close blocked on reactor physical close")
	}
	waitEpollEngineSignal(t, physicalEntered, "reactor physical close hook")
	select {
	case <-e.Done():
		t.Fatal("Engine.Done closed while physical close hook blocked")
	default:
	}
	releasePhysicalOnce.Do(func() { close(releasePhysical) })
	waitEpollEngineSignal(t, closeEntered, "Session OnClose after physical close")
	select {
	case <-e.Done():
		t.Fatal("Engine.Done closed while Session OnClose blocked")
	default:
	}
	releaseCloseOnce.Do(func() { close(releaseClose) })
	waitEpollEngineSignal(t, e.Done(), "Engine.Done after Session OnClose")
}

func TestEpollEngineBlockedObserverDoesNotHoldDone(t *testing.T) {
	observerEntered := make(chan struct{})
	releaseObserver := make(chan struct{})
	var observerOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseObserver) }) })
	observer := ogrenet.ObserverFunc(func(ogrenet.Event) {
		observerOnce.Do(func() { close(observerEntered) })
		<-releaseObserver
	})
	e, _, _, _ := newEpollEngineShutdownPeer(t, nil, WithObserver(observer), WithObserverBuffer(1))
	waitEpollEngineSignal(t, observerEntered, "blocking Observer entry")
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	waitEpollEngineSignal(t, e.Done(), "Engine.Done with blocked Observer")
	releaseOnce.Do(func() { close(releaseObserver) })
}
