//go:build linux

package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/wire"
)

func TestEpollNativeDialConnectTimeoutDuringCodecSetupDoesNotReadReactorStateOrAdopt(t *testing.T) {
	_, endpoint, accepted := newNativeDialTarget(t)
	observer := newEpollTestObserver()
	factoryEntered := make(chan struct{})
	factoryRelease := make(chan struct{})
	e := newEpollTestEngine(t, 1,
		WithObserver(observer),
		WithTimeouts(Timeouts{Connect: 3 * time.Second}),
		WithFramerFactory(func() wire.Framer {
			close(factoryEntered)
			<-factoryRelease
			return wire.New(nil)
		}),
	)

	result := make(chan error, 1)
	go func() {
		_, err := e.dialNativeTCP(context.Background(), endpoint, nil, nil)
		result <- err
	}()
	waitTestSignal(t, factoryEntered, "native dial framer factory before connect timeout")
	peer := waitNativeDialAccepted(t, accepted)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	var dialErr error
	select {
	case dialErr = <-result:
	case <-waitCtx.Done():
		t.Fatalf("waiting for native connect timeout: %v", context.Cause(waitCtx))
	}
	var typed *Error
	if !errors.As(dialErr, &typed) {
		t.Fatalf("timeout error type=%T err=%v", dialErr, dialErr)
	}
	if typed.Op != OpDial || typed.Protocol != ogrenet.SchemeTCP || typed.Kind != ErrorTimeout || !errors.Is(dialErr, ErrTimeout) {
		t.Fatalf("timeout error=%+v", typed)
	}

	close(factoryRelease)
	waitPeerClosed(t, peer)
	event := waitEpollEvent(t, observer, ogrenet.EventConnect)
	if event.ResourceID != 0 || event.Err != nil {
		t.Fatalf("connected-before-timeout event=%+v, want successful transport connect with no adopted ID", event)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	waitTestSignal(t, e.Done(), "engine barrier after native connect timeout")
	stats := e.Stats()
	if stats.OpeningConnections != 0 || stats.ActiveConnections != 0 || stats.DrainingConnections != 0 {
		t.Fatalf("engine stats after native connect timeout=%+v", stats)
	}
}

func TestEpollNativeDialConnectDeadlineRejectsStaleGeneration(t *testing.T) {
	r := newTestEpollReactor(t)
	e := &epollEngine{
		admission: newAdmissionController(Limits{}),
		state:     engineRunning,
		managed:   make(map[uint64]epollManagedResource),
	}
	s := newEpollBootstrapSession(
		e,
		r,
		71,
		-1,
		ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 1},
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	s.state = epollSessionConnecting
	s.dial = &epollDialState{result: make(chan epollDialResult, 1), connectGen: 7}
	r.resources[s.id] = s
	e.managed[s.id] = s

	s.onReactorDeadline(r, epollDeadlineConnect, 6)
	select {
	case got := <-s.dial.result:
		t.Fatalf("stale connect deadline completed dial: %+v", got)
	default:
	}
	if s.state != epollSessionConnecting || s.currentDeadlineGeneration(epollDeadlineConnect) != 7 {
		t.Fatalf("state=%v generation=%d", s.state, s.currentDeadlineGeneration(epollDeadlineConnect))
	}

	s.onReactorDeadline(r, epollDeadlineConnect, 7)
	got := <-s.dial.result
	var typed *Error
	if !errors.As(got.err, &typed) || typed.Op != OpDial || typed.Kind != ErrorTimeout || !errors.Is(got.err, ErrTimeout) {
		t.Fatalf("live deadline result=%T %v", got.err, got.err)
	}
	if s.state != epollSessionClosed {
		t.Fatalf("state after live deadline=%v", s.state)
	}
	if r.resources[s.id] != nil {
		t.Fatal("live deadline left reactor resource registered")
	}
	e.mu.Lock()
	managed := len(e.managed)
	e.mu.Unlock()
	if managed != 0 {
		t.Fatalf("live deadline left %d managed resources", managed)
	}
}
