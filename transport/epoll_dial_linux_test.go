//go:build linux

package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/wire"
	"golang.org/x/sys/unix"
)

func TestResolveNativeDialLiteralSkipsResolver(t *testing.T) {
	resolver := &testNativeIPResolver{err: errors.New("resolver should not run")}
	endpoint := ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 43210}

	got, err := resolveNativeDialTCP(context.Background(), endpoint, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if resolver.called != 0 {
		t.Fatalf("resolver called %d times for literal address", resolver.called)
	}
	if len(got) != 1 || got[0].Port != int(endpoint.Port) || !got[0].IP.Equal(net.ParseIP(endpoint.Host)) {
		t.Fatalf("resolved=%v", got)
	}
}

func TestResolveNativeDialPreservesResolverOrderAndPort(t *testing.T) {
	resolver := &testNativeIPResolver{addrs: []net.IPAddr{
		{IP: net.ParseIP("127.0.0.2")},
		{IP: net.ParseIP("127.0.0.1")},
	}}
	endpoint := ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "example.test", Port: 43211}

	got, err := resolveNativeDialTCP(context.Background(), endpoint, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if resolver.called != 1 {
		t.Fatalf("resolver called %d times, want 1", resolver.called)
	}
	if len(got) != 2 {
		t.Fatalf("resolved count=%d, want 2", len(got))
	}
	for i, wantIP := range []net.IP{net.ParseIP("127.0.0.2"), net.ParseIP("127.0.0.1")} {
		if got[i].Port != int(endpoint.Port) || !got[i].IP.Equal(wantIP) {
			t.Fatalf("resolved[%d]=%v, want %v:%d", i, got[i], wantIP, endpoint.Port)
		}
	}
}

func TestEpollNativeDialResolverErrorIsTypedAndCallerCauseWins(t *testing.T) {
	resolverErr := &net.DNSError{Err: "synthetic resolver failure", Name: "example.test"}
	resolver := &testNativeIPResolver{err: resolverErr}
	e := newEpollTestEngine(t, 1)
	endpoint := ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "example.test", Port: 43212}

	_, err := e.dialNativeTCP(context.Background(), endpoint, nil, resolver)
	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatalf("resolver error type=%T err=%v", err, err)
	}
	if typed.Op != OpDial || typed.Protocol != ogrenet.SchemeTCP || typed.Kind != ErrorDNS {
		t.Fatalf("resolver error=%+v", typed)
	}
	if !errors.Is(err, resolverErr) {
		t.Fatalf("resolver cause lost: %v", err)
	}

	callerCause := errors.New("caller canceled native dial")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(callerCause)
	resolver.called = 0
	_, err = e.dialNativeTCP(ctx, endpoint, nil, resolver)
	if err != callerCause {
		t.Fatalf("caller cause=%v, want exact %v", err, callerCause)
	}
	if resolver.called != 0 {
		t.Fatalf("resolver called %d times after caller cancellation", resolver.called)
	}
}

func newNativeDialTarget(t *testing.T) (net.Listener, ogrenet.Endpoint, <-chan net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	accepted := make(chan net.Conn, 8)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- conn
		}
	}()
	addr := ln.Addr().(*net.TCPAddr)
	return ln, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: uint16(addr.Port)}, accepted
}

func waitNativeDialAccepted(t *testing.T, accepted <-chan net.Conn) net.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	select {
	case conn := <-accepted:
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	case <-ctx.Done():
		t.Fatalf("waiting for native dial accept: %v", context.Cause(ctx))
		return nil
	}
}

func closedNativeDialEndpoint(t *testing.T) ogrenet.Endpoint {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().(*net.TCPAddr)
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: uint16(addr.Port)}
}

func TestEpollNativeDialSuccessStoresAddressesAndEmitsConnect(t *testing.T) {
	_, endpoint, accepted := newNativeDialTarget(t)
	observer := newEpollTestObserver()
	e := newEpollTestEngine(t, 1, WithObserver(observer))

	session, err := e.dialNativeTCP(context.Background(), endpoint, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = waitNativeDialAccepted(t, accepted)
	if session == nil || session.id == 0 || session.local == nil || session.remote == nil {
		t.Fatalf("session=%+v", session)
	}
	if session.local.Port == 0 || session.remote.Port != int(endpoint.Port) || !session.remote.IP.Equal(net.ParseIP("127.0.0.1")) {
		t.Fatalf("local=%v remote=%v", session.local, session.remote)
	}
	event := waitEpollEvent(t, observer, ogrenet.EventConnect)
	if event.Err != nil || event.ResourceID != session.id {
		t.Fatalf("connect event=%+v session=%d", event, session.id)
	}
}

func TestEpollNativeDialRefusedPreservesErrno(t *testing.T) {
	endpoint := closedNativeDialEndpoint(t)
	observer := newEpollTestObserver()
	e := newEpollTestEngine(t, 1, WithObserver(observer))

	_, err := e.dialNativeTCP(context.Background(), endpoint, nil, nil)
	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatalf("refused error type=%T err=%v", err, err)
	}
	if typed.Op != OpDial || typed.Protocol != ogrenet.SchemeTCP || typed.Kind != ErrorRefused {
		t.Fatalf("refused error=%+v", typed)
	}
	if !errors.Is(err, unix.ECONNREFUSED) {
		t.Fatalf("raw errno not reachable: %v", err)
	}
	event := waitEpollEvent(t, observer, ogrenet.EventConnect)
	if event.ResourceID != 0 || event.Err == nil || !errors.Is(event.Err, unix.ECONNREFUSED) {
		t.Fatalf("refused connect event=%+v", event)
	}
}

func TestEpollNativeDialResolverOrderFallsThroughToSecondAddress(t *testing.T) {
	_, endpoint, accepted := newNativeDialTarget(t)
	resolver := &testNativeIPResolver{addrs: []net.IPAddr{
		{IP: net.ParseIP("127.0.0.2")},
		{IP: net.ParseIP("127.0.0.1")},
	}}
	endpoint.Host = "example.test"
	e := newEpollTestEngine(t, 1)

	session, err := e.dialNativeTCP(context.Background(), endpoint, nil, resolver)
	if err != nil {
		t.Fatal(err)
	}
	_ = waitNativeDialAccepted(t, accepted)
	if resolver.called != 1 {
		t.Fatalf("resolver called %d times, want 1", resolver.called)
	}
	if session.remote == nil || !session.remote.IP.Equal(net.ParseIP("127.0.0.1")) {
		t.Fatalf("remote=%v, want second resolver address", session.remote)
	}
}

func TestEpollNativeDialCodecSetupFailureIsDirectAndReleasesOwnership(t *testing.T) {
	_, endpoint, accepted := newNativeDialTarget(t)
	observer := newEpollTestObserver()
	e := newEpollTestEngine(t, 1,
		WithObserver(observer),
		WithFramerFactory(func() wire.Framer { return nil }),
	)

	_, err := e.dialNativeTCP(context.Background(), endpoint, nil, nil)
	if err != ErrNilFramer {
		t.Fatalf("codec setup error=%T %v, want direct ErrNilFramer", err, err)
	}
	peer := waitNativeDialAccepted(t, accepted)
	waitPeerClosed(t, peer)
	event := waitEpollEvent(t, observer, ogrenet.EventConnect)
	if event.ResourceID != 0 || event.Err != nil {
		t.Fatalf("connected-before-setup event=%+v", event)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	waitTestSignal(t, e.Done(), "engine barrier after native dial codec failure")
	stats := e.Stats()
	if stats.OpeningConnections != 0 || stats.ActiveConnections != 0 || stats.DrainingConnections != 0 {
		t.Fatalf("engine stats after codec failure=%+v", stats)
	}
	e.mu.Lock()
	managed := len(e.managed)
	e.mu.Unlock()
	if managed != 0 {
		t.Fatalf("managed resources after codec failure=%d", managed)
	}
}

func TestEpollNativeDialAdmissionFailureReportsConnectedBeforeReject(t *testing.T) {
	_, endpoint, accepted := newNativeDialTarget(t)
	observer := newEpollTestObserver()
	e := newEpollTestEngine(t, 1,
		WithObserver(observer),
		WithLimits(Limits{MaxConnections: 1}),
	)

	first, err := e.dialNativeTCP(context.Background(), endpoint, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = waitNativeDialAccepted(t, accepted)
	firstEvent := waitEpollEvent(t, observer, ogrenet.EventConnect)
	if firstEvent.Err != nil || firstEvent.ResourceID != first.id {
		t.Fatalf("first connect event=%+v", firstEvent)
	}

	_, err = e.dialNativeTCP(context.Background(), endpoint, nil, nil)
	var typed *Error
	if !errors.As(err, &typed) || typed.Op != OpDial || typed.Kind != ErrorResourceExhausted {
		t.Fatalf("admission error=%T %v", err, err)
	}
	secondPeer := waitNativeDialAccepted(t, accepted)
	waitPeerClosed(t, secondPeer)
	secondEvent := waitEpollEvent(t, observer, ogrenet.EventConnect)
	if secondEvent.ResourceID != 0 || secondEvent.Err != nil {
		t.Fatalf("second connect event=%+v", secondEvent)
	}
	stats := e.Stats()
	if stats.OpeningConnections != 0 || stats.ActiveConnections != 1 {
		t.Fatalf("engine stats=%+v", stats)
	}
}
