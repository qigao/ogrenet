package transport

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestListenerCapacityLimitAndRelease(t *testing.T) {
	c := newListenerCapacity(1)
	if !c.acquire() {
		t.Fatal("first listener capacity acquire failed")
	}
	if c.acquire() {
		t.Fatal("second listener capacity acquire unexpectedly succeeded")
	}
	if got := c.current(); got != 1 {
		t.Fatalf("listener current = %d, want 1", got)
	}
	c.release()
	c.release()
	if got := c.current(); got != 0 {
		t.Fatalf("listener current after release = %d, want 0", got)
	}
}

func TestAdmissionOpeningLeaseTransfersListenerCapacity(t *testing.T) {
	a := newAdmissionController(Limits{MaxConnections: 4, MaxConnectionsPerListener: 1})
	listener := newListenerCapacity(1)
	lease, err := a.acquireOpeningWithListener("192.0.2.1", listener)
	if err != nil {
		t.Fatal(err)
	}
	if got := listener.current(); got != 1 {
		t.Fatalf("listener capacity while opening = %d, want 1", got)
	}
	if _, err := a.acquireOpeningWithListener("192.0.2.2", listener); !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("second opening = %v, want ErrResourceExhausted", err)
	}
	if !lease.activate() {
		t.Fatal("activate failed")
	}
	if got := listener.current(); got != 1 {
		t.Fatalf("listener capacity after activate = %d, want 1", got)
	}
	lease.release()
	if got := listener.current(); got != 0 {
		t.Fatalf("listener capacity after release = %d, want 0", got)
	}
}

func TestHTTPConnTrackerRebindAndTransfer(t *testing.T) {
	a := newAdmissionController(Limits{MaxConnections: 2})
	lease, err := a.acquireOpening("192.0.2.10")
	if err != nil {
		t.Fatal(err)
	}
	tracker := newHTTPConnTracker()
	raw1, raw1Peer := net.Pipe()
	defer raw1.Close()
	defer raw1Peer.Close()
	raw2, raw2Peer := net.Pipe()
	defer raw2.Close()
	defer raw2Peer.Close()

	holder := tracker.register(raw1, lease)
	if tracker.lookup(raw1) != holder {
		t.Fatal("raw1 holder lookup failed")
	}
	if !tracker.rebind(raw1, raw2) {
		t.Fatal("rebind failed")
	}
	if tracker.lookup(raw1) != nil || tracker.lookup(raw2) != holder {
		t.Fatal("rebind lookup state incorrect")
	}

	transferred := holder.take()
	if transferred == nil || !transferred.activate() {
		t.Fatal("holder transfer failed")
	}
	tracker.connState(raw2, http.StateClosed)
	if got := a.snapshot().ActiveConnections; got != 1 {
		t.Fatalf("ConnState closed released transferred lease: active=%d", got)
	}
	transferred.release()
	if got := a.snapshot(); got.OpeningConnections != 0 || got.ActiveConnections != 0 {
		t.Fatalf("admission leaked after transfer cleanup: %+v", got)
	}
}

func TestTCPPerListenerCapacity(t *testing.T) {
	e, err := New(WithLimits(Limits{MaxConnections: 8, MaxConnectionsPerListener: 1}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opens := make(chan ogrenet.Session, 2)
	ln, err := e.Listen(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, ogrenet.HandlerFuncs{
		Open: func(s ogrenet.Session) { opens <- s },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	internal := ln.(*listener)

	first, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	var firstSession ogrenet.Session
	select {
	case firstSession = <-opens:
	case <-time.After(2 * time.Second):
		t.Fatal("first TCP session did not open")
	}
	if got := internal.capacity.current(); got != 1 {
		t.Fatalf("listener capacity = %d, want 1", got)
	}

	second, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	_ = second.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := second.Read(make([]byte, 1)); err == nil {
		t.Fatal("second over-limit TCP connection remained open")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("second over-limit TCP connection was not closed promptly")
	}

	_ = first.Close()
	// Peer FIN/full close is a read-half close in the P0-3 Session model. The
	// server Session still owns its write side, so it must continue consuming
	// listener capacity until the Session itself is closed.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if half, ok := firstSession.(halfCloseProbe); ok {
			select {
			case <-half.ReadClosed():
				goto readHalfClosed
			default:
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("server read half did not close after peer close")

readHalfClosed:
	if got := internal.capacity.current(); got != 1 {
		t.Fatalf("listener capacity released on read half-close: %d", got)
	}
	if err := firstSession.Close(); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && internal.capacity.current() != 0 {
		time.Sleep(time.Millisecond)
	}
	if got := internal.capacity.current(); got != 0 {
		t.Fatalf("listener capacity did not return after Session close: %d", got)
	}

	third, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close()
	select {
	case <-opens:
	case <-time.After(2 * time.Second):
		t.Fatal("listener capacity was not reusable")
	}
}

func TestWSPreHeaderConnectionConsumesCapacity(t *testing.T) {
	e, err := New(WithLimits(Limits{MaxConnections: 1, MaxConnectionsPerListener: 1}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := e.Listen(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeWS, Host: "127.0.0.1", Port: 0, Path: "/"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	first, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := first.Write([]byte("GET / HTTP/1.1\r\nHost: example\r\n")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && e.admissionSnapshot().OpeningConnections != 1 {
		time.Sleep(time.Millisecond)
	}
	if got := e.admissionSnapshot(); got.OpeningConnections != 1 {
		t.Fatalf("pre-header WS connection not accounted: %+v", got)
	}

	second, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	_ = second.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := second.Read(make([]byte, 1)); err == nil {
		t.Fatal("over-limit WS TCP connection remained open")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("over-limit WS TCP connection was not closed promptly")
	}
}

func TestWSAdmissionLeaseTransfersOnUpgrade(t *testing.T) {
	tracker := newHTTPConnTracker()
	a := newAdmissionController(Limits{MaxConnections: 1})
	lease, err := a.acquireOpening("192.0.2.5")
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	holder := tracker.register(left, lease)
	transferred := holder.take()
	if transferred == nil || !transferred.activate() {
		t.Fatal("transfer failed")
	}
	if got := a.snapshot(); got.OpeningConnections != 0 || got.ActiveConnections != 1 {
		t.Fatalf("snapshot after transfer = %+v", got)
	}
	tracker.connState(left, http.StateClosed)
	if got := a.snapshot(); got.ActiveConnections != 1 {
		t.Fatalf("tracker released transferred connection: %+v", got)
	}
	transferred.release()
	if got := a.snapshot(); got.ActiveConnections != 0 {
		t.Fatalf("active after release = %+v", got)
	}
}
