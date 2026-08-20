package transport

import (
	"context"
	"errors"
	"net"
	"testing"
)

func TestLimitsValidation(t *testing.T) {
	for _, limits := range []Limits{
		{MaxConnections: -1},
		{MaxConnectionsPerPeer: -1},
		{MaxConcurrentHandshakes: -1},
		{MaxPendingUpgrades: -1},
		{MaxQueuedBytesTotal: -1},
	} {
		if _, err := New(WithLimits(limits)); !errors.Is(err, ErrInvalidLimits) {
			t.Fatalf("New(%+v) = %v, want ErrInvalidLimits", limits, err)
		}
	}
}

func TestAdmissionConnectionAndPeerLimits(t *testing.T) {
	a := newAdmissionController(Limits{MaxConnections: 2, MaxConnectionsPerPeer: 1})
	first, err := a.acquireConnection("192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.acquireConnection("192.0.2.1"); !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("same peer admission = %v, want ErrResourceExhausted", err)
	} else {
		var limitErr *LimitError
		if !errors.As(err, &limitErr) || limitErr.Kind != LimitConnectionsPerPeer {
			t.Fatalf("same peer error = %#v, want peer LimitError", err)
		}
	}
	second, err := a.acquireConnection("192.0.2.2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.acquireConnection("192.0.2.3"); !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("global admission = %v, want ErrResourceExhausted", err)
	}
	first.release()
	first.release()
	second.release()
	if got := a.snapshot().ActiveConnections; got != 0 {
		t.Fatalf("active connections = %d, want 0", got)
	}
}

func TestPeerKeyHandlesTypedNilNetAddrs(t *testing.T) {
	var tcp *net.TCPAddr
	var udp *net.UDPAddr

	for name, addr := range map[string]net.Addr{
		"tcp": tcp,
		"udp": udp,
	} {
		if got := peerKey(addr); got != "" {
			t.Fatalf("peerKey(%s typed nil) = %q, want empty key", name, got)
		}
	}
}

func TestGlobalQueuedByteQuotaAcrossConnections(t *testing.T) {
	global := newGlobalByteQuota(8)
	q1 := newByteQuota(8)
	q2 := newByteQuota(8)
	q1.setParent(global)
	q2.setParent(global)

	if err := q1.tryAcquire(6); err != nil {
		t.Fatal(err)
	}
	if err := q2.tryAcquire(3); !errors.Is(err, ErrWouldBlock) || !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("global quota TryAcquire = %v, want ErrWouldBlock + ErrResourceExhausted", err)
	}
	if got := global.current(); got != 6 {
		t.Fatalf("global used = %d, want 6", got)
	}
	if got := q2.current(); got != 0 {
		t.Fatalf("rolled-back local quota = %d, want 0", got)
	}
	q1.release(6)
	if err := q2.tryAcquire(3); err != nil {
		t.Fatal(err)
	}
	q2.release(3)
	if got := global.current(); got != 0 {
		t.Fatalf("global used after release = %d, want 0", got)
	}
}

func TestGlobalQueuedByteQuotaWaitCancellationRollsBackLocal(t *testing.T) {
	global := newGlobalByteQuota(4)
	q1 := newByteQuota(4)
	q2 := newByteQuota(4)
	q1.setParent(global)
	q2.setParent(global)
	closing := make(chan struct{})

	if err := q1.acquire(context.Background(), closing, 4); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := q2.acquire(ctx, closing, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire = %v, want context.Canceled", err)
	}
	if got := q2.current(); got != 0 {
		t.Fatalf("local quota leaked %d bytes", got)
	}
	q1.release(4)
}

func TestEngineTrackedResourcesOwnAdmissionLeaseAndGlobalQuota(t *testing.T) {
	e, err := New(WithLimits(Limits{MaxConnections: 1, MaxQueuedBytesTotal: 16}))
	if err != nil {
		t.Fatal(err)
	}
	left1, right1 := net.Pipe()
	defer left1.Close()
	defer right1.Close()
	c1 := &conn{raw: left1, quota: newByteQuota(16)}
	if err := e.addStream(c1); err != nil {
		t.Fatal(err)
	}
	if c1.quota.parent != e.admission.bytes {
		t.Fatal("stream quota not attached to engine global quota")
	}

	left2, right2 := net.Pipe()
	defer left2.Close()
	defer right2.Close()
	c2 := &conn{raw: left2, quota: newByteQuota(16)}
	if err := e.addStream(c2); !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("second stream = %v, want ErrResourceExhausted", err)
	}
	e.removeStream(c1)
	if err := e.addStream(c2); err != nil {
		t.Fatalf("capacity not returned after remove: %v", err)
	}
	e.removeStream(c2)
	if got := e.admissionSnapshot().ActiveConnections; got != 0 {
		t.Fatalf("active after cleanup = %d, want 0", got)
	}
}

func TestOpeningConnectionsCountAgainstCapacity(t *testing.T) {
	a := newAdmissionController(Limits{MaxConnections: 1})
	lease, err := a.acquireOpening("192.0.2.10")
	if err != nil {
		t.Fatal(err)
	}
	if got := a.snapshot(); got.OpeningConnections != 1 || got.ActiveConnections != 0 {
		t.Fatalf("snapshot while opening = %+v", got)
	}
	if _, err := a.acquireConnection("192.0.2.11"); !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("second admission = %v, want ErrResourceExhausted", err)
	}
	if !lease.activate() {
		t.Fatal("activate opening lease failed")
	}
	if got := a.snapshot(); got.OpeningConnections != 0 || got.ActiveConnections != 1 {
		t.Fatalf("snapshot after activate = %+v", got)
	}
	lease.release()
	if got := a.snapshot(); got.OpeningConnections != 0 || got.ActiveConnections != 0 {
		t.Fatalf("snapshot after release = %+v", got)
	}
}

func TestHandshakeAndUpgradeLimits(t *testing.T) {
	a := newAdmissionController(Limits{MaxConcurrentHandshakes: 1, MaxPendingUpgrades: 1})
	handshake, err := a.acquireHandshake()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.acquireHandshake(); !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("second handshake = %v, want ErrResourceExhausted", err)
	} else {
		var limitErr *LimitError
		if !errors.As(err, &limitErr) || limitErr.Kind != LimitHandshakes {
			t.Fatalf("handshake error = %#v, want handshake LimitError", err)
		}
	}
	upgrade, err := a.acquireUpgrade()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.acquireUpgrade(); !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("second upgrade = %v, want ErrResourceExhausted", err)
	} else {
		var limitErr *LimitError
		if !errors.As(err, &limitErr) || limitErr.Kind != LimitUpgrades {
			t.Fatalf("upgrade error = %#v, want upgrade LimitError", err)
		}
	}
	if got := a.snapshot(); got.ActiveHandshakes != 1 || got.PendingUpgrades != 1 {
		t.Fatalf("activity snapshot = %+v", got)
	}
	if !handshake.release() || handshake.release() {
		t.Fatal("handshake release must be exact once")
	}
	if !upgrade.release() || upgrade.release() {
		t.Fatal("upgrade release must be exact once")
	}
	if got := a.snapshot(); got.ActiveHandshakes != 0 || got.PendingUpgrades != 0 {
		t.Fatalf("activity snapshot after release = %+v", got)
	}
}

func TestEngineDoneWaitsForInFlightAdmission(t *testing.T) {
	e, err := New(WithLimits(Limits{MaxConnections: 1, MaxConcurrentHandshakes: 1}))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.beginOp(); err != nil {
		t.Fatal(err)
	}
	opening, err := e.acquireOpening(&net.TCPAddr{IP: net.ParseIP("192.0.2.20"), Port: 443})
	if err != nil {
		t.Fatal(err)
	}
	handshake, err := e.acquireHandshake()
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-e.Done():
		t.Fatal("Engine.Done closed with opening/handshake admission still held")
	default:
	}
	opening.release()
	handshake.release()
	e.endOp()
	select {
	case <-e.Done():
	default:
		t.Fatal("Engine.Done did not close after admission release")
	}
}
