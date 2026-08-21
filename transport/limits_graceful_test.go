package transport

import (
	"errors"
	"testing"
)

func TestConnectionLeaseDrainingCountsTowardGlobalLimit(t *testing.T) {
	a := newAdmissionController(Limits{MaxConnections: 1})
	lease, err := a.acquireConnection("192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	if !lease.beginDrain() {
		t.Fatal("beginDrain did not transition active lease")
	}
	if lease.beginDrain() {
		t.Fatal("second beginDrain unexpectedly transitioned")
	}

	snap := a.snapshot()
	if snap.OpeningConnections != 0 || snap.ActiveConnections != 0 || snap.DrainingConnections != 1 {
		t.Fatalf("snapshot while draining = %+v", snap)
	}
	if _, err := a.acquireConnection("192.0.2.2"); !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("new connection while draining = %v, want ErrResourceExhausted", err)
	}

	lease.release()
	snap = a.snapshot()
	if snap.DrainingConnections != 0 {
		t.Fatalf("draining connections after release = %d, want 0", snap.DrainingConnections)
	}
}

func TestConnectionLeaseDrainingCountsTowardPeerLimit(t *testing.T) {
	a := newAdmissionController(Limits{MaxConnectionsPerPeer: 1})
	lease, err := a.acquireConnection("192.0.2.9")
	if err != nil {
		t.Fatal(err)
	}
	if !lease.beginDrain() {
		t.Fatal("beginDrain did not transition active peer lease")
	}
	if _, err := a.acquireConnection("192.0.2.9"); !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("same-peer admission while draining = %v, want ErrResourceExhausted", err)
	}
	lease.release()
	if _, err := a.acquireConnection("192.0.2.9"); err != nil {
		t.Fatalf("same-peer admission after release = %v", err)
	}
}

func TestConnectionLeaseDrainingRetainsListenerCapacity(t *testing.T) {
	a := newAdmissionController(Limits{MaxConnections: 8, MaxConnectionsPerListener: 1})
	capacity := newListenerCapacity(1)
	lease, err := a.acquireOpeningWithListener("192.0.2.10", capacity)
	if err != nil {
		t.Fatal(err)
	}
	if !lease.activate() {
		t.Fatal("activate failed")
	}
	if !lease.beginDrain() {
		t.Fatal("beginDrain did not transition listener lease")
	}
	if got := capacity.current(); got != 1 {
		t.Fatalf("listener capacity while draining = %d, want 1", got)
	}
	if _, err := a.acquireOpeningWithListener("192.0.2.11", capacity); !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("listener admission while draining = %v, want ErrResourceExhausted", err)
	}
	lease.release()
	if got := capacity.current(); got != 0 {
		t.Fatalf("listener capacity after release = %d, want 0", got)
	}
}
