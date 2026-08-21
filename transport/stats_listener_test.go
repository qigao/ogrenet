package transport

import (
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestListenerCapacityTracksCurrentWithoutLimit(t *testing.T) {
	capacity := newListenerCapacity(0)
	if capacity == nil {
		t.Fatal("unlimited listener capacity must still exist for accounting")
	}
	if !capacity.acquire() {
		t.Fatal("unlimited capacity rejected acquisition")
	}
	if got := capacity.current(); got != 1 {
		t.Fatalf("current=%d, want 1", got)
	}
	capacity.release()
	if got := capacity.current(); got != 0 {
		t.Fatalf("current after release=%d, want 0", got)
	}
}

func TestListenerStatsExposeOwnedCounters(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	capacity := newListenerCapacity(1)
	if !capacity.acquire() {
		t.Fatal("first capacity acquire failed")
	}
	if capacity.acquire() {
		t.Fatal("second capacity acquire unexpectedly succeeded")
	}

	l := &listener{
		id:       41,
		endpoint: ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP},
		ln:       ln,
		capacity: capacity,
		stats:    newListenerCounters(),
	}
	l.stats.accepted.Add(1)

	got := l.Stats()
	if got.ResourceID != 41 || got.Protocol != ogrenet.SchemeTCP {
		t.Fatalf("identity stats=%+v", got)
	}
	if got.AcceptedConnections != 1 || got.RejectedConnections != 1 || got.CurrentConnections != 1 {
		t.Fatalf("counter stats=%+v, want accepted=1 rejected=1 current=1", got)
	}
}

func TestListenerStatsFreezeAge(t *testing.T) {
	stats := newListenerCounters()
	time.Sleep(time.Millisecond)
	before := stats.age.current()
	if before <= 0 {
		t.Fatalf("age before freeze=%v, want positive", before)
	}
	stats.age.freeze()
	frozen := stats.age.current()
	time.Sleep(time.Millisecond)
	if got := stats.age.current(); got != frozen {
		t.Fatalf("age changed after freeze: got %v want %v", got, frozen)
	}
}
