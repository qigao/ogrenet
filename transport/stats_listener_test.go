package transport

import (
	"context"
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

func TestAcceptEventCorrelatesListenerAndSessionIDs(t *testing.T) {
	events := make(chan ogrenet.Event, 16)
	server, err := New(WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) {
		events <- event
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	accepted := make(chan ogrenet.Session, 1)
	listener, err := server.Listen(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeTCP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.HandlerFuncs{Open: func(s ogrenet.Session) { accepted <- s }})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	clientSession, err := client.Dial(context.Background(), listener.Endpoint(), ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer clientSession.Close()

	var serverSession ogrenet.Session
	select {
	case serverSession = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("accept timeout")
	}

	deadline := time.After(2 * time.Second)
	for {
		select {
		case event := <-events:
			if event.Kind != ogrenet.EventAccept {
				continue
			}
			if event.Resource != ogrenet.ResourceSession || event.ResourceID != serverSession.ID() {
				t.Fatalf("accept event session identity=%+v, server session id=%d", event, serverSession.ID())
			}
			if event.ParentID != listener.Stats().ResourceID {
				t.Fatalf("accept event parent=%d, listener id=%d", event.ParentID, listener.Stats().ResourceID)
			}
			return
		case <-deadline:
			t.Fatal("accept event timeout")
		}
	}
}

func TestListenerCloseEventSeesFrozenFinalStats(t *testing.T) {
	events := make(chan ogrenet.Event, 16)
	server, err := New(WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) {
		events <- event
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	listener, err := server.Listen(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeTCP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	listenerID := listener.Stats().ResourceID
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-listener.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("listener close timeout")
	}

	final := listener.Stats()
	if final.Age <= 0 {
		t.Fatalf("final listener age=%v, want positive", final.Age)
	}
	time.Sleep(time.Millisecond)
	if got := listener.Stats().Age; got != final.Age {
		t.Fatalf("listener age changed after Done: got %v want %v", got, final.Age)
	}

	deadline := time.After(2 * time.Second)
	for {
		select {
		case event := <-events:
			if event.Kind != ogrenet.EventClose || event.Resource != ogrenet.ResourceListener || event.ResourceID != listenerID {
				continue
			}
			if event.Err != nil {
				t.Fatalf("clean listener close event error=%v", event.Err)
			}
			return
		case <-deadline:
			t.Fatal("listener close event timeout")
		}
	}
}
