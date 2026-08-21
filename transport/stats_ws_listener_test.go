package transport

import (
	"context"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestWebSocketListenerStatsAndEvents(t *testing.T) {
	events := make(chan ogrenet.Event, 32)
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
		Scheme: ogrenet.SchemeWS,
		Host:   "127.0.0.1",
		Port:   0,
		Path:   "/observability",
	}, ogrenet.HandlerFuncs{Open: func(s ogrenet.Session) { accepted <- s }})
	if err != nil {
		t.Fatal(err)
	}

	clientSession, err := client.Dial(context.Background(), listener.Endpoint(), ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer clientSession.Close()

	var serverSession ogrenet.Session
	select {
	case serverSession = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("websocket accept timeout")
	}

	deadline := time.After(2 * time.Second)
	for {
		select {
		case event := <-events:
			if event.Kind != ogrenet.EventAccept {
				continue
			}
			if event.ResourceID != serverSession.ID() || event.ParentID != listener.Stats().ResourceID {
				t.Fatalf("websocket accept event=%+v listener=%+v session=%d", event, listener.Stats(), serverSession.ID())
			}
			goto acceptedEvent
		case <-deadline:
			t.Fatal("websocket accept event timeout")
		}
	}

acceptedEvent:
	stats := listener.Stats()
	if stats.AcceptedConnections != 1 || stats.CurrentConnections != 1 {
		t.Fatalf("websocket listener stats after accept=%+v", stats)
	}

	if err := clientSession.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-serverSession.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("websocket server session close timeout")
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-listener.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("websocket listener close timeout")
	}

	final := listener.Stats()
	if final.CurrentConnections != 0 {
		t.Fatalf("websocket final current=%d, want 0", final.CurrentConnections)
	}
	age := final.Age
	time.Sleep(time.Millisecond)
	if got := listener.Stats().Age; got != age || age <= 0 {
		t.Fatalf("websocket final age changed/invalid: got %v want %v", got, age)
	}

	deadline = time.After(2 * time.Second)
	for {
		select {
		case event := <-events:
			if event.Kind == ogrenet.EventClose && event.Resource == ogrenet.ResourceListener && event.ResourceID == final.ResourceID {
				if event.Err != nil {
					t.Fatalf("clean websocket listener close event error=%v", event.Err)
				}
				return
			}
		case <-deadline:
			t.Fatal("websocket listener close event timeout")
		}
	}
}
