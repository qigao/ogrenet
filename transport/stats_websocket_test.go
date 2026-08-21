package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestWebSocketStatsCountApplicationPayload(t *testing.T) {
	for _, scheme := range []ogrenet.Scheme{ogrenet.SchemeWS, ogrenet.SchemeWSS} {
		t.Run(scheme.String(), func(t *testing.T) {
			p := dialSessionPair(t, scheme)
			defer p.close()
			payload := []byte("websocket-application-payload")
			if err := p.client.Send(context.Background(), ogrenet.Bin(payload)); err != nil {
				t.Fatalf("Send: %v", err)
			}
			select {
			case got := <-p.serverMsgs:
				if string(got.Data) != string(payload) {
					t.Fatalf("payload=%q want %q", got.Data, payload)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("message timeout")
			}
			clientStats := p.client.Stats()
			serverStats := p.server.Stats()
			if clientStats.BytesTX != uint64(len(payload)) || clientStats.MessagesTX != 1 {
				t.Fatalf("client stats=%+v", clientStats)
			}
			if serverStats.BytesRX != uint64(len(payload)) || serverStats.MessagesRX != 1 {
				t.Fatalf("server stats=%+v", serverStats)
			}
			if clientStats.Age <= 0 || serverStats.Age <= 0 {
				t.Fatalf("ages client=%v server=%v", clientStats.Age, serverStats.Age)
			}
		})
	}
}

func TestWebSocketStatsUseQueueOwnershipSources(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeWS)
	defer p.close()
	s, ok := p.client.(*wsSession)
	if !ok {
		t.Fatalf("client type=%T want *wsSession", p.client)
	}
	s.frameSlots <- struct{}{}
	if err := s.quota.tryAcquire(23); err != nil {
		t.Fatal(err)
	}
	stats := s.Stats()
	if stats.QueuedFrames != 1 || stats.QueuedBytes != 23 {
		t.Fatalf("queue stats=%+v want frames=1 bytes=23", stats)
	}
	s.quota.release(23)
	s.releaseFrameSlot()
}

func TestWebSocketTrySendBackpressureCountsOnce(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeWS)
	defer p.close()
	s, ok := p.client.(*wsSession)
	if !ok {
		t.Fatalf("client type=%T want *wsSession", p.client)
	}
	for i := 0; i < cap(s.frameSlots); i++ {
		s.frameSlots <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(s.frameSlots); i++ {
			<-s.frameSlots
		}
	}()
	err := s.TrySend(ogrenet.Bin([]byte("blocked")))
	if !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("TrySend=%v want ErrWouldBlock", err)
	}
	if got := s.Stats().Backpressure; got != 1 {
		t.Fatalf("backpressure=%d want 1", got)
	}
}

func TestWebSocketReadWriteCloseEventsAndFinalAge(t *testing.T) {
	for _, scheme := range []ogrenet.Scheme{ogrenet.SchemeWS, ogrenet.SchemeWSS} {
		t.Run(scheme.String(), func(t *testing.T) {
			serverEvents := make(chan ogrenet.Event, 64)
			clientEvents := make(chan ogrenet.Event, 64)
			serverOpts := []Option{WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) { serverEvents <- event }))}
			clientOpts := []Option{WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) { clientEvents <- event }))}
			if scheme == ogrenet.SchemeWSS {
				serverTLS, clientTLS := testTLSConfigs(t)
				serverOpts = append(serverOpts, WithTLSServerConfig(serverTLS))
				clientOpts = append(clientOpts, WithTLSClientConfig(clientTLS))
			}
			server, err := New(serverOpts...)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			client, err := New(clientOpts...)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			accepted := make(chan ogrenet.Session, 1)
			serverMsgs := make(chan ogrenet.Message, 1)
			listener, err := server.Listen(context.Background(), ogrenet.Endpoint{Scheme: scheme, Host: "127.0.0.1", Port: 0, Path: "/observability"}, ogrenet.HandlerFuncs{
				Open:    func(session ogrenet.Session) { accepted <- session },
				Message: func(_ ogrenet.Session, msg ogrenet.Message) { serverMsgs <- msg },
			})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			clientSession, err := client.Dial(context.Background(), listener.Endpoint(), ogrenet.HandlerFuncs{})
			if err != nil {
				t.Fatal(err)
			}
			var serverSession ogrenet.Session
			select {
			case serverSession = <-accepted:
			case <-time.After(2 * time.Second):
				t.Fatal("accept timeout")
			}

			payload := []byte("observed-websocket-payload")
			if err := clientSession.Send(context.Background(), ogrenet.Bin(payload)); err != nil {
				t.Fatalf("Send: %v", err)
			}
			select {
			case <-serverMsgs:
			case <-time.After(2 * time.Second):
				t.Fatal("message timeout")
			}
			writeEvent := waitObservedEvent(t, clientEvents, func(event ogrenet.Event) bool {
				return event.Kind == ogrenet.EventWrite && event.ResourceID == clientSession.ID()
			})
			if writeEvent.Bytes != uint64(len(payload)) || writeEvent.Protocol != scheme {
				t.Fatalf("write event=%+v", writeEvent)
			}
			readEvent := waitObservedEvent(t, serverEvents, func(event ogrenet.Event) bool {
				return event.Kind == ogrenet.EventRead && event.ResourceID == serverSession.ID()
			})
			if readEvent.Bytes != uint64(len(payload)) || readEvent.Protocol != scheme {
				t.Fatalf("read event=%+v", readEvent)
			}

			if err := clientSession.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-clientSession.Done():
			case <-time.After(2 * time.Second):
				t.Fatal("close timeout")
			}
			age := clientSession.Stats().Age
			if age <= 0 {
				t.Fatalf("final age=%v", age)
			}
			time.Sleep(time.Millisecond)
			if got := clientSession.Stats().Age; got != age {
				t.Fatalf("age changed after Done got=%v want=%v", got, age)
			}
			closeEvent := waitObservedEvent(t, clientEvents, func(event ogrenet.Event) bool {
				return event.Kind == ogrenet.EventClose && event.ResourceID == clientSession.ID()
			})
			if closeEvent.Err != nil {
				t.Fatalf("clean close event error=%v", closeEvent.Err)
			}
		})
	}
}
