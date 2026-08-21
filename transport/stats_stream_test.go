package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestStreamStatsCountApplicationPayload(t *testing.T) {
	for _, scheme := range []ogrenet.Scheme{ogrenet.SchemeTCP, ogrenet.SchemeTLS} {
		t.Run(scheme.String(), func(t *testing.T) {
			p := dialSessionPair(t, scheme)
			defer p.close()

			payload := []byte("application-payload-is-not-wire-frame")
			msg := ogrenet.Bin(payload)
			if err := p.client.Send(context.Background(), msg); err != nil {
				t.Fatalf("Send: %v", err)
			}
			select {
			case got := <-p.serverMsgs:
				if string(got.Data) != string(payload) {
					t.Fatalf("received payload=%q, want %q", got.Data, payload)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("message receive timeout")
			}

			clientStats := p.client.Stats()
			if clientStats.BytesTX != uint64(len(payload)) || clientStats.MessagesTX != 1 {
				t.Fatalf("client stats=%+v, want payload tx bytes=%d messages=1", clientStats, len(payload))
			}
			serverStats := p.server.Stats()
			if serverStats.BytesRX != uint64(len(payload)) || serverStats.MessagesRX != 1 {
				t.Fatalf("server stats=%+v, want payload rx bytes=%d messages=1", serverStats, len(payload))
			}
			if clientStats.Age <= 0 || serverStats.Age <= 0 {
				t.Fatalf("session ages must be positive: client=%v server=%v", clientStats.Age, serverStats.Age)
			}
		})
	}
}

func TestStreamTrySendBackpressureCountsOnce(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTCP)
	defer p.close()
	client, ok := p.client.(*conn)
	if !ok {
		t.Fatalf("client type=%T, want *conn", p.client)
	}

	for i := 0; i < cap(client.frameSlots); i++ {
		client.frameSlots <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(client.frameSlots); i++ {
			<-client.frameSlots
		}
	}()

	err := client.TrySend(ogrenet.Bin([]byte("blocked")))
	if !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("TrySend=%v, want ErrWouldBlock", err)
	}
	if got := client.Stats().Backpressure; got != 1 {
		t.Fatalf("backpressure=%d, want 1", got)
	}
}

func TestStreamStatsTrackOwnedQueuePressure(t *testing.T) {
	e, err := New(WithWriteQueue(2))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	left, right := net.Pipe()
	defer right.Close()
	c, err := e.adoptStream(&gracefulBenchmarkConn{Conn: left}, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.TrySend(ogrenet.Bin([]byte("queue-pressure"))); err != nil {
		t.Fatalf("TrySend: %v", err)
	}
	stats := c.Stats()
	if stats.QueuedFrames != 1 {
		t.Fatalf("queued frames=%d, want 1", stats.QueuedFrames)
	}
	if stats.QueuedBytes == 0 {
		t.Fatal("queued bytes=0, want retained encoded bytes")
	}

	_ = right.Close()
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("session did not terminate after peer close")
	}
	final := c.Stats()
	if final.QueuedFrames != 0 || final.QueuedBytes != 0 {
		t.Fatalf("final queue stats=%+v, want zero gauges", final)
	}
}

func TestStreamReadWriteCloseEventsAndFinalAge(t *testing.T) {
	for _, scheme := range []ogrenet.Scheme{ogrenet.SchemeTCP, ogrenet.SchemeTLS} {
		t.Run(scheme.String(), func(t *testing.T) {
			serverEvents := make(chan ogrenet.Event, 64)
			clientEvents := make(chan ogrenet.Event, 64)
			serverOpts := []Option{WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) { serverEvents <- event }))}
			clientOpts := []Option{WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) { clientEvents <- event }))}
			if scheme == ogrenet.SchemeTLS {
				serverTLS, clientTLS := newTestTLSConfigs(t)
				serverOpts = append(serverOpts, WithTLSServerConfig(serverTLS))
				clientOpts = append(clientOpts, WithTLSClientConfig(clientTLS))
			}

			serverEngine, err := New(serverOpts...)
			if err != nil {
				t.Fatal(err)
			}
			defer serverEngine.Close()
			clientEngine, err := New(clientOpts...)
			if err != nil {
				t.Fatal(err)
			}
			defer clientEngine.Close()

			accepted := make(chan ogrenet.Session, 1)
			serverMsgs := make(chan ogrenet.Message, 1)
			listener, err := serverEngine.Listen(context.Background(), ogrenet.Endpoint{Scheme: scheme, Host: "127.0.0.1", Port: 0}, ogrenet.HandlerFuncs{
				Open:    func(s ogrenet.Session) { accepted <- s },
				Message: func(_ ogrenet.Session, msg ogrenet.Message) { serverMsgs <- msg },
			})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()

			clientSession, err := clientEngine.Dial(context.Background(), listener.Endpoint(), ogrenet.HandlerFuncs{})
			if err != nil {
				t.Fatal(err)
			}
			var serverSession ogrenet.Session
			select {
			case serverSession = <-accepted:
			case <-time.After(2 * time.Second):
				t.Fatal("server accept timeout")
			}

			payload := []byte("observed-stream-payload")
			if err := clientSession.Send(context.Background(), ogrenet.Bin(payload)); err != nil {
				t.Fatalf("Send: %v", err)
			}
			select {
			case <-serverMsgs:
			case <-time.After(2 * time.Second):
				t.Fatal("server message timeout")
			}

			writeEvent := waitObservedEvent(t, clientEvents, func(event ogrenet.Event) bool {
				return event.Kind == ogrenet.EventWrite && event.Resource == ogrenet.ResourceSession && event.ResourceID == clientSession.ID()
			})
			if writeEvent.Bytes != uint64(len(payload)) || writeEvent.Protocol != scheme {
				t.Fatalf("write event=%+v", writeEvent)
			}
			readEvent := waitObservedEvent(t, serverEvents, func(event ogrenet.Event) bool {
				return event.Kind == ogrenet.EventRead && event.Resource == ogrenet.ResourceSession && event.ResourceID == serverSession.ID()
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
				t.Fatal("client session close timeout")
			}
			finalAge := clientSession.Stats().Age
			if finalAge <= 0 {
				t.Fatalf("final age=%v, want positive", finalAge)
			}
			time.Sleep(time.Millisecond)
			if got := clientSession.Stats().Age; got != finalAge {
				t.Fatalf("session age changed after Done: got %v want %v", got, finalAge)
			}
			closeEvent := waitObservedEvent(t, clientEvents, func(event ogrenet.Event) bool {
				return event.Kind == ogrenet.EventClose && event.Resource == ogrenet.ResourceSession && event.ResourceID == clientSession.ID()
			})
			if closeEvent.Err != nil {
				t.Fatalf("clean explicit close event error=%v", closeEvent.Err)
			}
		})
	}
}

func waitObservedEvent(t *testing.T, events <-chan ogrenet.Event, match func(ogrenet.Event) bool) ogrenet.Event {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event := <-events:
			if match(event) {
				return event
			}
		case <-timer.C:
			t.Fatal("observer event timeout")
		}
	}
}
