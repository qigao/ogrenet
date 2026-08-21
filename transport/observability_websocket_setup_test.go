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

func TestWebSocketSetupEventsUseStableSessionAndListenerIDs(t *testing.T) {
	for _, scheme := range []ogrenet.Scheme{ogrenet.SchemeWS, ogrenet.SchemeWSS} {
		t.Run(scheme.String(), func(t *testing.T) {
			serverEvents := make(chan ogrenet.Event, 32)
			clientEvents := make(chan ogrenet.Event, 32)
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
			listener, err := server.Listen(context.Background(), ogrenet.Endpoint{
				Scheme: scheme,
				Host:   "127.0.0.1",
				Port:   0,
				Path:   "/setup-events",
			}, ogrenet.HandlerFuncs{Open: func(session ogrenet.Session) { accepted <- session }})
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
				t.Fatal("websocket accept timeout")
			}

			connect := waitObservedEvent(t, clientEvents, func(event ogrenet.Event) bool {
				return event.Kind == ogrenet.EventConnect && event.ResourceID == clientSession.ID()
			})
			if connect.Resource != ogrenet.ResourceSession || connect.Protocol != scheme || connect.Duration <= 0 || connect.Err != nil {
				t.Fatalf("client connect event=%+v", connect)
			}

			handshake := waitObservedEvent(t, clientEvents, func(event ogrenet.Event) bool {
				return event.Kind == ogrenet.EventHandshake && event.ResourceID == clientSession.ID()
			})
			if handshake.Resource != ogrenet.ResourceSession || handshake.Protocol != scheme || handshake.Duration <= 0 || handshake.Err != nil {
				t.Fatalf("client handshake event=%+v", handshake)
			}

			serverHandshake := waitObservedEvent(t, serverEvents, func(event ogrenet.Event) bool {
				return event.Kind == ogrenet.EventHandshake && event.ResourceID == serverSession.ID()
			})
			if serverHandshake.ParentID != listener.Stats().ResourceID || serverHandshake.Protocol != scheme || serverHandshake.Duration <= 0 || serverHandshake.Err != nil {
				t.Fatalf("server handshake event=%+v listener=%+v", serverHandshake, listener.Stats())
			}
		})
	}
}

func TestWebSocketUpgradeFailureEmitsConnectThenTypedHandshakeFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not a websocket", http.StatusBadRequest)
	})}
	go func() { _ = server.Serve(ln) }()
	defer func() {
		_ = server.Close()
		_ = ln.Close()
	}()

	endpoint := endpointFromAddr(t, ogrenet.SchemeWS, ln.Addr())
	endpoint.Path = "/upgrade-failure"
	events := make(chan ogrenet.Event, 16)
	client, err := New(WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) { events <- event })))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, dialErr := client.Dial(context.Background(), endpoint, ogrenet.HandlerFuncs{})
	if dialErr == nil {
		t.Fatal("websocket Dial unexpectedly succeeded")
	}

	connect := waitObservedEvent(t, events, func(event ogrenet.Event) bool { return event.Kind == ogrenet.EventConnect })
	if connect.ResourceID != 0 || connect.Protocol != ogrenet.SchemeWS || connect.Duration <= 0 || connect.Err != nil {
		t.Fatalf("failed upgrade connect event=%+v", connect)
	}

	handshake := waitObservedEvent(t, events, func(event ogrenet.Event) bool { return event.Kind == ogrenet.EventHandshake })
	if handshake.ResourceID != 0 || handshake.Protocol != ogrenet.SchemeWS || handshake.Duration <= 0 || handshake.Err == nil {
		t.Fatalf("failed upgrade handshake event=%+v", handshake)
	}
	var typed *Error
	if !errors.As(handshake.Err, &typed) {
		t.Fatalf("handshake error type=%T want *transport.Error", handshake.Err)
	}
	if typed.Op != OpUpgrade {
		t.Fatalf("handshake typed op=%v want %v", typed.Op, OpUpgrade)
	}
}
