package transport

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestConnectEventUsesAdoptedSessionID(t *testing.T) {
	server, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	listener, err := server.Listen(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	events := make(chan ogrenet.Event, 16)
	client, err := New(WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) { events <- event })))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	session, err := client.Dial(context.Background(), listener.Endpoint(), ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	event := waitObservedEvent(t, events, func(event ogrenet.Event) bool { return event.Kind == ogrenet.EventConnect })
	if event.Resource != ogrenet.ResourceSession || event.ResourceID != session.ID() {
		t.Fatalf("connect event=%+v session id=%d", event, session.ID())
	}
	if event.Protocol != ogrenet.SchemeTCP || event.Duration <= 0 || event.Err != nil {
		t.Fatalf("connect event semantics=%+v", event)
	}
}

func TestConnectFailureEventHasNoSessionAndPreservesTypedError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := endpointFromAddr(t, ogrenet.SchemeTCP, ln.Addr())
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	events := make(chan ogrenet.Event, 16)
	client, err := New(WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) { events <- event })))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, dialErr := client.Dial(context.Background(), endpoint, ogrenet.HandlerFuncs{})
	if dialErr == nil {
		t.Fatal("Dial unexpectedly succeeded")
	}

	event := waitObservedEvent(t, events, func(event ogrenet.Event) bool { return event.Kind == ogrenet.EventConnect })
	if event.Resource != ogrenet.ResourceSession || event.ResourceID != 0 || event.Protocol != ogrenet.SchemeTCP {
		t.Fatalf("failed connect identity=%+v", event)
	}
	if event.Duration <= 0 || event.Err == nil {
		t.Fatalf("failed connect event=%+v", event)
	}
	var typed *Error
	if !errors.As(event.Err, &typed) {
		t.Fatalf("connect event error type=%T, want *transport.Error", event.Err)
	}
	if !errors.Is(event.Err, dialErr) && event.Err != dialErr {
		t.Fatalf("connect event error=%v does not preserve returned error=%v", event.Err, dialErr)
	}
}

func TestTLSHandshakeEventsUseSessionAndListenerCorrelation(t *testing.T) {
	serverTLS, clientTLS := testTLSConfigs(t)
	serverEvents := make(chan ogrenet.Event, 32)
	server, err := New(
		WithTLSServerConfig(serverTLS),
		WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) { serverEvents <- event })),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	accepted := make(chan ogrenet.Session, 1)
	listener, err := server.Listen(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeTLS, Host: "127.0.0.1", Port: 0}, ogrenet.HandlerFuncs{
		Open: func(session ogrenet.Session) { accepted <- session },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	clientEvents := make(chan ogrenet.Event, 32)
	client, err := New(
		WithTLSClientConfig(clientTLS),
		WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) { clientEvents <- event })),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	clientSession, err := client.Dial(context.Background(), listener.Endpoint(), ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer clientSession.Close()

	var serverSession ogrenet.Session
	select {
	case serverSession = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("TLS accept timeout")
	}

	clientHandshake := waitObservedEvent(t, clientEvents, func(event ogrenet.Event) bool { return event.Kind == ogrenet.EventHandshake })
	if clientHandshake.ResourceID != clientSession.ID() || clientHandshake.ParentID != 0 || clientHandshake.Protocol != ogrenet.SchemeTLS || clientHandshake.Duration <= 0 || clientHandshake.Err != nil {
		t.Fatalf("client handshake event=%+v", clientHandshake)
	}
	clientConnect := waitObservedEvent(t, clientEvents, func(event ogrenet.Event) bool { return event.Kind == ogrenet.EventConnect })
	if clientConnect.ResourceID != clientSession.ID() || clientConnect.Duration <= 0 || clientConnect.Err != nil {
		t.Fatalf("client connect event=%+v", clientConnect)
	}

	serverHandshake := waitObservedEvent(t, serverEvents, func(event ogrenet.Event) bool { return event.Kind == ogrenet.EventHandshake })
	if serverHandshake.ResourceID != serverSession.ID() || serverHandshake.ParentID != listener.Stats().ResourceID || serverHandshake.Protocol != ogrenet.SchemeTLS || serverHandshake.Duration <= 0 || serverHandshake.Err != nil {
		t.Fatalf("server handshake event=%+v listener=%+v serverSession=%d", serverHandshake, listener.Stats(), serverSession.ID())
	}
}

func TestTLSHandshakeFailureEventHasNoSessionAndTypedError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr == nil {
			_ = conn.Close()
		}
	}()

	_, clientTLS := testTLSConfigs(t)
	events := make(chan ogrenet.Event, 32)
	client, err := New(
		WithTLSClientConfig(clientTLS),
		WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) { events <- event })),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	endpoint := endpointFromAddr(t, ogrenet.SchemeTLS, ln.Addr())
	_, dialErr := client.Dial(context.Background(), endpoint, ogrenet.HandlerFuncs{})
	if dialErr == nil {
		t.Fatal("TLS Dial unexpectedly succeeded")
	}

	connectEvent := waitObservedEvent(t, events, func(event ogrenet.Event) bool { return event.Kind == ogrenet.EventConnect })
	if connectEvent.ResourceID != 0 || connectEvent.Err != nil || connectEvent.Duration <= 0 {
		t.Fatalf("physical connect event before failed handshake=%+v", connectEvent)
	}
	handshakeEvent := waitObservedEvent(t, events, func(event ogrenet.Event) bool { return event.Kind == ogrenet.EventHandshake })
	if handshakeEvent.ResourceID != 0 || handshakeEvent.Protocol != ogrenet.SchemeTLS || handshakeEvent.Duration <= 0 || handshakeEvent.Err == nil {
		t.Fatalf("failed handshake event=%+v", handshakeEvent)
	}
	var typed *Error
	if !errors.As(handshakeEvent.Err, &typed) {
		t.Fatalf("handshake event error type=%T, want *transport.Error", handshakeEvent.Err)
	}
}

func endpointFromAddr(t *testing.T, scheme ogrenet.Scheme, addr net.Addr) ogrenet.Endpoint {
	t.Helper()
	host, portText, err := net.SplitHostPort(addr.String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	return ogrenet.Endpoint{Scheme: scheme, Host: host, Port: uint16(port)}
}
