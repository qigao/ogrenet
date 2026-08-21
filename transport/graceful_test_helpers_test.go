package transport

import (
	"context"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

type gracefulShutdowner interface {
	Shutdown(context.Context) error
}

type halfCloseProbe interface {
	CloseWrite(context.Context) error
	ReadClosed() <-chan struct{}
}

type sessionPair struct {
	clientEngine *Engine
	serverEngine *Engine
	client       ogrenet.Session
	server       ogrenet.Session
	clientMsgs   chan ogrenet.Message
	serverMsgs   chan ogrenet.Message
}

func (p *sessionPair) close() {
	_ = p.clientEngine.Close()
	_ = p.serverEngine.Close()
}

func dialSessionPair(t *testing.T, scheme ogrenet.Scheme) *sessionPair {
	t.Helper()

	serverTLS, clientTLS := testTLSConfigs(t)
	var serverOpts, clientOpts []Option
	if scheme == ogrenet.SchemeTLS || scheme == ogrenet.SchemeWSS {
		serverOpts = []Option{WithTLSServerConfig(serverTLS)}
		clientOpts = []Option{WithTLSClientConfig(clientTLS)}
	}

	server, err := New(serverOpts...)
	if err != nil {
		t.Fatal(err)
	}
	client, err := New(clientOpts...)
	if err != nil {
		_ = server.Close()
		t.Fatal(err)
	}

	path := ""
	if scheme == ogrenet.SchemeWS || scheme == ogrenet.SchemeWSS {
		path = "/graceful"
	}
	accepted := make(chan ogrenet.Session, 1)
	serverMsgs := make(chan ogrenet.Message, 512)
	listener, err := server.Listen(context.Background(), ogrenet.Endpoint{
		Scheme: scheme,
		Host:   "127.0.0.1",
		Port:   0,
		Path:   path,
	}, ogrenet.HandlerFuncs{
		Open: func(s ogrenet.Session) {
			accepted <- s
		},
		Message: func(_ ogrenet.Session, msg ogrenet.Message) {
			serverMsgs <- cloneMessage(msg)
		},
	})
	if err != nil {
		_ = client.Close()
		_ = server.Close()
		t.Fatal(err)
	}

	clientMsgs := make(chan ogrenet.Message, 512)
	clientSession, err := client.Dial(context.Background(), listener.Endpoint(), ogrenet.HandlerFuncs{
		Message: func(_ ogrenet.Session, msg ogrenet.Message) {
			clientMsgs <- cloneMessage(msg)
		},
	})
	if err != nil {
		_ = client.Close()
		_ = server.Close()
		t.Fatal(err)
	}

	var serverSession ogrenet.Session
	select {
	case serverSession = <-accepted:
	case <-time.After(2 * time.Second):
		_ = client.Close()
		_ = server.Close()
		t.Fatal("accepted session timeout")
	}

	return &sessionPair{
		clientEngine: client,
		serverEngine: server,
		client:       clientSession,
		server:       serverSession,
		clientMsgs:   clientMsgs,
		serverMsgs:   serverMsgs,
	}
}

func cloneMessage(msg ogrenet.Message) ogrenet.Message {
	return ogrenet.Message{Type: msg.Type, Data: append([]byte(nil), msg.Data...)}
}

func waitClosed(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not close", name)
	}
}

func assertStillOpen(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s closed unexpectedly", name)
	default:
	}
}

func requireHalfClose(t *testing.T, s ogrenet.Session) halfCloseProbe {
	t.Helper()
	h, ok := s.(halfCloseProbe)
	if !ok {
		t.Fatalf("%T does not implement halfCloseProbe", s)
	}
	return h
}

func requireGracefulShutdown(t *testing.T, s ogrenet.Session) gracefulShutdowner {
	t.Helper()
	g, ok := s.(gracefulShutdowner)
	if !ok {
		t.Fatalf("%T does not implement gracefulShutdowner", s)
	}
	return g
}
