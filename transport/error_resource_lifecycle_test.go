package transport

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestTransportErrorListenerOwnerContextLeavesErrNil(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx, cancel := context.WithCancel(context.Background())
	ln, err := e.Listen(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	waitClosed(t, ln.Done(), "context-owned listener")
	if err := ln.Err(); err != nil {
		t.Fatalf("owner context populated Listener.Err: %v", err)
	}
}

func TestTransportErrorPacketListenerOwnerContextLeavesErrNil(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx, cancel := context.WithCancel(context.Background())
	p, err := e.ListenPacket(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 0}, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	waitClosed(t, p.Done(), "context-owned packet listener")
	if err := p.Err(); err != nil {
		t.Fatalf("owner context populated PacketConn.Err: %v", err)
	}
}

func TestTransportErrorListenerFatalAcceptUsesOpAccept(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx, cancel := context.WithCancel(context.Background())
	l := &listener{
		engine:   e,
		endpoint: ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 1},
		ln:       failingAcceptListener{addr: staticAddr{network: "tcp", value: "127.0.0.1:1"}, err: syscall.EIO},
		handler:  normalizeHandler(nil),
		ctx:      ctx,
		cancel:   cancel,
		capacity: newListenerCapacity(0),
		closing:  make(chan struct{}),
		done:     make(chan struct{}),
	}
	if err := e.addStreamListener(l); err != nil {
		t.Fatal(err)
	}
	go l.acceptLoop()
	waitClosed(t, l.Done(), "fatal-accept listener")
	assertTransportError(t, l.Err(), OpAccept, ogrenet.SchemeTCP, ErrorUnknown)
	if !errors.Is(l.Err(), syscall.EIO) {
		t.Fatalf("fatal Accept lost raw EIO: %v", l.Err())
	}
}

func TestTransportErrorResourceOutboundLimitUsesOpDial(t *testing.T) {
	server, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ln, err := server.Listen(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	client, err := New(WithLimits(Limits{MaxConnections: 1}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	first, err := client.Dial(context.Background(), ln.Endpoint(), ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	_, err = client.Dial(context.Background(), ln.Endpoint(), ogrenet.HandlerFuncs{})
	assertTransportError(t, err, OpDial, ogrenet.SchemeTCP, ErrorResourceExhausted)
	var limit *LimitError
	if !errors.As(err, &limit) || limit.Kind != LimitConnections || !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("outbound limit chain = %#v", err)
	}
}

func TestTransportErrorListenerInboundRejectionDoesNotPoisonErr(t *testing.T) {
	e, err := New(WithLimits(Limits{MaxConnectionsPerListener: 1}))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	public, err := e.Listen(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer public.Close()
	l := public.(*listener)

	first, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	waitAtomicAtLeast(t, func() int64 { return l.capacity.used.Load() }, 1, "listener capacity use")

	second, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	waitAtomicAtLeast(t, func() int64 { return int64(l.capacity.rejected.Load()) }, 1, "listener rejection")
	if err := l.Err(); err != nil {
		t.Fatalf("inbound rejection poisoned Listener.Err: %v", err)
	}
}

type failingAcceptListener struct {
	addr net.Addr
	err  error
}

func (l failingAcceptListener) Accept() (net.Conn, error) { return nil, l.err }
func (l failingAcceptListener) Close() error              { return nil }
func (l failingAcceptListener) Addr() net.Addr            { return l.addr }

func waitAtomicAtLeast(t *testing.T, load func() int64, want int64, name string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if load() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s = %d, want >= %d", name, load(), want)
}
