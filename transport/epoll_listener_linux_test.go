//go:build linux

package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/wire"
)

type epollTestObserver struct {
	events chan ogrenet.Event
}

func newEpollTestObserver() *epollTestObserver {
	return &epollTestObserver{events: make(chan ogrenet.Event, 64)}
}

func (o *epollTestObserver) Observe(event ogrenet.Event) {
	o.events <- event
}

func waitEpollEvent(t *testing.T, o *epollTestObserver, kind ogrenet.EventKind) ogrenet.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		select {
		case event := <-o.events:
			if event.Kind == kind {
				return event
			}
		case <-ctx.Done():
			t.Fatalf("waiting for event %v: %v", kind, context.Cause(ctx))
		}
	}
}

func assertNoEpollEvent(t *testing.T, o *epollTestObserver, kind ogrenet.EventKind) {
	t.Helper()
	for {
		select {
		case event := <-o.events:
			if event.Kind == kind {
				t.Fatalf("unexpected event %+v", event)
			}
		default:
			return
		}
	}
}

func newEpollTestEngine(t *testing.T, pollers int, opts ...Option) *epollEngine {
	t.Helper()
	engine, err := NewEpoll(EpollConfig{
		Pollers:         pollers,
		EventBatch:      32,
		CallbackWorkers: 2,
		CallbackQueue:   16,
		IOBudgetBytes:   64 << 10,
		IOBudgetOps:     16,
	}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	e := engine.(*epollEngine)
	t.Cleanup(func() {
		_ = e.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		select {
		case <-e.Done():
		case <-ctx.Done():
			t.Errorf("engine cleanup: %v", context.Cause(ctx))
		}
	})
	return e
}

func reactorOwnsEpollResource(t *testing.T, r *epollReactor, id uint64, want epollEventResource) bool {
	t.Helper()
	result := make(chan bool, 1)
	query := newTestInboxItem(func(rr *epollReactor) {
		result <- rr.resources[id] == want
	})
	r.signal(query)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	select {
	case got := <-result:
		return got
	case <-ctx.Done():
		t.Fatalf("reactor registry query: %v", context.Cause(ctx))
		return false
	}
}

func dialNativeListener(t *testing.T, l ogrenet.Listener) net.Conn {
	t.Helper()
	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func waitPeerClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	_, err := conn.Read(one[:])
	if err == nil {
		t.Fatal("peer connection remained open")
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatalf("waiting for peer close: %v", err)
	}
	if !errors.Is(err, io.EOF) {
		// A reset is also a valid observable close for a rejected accepted fd.
		var opErr *net.OpError
		if !errors.As(err, &opErr) {
			t.Fatalf("unexpected peer-close error %T: %v", err, err)
		}
	}
}

func TestEpollListenNativeCreatesReactorOwnedListener(t *testing.T) {
	e := newEpollTestEngine(t, 2)
	endpoint := ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}
	l, err := e.listenNativeTCP(context.Background(), endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	if l.id == 0 {
		t.Fatal("listener ID is zero")
	}
	if l.reactor != e.reactors[0] {
		t.Fatalf("listener reactor=%d, want 0", l.reactor.index)
	}
	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok || addr.Port == 0 {
		t.Fatalf("listener addr=%v", l.Addr())
	}
	if !reactorOwnsEpollResource(t, l.reactor, l.id, l) {
		t.Fatal("listener missing from owning reactor registry")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	waitTestSignal(t, l.Done(), "native listener close")
}

func TestEpollListenNativeContextCancellationClosesCleanly(t *testing.T) {
	e := newEpollTestEngine(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	l, err := e.listenNativeTCP(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	waitTestSignal(t, l.Done(), "listener context cancellation")
	if err := l.Err(); err != nil {
		t.Fatalf("clean context close Err=%v", err)
	}
	if reactorOwnsEpollResource(t, l.reactor, l.id, l) {
		t.Fatal("closed listener remains in reactor registry")
	}
}

func TestEpollAcceptTransfersFDToDeterministicTarget(t *testing.T) {
	observer := newEpollTestObserver()
	e := newEpollTestEngine(t, 2, WithObserver(observer))
	l, err := e.listenNativeTCP(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, nil)
	if err != nil {
		t.Fatal(err)
	}
	conn := dialNativeListener(t, l)
	defer conn.Close()

	event := waitEpollEvent(t, observer, ogrenet.EventAccept)
	if event.ResourceID == 0 || event.ParentID != l.id {
		t.Fatalf("accept correlation=%+v", event)
	}
	e.mu.Lock()
	managed := e.managed[event.ResourceID]
	e.mu.Unlock()
	session, ok := managed.(*epollSession)
	if !ok || session == nil {
		t.Fatalf("managed accept resource=%T", managed)
	}
	if session.reactor != e.reactors[1] {
		t.Fatalf("session reactor=%d, want 1", session.reactor.index)
	}
	if !reactorOwnsEpollResource(t, session.reactor, session.id, session) {
		t.Fatal("accepted session missing from target reactor registry")
	}
}

func TestEpollAcceptPerListenerLimitRejectsSecondConnection(t *testing.T) {
	observer := newEpollTestObserver()
	e := newEpollTestEngine(t, 1,
		WithObserver(observer),
		WithLimits(Limits{MaxConnectionsPerListener: 1}),
	)
	l, err := e.listenNativeTCP(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, nil)
	if err != nil {
		t.Fatal(err)
	}
	first := dialNativeListener(t, l)
	defer first.Close()
	_ = waitEpollEvent(t, observer, ogrenet.EventAccept)

	second := dialNativeListener(t, l)
	defer second.Close()
	waitPeerClosed(t, second)

	stats := l.Stats()
	if stats.AcceptedConnections != 1 || stats.RejectedConnections != 1 || stats.CurrentConnections != 1 {
		t.Fatalf("listener stats=%+v", stats)
	}
	if opening := e.Stats().OpeningConnections; opening != 0 {
		t.Fatalf("engine opening=%d, want 0", opening)
	}
}

func TestEpollAcceptCodecSetupFailureReleasesOwnership(t *testing.T) {
	observer := newEpollTestObserver()
	var callbacks atomic.Int32
	handler := ogrenet.HandlerFuncs{
		Open: func(ogrenet.Session) { callbacks.Add(1) },
		Message: func(ogrenet.Session, ogrenet.Message) { callbacks.Add(1) },
		Close: func(ogrenet.Session, error) { callbacks.Add(1) },
	}
	e := newEpollTestEngine(t, 1,
		WithObserver(observer),
		WithFramerFactory(func() wire.Framer { return nil }),
	)
	l, err := e.listenNativeTCP(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, handler)
	if err != nil {
		t.Fatal(err)
	}
	conn := dialNativeListener(t, l)
	defer conn.Close()
	waitPeerClosed(t, conn)

	if got := callbacks.Load(); got != 0 {
		t.Fatalf("handler callbacks=%d, want 0", got)
	}
	assertNoEpollEvent(t, observer, ogrenet.EventAccept)
	stats := l.Stats()
	if stats.AcceptedConnections != 0 || stats.CurrentConnections != 0 {
		t.Fatalf("listener stats after codec failure=%+v", stats)
	}
	if opening := e.Stats().OpeningConnections; opening != 0 {
		t.Fatalf("engine opening after codec failure=%d, want 0", opening)
	}
}

var _ ogrenet.Listener = (*epollListener)(nil)
