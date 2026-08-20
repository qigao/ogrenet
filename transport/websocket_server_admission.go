package transport

import (
	"context"
	"net"
	"net/http"
	"sync"
)

type httpConnLeaseState uint8

const (
	httpConnLeaseHeld httpConnLeaseState = iota + 1
	httpConnLeaseTransferred
	httpConnLeaseReleased
)

type httpConnLease struct {
	tracker *httpConnTracker
	lease   *connectionLease
	mu      sync.Mutex
	state   httpConnLeaseState
}

func (l *httpConnLease) take() *connectionLease {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	if l.state != httpConnLeaseHeld {
		l.mu.Unlock()
		return nil
	}
	l.state = httpConnLeaseTransferred
	lease := l.lease
	l.mu.Unlock()
	l.tracker.detachHolder(l)
	return lease
}

func (l *httpConnLease) release() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.state != httpConnLeaseHeld {
		l.mu.Unlock()
		return
	}
	l.state = httpConnLeaseReleased
	lease := l.lease
	l.mu.Unlock()
	l.tracker.detachHolder(l)
	lease.release()
}

type httpConnTracker struct {
	mu      sync.Mutex
	leases  map[net.Conn]*httpConnLease
	byLease map[*httpConnLease]net.Conn
}

func newHTTPConnTracker() *httpConnTracker {
	return &httpConnTracker{
		leases:  make(map[net.Conn]*httpConnLease),
		byLease: make(map[*httpConnLease]net.Conn),
	}
}

func (t *httpConnTracker) register(conn net.Conn, lease *connectionLease) *httpConnLease {
	h := &httpConnLease{tracker: t, lease: lease, state: httpConnLeaseHeld}
	t.mu.Lock()
	t.leases[conn] = h
	t.byLease[h] = conn
	t.mu.Unlock()
	return h
}

func (t *httpConnTracker) lookup(conn net.Conn) *httpConnLease {
	t.mu.Lock()
	h := t.leases[conn]
	t.mu.Unlock()
	return h
}

func (t *httpConnTracker) rebind(oldConn, newConn net.Conn) bool {
	t.mu.Lock()
	h := t.leases[oldConn]
	if h == nil {
		t.mu.Unlock()
		return false
	}
	delete(t.leases, oldConn)
	t.leases[newConn] = h
	t.byLease[h] = newConn
	t.mu.Unlock()
	return true
}

func (t *httpConnTracker) detachHolder(want *httpConnLease) {
	t.mu.Lock()
	if conn, ok := t.byLease[want]; ok {
		delete(t.byLease, want)
		delete(t.leases, conn)
	}
	t.mu.Unlock()
}

func (t *httpConnTracker) releaseConn(conn net.Conn) {
	if h := t.lookup(conn); h != nil {
		h.release()
	}
}

func (t *httpConnTracker) closeAll() {
	t.mu.Lock()
	all := make([]*httpConnLease, 0, len(t.leases))
	for _, h := range t.leases {
		all = append(all, h)
	}
	t.mu.Unlock()
	for _, h := range all {
		h.release()
	}
}

type admittedHTTPListener struct {
	net.Listener
	engine   *Engine
	capacity *listenerCapacity
	tracker  *httpConnTracker
}

func (l *admittedHTTPListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if err := l.engine.beginOp(); err != nil {
			_ = conn.Close()
			continue
		}
		lease, err := l.engine.acquireOpeningForListener(conn.RemoteAddr(), l.capacity)
		if err == nil {
			l.tracker.register(conn, lease)
		}
		l.engine.endOp()
		if err != nil {
			_ = conn.Close()
			continue
		}
		return conn, nil
	}
}

type httpConnLeaseContextKey struct{}

func (t *httpConnTracker) connContext(ctx context.Context, conn net.Conn) context.Context {
	if h := t.lookup(conn); h != nil {
		return context.WithValue(ctx, httpConnLeaseContextKey{}, h)
	}
	return ctx
}

func httpConnLeaseFromContext(ctx context.Context) *httpConnLease {
	h, _ := ctx.Value(httpConnLeaseContextKey{}).(*httpConnLease)
	return h
}

func (t *httpConnTracker) connState(conn net.Conn, state http.ConnState) {
	switch state {
	case http.StateClosed:
		t.releaseConn(conn)
	case http.StateHijacked:
		// The WebSocket handler transfers or releases the lease after Hijack.
	}
}
