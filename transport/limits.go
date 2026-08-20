package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
)

// Limits configures Engine-wide resource bounds. Zero means unlimited.
type Limits struct {
	MaxConnections        int
	MaxConnectionsPerPeer int
	MaxQueuedBytesTotal   int64
}

func (l Limits) validate() error {
	if l.MaxConnections < 0 || l.MaxConnectionsPerPeer < 0 || l.MaxQueuedBytesTotal < 0 {
		return ErrInvalidLimits
	}
	return nil
}

// LimitKind identifies the Engine resource that rejected an operation.
type LimitKind uint8

const (
	LimitConnections LimitKind = iota + 1
	LimitConnectionsPerPeer
	LimitQueuedBytes
)

func (k LimitKind) String() string {
	switch k {
	case LimitConnections:
		return "connections"
	case LimitConnectionsPerPeer:
		return "connections-per-peer"
	case LimitQueuedBytes:
		return "queued-bytes"
	default:
		return "unknown"
	}
}

// LimitError reports a concrete resource limit rejection. It unwraps to
// ErrResourceExhausted so callers can use errors.Is/errors.As.
type LimitError struct {
	Kind  LimitKind
	Limit int64
}

func (e *LimitError) Error() string {
	if e == nil {
		return ErrResourceExhausted.Error()
	}
	return fmt.Sprintf("transport: %s limit exhausted (limit=%d)", e.Kind, e.Limit)
}

func (e *LimitError) Unwrap() error { return ErrResourceExhausted }

var (
	ErrResourceExhausted = errors.New("transport: engine resource limit exhausted")
	ErrInvalidLimits     = errors.New("transport: resource limits must be non-negative")
)

type admissionController struct {
	limits Limits

	mu      sync.Mutex
	active  int
	perPeer map[string]int

	rejectedConnections atomic.Uint64
	rejectedPeers       atomic.Uint64
	bytes               *globalByteQuota
}

func newAdmissionController(limits Limits) *admissionController {
	return &admissionController{
		limits:  limits,
		perPeer: make(map[string]int),
		bytes:   newGlobalByteQuota(limits.MaxQueuedBytesTotal),
	}
}

func (a *admissionController) acquireConnection(peer string) (*connectionLease, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.limits.MaxConnections > 0 && a.active >= a.limits.MaxConnections {
		a.rejectedConnections.Add(1)
		return nil, &LimitError{Kind: LimitConnections, Limit: int64(a.limits.MaxConnections)}
	}
	if peer != "" && a.limits.MaxConnectionsPerPeer > 0 && a.perPeer[peer] >= a.limits.MaxConnectionsPerPeer {
		a.rejectedPeers.Add(1)
		return nil, &LimitError{Kind: LimitConnectionsPerPeer, Limit: int64(a.limits.MaxConnectionsPerPeer)}
	}

	a.active++
	if peer != "" {
		a.perPeer[peer]++
	}
	return &connectionLease{owner: a, peer: peer}, nil
}

type connectionLease struct {
	owner    *admissionController
	peer     string
	released atomic.Bool
}

func (l *connectionLease) release() {
	if l == nil || l.owner == nil || !l.released.CompareAndSwap(false, true) {
		return
	}
	a := l.owner
	a.mu.Lock()
	if a.active > 0 {
		a.active--
	}
	if l.peer != "" {
		if n := a.perPeer[l.peer]; n <= 1 {
			delete(a.perPeer, l.peer)
		} else {
			a.perPeer[l.peer] = n - 1
		}
	}
	a.mu.Unlock()
}

type admissionSnapshot struct {
	ActiveConnections   int
	GlobalQueuedBytes   int64
	RejectedConnections uint64
	RejectedPeers       uint64
	RejectedQueuedBytes uint64
}

func (a *admissionController) snapshot() admissionSnapshot {
	a.mu.Lock()
	active := a.active
	a.mu.Unlock()
	return admissionSnapshot{
		ActiveConnections:   active,
		GlobalQueuedBytes:   a.bytes.current(),
		RejectedConnections: a.rejectedConnections.Load(),
		RejectedPeers:       a.rejectedPeers.Load(),
		RejectedQueuedBytes: a.bytes.rejected.Load(),
	}
}

func peerKey(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	switch a := addr.(type) {
	case *net.TCPAddr:
		if a == nil {
			return ""
		}
		return canonicalIP(a.IP)
	case *net.UDPAddr:
		if a == nil {
			return ""
		}
		return canonicalIP(a.IP)
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return ""
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return host
	}
	return canonicalIP(ip)
}

func canonicalIP(ip net.IP) string {
	if v4 := ip.To4(); v4 != nil {
		return v4.String()
	}
	return ip.String()
}

type globalByteQuota struct {
	mu       sync.Mutex
	limit    int64
	used     int64
	changed  chan struct{}
	rejected atomic.Uint64
}

func newGlobalByteQuota(limit int64) *globalByteQuota {
	return &globalByteQuota{limit: limit, changed: make(chan struct{})}
}

func (q *globalByteQuota) acquire(ctx context.Context, closing <-chan struct{}, n int) error {
	if q == nil || q.limit == 0 || n == 0 {
		return nil
	}
	want := int64(n)
	if want > q.limit {
		q.rejected.Add(1)
		return &LimitError{Kind: LimitQueuedBytes, Limit: q.limit}
	}
	for {
		q.mu.Lock()
		if q.used+want <= q.limit {
			q.used += want
			q.mu.Unlock()
			return nil
		}
		changed := q.changed
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-closing:
			return ErrClosed
		case <-changed:
		}
	}
}

func (q *globalByteQuota) tryAcquire(n int) error {
	if q == nil || q.limit == 0 || n == 0 {
		return nil
	}
	want := int64(n)
	if want > q.limit {
		q.rejected.Add(1)
		return &LimitError{Kind: LimitQueuedBytes, Limit: q.limit}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.used+want > q.limit {
		q.rejected.Add(1)
		return errors.Join(ErrWouldBlock, &LimitError{Kind: LimitQueuedBytes, Limit: q.limit})
	}
	q.used += want
	return nil
}

func (q *globalByteQuota) release(n int) {
	if q == nil || q.limit == 0 || n <= 0 {
		return
	}
	q.mu.Lock()
	q.used -= int64(n)
	if q.used < 0 {
		q.used = 0
	}
	close(q.changed)
	q.changed = make(chan struct{})
	q.mu.Unlock()
}

func (q *globalByteQuota) current() int64 {
	if q == nil || q.limit == 0 {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.used
}

func (e *Engine) admissionSnapshot() admissionSnapshot { return e.admission.snapshot() }
