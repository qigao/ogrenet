package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
)

// Limits configures Engine-wide resource bounds. Every zero value means
// unlimited; negative values are rejected by WithLimits.
//
// Inbound connection overload is rejected promptly rather than queued without
// bound. Outbound operations return a typed LimitError that unwraps to
// ErrResourceExhausted.
type Limits struct {
	// MaxConnections bounds opening plus active and draining Sessions and
	// PacketConns owned by the Engine.
	MaxConnections int

	// MaxConnectionsPerPeer bounds opening plus active and draining connections
	// for one canonical remote IP when a peer address is available.
	MaxConnectionsPerPeer int

	// MaxConnectionsPerListener bounds opening plus active and draining inbound
	// Sessions for each TCP/TLS/WS/WSS Listener independently.
	MaxConnectionsPerListener int

	// MaxConcurrentHandshakes bounds TLS and WSS handshakes across the Engine.
	MaxConcurrentHandshakes int

	// MaxPendingUpgrades bounds WS/WSS HTTP upgrades across clients and servers.
	MaxPendingUpgrades int

	// MaxQueuedBytesTotal bounds encoded queued plus in-flight application data
	// across all Sessions and PacketConns in the Engine.
	MaxQueuedBytesTotal int64
}

func (l Limits) validate() error {
	if l.MaxConnections < 0 || l.MaxConnectionsPerPeer < 0 || l.MaxConnectionsPerListener < 0 || l.MaxConcurrentHandshakes < 0 || l.MaxPendingUpgrades < 0 || l.MaxQueuedBytesTotal < 0 {
		return ErrInvalidLimits
	}
	return nil
}

type LimitKind uint8

const (
	LimitConnections            LimitKind = 0x01
	LimitConnectionsPerPeer     LimitKind = 0x02
	LimitConnectionsPerListener LimitKind = 0x03
	LimitHandshakes             LimitKind = 0x04
	LimitUpgrades               LimitKind = 0x05
	LimitQueuedBytes            LimitKind = 0x06
)

func (k LimitKind) String() string {
	switch k {
	case LimitConnections:
		return "connections"
	case LimitConnectionsPerPeer:
		return "connections-per-peer"
	case LimitConnectionsPerListener:
		return "connections-per-listener"
	case LimitHandshakes:
		return "handshakes"
	case LimitUpgrades:
		return "websocket-upgrades"
	case LimitQueuedBytes:
		return "queued-bytes"
	default:
		return "unknown"
	}
}

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

type listenerCapacity struct {
	limit    int64
	used     atomic.Int64
	rejected atomic.Uint64
}

func newListenerCapacity(limit int) *listenerCapacity {
	if limit <= 0 {
		return nil
	}
	return &listenerCapacity{limit: int64(limit)}
}
func (c *listenerCapacity) acquire() bool {
	if c == nil {
		return true
	}
	for {
		n := c.used.Load()
		if n >= c.limit {
			c.rejected.Add(1)
			return false
		}
		if c.used.CompareAndSwap(n, n+1) {
			return true
		}
	}
}
func (c *listenerCapacity) release() {
	if c == nil {
		return
	}
	for {
		n := c.used.Load()
		if n <= 0 {
			return
		}
		if c.used.CompareAndSwap(n, n-1) {
			return
		}
	}
}
func (c *listenerCapacity) current() int64 {
	if c == nil {
		return 0
	}
	return c.used.Load()
}

type admissionController struct {
	limits              Limits
	mu                  sync.Mutex
	opening             int
	active              int
	draining            int
	handshakes          int
	upgrades            int
	perPeer             map[string]int
	rejectedConnections atomic.Uint64
	rejectedPeers       atomic.Uint64
	rejectedListeners   atomic.Uint64
	rejectedHandshakes  atomic.Uint64
	rejectedUpgrades    atomic.Uint64
	bytes               *globalByteQuota
}

func newAdmissionController(limits Limits) *admissionController {
	return &admissionController{limits: limits, perPeer: make(map[string]int), bytes: newGlobalByteQuota(limits.MaxQueuedBytesTotal)}
}

func (a *admissionController) acquireOpening(peer string) (*connectionLease, error) {
	return a.acquireOpeningWithListener(peer, nil)
}
func (a *admissionController) acquireOpeningWithListener(peer string, listener *listenerCapacity) (*connectionLease, error) {
	if listener != nil && !listener.acquire() {
		a.rejectedListeners.Add(1)
		return nil, &LimitError{Kind: LimitConnectionsPerListener, Limit: listener.limit}
	}
	a.mu.Lock()
	if a.limits.MaxConnections > 0 && a.opening+a.active+a.draining >= a.limits.MaxConnections {
		a.rejectedConnections.Add(1)
		a.mu.Unlock()
		listener.release()
		return nil, &LimitError{Kind: LimitConnections, Limit: int64(a.limits.MaxConnections)}
	}
	if peer != "" && a.limits.MaxConnectionsPerPeer > 0 && a.perPeer[peer] >= a.limits.MaxConnectionsPerPeer {
		a.rejectedPeers.Add(1)
		a.mu.Unlock()
		listener.release()
		return nil, &LimitError{Kind: LimitConnectionsPerPeer, Limit: int64(a.limits.MaxConnectionsPerPeer)}
	}
	a.opening++
	if peer != "" {
		a.perPeer[peer]++
	}
	a.mu.Unlock()
	return &connectionLease{owner: a, peer: peer, listener: listener, state: connectionOpening}, nil
}
func (a *admissionController) acquireConnection(peer string) (*connectionLease, error) {
	lease, err := a.acquireOpening(peer)
	if err != nil {
		return nil, err
	}
	lease.activate()
	return lease, nil
}

type connectionLeaseState uint8

const (
	connectionOpening connectionLeaseState = iota + 1
	connectionActive
	connectionDraining
	connectionReleased
)

type connectionLease struct {
	owner    *admissionController
	peer     string
	listener *listenerCapacity
	mu       sync.Mutex
	state    connectionLeaseState
}

func (l *connectionLease) activate() bool {
	if l == nil || l.owner == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	switch l.state {
	case connectionActive:
		return true
	case connectionDraining, connectionReleased:
		return false
	case connectionOpening:
		a := l.owner
		a.mu.Lock()
		if a.opening > 0 {
			a.opening--
		}
		a.active++
		a.mu.Unlock()
		l.state = connectionActive
		return true
	}
	return false
}

func (l *connectionLease) beginDrain() bool {
	if l == nil || l.owner == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state != connectionActive {
		return false
	}
	a := l.owner
	a.mu.Lock()
	if a.active > 0 {
		a.active--
	}
	a.draining++
	a.mu.Unlock()
	l.state = connectionDraining
	return true
}

func (l *connectionLease) release() {
	if l == nil || l.owner == nil {
		return
	}
	l.mu.Lock()
	if l.state == connectionReleased {
		l.mu.Unlock()
		return
	}
	state := l.state
	l.state = connectionReleased
	l.mu.Unlock()
	a := l.owner
	a.mu.Lock()
	switch state {
	case connectionOpening:
		if a.opening > 0 {
			a.opening--
		}
	case connectionActive:
		if a.active > 0 {
			a.active--
		}
	case connectionDraining:
		if a.draining > 0 {
			a.draining--
		}
	}
	if l.peer != "" {
		if n := a.perPeer[l.peer]; n <= 1 {
			delete(a.perPeer, l.peer)
		} else {
			a.perPeer[l.peer] = n - 1
		}
	}
	a.mu.Unlock()
	l.listener.release()
}

type activityLease struct {
	owner    *admissionController
	kind     LimitKind
	released atomic.Bool
}

func (a *admissionController) acquireHandshake() (*activityLease, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.limits.MaxConcurrentHandshakes > 0 && a.handshakes >= a.limits.MaxConcurrentHandshakes {
		a.rejectedHandshakes.Add(1)
		return nil, &LimitError{Kind: LimitHandshakes, Limit: int64(a.limits.MaxConcurrentHandshakes)}
	}
	a.handshakes++
	return &activityLease{owner: a, kind: LimitHandshakes}, nil
}
func (a *admissionController) acquireUpgrade() (*activityLease, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.limits.MaxPendingUpgrades > 0 && a.upgrades >= a.limits.MaxPendingUpgrades {
		a.rejectedUpgrades.Add(1)
		return nil, &LimitError{Kind: LimitUpgrades, Limit: int64(a.limits.MaxPendingUpgrades)}
	}
	a.upgrades++
	return &activityLease{owner: a, kind: LimitUpgrades}, nil
}
func (l *activityLease) release() bool {
	if l == nil || l.owner == nil || !l.released.CompareAndSwap(false, true) {
		return false
	}
	a := l.owner
	a.mu.Lock()
	switch l.kind {
	case LimitHandshakes:
		if a.handshakes > 0 {
			a.handshakes--
		}
	case LimitUpgrades:
		if a.upgrades > 0 {
			a.upgrades--
	}
	a.mu.Unlock()
	return true
}

type admissionSnapshot struct {
	OpeningConnections   int
	ActiveConnections    int
	DrainingConnections  int
	ActiveHandshakes     int
	PendingUpgrades      int
	GlobalQueuedBytes    int64
	RejectedConnections  uint64
	RejectedPeers        uint64
	RejectedListeners    uint64
	RejectedHandshakes   uint64
	RejectedUpgrades     uint64
	RejectedQueuedBytes  uint64
}

func (a *admissionController) snapshot() admissionSnapshot {
	a.mu.Lock()
	opening, active, draining, handshakes, upgrades := a.opening, a.active, a.draining, a.handshakes, a.upgrades
	a.mu.Unlock()
	return admissionSnapshot{
		OpeningConnections:  opening,
		ActiveConnections:   active,
		DrainingConnections: draining,
		ActiveHandshakes:    handshakes,
		PendingUpgrades:     upgrades,
		GlobalQueuedBytes:   a.bytes.current(),
		RejectedConnections: a.rejectedConnections.Load(),
		RejectedPeers:       a.rejectedPeers.Load(),
		RejectedListeners:   a.rejectedListeners.Load(),
		RejectedHandshakes:  a.rejectedHandshakes.Load(),
		RejectedUpgrades:    a.rejectedUpgrades.Load(),
		RejectedQueuedBytes: a.bytes.rejected.Load(),
	}
}
func (a *admissionController) idle() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.opening == 0 && a.active == 0 && a.draining == 0 && a.handshakes == 0 && a.upgrades == 0
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
	return &globalByteQuota{limit: limit}
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
		if q.changed == nil {
			q.changed = make(chan struct{})
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
	if q.changed != nil {
		close(q.changed)
		q.changed = nil
	}
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
