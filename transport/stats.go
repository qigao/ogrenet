package transport

import (
	"sync/atomic"
	"time"

	"github.com/qigao/ogrenet"
)

type resourceAge struct {
	started time.Time
	finalNS atomic.Int64
}

func newResourceAge() resourceAge { return resourceAge{started: time.Now()} }

func (a *resourceAge) current() time.Duration {
	if a == nil || a.started.IsZero() {
		return 0
	}
	if v := a.finalNS.Load(); v > 0 {
		return time.Duration(v - 1)
	}
	return time.Since(a.started)
}

func (a *resourceAge) freeze() {
	if a == nil || a.started.IsZero() {
		return
	}
	a.finalNS.CompareAndSwap(0, int64(time.Since(a.started))+1)
}

type listenerCounters struct {
	accepted atomic.Uint64
	age      resourceAge
}

func newListenerCounters() *listenerCounters {
	return &listenerCounters{age: newResourceAge()}
}

func nonNegativeUint64[T ~int | ~int64](v T) uint64 {
	if v <= 0 {
		return 0
	}
	return uint64(v)
}

func (e *Engine) Stats() ogrenet.EngineStats {
	if e == nil || e.admission == nil {
		return ogrenet.EngineStats{}
	}
	s := e.admission.snapshot()
	out := ogrenet.EngineStats{
		OpeningConnections:  nonNegativeUint64(s.OpeningConnections),
		ActiveConnections:   nonNegativeUint64(s.ActiveConnections),
		DrainingConnections: nonNegativeUint64(s.DrainingConnections),
		ActiveHandshakes:    nonNegativeUint64(s.ActiveHandshakes),
		PendingUpgrades:     nonNegativeUint64(s.PendingUpgrades),
		GlobalQueuedBytes:   nonNegativeUint64(s.GlobalQueuedBytes),
		RejectedConnections: s.RejectedConnections,
		RejectedPeers:       s.RejectedPeers,
		RejectedListeners:   s.RejectedListeners,
		RejectedHandshakes:  s.RejectedHandshakes,
		RejectedUpgrades:    s.RejectedUpgrades,
		RejectedQueuedBytes: s.RejectedQueuedBytes,
	}
	if e.observer != nil {
		out.ObserverDroppedEvents = e.observer.dropped.Load()
		out.ObserverPanics = e.observer.panics.Load()
	}
	return out
}

func (c *conn) Stats() ogrenet.SessionStats {
	if c == nil {
		return ogrenet.SessionStats{}
	}
	return ogrenet.SessionStats{
		ResourceID: c.id,
		Protocol:   c.protocol,
		Local:      c.LocalAddr(),
		Remote:     c.RemoteAddr(),
	}
}

func (s *wsSession) Stats() ogrenet.SessionStats {
	if s == nil {
		return ogrenet.SessionStats{}
	}
	return ogrenet.SessionStats{
		ResourceID: s.id,
		Protocol:   s.protocol,
		Local:      s.LocalAddr(),
		Remote:     s.RemoteAddr(),
	}
}

func (p *packetConn) Stats() ogrenet.PacketConnStats {
	if p == nil {
		return ogrenet.PacketConnStats{}
	}
	return ogrenet.PacketConnStats{
		Protocol: ogrenet.SchemeUDP,
		Local:    p.LocalAddr(),
		Remote:   p.RemoteAddr(),
	}
}

func (l *listener) Stats() ogrenet.ListenerStats {
	if l == nil {
		return ogrenet.ListenerStats{}
	}
	out := ogrenet.ListenerStats{
		ResourceID: l.id,
		Protocol:   l.endpoint.Scheme,
		Local:      l.Addr(),
	}
	if l.stats != nil {
		out.Age = l.stats.age.current()
		out.AcceptedConnections = l.stats.accepted.Load()
	}
	if l.capacity != nil {
		out.RejectedConnections = l.capacity.rejected.Load()
		out.CurrentConnections = nonNegativeUint64(l.capacity.current())
	}
	return out
}

func (l *wsListener) Stats() ogrenet.ListenerStats {
	if l == nil {
		return ogrenet.ListenerStats{}
	}
	out := ogrenet.ListenerStats{
		ResourceID: l.id,
		Protocol:   l.endpoint.Scheme,
		Local:      l.Addr(),
	}
	if l.stats != nil {
		out.Age = l.stats.age.current()
		out.AcceptedConnections = l.stats.accepted.Load()
	}
	if l.capacity != nil {
		out.RejectedConnections = l.capacity.rejected.Load()
		out.CurrentConnections = nonNegativeUint64(l.capacity.current())
	}
	return out
}
