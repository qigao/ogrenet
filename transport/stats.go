package transport

import (
	"net"
	"sync/atomic"
	"time"

	"github.com/qigao/ogrenet"
)

type resourceAge struct {
	started time.Time
	finalNS atomic.Int64
}

func newResourceAge() resourceAge { return resourceAge{started: time.Now()} }

func positiveElapsed(start time.Time) time.Duration {
	if start.IsZero() {
		return 0
	}
	elapsed := time.Since(start)
	if elapsed <= 0 {
		return time.Nanosecond
	}
	return elapsed
}

func (a *resourceAge) current() time.Duration {
	if a == nil || a.started.IsZero() {
		return 0
	}
	if v := a.finalNS.Load(); v > 0 {
		return time.Duration(v - 1)
	}
	return positiveElapsed(a.started)
}

func (a *resourceAge) freeze() {
	if a == nil || a.started.IsZero() {
		return
	}
	a.finalNS.CompareAndSwap(0, int64(positiveElapsed(a.started))+1)
}

type listenerCounters struct {
	accepted atomic.Uint64
	age      resourceAge
}

func newListenerCounters() *listenerCounters {
	return &listenerCounters{age: newResourceAge()}
}

type sessionCounters struct {
	bytesRX      atomic.Uint64
	bytesTX      atomic.Uint64
	messagesRX   atomic.Uint64
	messagesTX   atomic.Uint64
	backpressure atomic.Uint64
	decodeErrors atomic.Uint64
	age          resourceAge
}

func newSessionCounters() *sessionCounters {
	return &sessionCounters{age: newResourceAge()}
}

type packetCounters struct {
	bytesRX          atomic.Uint64
	bytesTX          atomic.Uint64
	packetsRX        atomic.Uint64
	packetsTX        atomic.Uint64
	backpressure     atomic.Uint64
	droppedDatagrams atomic.Uint64
	age              resourceAge
}

func newPacketCounters() *packetCounters {
	return &packetCounters{age: newResourceAge()}
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
		out.ObserverDroppedEvents = e.observer.droppedCount()
		out.ObserverPanics = e.observer.panicCount()
	}
	return out
}

func sessionStatsSnapshot(id uint64, protocol ogrenet.Scheme, local, remote net.Addr, counters *sessionCounters) ogrenet.SessionStats {
	out := ogrenet.SessionStats{ResourceID: id, Protocol: protocol, Local: local, Remote: remote}
	if counters != nil {
		out.Age = counters.age.current()
		out.BytesRX = counters.bytesRX.Load()
		out.BytesTX = counters.bytesTX.Load()
		out.MessagesRX = counters.messagesRX.Load()
		out.MessagesTX = counters.messagesTX.Load()
		out.Backpressure = counters.backpressure.Load()
		out.DecodeErrors = counters.decodeErrors.Load()
	}
	return out
}

func (c *conn) Stats() ogrenet.SessionStats {
	if c == nil {
		return ogrenet.SessionStats{}
	}
	out := sessionStatsSnapshot(c.id, c.protocol, c.LocalAddr(), c.RemoteAddr(), c.stats)
	if c.frameSlots != nil {
		out.QueuedFrames = uint64(len(c.frameSlots))
	}
	if c.quota != nil {
		out.QueuedBytes = nonNegativeUint64(c.quota.current())
	}
	return out
}

func (s *wsSession) Stats() ogrenet.SessionStats {
	if s == nil {
		return ogrenet.SessionStats{}
	}
	out := sessionStatsSnapshot(s.id, s.protocol, s.LocalAddr(), s.RemoteAddr(), s.stats)
	if s.frameSlots != nil {
		out.QueuedFrames = uint64(len(s.frameSlots))
	}
	if s.quota != nil {
		out.QueuedBytes = nonNegativeUint64(s.quota.current())
	}
	return out
}

func (p *packetConn) Stats() ogrenet.PacketConnStats {
	if p == nil {
		return ogrenet.PacketConnStats{}
	}
	out := ogrenet.PacketConnStats{
		ResourceID: p.id,
		Protocol:   ogrenet.SchemeUDP,
		Local:      p.LocalAddr(),
		Remote:     p.RemoteAddr(),
	}
	if p.stats != nil {
		out.Age = p.stats.age.current()
		out.BytesRX = p.stats.bytesRX.Load()
		out.BytesTX = p.stats.bytesTX.Load()
		out.PacketsRX = p.stats.packetsRX.Load()
		out.PacketsTX = p.stats.packetsTX.Load()
		out.Backpressure = p.stats.backpressure.Load()
		out.DroppedDatagrams = p.stats.droppedDatagrams.Load()
	}
	if p.slots != nil {
		out.QueuedPackets = uint64(len(p.slots))
	}
	if p.quota != nil {
		out.QueuedBytes = nonNegativeUint64(p.quota.current())
	}
	return out
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
