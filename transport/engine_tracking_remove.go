package transport

import "github.com/qigao/ogrenet"

func (e *Engine) removeStreamListener(v *listener) { removeTracked(e, e.streamListeners, v) }
func (e *Engine) removeWSListener(v *wsListener)   { removeTracked(e, e.wsListeners, v) }

func (e *Engine) removeStream(v *conn) {
	e.mu.Lock()
	delete(e.streams, v)
	lease := e.streamLeases[v]
	delete(e.streamLeases, v)
	lease.release()
	e.maybeDoneLocked()
	e.mu.Unlock()
}

func (e *Engine) removeWebSocket(v *wsSession) {
	e.mu.Lock()
	delete(e.websockets, v)
	lease := e.wsLeases[v]
	delete(e.wsLeases, v)
	lease.release()
	e.maybeDoneLocked()
	e.mu.Unlock()
}

func (e *Engine) removePacket(v *packetConn) {
	e.mu.Lock()
	delete(e.packets, v)
	lease := e.packetLeases[v]
	delete(e.packetLeases, v)
	lease.release()
	e.maybeDoneLocked()
	e.mu.Unlock()
}

func removeTracked[T comparable](e *Engine, m map[T]struct{}, v T) {
	e.mu.Lock()
	delete(m, v)
	e.maybeDoneLocked()
	e.mu.Unlock()
}

func (e *Engine) maybeDoneLocked() {
	if e.state != engineRunning && e.activeOps == 0 && len(e.streamListeners) == 0 && len(e.wsListeners) == 0 && len(e.streams) == 0 && len(e.websockets) == 0 && len(e.packets) == 0 && e.admission.idle() {
		e.state = engineDone
		e.doneOnce.Do(func() {
			e.observer.stop()
			close(e.done)
		})
	}
}

func normalizeHandler(h ogrenet.Handler) ogrenet.Handler {
	if h == nil {
		return ogrenet.HandlerFuncs{}
	}
	return h
}

func normalizePacketHandler(h ogrenet.PacketHandler) ogrenet.PacketHandler {
	if h == nil {
		return ogrenet.PacketHandlerFuncs{}
	}
	return h
}

var _ ogrenet.Engine = (*Engine)(nil)
