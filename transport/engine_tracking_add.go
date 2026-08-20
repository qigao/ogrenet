package transport

func (e *Engine) beginOp() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	e.activeOps++
	return nil
}

func (e *Engine) endOp() {
	e.mu.Lock()
	e.activeOps--
	if e.activeOps < 0 {
		e.activeOps = 0
	}
	e.maybeDoneLocked()
	e.mu.Unlock()
}

func (e *Engine) addStreamListener(v *listener) error { return addTracked(e, e.streamListeners, v) }
func (e *Engine) addWSListener(v *wsListener) error   { return addTracked(e, e.wsListeners, v) }

func (e *Engine) addStream(v *conn) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	lease, err := e.admission.acquireConnection(peerKey(v.raw.RemoteAddr()))
	if err != nil {
		return err
	}
	v.quota.setParent(e.admission.bytes)
	e.streams[v] = struct{}{}
	e.streamLeases[v] = lease
	return nil
}

func (e *Engine) addWebSocket(v *wsSession) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	lease, err := e.admission.acquireConnection(peerKey(v.remote))
	if err != nil {
		return err
	}
	v.quota.setParent(e.admission.bytes)
	e.websockets[v] = struct{}{}
	e.wsLeases[v] = lease
	return nil
}

func (e *Engine) addPacket(v *packetConn) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	lease, err := e.admission.acquireConnection(peerKey(v.remote))
	if err != nil {
		return err
	}
	v.quota.setParent(e.admission.bytes)
	e.packets[v] = struct{}{}
	e.packetLeases[v] = lease
	return nil
}

func addTracked[T comparable](e *Engine, m map[T]struct{}, v T) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	m[v] = struct{}{}
	return nil
}
