package transport

import "net"

func (e *Engine) acquireOpening(addr net.Addr) (*connectionLease, error) {
	return e.admission.acquireOpening(peerKey(addr))
}

func (e *Engine) acquireOpeningForListener(addr net.Addr, listener *listenerCapacity) (*connectionLease, error) {
	return e.admission.acquireOpeningWithListener(peerKey(addr), listener)
}

type engineActivityLease struct {
	engine *Engine
	lease  *activityLease
}

func (e *Engine) acquireHandshake() (*engineActivityLease, error) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, ErrClosed
	}
	lease, err := e.admission.acquireHandshake()
	e.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &engineActivityLease{engine: e, lease: lease}, nil
}
func (e *Engine) acquireUpgrade() (*engineActivityLease, error) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, ErrClosed
	}
	lease, err := e.admission.acquireUpgrade()
	e.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &engineActivityLease{engine: e, lease: lease}, nil
}
func (l *engineActivityLease) release() {
	if l == nil || l.lease == nil || !l.lease.release() {
		return
	}
	l.engine.maybeDone()
}
func (e *Engine) maybeDone() { e.mu.Lock(); e.maybeDoneLocked(); e.mu.Unlock() }
