package transport

import "sync"

type sendGate struct {
	mu      sync.Mutex
	closed  bool
	active  int
	drained chan struct{}
}

func newSendGate() *sendGate {
	return &sendGate{drained: make(chan struct{})}
}

func (g *sendGate) enter() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false
	}
	g.active++
	return true
}

func (g *sendGate) leave() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active--
	if g.closed && g.active == 0 {
		close(g.drained)
	}
}

func (g *sendGate) close() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed {
		g.closed = true
		if g.active == 0 {
			close(g.drained)
		}
	}
	return g.drained
}

func (g *sendGate) done() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.drained
}
