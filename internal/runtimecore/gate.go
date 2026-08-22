package runtimecore

import "sync"

type SendGate struct {
	mu      sync.Mutex
	closed  bool
	active  int
	drained chan struct{}
}

func NewSendGate() *SendGate {
	return &SendGate{drained: make(chan struct{})}
}

func (g *SendGate) Enter() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false
	}
	g.active++
	return true
}

func (g *SendGate) Leave() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active--
	if g.closed && g.active == 0 {
		close(g.drained)
	}
}

func (g *SendGate) Close() <-chan struct{} {
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

func (g *SendGate) Done() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.drained
}
