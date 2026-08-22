//go:build linux

package transport

import (
	"context"
	"time"
)

func (p *epollPacketConn) onEpollReactorRegistered(r *epollReactor) {
	if p == nil || r == nil || p.engine == nil || p.remote == nil {
		return
	}
	if timeout := p.engine.cfg.timeouts.ReadIdle; timeout > 0 {
		r.scheduleDeadline(p.id, epollDeadlineReadIdle, 1, time.Now().Add(timeout))
	}
}

func (p *epollPacketConn) currentRuntimeDeadlineGeneration(kind epollDeadlineKind) uint64 {
	if p == nil || p.remote == nil || p.state != epollPacketActive || kind != epollDeadlineReadIdle {
		return 0
	}
	return 1
}

func (p *epollPacketConn) onRuntimeReactorDeadline(r *epollReactor, kind epollDeadlineKind, generation uint64) {
	if p == nil || r == nil || p.remote == nil || p.state != epollPacketActive || kind != epollDeadlineReadIdle || generation != 1 {
		return
	}
	timeout := &TimeoutError{Kind: TimeoutReadIdle, Cause: context.DeadlineExceeded}
	p.failNativePacketRead(r, timeout, p.remote)
}
