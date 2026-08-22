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
	now := time.Now()
	if timeout := p.engine.cfg.timeouts.ReadIdle; timeout > 0 {
		r.scheduleDeadline(p.id, epollDeadlineReadIdle, p.nativePacketReadIdleGeneration(), now.Add(timeout))
	}
	if timeout := p.engine.cfg.timeouts.ConnectionIdle; timeout > 0 {
		r.scheduleDeadline(p.id, epollDeadlineConnectionIdle, p.nativePacketConnectionIdleGeneration(), now.Add(timeout))
	}
	if timeout := p.engine.cfg.timeouts.MaxLifetime; timeout > 0 {
		deadline := now.Add(timeout)
		if p.stats != nil && !p.stats.age.started.IsZero() {
			deadline = p.stats.age.started.Add(timeout)
		}
		r.scheduleDeadline(p.id, epollDeadlineMaxLifetime, 1, deadline)
	}
}

func (p *epollPacketConn) nativePacketReadIdleGeneration() uint64 {
	if p == nil || p.stats == nil {
		return 1
	}
	return p.stats.packetsRX.Load() + p.stats.droppedDatagrams.Load() + 1
}

func (p *epollPacketConn) nativePacketConnectionIdleGeneration() uint64 {
	if p == nil || p.stats == nil {
		return 1
	}
	return p.stats.packetsTX.Load() + p.stats.bytesRX.Load() + p.stats.droppedDatagrams.Load() + 1
}

func (p *epollPacketConn) noteNativePacketReadProgress(r *epollReactor, bytes int) {
	if p == nil || r == nil || p.engine == nil || p.remote == nil || p.state != epollPacketActive {
		return
	}
	now := time.Now()
	if timeout := p.engine.cfg.timeouts.ReadIdle; timeout > 0 {
		r.scheduleDeadline(p.id, epollDeadlineReadIdle, p.nativePacketReadIdleGeneration(), now.Add(timeout))
	}
	if bytes > 0 {
		if timeout := p.engine.cfg.timeouts.ConnectionIdle; timeout > 0 {
			r.scheduleDeadline(p.id, epollDeadlineConnectionIdle, p.nativePacketConnectionIdleGeneration(), now.Add(timeout))
		}
	}
}

func (p *epollPacketConn) noteNativePacketWriteProgress(r *epollReactor) {
	if p == nil || r == nil || p.engine == nil || p.remote == nil || p.state != epollPacketActive {
		return
	}
	if timeout := p.engine.cfg.timeouts.ConnectionIdle; timeout > 0 {
		r.scheduleDeadline(p.id, epollDeadlineConnectionIdle, p.nativePacketConnectionIdleGeneration(), time.Now().Add(timeout))
	}
}

func (p *epollPacketConn) currentRuntimeDeadlineGeneration(kind epollDeadlineKind) uint64 {
	if p == nil || p.engine == nil || p.remote == nil || p.state != epollPacketActive {
		return 0
	}
	switch kind {
	case epollDeadlineReadIdle:
		if p.engine.cfg.timeouts.ReadIdle <= 0 {
			return 0
		}
		return p.nativePacketReadIdleGeneration()
	case epollDeadlineConnectionIdle:
		if p.engine.cfg.timeouts.ConnectionIdle <= 0 {
			return 0
		}
		return p.nativePacketConnectionIdleGeneration()
	case epollDeadlineMaxLifetime:
		if p.engine.cfg.timeouts.MaxLifetime <= 0 {
			return 0
		}
		return 1
	default:
		return 0
	}
}

func (p *epollPacketConn) onRuntimeReactorDeadline(r *epollReactor, kind epollDeadlineKind, generation uint64) {
	if p == nil || r == nil || p.remote == nil || p.state != epollPacketActive || generation != p.currentRuntimeDeadlineGeneration(kind) {
		return
	}
	var timeout *TimeoutError
	switch kind {
	case epollDeadlineReadIdle:
		timeout = &TimeoutError{Kind: TimeoutReadIdle, Cause: context.DeadlineExceeded}
	case epollDeadlineConnectionIdle:
		timeout = &TimeoutError{Kind: TimeoutConnectionIdle, Cause: context.DeadlineExceeded}
	case epollDeadlineMaxLifetime:
		timeout = &TimeoutError{Kind: TimeoutMaxLifetime, Cause: context.DeadlineExceeded}
	default:
		return
	}
	p.failNativePacketRead(r, timeout, p.remote)
}
