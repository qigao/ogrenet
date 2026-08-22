//go:build linux

package transport

import (
	"context"
	"time"
)

type epollSessionRuntimeDeadlines struct {
	initialized bool

	readIdleGen   uint64
	readIdleArmed bool

	connectionIdleGen        uint64
	connectionIdleArmed      bool
	connectionIdleActivityNS int64

	maxLifetimeGen   uint64
	maxLifetimeArmed bool
}

func (s *epollSession) currentRuntimeDeadlineGeneration(kind epollDeadlineKind) uint64 {
	if s == nil {
		return 0
	}
	d := &s.runtimeDeadlines
	switch kind {
	case epollDeadlineReadIdle:
		return d.readIdleGen
	case epollDeadlineConnectionIdle:
		return d.connectionIdleGen
	case epollDeadlineMaxLifetime:
		return d.maxLifetimeGen
	default:
		return 0
	}
}

func (s *epollSession) reconcileNativeActivityDeadlines(r *epollReactor) {
	if s == nil || r == nil || s.engine == nil || s.activity == nil || s.terminalPrepared || (s.state != epollSessionOpening && s.state != epollSessionActive) {
		return
	}
	d := &s.runtimeDeadlines
	timeouts := s.engine.cfg.timeouts
	lastActivityNS := s.activity.lastActivityNS.Load()

	if !d.initialized {
		d.initialized = true
		if timeouts.ConnectionIdle > 0 {
			d.connectionIdleGen++
			d.connectionIdleArmed = true
			d.connectionIdleActivityNS = lastActivityNS
			r.scheduleDeadline(
				s.id,
				epollDeadlineConnectionIdle,
				d.connectionIdleGen,
				s.activity.born.Add(time.Duration(lastActivityNS)+timeouts.ConnectionIdle),
			)
		}
		if timeouts.MaxLifetime > 0 {
			d.maxLifetimeGen++
			d.maxLifetimeArmed = true
			r.scheduleDeadline(
				s.id,
				epollDeadlineMaxLifetime,
				d.maxLifetimeGen,
				s.activity.born.Add(timeouts.MaxLifetime),
			)
		}
		return
	}

	if timeouts.ConnectionIdle > 0 && d.connectionIdleArmed && lastActivityNS != d.connectionIdleActivityNS {
		d.connectionIdleGen++
		d.connectionIdleActivityNS = lastActivityNS
		r.scheduleDeadline(
			s.id,
			epollDeadlineConnectionIdle,
			d.connectionIdleGen,
			s.activity.born.Add(time.Duration(lastActivityNS)+timeouts.ConnectionIdle),
		)
	}
}

func (s *epollSession) ensureNativeReadIdleDeadline(r *epollReactor) {
	if s == nil || r == nil || s.engine == nil || s.terminalPrepared || s.state != epollSessionActive || s.callbackState != epollCallbackIdle || s.nativeReadHalfClosed() {
		return
	}
	timeout := s.engine.cfg.timeouts.ReadIdle
	if timeout <= 0 || s.runtimeDeadlines.readIdleArmed {
		return
	}
	d := &s.runtimeDeadlines
	d.readIdleGen++
	d.readIdleArmed = true
	r.scheduleDeadline(s.id, epollDeadlineReadIdle, d.readIdleGen, time.Now().Add(timeout))
}

func (s *epollSession) disarmNativeReadIdle() {
	if s == nil || !s.runtimeDeadlines.readIdleArmed {
		return
	}
	s.runtimeDeadlines.readIdleGen++
	s.runtimeDeadlines.readIdleArmed = false
}

func (s *epollSession) invalidateNativeRuntimeDeadlines() {
	if s == nil {
		return
	}
	d := &s.runtimeDeadlines
	d.readIdleGen++
	d.readIdleArmed = false
	d.connectionIdleGen++
	d.connectionIdleArmed = false
	d.maxLifetimeGen++
	d.maxLifetimeArmed = false
}

func (s *epollSession) onRuntimeReactorDeadline(r *epollReactor, kind epollDeadlineKind, generation uint64) {
	if s == nil || r == nil || s.terminalPrepared || (s.state != epollSessionOpening && s.state != epollSessionActive) {
		return
	}
	d := &s.runtimeDeadlines
	switch kind {
	case epollDeadlineReadIdle:
		if generation != d.readIdleGen || !d.readIdleArmed || s.state != epollSessionActive || s.callbackState != epollCallbackIdle || s.nativeReadHalfClosed() {
			return
		}
		d.readIdleGen++
		d.readIdleArmed = false
		timeout := &TimeoutError{Kind: TimeoutReadIdle, Cause: context.DeadlineExceeded}
		s.failNativeLifecycle(r, s.nativeReadError(timeout, hintNone))
	case epollDeadlineConnectionIdle:
		if generation != d.connectionIdleGen || !d.connectionIdleArmed {
			return
		}
		d.connectionIdleGen++
		d.connectionIdleArmed = false
		s.failNativeLifecycle(r, &TimeoutError{Kind: TimeoutConnectionIdle, Cause: context.DeadlineExceeded})
	case epollDeadlineMaxLifetime:
		if generation != d.maxLifetimeGen || !d.maxLifetimeArmed {
			return
		}
		d.maxLifetimeGen++
		d.maxLifetimeArmed = false
		s.failNativeLifecycle(r, &TimeoutError{Kind: TimeoutMaxLifetime, Cause: context.DeadlineExceeded})
	}
}
