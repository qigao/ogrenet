//go:build linux

package transport

import "context"

type epollEngineShutdownSet struct {
	listeners []epollManagedResource
	sessions  []epollManagedResource
}

func (e *epollEngine) shutdownNative(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}

	owner, set := e.beginNativeGracefulShutdown()
	if owner {
		e.requestNativeGracefulShutdown(set)
	}

	select {
	case <-e.done:
		return e.nativeShutdownResult()
	case <-ctx.Done():
		if owner {
			e.abortNativeRemaining(abortCaller)
		}
		return context.Cause(ctx)
	}
}

func (e *epollEngine) closeNative() error {
	if e == nil {
		return nil
	}
	set, transitioned := e.beginNativeAbort(abortExplicit)
	if !transitioned {
		return nil
	}
	e.requestNativeAbort(set, abortExplicit)
	return nil
}

func (e *epollEngine) beginNativeGracefulShutdown() (bool, epollEngineShutdownSet) {
	if e == nil {
		return false, epollEngineShutdownSet{}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != engineRunning {
		return false, epollEngineShutdownSet{}
	}
	e.state = engineDraining
	set := e.snapshotNativeShutdownSetLocked()
	e.maybeQuiescentLocked()
	return true, set
}

func (e *epollEngine) requestNativeGracefulShutdown(set epollEngineShutdownSet) {
	// Admission ownership changes before any Session starts its graceful drain.
	// That keeps Engine Stats and final quiescence aligned with the portable
	// owner model: Active -> Draining -> Released.
	for _, session := range set.sessions {
		session.prepareEngineDrain()
	}
	// Stop new inbound adoption before asking established Sessions to drain.
	for _, listener := range set.listeners {
		listener.requestEngineShutdown()
	}
	for _, session := range set.sessions {
		session.requestEngineShutdown()
	}
	e.wakeAll()
}

func (e *epollEngine) abortNativeRemaining(reason abortReason) {
	set, transitioned := e.beginNativeAbort(reason)
	if !transitioned {
		return
	}
	e.requestNativeAbort(set, reason)
}

func (e *epollEngine) beginNativeAbort(reason abortReason) (epollEngineShutdownSet, bool) {
	if e == nil {
		return epollEngineShutdownSet{}, false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state == engineDone || e.state == engineAborting {
		return epollEngineShutdownSet{}, false
	}
	e.state = engineAborting
	if e.shutdownReason == abortNone {
		e.shutdownReason = reason
	}
	set := e.snapshotNativeShutdownSetLocked()
	e.maybeQuiescentLocked()
	return set, true
}

func (e *epollEngine) snapshotNativeShutdownSetLocked() epollEngineShutdownSet {
	set := epollEngineShutdownSet{}
	for _, resource := range e.managed {
		switch resource.managedKind() {
		case epollManagedListener:
			set.listeners = append(set.listeners, resource)
		case epollManagedSession:
			set.sessions = append(set.sessions, resource)
		}
	}
	return set
}

func (e *epollEngine) requestNativeAbort(set epollEngineShutdownSet, reason abortReason) {
	for _, listener := range set.listeners {
		listener.requestEngineAbort(reason)
	}
	for _, session := range set.sessions {
		session.requestEngineAbort(reason)
	}
	e.wakeAll()
}

func (e *epollEngine) nativeShutdownResult() error {
	if e == nil {
		return ErrClosed
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.shutdownErr != nil {
		return e.shutdownErr
	}
	switch e.shutdownReason {
	case abortExplicit, abortCaller, abortFailure:
		return ErrClosed
	default:
		return nil
	}
}
