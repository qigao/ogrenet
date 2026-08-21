package transport

import (
	"context"
	"errors"
)

type engineShutdownSet struct {
	streamListeners []*listener
	wsListeners     []*wsListener
	streams         []*conn
	websockets      []*wsSession
	packets         []*packetConn
}

func (e *Engine) Done() <-chan struct{} { return e.done }

func (e *Engine) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}

	owner, set := e.beginGracefulShutdown()
	if owner {
		if err := e.requestGracefulShutdown(set); err != nil {
			e.recordShutdownError(err)
			e.abortRemaining(abortFailure)
		}
	}

	select {
	case <-e.done:
		return e.shutdownResult()
	case <-ctx.Done():
		if owner {
			e.abortRemaining(abortCaller)
		}
		return context.Cause(ctx)
	}
}

func (e *Engine) Close() error {
	set, transitioned := e.beginAbort(abortExplicit)
	if !transitioned {
		return nil
	}
	return e.closeSet(set)
}

func (e *Engine) beginGracefulShutdown() (bool, engineShutdownSet) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != engineRunning {
		return false, engineShutdownSet{}
	}
	e.state = engineDraining
	set := e.snapshotShutdownSetLocked()
	for _, lease := range e.streamLeases {
		lease.beginDrain()
	}
	for _, lease := range e.wsLeases {
		lease.beginDrain()
	}
	for _, lease := range e.packetLeases {
		lease.beginDrain()
	}
	e.maybeDoneLocked()
	return true, set
}

func (e *Engine) requestGracefulShutdown(set engineShutdownSet) error {
	var errs []error
	for _, l := range set.streamListeners {
		errs = appendCloseErr(errs, l.Close())
	}
	for _, l := range set.wsListeners {
		errs = appendCloseErr(errs, l.Close())
	}
	for _, s := range set.streams {
		s.requestShutdown()
	}
	for _, s := range set.websockets {
		s.requestShutdown()
	}
	for _, p := range set.packets {
		p.requestDrain()
	}
	return errors.Join(errs...)
}

func (e *Engine) abortRemaining(reason abortReason) {
	set, transitioned := e.beginAbort(reason)
	if !transitioned {
		return
	}
	if err := e.closeSet(set); err != nil {
		e.recordShutdownError(err)
	}
}

func (e *Engine) beginAbort(reason abortReason) (engineShutdownSet, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state == engineDone || e.state == engineAborting {
		return engineShutdownSet{}, false
	}
	e.state = engineAborting
	if e.shutdownReason == abortNone {
		e.shutdownReason = reason
	}
	set := e.snapshotShutdownSetLocked()
	e.maybeDoneLocked()
	return set, true
}

func (e *Engine) snapshotShutdownSetLocked() engineShutdownSet {
	return engineShutdownSet{
		streamListeners: keys(e.streamListeners),
		wsListeners:     keys(e.wsListeners),
		streams:         keys(e.streams),
		websockets:      keys(e.websockets),
		packets:         keys(e.packets),
	}
}

func (e *Engine) closeSet(set engineShutdownSet) error {
	var errs []error
	for _, l := range set.streamListeners {
		errs = appendCloseErr(errs, l.Close())
	}
	for _, l := range set.wsListeners {
		errs = appendCloseErr(errs, l.Close())
	}
	for _, s := range set.streams {
		errs = appendCloseErr(errs, s.Close())
	}
	for _, s := range set.websockets {
		errs = appendCloseErr(errs, s.Close())
	}
	for _, p := range set.packets {
		errs = appendCloseErr(errs, p.Close())
	}
	return errors.Join(errs...)
}

func (e *Engine) recordShutdownError(err error) {
	if err == nil {
		return
	}
	e.mu.Lock()
	if e.shutdownErr == nil {
		e.shutdownErr = err
	} else {
		e.shutdownErr = errors.Join(e.shutdownErr, err)
	}
	e.mu.Unlock()
}

func (e *Engine) shutdownResult() error {
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

func appendCloseErr(dst []error, err error) []error {
	if err != nil {
		return append(dst, err)
	}
	return dst
}

func keys[T comparable](m map[T]struct{}) []T {
	out := make([]T, 0, len(m))
	for v := range m {
		out = append(out, v)
	}
	return out
}
