package transport

import (
	"context"
	"errors"
)

func (e *Engine) Done() <-chan struct{} { return e.done }

func (e *Engine) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	closeErr := e.Close()
	select {
	case <-e.done:
		return closeErr
	case <-ctx.Done():
		return errors.Join(closeErr, ctx.Err())
	}
}

func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true

	streamListeners := keys(e.streamListeners)
	wsListeners := keys(e.wsListeners)
	streams := keys(e.streams)
	websockets := keys(e.websockets)
	packets := keys(e.packets)
	e.maybeDoneLocked()
	e.mu.Unlock()

	var errs []error
	for _, l := range streamListeners {
		errs = appendCloseErr(errs, l.Close())
	}
	for _, l := range wsListeners {
		errs = appendCloseErr(errs, l.Close())
	}
	for _, s := range streams {
		errs = appendCloseErr(errs, s.Close())
	}
	for _, s := range websockets {
		errs = appendCloseErr(errs, s.Close())
	}
	for _, p := range packets {
		errs = appendCloseErr(errs, p.Close())
	}
	return errors.Join(errs...)
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
