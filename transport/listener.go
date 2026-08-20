package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/qigao/ogrenet"
)

type listener struct {
	engine  *Engine
	ln      net.Listener
	handler ogrenet.Handler
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}

	closeOnce sync.Once
	errMu     sync.RWMutex
	err       error
}

func (l *listener) Addr() net.Addr { return l.ln.Addr() }

func (l *listener) Done() <-chan struct{} { return l.done }

func (l *listener) Err() error {
	l.errMu.RLock()
	defer l.errMu.RUnlock()
	return l.err
}

func (l *listener) Close() error {
	return l.closeWithError(nil)
}

func (l *listener) watchContext() {
	select {
	case <-l.ctx.Done():
		_ = l.closeWithError(l.ctx.Err())
	case <-l.done:
	}
}

func (l *listener) acceptLoop() {
	var delay time.Duration
	for {
		raw, err := l.ln.Accept()
		if err != nil {
			select {
			case <-l.done:
				return
			default:
			}

			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if delay == 0 {
					delay = 5 * time.Millisecond
				} else {
					delay *= 2
					if delay > time.Second {
						delay = time.Second
					}
				}
				timer := time.NewTimer(delay)
				select {
				case <-timer.C:
					continue
				case <-l.done:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					return
				}
			}

			_ = l.closeWithError(normalizeListenerError(err))
			return
		}
		delay = 0

		if _, err := l.engine.adopt(raw, l.handler); err != nil {
			_ = raw.Close()
			if errors.Is(err, ErrClosed) {
				_ = l.closeWithError(nil)
			} else {
				_ = l.closeWithError(err)
			}
			return
		}
	}
}

func (l *listener) closeWithError(cause error) error {
	var closeErr error
	l.closeOnce.Do(func() {
		l.cancel()
		closeErr = l.ln.Close()
		if errors.Is(closeErr, net.ErrClosed) {
			closeErr = nil
		}
		if cause == nil && closeErr != nil {
			cause = closeErr
		}
		l.errMu.Lock()
		l.err = cause
		l.errMu.Unlock()
		l.engine.removeListener(l)
		close(l.done)
	})
	return closeErr
}

func normalizeListenerError(err error) error {
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

var _ ogrenet.Listener = (*listener)(nil)
