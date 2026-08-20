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
	closing chan struct{}
	done    chan struct{}

	closeOnce sync.Once
	finalOnce sync.Once
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
	return l.initiateClose(nil)
}

func (l *listener) watchContext() {
	select {
	case <-l.ctx.Done():
		_ = l.initiateClose(l.ctx.Err())
	case <-l.done:
	}
}

func (l *listener) acceptLoop() {
	defer l.finalize()
	var delay time.Duration
	for {
		raw, err := l.ln.Accept()
		if err != nil {
			if l.isClosing() {
				return
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
				case <-l.closing:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					return
				}
			}

			_ = l.initiateClose(normalizeListenerError(err))
			return
		}
		delay = 0

		if _, err := l.engine.adopt(raw, l.handler); err != nil {
			_ = raw.Close()
			if errors.Is(err, ErrClosed) {
				_ = l.initiateClose(nil)
			} else {
				_ = l.initiateClose(err)
			}
			return
		}
	}
}

func (l *listener) initiateClose(cause error) error {
	var closeErr error
	l.closeOnce.Do(func() {
		cause = normalizeListenerError(cause)
		l.errMu.Lock()
		l.err = cause
		l.errMu.Unlock()
		close(l.closing)
		l.cancel()
		closeErr = l.ln.Close()
		if errors.Is(closeErr, net.ErrClosed) {
			closeErr = nil
		}
		if cause == nil && closeErr != nil {
			l.errMu.Lock()
			l.err = closeErr
			l.errMu.Unlock()
		}
	})
	return closeErr
}

func (l *listener) finalize() {
	l.finalOnce.Do(func() {
		_ = l.initiateClose(nil)
		l.engine.removeListener(l)
		close(l.done)
	})
}

func (l *listener) isClosing() bool {
	select {
	case <-l.closing:
		return true
	default:
		return false
	}
}

func normalizeListenerError(err error) error {
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

var _ ogrenet.Listener = (*listener)(nil)
