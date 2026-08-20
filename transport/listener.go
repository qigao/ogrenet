package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/qigao/ogrenet"
)

type streamPrepare func(context.Context, net.Conn) (net.Conn, error)

type listener struct {
	engine   *Engine
	endpoint ogrenet.Endpoint
	ln       net.Listener
	handler  ogrenet.Handler
	prepare  streamPrepare
	ctx      context.Context
	cancel   context.CancelFunc
	closing  chan struct{}
	done     chan struct{}

	closeOnce sync.Once
	finalOnce sync.Once
	errMu     sync.RWMutex
	err       error
}

func (l *listener) Endpoint() ogrenet.Endpoint { return l.endpoint }
func (l *listener) Addr() net.Addr             { return l.ln.Addr() }
func (l *listener) Done() <-chan struct{}      { return l.done }

func (l *listener) Err() error {
	l.errMu.RLock()
	defer l.errMu.RUnlock()
	return l.err
}

func (l *listener) Close() error { return l.initiateClose(nil) }

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
				if !waitOrClosed(delay, l.closing) {
					return
				}
				continue
			}
			_ = l.initiateClose(normalizeListenerError(err))
			return
		}
		delay = 0

		if tcp, ok := raw.(*net.TCPConn); ok {
			if err := l.engine.configureTCP(tcp); err != nil {
				_ = raw.Close()
				continue
			}
		}
		go l.handleAccepted(raw)
	}
}

func (l *listener) handleAccepted(raw net.Conn) {
	if err := l.engine.beginOp(); err != nil {
		_ = raw.Close()
		return
	}
	defer l.engine.endOp()

	lease, err := l.engine.acquireOpening(raw.RemoteAddr())
	if err != nil {
		_ = raw.Close()
		return
	}
	transferred := false
	defer func() {
		if !transferred {
			lease.release()
		}
	}()

	if l.prepare != nil {
		prepared, err := l.prepare(l.ctx, raw)
		if err != nil {
			_ = raw.Close()
			return
		}
		raw = prepared
	}

	if _, err := l.engine.adoptStreamWithLease(raw, l.endpoint, l.handler, lease); err != nil {
		_ = raw.Close()
		if !errors.Is(err, ErrClosed) && !errors.Is(err, ErrResourceExhausted) {
			_ = l.initiateClose(err)
		}
		return
	}
	transferred = true
}

func waitOrClosed(delay time.Duration, closing <-chan struct{}) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-closing:
		return false
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
		close(l.done)
		l.engine.removeStreamListener(l)
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
	if err == nil || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

var _ ogrenet.Listener = (*listener)(nil)
