package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
)

type tlsAcceptResult struct {
	conn net.Conn
	err  error
}

// gatedTLSListener accepts TCP connections continuously and performs TLS
// handshakes concurrently behind the Engine handshake admission gate. Successful
// accepts are returned as the concrete *tls.Conn so net/http preserves Request.TLS.
type gatedTLSListener struct {
	base   net.Listener
	engine *Engine
	cfg    *tls.Config
	ctx    context.Context
	cancel context.CancelFunc
	ready  chan tlsAcceptResult

	closeOnce sync.Once
	errMu     sync.Mutex
	err       error
}

func newGatedTLSListener(ctx context.Context, base net.Listener, engine *Engine, cfg *tls.Config) *gatedTLSListener {
	lctx, cancel := context.WithCancel(ctx)
	l := &gatedTLSListener{
		base:   base,
		engine: engine,
		cfg:    cfg,
		ctx:    lctx,
		cancel: cancel,
		ready:  make(chan tlsAcceptResult),
	}
	go l.acceptLoop()
	return l
}

func (l *gatedTLSListener) Accept() (net.Conn, error) {
	select {
	case result := <-l.ready:
		return result.conn, result.err
	case <-l.ctx.Done():
		l.errMu.Lock()
		err := l.err
		l.errMu.Unlock()
		if err != nil {
			return nil, err
		}
		return nil, net.ErrClosed
	}
}

func (l *gatedTLSListener) Close() error {
	var err error
	l.closeOnce.Do(func() {
		l.cancel()
		err = l.base.Close()
	})
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func (l *gatedTLSListener) Addr() net.Addr { return l.base.Addr() }

func (l *gatedTLSListener) acceptLoop() {
	for {
		raw, err := l.base.Accept()
		if err != nil {
			if l.ctx.Err() == nil {
				l.errMu.Lock()
				l.err = err
				l.errMu.Unlock()
			}
			l.cancel()
			return
		}
		go l.handshake(raw)
	}
}

func (l *gatedTLSListener) handshake(raw net.Conn) {
	lease, err := l.engine.acquireHandshake()
	if err != nil {
		_ = raw.Close()
		return
	}
	defer lease.release()

	tlsConn := tls.Server(raw, l.cfg.Clone())
	if err := l.engine.cfg.handshakeServer(l.ctx, tlsConn); err != nil {
		_ = tlsConn.Close()
		return
	}
	select {
	case l.ready <- tlsAcceptResult{conn: tlsConn}:
	case <-l.ctx.Done():
		_ = tlsConn.Close()
	}
}
