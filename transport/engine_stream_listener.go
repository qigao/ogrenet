package transport

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/qigao/ogrenet"
)

func (e *Engine) listenStream(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.Handler) (ogrenet.Listener, error) {
	ln, err := e.listenTCP(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	bound := boundEndpoint(endpoint, ln.Addr())
	var prepare streamPrepare
	if endpoint.Scheme == ogrenet.SchemeTLS {
		cfg, err := e.cfg.serverTLSConfig()
		if err != nil {
			_ = ln.Close()
			return nil, err
		}
		prepare = func(ctx context.Context, raw net.Conn) (net.Conn, time.Duration, error) {
			observing := e.observer != nil
			var started time.Time
			if observing {
				started = time.Now()
			}
			handshake, err := e.acquireHandshake()
			var duration time.Duration
			if observing {
				duration = time.Since(started)
			}
			if err != nil {
				return nil, duration, err
			}
			defer handshake.release()
			tlsConn := tls.Server(raw, cfg.Clone())
			if observing {
				started = time.Now()
			}
			err = e.cfg.handshakeServer(ctx, tlsConn)
			if observing {
				duration = time.Since(started)
			}
			if err != nil {
				return nil, duration, err
			}
			return tlsConn, duration, nil
		}
	}
	lctx, cancel := context.WithCancel(ctx)
	l := &listener{
		engine:   e,
		id:       e.nextID.Add(1),
		endpoint: bound,
		ln:       ln,
		handler:  h,
		prepare:  prepare,
		ctx:      lctx,
		cancel:   cancel,
		capacity: newListenerCapacity(e.cfg.limits.MaxConnectionsPerListener),
		stats:    newListenerCounters(),
		closing:  make(chan struct{}),
		done:     make(chan struct{}),
	}
	if err := e.addStreamListener(l); err != nil {
		cancel()
		_ = ln.Close()
		return nil, err
	}
	go l.watchContext()
	go l.acceptLoop()
	return l, nil
}
