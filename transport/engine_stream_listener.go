package transport

import (
	"context"
	"crypto/tls"
	"github.com/qigao/ogrenet"
	"net"
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
		prepare = func(ctx context.Context, raw net.Conn) (net.Conn, error) {
			handshake, err := e.acquireHandshake()
			if err != nil {
				return nil, err
			}
			defer handshake.release()
			tlsConn := tls.Server(raw, cfg.Clone())
			if err := e.cfg.handshakeServer(ctx, tlsConn); err != nil {
				return nil, err
			}
			return tlsConn, nil
		}
	}
	lctx, cancel := context.WithCancel(ctx)
	l := &listener{engine: e, endpoint: bound, ln: ln, handler: h, prepare: prepare, ctx: lctx, cancel: cancel, capacity: newListenerCapacity(e.cfg.limits.MaxConnectionsPerListener), closing: make(chan struct{}), done: make(chan struct{})}
	if err := e.addStreamListener(l); err != nil {
		cancel()
		_ = ln.Close()
		return nil, err
	}
	go l.watchContext()
	go l.acceptLoop()
	return l, nil
}
