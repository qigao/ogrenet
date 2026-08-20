package transport

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/qigao/ogrenet"
)

func (e *Engine) dialStream(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.Handler) (ogrenet.Session, error) {
	raw, err := e.dialTCP(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	var stream net.Conn = raw
	if endpoint.Scheme == ogrenet.SchemeTLS {
		cfg, err := e.cfg.clientTLSConfig(endpoint)
		if err != nil {
			_ = raw.Close()
			return nil, err
		}
		tlsConn := tls.Client(raw, cfg)
		if err := e.cfg.handshakeClient(ctx, tlsConn); err != nil {
			_ = tlsConn.Close()
			return nil, err
		}
		stream = tlsConn
	}
	c, err := e.adoptStream(stream, endpoint, h)
	if err != nil {
		_ = stream.Close()
		return nil, err
	}
	return c, nil
}

func (e *Engine) adoptStream(raw net.Conn, endpoint ogrenet.Endpoint, h ogrenet.Handler) (*conn, error) {
	framer, err := e.cfg.newFramer()
	if err != nil {
		return nil, err
	}
	c := &conn{
		engine:     e,
		id:         e.nextID.Add(1),
		protocol:   endpoint.Scheme,
		endpoint:   endpoint,
		raw:        raw,
		framer:     framer,
		handler:    h,
		queue:      make(chan outbound, e.cfg.writeQueue),
		quota:      newByteQuota(e.cfg.maxQueuedBytes),
		gate:       newSendGate(),
		frameSlots: make(chan struct{}, e.cfg.writeQueue+1),
		encodeSlot: make(chan struct{}, 1),
		closing:    make(chan struct{}),
		done:       make(chan struct{}),
		readSize:   e.cfg.readBuffer,
		maxRead:    e.cfg.maxBufferedRead,
	}
	if err := e.addStream(c); err != nil {
		return nil, err
	}
	c.start()
	return c, nil
}
