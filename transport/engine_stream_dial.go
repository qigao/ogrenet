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
	lease, err := e.acquireOpening(raw.RemoteAddr())
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	transferred := false
	defer func() {
		if !transferred {
			lease.release()
		}
	}()

	var stream net.Conn = raw
	if endpoint.Scheme == ogrenet.SchemeTLS {
		cfg, err := e.cfg.clientTLSConfig(endpoint)
		if err != nil {
			_ = raw.Close()
			return nil, err
		}
		handshake, err := e.acquireHandshake()
		if err != nil {
			_ = raw.Close()
			return nil, err
		}
		tlsConn := tls.Client(raw, cfg)
		err = e.cfg.handshakeClient(ctx, tlsConn)
		handshake.release()
		if err != nil {
			_ = tlsConn.Close()
			return nil, err
		}
		stream = tlsConn
	}
	c, err := e.adoptStreamWithLease(stream, endpoint, h, lease)
	if err != nil {
		_ = stream.Close()
		return nil, err
	}
	transferred = true
	return c, nil
}

func (e *Engine) adoptStream(raw net.Conn, endpoint ogrenet.Endpoint, h ogrenet.Handler) (*conn, error) {
	return e.adoptStreamWithLease(raw, endpoint, h, nil)
}

func (e *Engine) adoptStreamWithLease(raw net.Conn, endpoint ogrenet.Endpoint, h ogrenet.Handler, lease *connectionLease) (*conn, error) {
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
	if err := e.addStreamWithLease(c, lease); err != nil {
		return nil, err
	}
	c.start()
	return c, nil
}
