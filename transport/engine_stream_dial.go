package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"time"

	"github.com/qigao/ogrenet"
)

func (e *Engine) dialStream(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.Handler) (ogrenet.Session, error) {
	observing := e.observer != nil
	var connectStart time.Time
	if observing {
		connectStart = time.Now()
	}
	raw, err := e.dialTCP(ctx, endpoint)
	var connectDuration time.Duration
	if observing {
		connectDuration = positiveElapsed(connectStart)
	}
	if err != nil {
		if observing {
			e.observeSetup(ogrenet.EventConnect, 0, 0, endpoint.Scheme, nil, nil, connectDuration, err)
		}
		return nil, err
	}
	local, remote := raw.LocalAddr(), raw.RemoteAddr()

	lease, err := e.acquireOpening(raw.RemoteAddr())
	if err != nil {
		opErr := classifyOperational(OpDial, endpoint.Scheme, local, remote, err, hintNone)
		if observing {
			e.observeSetup(ogrenet.EventConnect, 0, 0, endpoint.Scheme, local, remote, connectDuration, nil)
		}
		_ = raw.Close()
		return nil, opErr
	}
	transferred := false
	defer func() {
		if !transferred {
			lease.release()
		}
	}()

	var (
		stream            net.Conn = raw
		handshakeDuration time.Duration
		handshakeComplete bool
	)
	if endpoint.Scheme == ogrenet.SchemeTLS {
		cfg, err := e.cfg.clientTLSConfig(endpoint)
		if err != nil {
			if observing {
				e.observeSetup(ogrenet.EventConnect, 0, 0, endpoint.Scheme, local, remote, connectDuration, nil)
			}
			_ = raw.Close()
			return nil, err
		}
		var handshakeStart time.Time
		if observing {
			handshakeStart = time.Now()
		}
		handshake, err := e.acquireHandshake()
		if observing {
			handshakeDuration = positiveElapsed(handshakeStart)
		}
		if err != nil {
			opErr := classifyOperational(OpHandshake, endpoint.Scheme, local, remote, err, hintNone)
			if observing {
				e.observeSetup(ogrenet.EventConnect, 0, 0, endpoint.Scheme, local, remote, connectDuration, nil)
				e.observeSetup(ogrenet.EventHandshake, 0, 0, endpoint.Scheme, local, remote, handshakeDuration, opErr)
			}
			_ = raw.Close()
			return nil, opErr
		}
		tlsConn := tls.Client(raw, cfg)
		if observing {
			handshakeStart = time.Now()
		}
		err = e.cfg.handshakeClient(ctx, tlsConn)
		if observing {
			handshakeDuration = positiveElapsed(handshakeStart)
		}
		handshake.release()
		if err != nil {
			local, remote = raw.LocalAddr(), raw.RemoteAddr()
			_ = tlsConn.Close()
			var opErr error
			if cause := context.Cause(ctx); cause != nil {
				opErr = cause
			} else {
				opErr = classifyOperational(OpHandshake, endpoint.Scheme, local, remote, err, hintTLSHandshake)
			}
			if observing {
				e.observeSetup(ogrenet.EventConnect, 0, 0, endpoint.Scheme, local, remote, connectDuration, nil)
				e.observeSetup(ogrenet.EventHandshake, 0, 0, endpoint.Scheme, local, remote, handshakeDuration, opErr)
			}
			return nil, opErr
		}
		handshakeComplete = true
		stream = tlsConn
	}
	c, err := e.adoptStreamWithLease(stream, endpoint, h, lease)
	if err != nil {
		local, remote = stream.LocalAddr(), stream.RemoteAddr()
		if observing {
			e.observeSetup(ogrenet.EventConnect, 0, 0, endpoint.Scheme, local, remote, connectDuration, nil)
			if handshakeComplete {
				e.observeSetup(ogrenet.EventHandshake, 0, 0, endpoint.Scheme, local, remote, handshakeDuration, nil)
			}
		}
		_ = stream.Close()
		if errors.Is(err, ErrClosed) || errors.Is(err, ErrResourceExhausted) {
			return nil, classifyOperational(OpDial, endpoint.Scheme, local, remote, err, hintNone)
		}
		return nil, err
	}
	transferred = true
	if observing {
		e.observeSetup(ogrenet.EventConnect, c.id, 0, endpoint.Scheme, c.LocalAddr(), c.RemoteAddr(), connectDuration, nil)
		if handshakeComplete {
			e.observeSetup(ogrenet.EventHandshake, c.id, 0, endpoint.Scheme, c.LocalAddr(), c.RemoteAddr(), handshakeDuration, nil)
		}
	}
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
		engine:        e,
		id:            e.nextID.Add(1),
		protocol:      endpoint.Scheme,
		endpoint:      endpoint,
		raw:           raw,
		physical:      physicalStreamCloser(raw),
		framer:        framer,
		wireFramer:    e.cfg.framerFactory == nil,
		handler:       h,
		queue:         make(chan outbound, e.cfg.writeQueue),
		quota:         newByteQuota(e.cfg.maxQueuedBytes),
		gate:          newSendGate(),
		frameSlots:    make(chan struct{}, e.cfg.writeQueue+1),
		encodeSlot:    make(chan struct{}, 1),
		life:          newSessionLifecycle(),
		stats:         newSessionCounters(),
		closing:       make(chan struct{}),
		writerDrained: make(chan struct{}),
		done:          make(chan struct{}),
		readSize:      e.cfg.readBuffer,
		maxRead:       e.cfg.maxBufferedRead,
		timeouts:      e.cfg.timeouts,
	}
	if err := e.addStreamWithLease(c, lease); err != nil {
		return nil, err
	}
	c.activity = newActivityClock(c.timeouts.ConnectionIdle, c.timeouts.MaxLifetime)
	c.start()
	return c, nil
}
