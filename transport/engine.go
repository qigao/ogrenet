package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/qigao/ogrenet"
)

// Engine is the portable production implementation of ogrenet.Engine. Native
// poller packages remain independently available below this layer.
type Engine struct {
	cfg config

	mu              sync.Mutex
	closed          bool
	activeOps       int
	streamListeners map[*listener]struct{}
	wsListeners     map[*wsListener]struct{}
	streams         map[*conn]struct{}
	websockets      map[*wsSession]struct{}
	packets         map[*packetConn]struct{}
	done            chan struct{}
	doneOnce        sync.Once
	nextID          atomic.Uint64
}

func New(opts ...Option) (*Engine, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	return &Engine{
		cfg:             cfg,
		streamListeners: make(map[*listener]struct{}),
		wsListeners:     make(map[*wsListener]struct{}),
		streams:         make(map[*conn]struct{}),
		websockets:      make(map[*wsSession]struct{}),
		packets:         make(map[*packetConn]struct{}),
		done:            make(chan struct{}),
	}, nil
}

func (e *Engine) Listen(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.Handler) (ogrenet.Listener, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.Validate(); err != nil {
		return nil, err
	}
	if !endpoint.Scheme.IsSession() {
		return nil, ErrProtocolMismatch
	}
	if err := e.beginOp(); err != nil {
		return nil, err
	}
	defer e.endOp()

	switch endpoint.Scheme {
	case ogrenet.SchemeTCP, ogrenet.SchemeTLS:
		return e.listenStream(ctx, endpoint, normalizeHandler(h))
	case ogrenet.SchemeWS, ogrenet.SchemeWSS:
		return e.listenWebSocket(ctx, endpoint, normalizeHandler(h))
	default:
		return nil, ErrProtocolMismatch
	}
}

func (e *Engine) Dial(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.Handler) (ogrenet.Session, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.ValidateDial(); err != nil {
		return nil, err
	}
	if !endpoint.Scheme.IsSession() {
		return nil, ErrProtocolMismatch
	}
	if err := e.beginOp(); err != nil {
		return nil, err
	}
	defer e.endOp()

	switch endpoint.Scheme {
	case ogrenet.SchemeTCP, ogrenet.SchemeTLS:
		return e.dialStream(ctx, endpoint, normalizeHandler(h))
	case ogrenet.SchemeWS, ogrenet.SchemeWSS:
		return e.dialWebSocket(ctx, endpoint, normalizeHandler(h))
	default:
		return nil, ErrProtocolMismatch
	}
}

func (e *Engine) ListenPacket(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.PacketHandler) (ogrenet.PacketConn, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.Validate(); err != nil {
		return nil, err
	}
	if endpoint.Scheme != ogrenet.SchemeUDP {
		return nil, ErrProtocolMismatch
	}
	if err := e.beginOp(); err != nil {
		return nil, err
	}
	defer e.endOp()
	return e.listenPacket(ctx, endpoint, normalizePacketHandler(h))
}

func (e *Engine) DialPacket(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.PacketHandler) (ogrenet.PacketConn, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.ValidateDial(); err != nil {
		return nil, err
	}
	if endpoint.Scheme != ogrenet.SchemeUDP {
		return nil, ErrProtocolMismatch
	}
	if err := e.beginOp(); err != nil {
		return nil, err
	}
	defer e.endOp()
	return e.dialPacket(ctx, endpoint, normalizePacketHandler(h))
}

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
			tlsConn := tls.Server(raw, cfg.Clone())
			if err := e.cfg.handshakeServer(ctx, tlsConn); err != nil {
				return nil, err
			}
			return tlsConn, nil
		}
	}

	lctx, cancel := context.WithCancel(ctx)
	l := &listener{
		engine:   e,
		endpoint: bound,
		ln:       ln,
		handler:  h,
		prepare:  prepare,
		ctx:      lctx,
		cancel:   cancel,
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

func (e *Engine) beginOp() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	e.activeOps++
	return nil
}

func (e *Engine) endOp() {
	e.mu.Lock()
	e.activeOps--
	if e.activeOps < 0 {
		e.activeOps = 0
	}
	e.maybeDoneLocked()
	e.mu.Unlock()
}

func (e *Engine) addStreamListener(v *listener) error { return addTracked(e, e.streamListeners, v) }
func (e *Engine) addWSListener(v *wsListener) error   { return addTracked(e, e.wsListeners, v) }
func (e *Engine) addStream(v *conn) error             { return addTracked(e, e.streams, v) }
func (e *Engine) addWebSocket(v *wsSession) error     { return addTracked(e, e.websockets, v) }
func (e *Engine) addPacket(v *packetConn) error       { return addTracked(e, e.packets, v) }

func addTracked[T comparable](e *Engine, m map[T]struct{}, v T) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	m[v] = struct{}{}
	return nil
}

func (e *Engine) removeStreamListener(v *listener) { removeTracked(e, e.streamListeners, v) }
func (e *Engine) removeWSListener(v *wsListener)   { removeTracked(e, e.wsListeners, v) }
func (e *Engine) removeStream(v *conn)             { removeTracked(e, e.streams, v) }
func (e *Engine) removeWebSocket(v *wsSession)     { removeTracked(e, e.websockets, v) }
func (e *Engine) removePacket(v *packetConn)       { removeTracked(e, e.packets, v) }

func removeTracked[T comparable](e *Engine, m map[T]struct{}, v T) {
	e.mu.Lock()
	delete(m, v)
	e.maybeDoneLocked()
	e.mu.Unlock()
}

func (e *Engine) maybeDoneLocked() {
	if e.closed && e.activeOps == 0 && len(e.streamListeners) == 0 && len(e.wsListeners) == 0 && len(e.streams) == 0 && len(e.websockets) == 0 && len(e.packets) == 0 {
		e.doneOnce.Do(func() { close(e.done) })
	}
}

func normalizeHandler(h ogrenet.Handler) ogrenet.Handler {
	if h == nil {
		return ogrenet.HandlerFuncs{}
	}
	return h
}

func normalizePacketHandler(h ogrenet.PacketHandler) ogrenet.PacketHandler {
	if h == nil {
		return ogrenet.PacketHandlerFuncs{}
	}
	return h
}

var _ ogrenet.Engine = (*Engine)(nil)
