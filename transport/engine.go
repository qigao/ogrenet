package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/qigao/ogrenet"
)

// Engine is the portable stream implementation of ogrenet.Engine. It uses Go's
// net package for socket I/O while preserving ogrenet's message, framing,
// security, backpressure, and lifecycle contracts.
type Engine struct {
	cfg config

	mu        sync.Mutex
	closed    bool
	listeners map[*listener]struct{}
	conns     map[*conn]struct{}
	done      chan struct{}
	doneOnce  sync.Once
	nextID    atomic.Uint64
}

// New creates a portable stream Engine.
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
		cfg:       cfg,
		listeners: make(map[*listener]struct{}),
		conns:     make(map[*conn]struct{}),
		done:      make(chan struct{}),
	}, nil
}

// Listen starts accepting stream connections in the background.
func (e *Engine) Listen(ctx context.Context, network, address string, h ogrenet.Handler) (ogrenet.Listener, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if !isStreamNetwork(network) {
		return nil, ErrUnsupportedNet
	}
	if e.isClosed() {
		return nil, ErrClosed
	}

	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, network, address)
	if err != nil {
		return nil, err
	}

	lctx, cancel := context.WithCancel(ctx)
	l := &listener{
		engine:  e,
		ln:      ln,
		handler: normalizeHandler(h),
		ctx:     lctx,
		cancel:  cancel,
		closing: make(chan struct{}),
		done:    make(chan struct{}),
	}
	if err := e.addListener(l); err != nil {
		cancel()
		_ = ln.Close()
		return nil, err
	}

	go l.watchContext()
	go l.acceptLoop()
	return l, nil
}

// Dial establishes one outbound stream connection and starts its read/write
// loops. The handler receives OnOpen/OnMessage/OnClose for the new connection.
func (e *Engine) Dial(ctx context.Context, network, address string, h ogrenet.Handler) (ogrenet.Conn, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if !isStreamNetwork(network) {
		return nil, ErrUnsupportedNet
	}
	if e.isClosed() {
		return nil, ErrClosed
	}

	var d net.Dialer
	raw, err := d.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	c, err := e.adopt(raw, normalizeHandler(h))
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	return c, nil
}

// Done closes after Close has been initiated and every listener and connection
// has reached its own Done barrier.
func (e *Engine) Done() <-chan struct{} { return e.done }

// Shutdown initiates shutdown and waits for the Engine Done barrier or ctx.
// Do not call Shutdown synchronously from a Handler callback on the same Engine:
// that callback is part of the barrier being awaited.
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

// Close initiates shutdown of all listeners and connections and is idempotent.
// It does not wait for user callbacks to return. Use Shutdown or wait on Done
// when a global shutdown barrier is required.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	listeners := make([]*listener, 0, len(e.listeners))
	for l := range e.listeners {
		listeners = append(listeners, l)
	}
	conns := make([]*conn, 0, len(e.conns))
	for c := range e.conns {
		conns = append(conns, c)
	}
	e.maybeDoneLocked()
	e.mu.Unlock()

	var errs []error
	for _, l := range listeners {
		if err := l.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, c := range conns {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (e *Engine) adopt(raw net.Conn, h ogrenet.Handler) (*conn, error) {
	framer, err := e.cfg.newFramer()
	if err != nil {
		return nil, err
	}
	c := &conn{
		engine:   e,
		id:       e.nextID.Add(1),
		raw:      raw,
		framer:   framer,
		handler:  h,
		queue:    make(chan outbound, e.cfg.writeQueue),
		quota:    newByteQuota(e.cfg.maxQueuedBytes),
		gate:     newSendGate(),
		closing:  make(chan struct{}),
		done:     make(chan struct{}),
		readSize: e.cfg.readBuffer,
		maxRead:  e.cfg.maxBufferedRead,
	}
	if err := e.addConn(c); err != nil {
		return nil, err
	}
	c.start()
	return c, nil
}

func (e *Engine) isClosed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.closed
}

func (e *Engine) addListener(l *listener) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	e.listeners[l] = struct{}{}
	return nil
}

func (e *Engine) removeListener(l *listener) {
	e.mu.Lock()
	delete(e.listeners, l)
	e.maybeDoneLocked()
	e.mu.Unlock()
}

func (e *Engine) addConn(c *conn) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	e.conns[c] = struct{}{}
	return nil
}

func (e *Engine) removeConn(c *conn) {
	e.mu.Lock()
	delete(e.conns, c)
	e.maybeDoneLocked()
	e.mu.Unlock()
}

func (e *Engine) maybeDoneLocked() {
	if e.closed && len(e.listeners) == 0 && len(e.conns) == 0 {
		e.doneOnce.Do(func() { close(e.done) })
	}
}

func normalizeHandler(h ogrenet.Handler) ogrenet.Handler {
	if h == nil {
		return ogrenet.HandlerFuncs{}
	}
	return h
}

func isStreamNetwork(network string) bool {
	switch network {
	case "tcp", "tcp4", "tcp6", "unix":
		return true
	default:
		return false
	}
}

var _ ogrenet.Engine = (*Engine)(nil)
