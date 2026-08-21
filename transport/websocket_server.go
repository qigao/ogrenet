package transport

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"github.com/qigao/ogrenet"
)

type wsListener struct {
	engine   *Engine
	id       uint64
	endpoint ogrenet.Endpoint
	ln       net.Listener
	server   *http.Server
	tracker  *httpConnTracker
	capacity *listenerCapacity
	stats    *listenerCounters
	closing  chan struct{}
	done     chan struct{}
	cancel   context.CancelFunc

	closeOnce sync.Once
	errMu     sync.RWMutex
	err       error
}

func (l *wsListener) Endpoint() ogrenet.Endpoint { return l.endpoint }
func (l *wsListener) Addr() net.Addr             { return l.ln.Addr() }
func (l *wsListener) Done() <-chan struct{}      { return l.done }

func (l *wsListener) Err() error {
	l.errMu.RLock()
	defer l.errMu.RUnlock()
	return l.err
}

func (l *wsListener) Close() error {
	var err error
	l.closeOnce.Do(func() {
		close(l.closing)
		l.cancel()
		err = l.server.Close()
		if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
			err = nil
		}
		_ = l.ln.Close()
		if l.tracker != nil {
			l.tracker.closeAll()
		}
	})
	return err
}

func (e *Engine) listenWebSocket(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.Handler) (ogrenet.Listener, error) {
	if e.cfg.framerFactory != nil {
		return nil, ErrFramerNotSupported
	}
	if endpoint.RawQuery != "" {
		return nil, ErrInvalidWebSocketConfig
	}
	rawLn, err := e.listenTCP(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	bound := boundEndpoint(endpoint, rawLn.Addr())
	lctx, cancel := context.WithCancel(ctx)
	tracker := newHTTPConnTracker()
	capacity := newListenerCapacity(e.cfg.limits.MaxConnectionsPerListener)
	base := net.Listener(configuredTCPListener{Listener: rawLn, engine: e})
	serveLn := net.Listener(&admittedHTTPListener{Listener: base, engine: e, capacity: capacity, tracker: tracker})
	if endpoint.Scheme == ogrenet.SchemeWSS {
		tlsCfg, err := e.cfg.serverTLSConfig()
		if err != nil {
			cancel()
			_ = rawLn.Close()
			return nil, err
		}
		serveLn = newGatedTLSListener(lctx, serveLn, e, tlsCfg, tracker)
	}

	l := &wsListener{
		engine:   e,
		id:       e.nextID.Add(1),
		endpoint: bound,
		ln:       serveLn,
		tracker:  tracker,
		capacity: capacity,
		stats:    newListenerCounters(),
		closing:  make(chan struct{}),
		done:     make(chan struct{}),
		cancel:   cancel,
	}
	path := endpoint.Path
	if path == "" {
		path = "/"
	}

	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != path {
			http.NotFound(w, r)
			return
		}
		if err := e.beginOp(); err != nil {
			w.Header().Set("Connection", "close")
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}
		defer e.endOp()

		holder := httpConnLeaseFromContext(r.Context())
		if holder == nil {
			w.Header().Set("Connection", "close")
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}
		upgrade, err := e.acquireUpgrade()
		if err != nil {
			w.Header().Set("Connection", "close")
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols:    e.cfg.ws.Subprotocols,
			OriginPatterns:  e.cfg.ws.OriginPatterns,
			CompressionMode: websocket.CompressionDisabled,
		})
		upgrade.release()
		if err != nil {
			return
		}
		connLease, physical := holder.takeWithPhysical()
		if connLease == nil {
			_ = ws.CloseNow()
			return
		}
		transferred := false
		defer func() {
			if !transferred {
				connLease.release()
				if physical != nil {
					_ = physical.Close()
				}
			}
		}()

		ws.SetReadLimit(int64(e.cfg.maxMessageBytes))
		cipher, err := e.cfg.newCipher()
		if err != nil {
			if physical != nil {
				_ = physical.Close()
			} else {
				_ = ws.CloseNow()
			}
			return
		}
		remote := parseRemoteAddr(r.RemoteAddr)
		s := e.newWSSession(ws, bound, serveLn.Addr(), remote, h, cipher)
		s.physical = physical
		if err := e.addWebSocketWithLease(s, connLease); err != nil {
			if physical != nil {
				_ = physical.Close()
			} else {
				_ = ws.CloseNow()
			}
			return
		}
		transferred = true
		s.start()
	})
	wsHandshakeTimeout := e.cfg.effectiveWSHandshakeTimeout()
	l.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: wsHandshakeTimeout,
		WriteTimeout:      wsHandshakeTimeout,
		IdleTimeout:       wsHandshakeTimeout,
		MaxHeaderBytes:    32 << 10,
		ConnContext:       tracker.connContext,
		ConnState:         tracker.connState,
	}
	if err := e.addWSListener(l); err != nil {
		cancel()
		_ = serveLn.Close()
		tracker.closeAll()
		return nil, err
	}
	go func() {
		select {
		case <-lctx.Done():
			_ = l.Close()
		case <-l.done:
		}
	}()
	go func() {
		err := l.server.Serve(serveLn)
		if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
			err = nil
		}
		l.errMu.Lock()
		l.err = err
		l.errMu.Unlock()
		_ = l.Close()
		close(l.done)
		e.removeWSListener(l)
	}()
	return l, nil
}
