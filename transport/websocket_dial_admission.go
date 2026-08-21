package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/qigao/ogrenet"
)

type wsDialAdmission struct {
	mu              sync.Mutex
	lease           *connectionLease
	local           net.Addr
	remote          net.Addr
	physical        net.Conn
	connectDone     bool
	connectDuration time.Duration
	connectErr      error
}

func (s *wsDialAdmission) recordConnect(local, remote net.Addr, duration time.Duration, err error) {
	s.mu.Lock()
	if !s.connectDone {
		s.connectDone = true
		s.local = local
		s.remote = remote
		s.connectDuration = duration
		s.connectErr = err
	}
	s.mu.Unlock()
}

func (s *wsDialAdmission) connectInfo() (net.Addr, net.Addr, time.Duration, error, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.local, s.remote, s.connectDuration, s.connectErr, s.connectDone
}

func (s *wsDialAdmission) set(lease *connectionLease, local, remote net.Addr, physical net.Conn) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease != nil {
		return fmt.Errorf("transport: websocket dial created multiple admitted connections")
	}
	s.lease = lease
	s.local = local
	s.remote = remote
	s.physical = physical
	return nil
}

func (s *wsDialAdmission) take() (*connectionLease, net.Addr, net.Addr, net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lease, local, remote, physical := s.lease, s.local, s.remote, s.physical
	s.lease = nil
	s.physical = nil
	return lease, local, remote, physical
}

func (s *wsDialAdmission) release() {
	lease, _, _, physical := s.take()
	if physical != nil {
		_ = physical.Close()
	}
	lease.release()
}

func (e *Engine) newWebSocketHTTPTransport(endpoint ogrenet.Endpoint) (*http.Transport, *wsDialAdmission, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ForceAttemptHTTP2 = false
	transport.Proxy = nil
	transport.TLSHandshakeTimeout = e.cfg.effectiveTLSHandshakeTimeout()
	transport.ResponseHeaderTimeout = e.cfg.effectiveWSHandshakeTimeout()

	state := &wsDialAdmission{}
	dialer := net.Dialer{}
	if e.cfg.tcp.KeepAlive {
		dialer.KeepAlive = e.cfg.tcp.KeepAlivePeriod
	} else {
		dialer.KeepAlive = -1
	}

	dialRaw := func(ctx context.Context, network, address string) (net.Conn, *connectionLease, error) {
		observing := e.observer != nil
		var started time.Time
		if observing {
			started = time.Now()
		}
		dctx, cancel := boundedOperationContext(ctx, e.cfg.timeouts.Connect)
		defer cancel()
		raw, err := dialer.DialContext(dctx, network, address)
		var duration time.Duration
		if observing {
			duration = positiveElapsed(started)
		}
		if err != nil {
			mapped := mapOperationTimeout(ctx, dctx, TimeoutConnect, err)
			if observing {
				state.recordConnect(nil, nil, duration, mapped)
			}
			return nil, nil, mapped
		}
		if observing {
			state.recordConnect(raw.LocalAddr(), raw.RemoteAddr(), duration, nil)
		}
		if tcp, ok := raw.(*net.TCPConn); ok {
			if err := e.configureTCP(tcp); err != nil {
				_ = raw.Close()
				return nil, nil, err
			}
		}
		lease, err := e.acquireOpening(raw.RemoteAddr())
		if err != nil {
			_ = raw.Close()
			return nil, nil, err
		}
		return raw, lease, nil
	}

	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		raw, lease, err := dialRaw(ctx, network, address)
		if err != nil {
			return nil, err
		}
		if err := state.set(lease, raw.LocalAddr(), raw.RemoteAddr(), raw); err != nil {
			lease.release()
			_ = raw.Close()
			return nil, err
		}
		return raw, nil
	}

	if endpoint.Scheme == ogrenet.SchemeWSS {
		tlsCfg, err := e.cfg.clientTLSConfig(endpoint)
		if err != nil {
			return nil, nil, err
		}
		transport.DialTLSContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			raw, lease, err := dialRaw(ctx, network, address)
			if err != nil {
				return nil, err
			}
			handshake, err := e.acquireHandshake()
			if err != nil {
				lease.release()
				_ = raw.Close()
				return nil, err
			}
			tlsConn := tls.Client(raw, tlsCfg.Clone())
			err = e.cfg.handshakeClient(ctx, tlsConn)
			handshake.release()
			if err != nil {
				lease.release()
				_ = raw.Close()
				return nil, err
			}
			if err := state.set(lease, raw.LocalAddr(), raw.RemoteAddr(), raw); err != nil {
				lease.release()
				_ = raw.Close()
				return nil, err
			}
			return tlsConn, nil
		}
	}
	return transport, state, nil
}
