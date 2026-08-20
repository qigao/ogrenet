package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/qigao/ogrenet"
)

type wsDialAdmission struct {
	mu     sync.Mutex
	lease  *connectionLease
	local  net.Addr
	remote net.Addr
}

func (s *wsDialAdmission) set(lease *connectionLease, local, remote net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease != nil {
		return fmt.Errorf("transport: websocket dial created multiple admitted connections")
	}
	s.lease = lease
	s.local = local
	s.remote = remote
	return nil
}

func (s *wsDialAdmission) take() (*connectionLease, net.Addr, net.Addr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lease, local, remote := s.lease, s.local, s.remote
	s.lease = nil
	return lease, local, remote
}

func (s *wsDialAdmission) release() {
	lease, _, _ := s.take()
	lease.release()
}

func (e *Engine) newWebSocketHTTPTransport(endpoint ogrenet.Endpoint) (*http.Transport, *wsDialAdmission, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ForceAttemptHTTP2 = false
	transport.Proxy = nil
	transport.TLSHandshakeTimeout = e.cfg.tlsHandshakeTimeout
	transport.ResponseHeaderTimeout = e.cfg.ws.HandshakeTimeout

	state := &wsDialAdmission{}
	dialer := net.Dialer{}
	if e.cfg.tcp.KeepAlive {
		dialer.KeepAlive = e.cfg.tcp.KeepAlivePeriod
	} else {
		dialer.KeepAlive = -1
	}

	dialRaw := func(ctx context.Context, network, address string) (net.Conn, *connectionLease, error) {
		raw, err := dialer.DialContext(ctx, network, address)
		if err != nil {
			return nil, nil, err
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
		if err := state.set(lease, raw.LocalAddr(), raw.RemoteAddr()); err != nil {
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
				_ = tlsConn.Close()
				return nil, err
			}
			if err := state.set(lease, raw.LocalAddr(), raw.RemoteAddr()); err != nil {
				lease.release()
				_ = tlsConn.Close()
				return nil, err
			}
			return tlsConn, nil
		}
	}
	return transport, state, nil
}
