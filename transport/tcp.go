package transport

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/qigao/ogrenet"
)

func (e *Engine) dialTCP(ctx context.Context, endpoint ogrenet.Endpoint) (*net.TCPConn, error) {
	dialer := net.Dialer{}
	if e.cfg.tcp.KeepAlive {
		dialer.KeepAlive = e.cfg.tcp.KeepAlivePeriod
	} else {
		dialer.KeepAlive = -1
	}
	dctx, cancel := boundedOperationContext(ctx, e.cfg.timeouts.Connect)
	defer cancel()
	raw, err := dialer.DialContext(dctx, "tcp", endpoint.Address())
	if err != nil {
		mapped := mapOperationTimeout(ctx, dctx, TimeoutConnect, err)
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		return nil, classifyOperational(OpDial, endpoint.Scheme, nil, nil, mapped, hintNone)
	}
	tcp, ok := raw.(*net.TCPConn)
	if !ok {
		local, remote := raw.LocalAddr(), raw.RemoteAddr()
		_ = raw.Close()
		return nil, classifyOperational(OpDial, endpoint.Scheme, local, remote, fmt.Errorf("transport: tcp dial returned %T", raw), hintNone)
	}
	if err := e.configureTCP(tcp); err != nil {
		local, remote := tcp.LocalAddr(), tcp.RemoteAddr()
		_ = tcp.Close()
		return nil, classifyOperational(OpDial, endpoint.Scheme, local, remote, err, hintNone)
	}
	return tcp, nil
}

func (e *Engine) listenTCP(ctx context.Context, endpoint ogrenet.Endpoint) (*net.TCPListener, error) {
	lc := net.ListenConfig{}
	raw, err := lc.Listen(ctx, "tcp", endpoint.Address())
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		return nil, classifyOperational(OpListen, endpoint.Scheme, nil, nil, err, hintNone)
	}
	ln, ok := raw.(*net.TCPListener)
	if !ok {
		local := raw.Addr()
		_ = raw.Close()
		return nil, classifyOperational(OpListen, endpoint.Scheme, local, nil, fmt.Errorf("transport: tcp listen returned %T", raw), hintNone)
	}
	return ln, nil
}

func (e *Engine) configureTCP(conn *net.TCPConn) error {
	if err := conn.SetNoDelay(e.cfg.tcp.NoDelay); err != nil {
		return fmt.Errorf("transport: set TCP_NODELAY: %w", err)
	}
	if err := conn.SetKeepAlive(e.cfg.tcp.KeepAlive); err != nil {
		return fmt.Errorf("transport: set TCP keepalive: %w", err)
	}
	if e.cfg.tcp.KeepAlive {
		if err := conn.SetKeepAlivePeriod(e.cfg.tcp.KeepAlivePeriod); err != nil {
			return fmt.Errorf("transport: set TCP keepalive period: %w", err)
		}
	}
	if e.cfg.tcp.ReadBufferBytes > 0 {
		if err := conn.SetReadBuffer(e.cfg.tcp.ReadBufferBytes); err != nil {
			return fmt.Errorf("transport: set TCP read buffer: %w", err)
		}
	}
	if e.cfg.tcp.WriteBufferBytes > 0 {
		if err := conn.SetWriteBuffer(e.cfg.tcp.WriteBufferBytes); err != nil {
			return fmt.Errorf("transport: set TCP write buffer: %w", err)
		}
	}
	return nil
}

type configuredTCPListener struct {
	net.Listener
	engine *Engine
}

func (l configuredTCPListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		tcp, ok := conn.(*net.TCPConn)
		if !ok {
			_ = conn.Close()
			continue
		}
		if err := l.engine.configureTCP(tcp); err != nil {
			_ = tcp.Close()
			continue
		}
		return tcp, nil
	}
}

func boundEndpoint(endpoint ogrenet.Endpoint, addr net.Addr) ogrenet.Endpoint {
	host, portText, err := net.SplitHostPort(addr.String())
	if err != nil {
		return endpoint
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		return endpoint
	}
	endpoint.Port = uint16(port)
	if endpoint.Host != "" && endpoint.Host != "0.0.0.0" && endpoint.Host != "::" {
		return endpoint
	}
	endpoint.Host = host
	return endpoint
}
