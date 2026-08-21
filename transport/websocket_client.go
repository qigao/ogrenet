package transport

import (
	"context"
	"errors"
	"net"
	"net/http"

	"github.com/coder/websocket"
	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/secure"
)

func (e *Engine) dialWebSocket(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.Handler) (ogrenet.Session, error) {
	if e.cfg.framerFactory != nil {
		return nil, ErrFramerNotSupported
	}

	upgrade, err := e.acquireUpgrade()
	if err != nil {
		return nil, err
	}
	defer upgrade.release()

	transport, state, err := e.newWebSocketHTTPTransport(endpoint)
	if err != nil {
		return nil, err
	}
	defer transport.CloseIdleConnections()
	defer state.release()
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	ws, _, err := websocket.Dial(ctx, endpoint.URL(), &websocket.DialOptions{HTTPClient: client, Subprotocols: e.cfg.ws.Subprotocols, CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		var timeoutErr *TimeoutError
		if errors.As(err, &timeoutErr) {
			return nil, err
		}
		if isTimeoutFailure(err) {
			return nil, &TimeoutError{Kind: TimeoutHandshake, Cause: err}
		}
		return nil, err
	}
	ws.SetReadLimit(int64(e.cfg.maxMessageBytes))
	cipher, err := e.cfg.newCipher()
	if err != nil {
		_ = ws.CloseNow()
		return nil, err
	}
	lease, local, remote, physical := state.take()
	if lease == nil {
		if physical != nil {
			_ = physical.Close()
		}
		_ = ws.CloseNow()
		return nil, errors.New("transport: websocket dial completed without connection admission")
	}
	transferred := false
	defer func() {
		if !transferred {
			lease.release()
			if physical != nil {
				_ = physical.Close()
			}
		}
	}()
	if local == nil {
		local = staticAddr{network: endpoint.Scheme.String(), value: "unknown"}
	}
	if remote == nil {
		remote = staticAddr{network: endpoint.Scheme.String(), value: endpoint.Address()}
	}
	sess := e.newWSSession(ws, endpoint, local, remote, h, cipher)
	sess.physical = physical
	if err := e.addWebSocketWithLease(sess, lease); err != nil {
		if physical != nil {
			_ = physical.Close()
		} else {
			_ = ws.CloseNow()
		}
		return nil, err
	}
	transferred = true
	sess.start()
	return sess, nil
}

func (e *Engine) newWSSession(ws *websocket.Conn, endpoint ogrenet.Endpoint, local, remote net.Addr, h ogrenet.Handler, cipher secure.Cipher) *wsSession {
	return &wsSession{
		engine:        e,
		id:            e.nextID.Add(1),
		protocol:      endpoint.Scheme,
		endpoint:      endpoint,
		ws:            ws,
		local:         local,
		remote:        remote,
		handler:       h,
		cipher:        cipher,
		maxMessage:    e.cfg.maxMessageBytes,
		writeTO:       e.cfg.effectiveWSWriteTimeout(),
		closeTO:       e.cfg.ws.CloseTimeout,
		readIdle:      e.cfg.timeouts.ReadIdle,
		activity:      newActivityClock(e.cfg.timeouts.ConnectionIdle, e.cfg.timeouts.MaxLifetime),
		pingEvery:     e.cfg.ws.PingInterval,
		pongTO:        e.cfg.ws.PongTimeout,
		queue:         make(chan wsOutbound, e.cfg.writeQueue),
		quota:         newByteQuota(e.cfg.maxQueuedBytes),
		gate:          newSendGate(),
		frameSlots:    make(chan struct{}, e.cfg.writeQueue+1),
		encodeSlot:    make(chan struct{}, 1),
		life:          newSessionLifecycle(),
		closing:       make(chan struct{}),
		writerDrained: make(chan struct{}),
		done:          make(chan struct{}),
	}
}

func parseRemoteAddr(raw string) net.Addr {
	addr, err := net.ResolveTCPAddr("tcp", raw)
	if err == nil {
		return addr
	}
	return staticAddr{network: "tcp", value: raw}
}

type staticAddr struct {
	network string
	value   string
}

func (a staticAddr) Network() string { return a.network }
func (a staticAddr) String() string  { return a.value }

var _ ogrenet.Listener = (*wsListener)(nil)
