package transport

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptrace"
	"time"

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
		return nil, classifyOperational(OpUpgrade, endpoint.Scheme, nil, nil, err, hintNone)
	}
	defer upgrade.release()

	transport, state, err := e.newWebSocketHTTPTransport(endpoint)
	if err != nil {
		return nil, err
	}
	defer transport.CloseIdleConnections()
	defer state.release()
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	observing := e.observer != nil
	dialCtx := ctx
	var setupStart time.Time
	var gotConnCh chan time.Time
	if observing {
		setupStart = time.Now()
		gotConnCh = make(chan time.Time, 1)
		dialCtx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
			GotConn: func(httptrace.GotConnInfo) {
				select {
				case gotConnCh <- time.Now():
				default:
				}
			},
		})
	}
	ws, _, err := websocket.Dial(dialCtx, endpoint.URL(), &websocket.DialOptions{HTTPClient: client, Subprotocols: e.cfg.ws.Subprotocols, CompressionMode: websocket.CompressionDisabled})

	var handshakeDuration time.Duration
	var connectLocal, connectRemote net.Addr
	var connectDuration time.Duration
	var connectErr error
	var connectDone bool
	if observing {
		connectLocal, connectRemote, connectDuration, connectErr, connectDone = state.connectInfo()
		var gotConn time.Time
		select {
		case gotConn = <-gotConnCh:
		default:
		}
		if !gotConn.IsZero() {
			handshakeDuration = positiveElapsed(gotConn)
		} else {
			total := positiveElapsed(setupStart)
			handshakeDuration = total - connectDuration
			if handshakeDuration <= 0 {
				handshakeDuration = time.Nanosecond
			}
		}
	}

	if err != nil {
		var resultErr error
		if cause := context.Cause(ctx); cause != nil {
			resultErr = cause
		} else {
			if isTimeoutFailure(err) {
				var timeoutErr *TimeoutError
				if !errors.As(err, &timeoutErr) {
					err = &TimeoutError{Kind: TimeoutHandshake, Cause: err}
				}
			}
			resultErr = classifyOperational(OpUpgrade, endpoint.Scheme, connectLocal, connectRemote, err, hintWSUpgrade)
			if endpoint.Scheme == ogrenet.SchemeWSS {
				var te *Error
				if errors.As(resultErr, &te) && te.Kind == ErrorTLS {
					resultErr = &Error{Op: OpHandshake, Protocol: te.Protocol, Kind: te.Kind, Local: te.Local, Remote: te.Remote, Cause: te.Cause}
				}
			}
		}
		if observing && connectDone {
			if connectErr != nil {
				connectFailure := classifyOperational(OpDial, endpoint.Scheme, connectLocal, connectRemote, connectErr, hintNone)
				e.observeSetup(ogrenet.EventConnect, 0, 0, endpoint.Scheme, connectLocal, connectRemote, connectDuration, connectFailure)
			} else {
				e.observeSetup(ogrenet.EventConnect, 0, 0, endpoint.Scheme, connectLocal, connectRemote, connectDuration, nil)
				e.observeSetup(ogrenet.EventHandshake, 0, 0, endpoint.Scheme, connectLocal, connectRemote, handshakeDuration, resultErr)
			}
		}
		return nil, resultErr
	}
	ws.SetReadLimit(int64(e.cfg.maxMessageBytes))
	cipher, err := e.cfg.newCipher()
	if err != nil {
		_ = ws.CloseNow()
		return nil, classifyOperational(OpUpgrade, endpoint.Scheme, connectLocal, connectRemote, err, hintNone)
	}
	lease, local, remote, physical := state.take()
	if lease == nil {
		if physical != nil {
			_ = physical.Close()
		}
		_ = ws.CloseNow()
		return nil, classifyOperational(OpUpgrade, endpoint.Scheme, local, remote, errors.New("transport: websocket dial completed without connection admission"), hintNone)
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
		return nil, classifyOperational(OpUpgrade, endpoint.Scheme, local, remote, err, hintNone)
	}
	transferred = true
	if observing {
		e.observeSetup(ogrenet.EventConnect, sess.id, 0, endpoint.Scheme, local, remote, connectDuration, nil)
		e.observeSetup(ogrenet.EventHandshake, sess.id, 0, endpoint.Scheme, local, remote, handshakeDuration, nil)
	}
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
		readIdle:      e.cfg.timeouts.ReadIdle,
		activity:      newActivityClock(e.cfg.timeouts.ConnectionIdle, e.cfg.timeouts.MaxLifetime),
		writeState:    wsWriteState{},
		pingEvery:     e.cfg.ws.PingInterval,
		pongTO:        e.cfg.ws.PongTimeout,
		queue:         make(chan wsOutbound, e.cfg.writeQueue),
		quota:         newByteQuota(e.cfg.maxQueuedBytes),
		gate:          newSendGate(),
		frameSlots:    make(chan struct{}, e.cfg.writeQueue+1),
		encodeSlot:    make(chan struct{}, 1),
		life:          newSessionLifecycle(),
		stats:         newSessionCounters(),
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
