package transport

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/secure"
)

type wsOutbound struct {
	typ     websocket.MessageType
	payload []byte
	ack     chan error
	bytes   int
}

type wsSession struct {
	engine     *Engine
	id         uint64
	protocol   ogrenet.Scheme
	endpoint   ogrenet.Endpoint
	ws         *websocket.Conn
	local      net.Addr
	remote     net.Addr
	handler    ogrenet.Handler
	cipher     secure.Cipher
	maxMessage int
	writeTO    time.Duration
	pingEvery  time.Duration
	pongTO     time.Duration
	queue      chan wsOutbound
	quota      *byteQuota
	gate       *sendGate
	frameSlots chan struct{}
	encodeSlot chan struct{}
	closing    chan struct{}
	done       chan struct{}

	closeOnce sync.Once
	finalOnce sync.Once
	loops     sync.WaitGroup
	errMu     sync.RWMutex
	err       error
}

func (s *wsSession) ID() uint64                 { return s.id }
func (s *wsSession) Protocol() ogrenet.Scheme   { return s.protocol }
func (s *wsSession) Endpoint() ogrenet.Endpoint { return s.endpoint }
func (s *wsSession) LocalAddr() net.Addr        { return s.local }
func (s *wsSession) RemoteAddr() net.Addr       { return s.remote }
func (s *wsSession) Done() <-chan struct{}      { return s.done }

func (s *wsSession) Err() error {
	s.errMu.RLock()
	defer s.errMu.RUnlock()
	return s.err
}

func (s *wsSession) Send(ctx context.Context, msg ogrenet.Message) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.gate.enter() {
		return ErrClosed
	}
	defer s.gate.leave()

	if err := s.acquireFrameSlot(ctx); err != nil {
		return err
	}
	held := true
	defer func() {
		if held {
			s.releaseFrameSlot()
		}
	}()
	if err := s.acquireEncoder(ctx); err != nil {
		return err
	}
	typ, payload, err := s.encode(msg)
	s.releaseEncoder()
	if err != nil {
		return err
	}
	if err := s.quota.acquire(ctx, s.closing, len(payload)); err != nil {
		return err
	}
	held = false

	ack := make(chan error, 1)
	req := wsOutbound{typ: typ, payload: payload, ack: ack, bytes: len(payload)}
	select {
	case <-ctx.Done():
		s.quota.release(req.bytes)
		s.releaseFrameSlot()
		return ctx.Err()
	case <-s.closing:
		s.quota.release(req.bytes)
		s.releaseFrameSlot()
		return ErrClosed
	case s.queue <- req:
	}

	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closing:
		select {
		case err := <-ack:
			return err
		default:
			return ErrClosed
		}
	}
}

func (s *wsSession) TrySend(msg ogrenet.Message) error {
	if !s.gate.enter() {
		return ErrClosed
	}
	defer s.gate.leave()
	if err := s.tryAcquireFrameSlot(); err != nil {
		return err
	}
	held := true
	defer func() {
		if held {
			s.releaseFrameSlot()
		}
	}()
	if err := s.tryAcquireEncoder(); err != nil {
		return err
	}
	typ, payload, err := s.encode(msg)
	s.releaseEncoder()
	if err != nil {
		return err
	}
	if err := s.quota.tryAcquire(len(payload)); err != nil {
		return err
	}
	held = false
	req := wsOutbound{typ: typ, payload: payload, bytes: len(payload)}
	select {
	case <-s.closing:
		s.quota.release(req.bytes)
		s.releaseFrameSlot()
		return ErrClosed
	case s.queue <- req:
		return nil
	default:
		s.quota.release(req.bytes)
		s.releaseFrameSlot()
		return ErrWouldBlock
	}
}

func (s *wsSession) Close() error {
	s.initiateClose(nil)
	return nil
}

func (s *wsSession) start() {
	loopCount := 2
	if s.pingEvery > 0 {
		loopCount++
	}
	s.loops.Add(loopCount)
	go func() {
		defer s.loops.Done()
		s.writerLoop()
	}()
	go func() {
		defer s.loops.Done()
		s.readerLoop()
	}()
	if s.pingEvery > 0 {
		go func() {
			defer s.loops.Done()
			s.pingLoop()
		}()
	}
	go func() {
		s.loops.Wait()
		s.finalize()
	}()
}

func (s *wsSession) writerLoop() {
	defer func() {
		<-s.gate.done()
		s.failPending(ErrClosed)
	}()
	for {
		select {
		case <-s.closing:
			return
		case req := <-s.queue:
			ctx, cancel := context.WithTimeout(context.Background(), s.writeTO)
			err := s.ws.Write(ctx, req.typ, req.payload)
			cancel()
			s.quota.release(req.bytes)
			s.releaseFrameSlot()
			sendErr := err
			if err != nil && s.isClosing() {
				sendErr = ErrClosed
			}
			if req.ack != nil {
				req.ack <- sendErr
			}
			if err != nil {
				s.initiateClose(fmt.Errorf("transport: websocket write: %w", err))
				return
			}
		}
	}
}

func (s *wsSession) failPending(err error) {
	for {
		select {
		case req := <-s.queue:
			s.quota.release(req.bytes)
			s.releaseFrameSlot()
			if req.ack != nil {
				req.ack <- err
			}
		default:
			return
		}
	}
}

func (s *wsSession) readerLoop() {
	s.handler.OnOpen(s)
	if s.isClosing() {
		return
	}
	for {
		typ, payload, err := s.ws.Read(context.Background())
		if err != nil {
			s.initiateClose(normalizeWSError(err))
			return
		}
		msg, err := s.decode(typ, payload)
		if err != nil {
			s.initiateClose(err)
			return
		}
		s.handler.OnMessage(s, msg)
		if s.isClosing() {
			return
		}
	}
}

func (s *wsSession) pingLoop() {
	ticker := time.NewTicker(s.pingEvery)
	defer ticker.Stop()
	for {
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), s.pongTO)
			err := s.ws.Ping(ctx)
			cancel()
			if err != nil {
				s.initiateClose(fmt.Errorf("transport: websocket ping: %w", err))
				return
			}
		}
	}
}

func (s *wsSession) encode(msg ogrenet.Message) (websocket.MessageType, []byte, error) {
	if err := msg.Validate(); err != nil {
		return 0, nil, err
	}
	if len(msg.Data) > s.maxMessage {
		return 0, nil, ErrMessageTooLarge
	}

	typ := websocket.MessageBinary
	if msg.Type == ogrenet.PayloadText {
		typ = websocket.MessageText
	}
	payload := append([]byte(nil), msg.Data...)
	if s.cipher != nil {
		aad := wsAAD(s.protocol, msg.Type)
		var err error
		if authenticated, ok := s.cipher.(secure.AuthenticatedCipher); ok {
			payload, err = authenticated.SealAAD(nil, msg.Data, aad[:])
		} else {
			payload, err = s.cipher.Seal(nil, msg.Data)
		}
		if err != nil {
			return 0, nil, fmt.Errorf("transport: websocket encrypt: %w", err)
		}
		if msg.Type == ogrenet.PayloadText {
			encoded := make([]byte, base64.RawStdEncoding.EncodedLen(len(payload)))
			base64.RawStdEncoding.Encode(encoded, payload)
			payload = encoded
		}
	}
	if len(payload) > s.maxMessage {
		return 0, nil, ErrMessageTooLarge
	}
	return typ, payload, nil
}

func (s *wsSession) decode(typ websocket.MessageType, payload []byte) (ogrenet.Message, error) {
	kind := ogrenet.PayloadBinary
	switch typ {
	case websocket.MessageText:
		kind = ogrenet.PayloadText
	case websocket.MessageBinary:
	default:
		return ogrenet.Message{}, fmt.Errorf("transport: unsupported websocket message type %v", typ)
	}
	if len(payload) > s.maxMessage {
		return ogrenet.Message{}, ErrMessageTooLarge
	}

	data := append([]byte(nil), payload...)
	if s.cipher != nil {
		if kind == ogrenet.PayloadText {
			decoded := make([]byte, base64.RawStdEncoding.DecodedLen(len(data)))
			n, err := base64.RawStdEncoding.Decode(decoded, data)
			if err != nil {
				return ogrenet.Message{}, fmt.Errorf("transport: websocket text base64: %w", err)
			}
			data = decoded[:n]
		}
		aad := wsAAD(s.protocol, kind)
		var plaintext []byte
		var err error
		if authenticated, ok := s.cipher.(secure.AuthenticatedCipher); ok {
			plaintext, err = authenticated.OpenAAD(nil, data, aad[:])
		} else {
			plaintext, err = s.cipher.Open(nil, data)
		}
		if err != nil {
			return ogrenet.Message{}, fmt.Errorf("transport: websocket decrypt: %w", err)
		}
		data = plaintext
	}
	if len(data) > s.maxMessage {
		return ogrenet.Message{}, ErrMessageTooLarge
	}
	msg := ogrenet.Message{Type: kind, Data: data}
	if err := msg.Validate(); err != nil {
		return ogrenet.Message{}, err
	}
	return msg, nil
}

func wsAAD(protocol ogrenet.Scheme, kind ogrenet.PayloadType) [4]byte {
	return [4]byte{'O', 'G', byte(protocol), byte(kind)}
}

func (s *wsSession) initiateClose(cause error) {
	s.closeOnce.Do(func() {
		cause = normalizeWSError(cause)
		s.errMu.Lock()
		s.err = cause
		s.errMu.Unlock()
		s.gate.close()
		close(s.closing)
		_ = s.ws.CloseNow()
	})
}

func (s *wsSession) finalize() {
	s.finalOnce.Do(func() {
		s.initiateClose(nil)
		defer func() {
			close(s.done)
			s.engine.removeWebSocket(s)
		}()
		s.handler.OnClose(s, s.Err())
	})
}

func (s *wsSession) isClosing() bool {
	select {
	case <-s.closing:
		return true
	default:
		return false
	}
}

func normalizeWSError(err error) error {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	status := websocket.CloseStatus(err)
	if status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway {
		return nil
	}
	return err
}

func (s *wsSession) acquireFrameSlot(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closing:
		return ErrClosed
	case s.frameSlots <- struct{}{}:
		return nil
	}
}

func (s *wsSession) tryAcquireFrameSlot() error {
	select {
	case <-s.closing:
		return ErrClosed
	case s.frameSlots <- struct{}{}:
		return nil
	default:
		return ErrWouldBlock
	}
}

func (s *wsSession) releaseFrameSlot() { <-s.frameSlots }

func (s *wsSession) acquireEncoder(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closing:
		return ErrClosed
	case s.encodeSlot <- struct{}{}:
		return nil
	}
}

func (s *wsSession) tryAcquireEncoder() error {
	select {
	case <-s.closing:
		return ErrClosed
	case s.encodeSlot <- struct{}{}:
		return nil
	default:
		return ErrWouldBlock
	}
}

func (s *wsSession) releaseEncoder() { <-s.encodeSlot }

var _ ogrenet.Session = (*wsSession)(nil)

type wsListener struct {
	engine   *Engine
	endpoint ogrenet.Endpoint
	ln       net.Listener
	server   *http.Server
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
	serveLn := net.Listener(configuredTCPListener{Listener: rawLn, engine: e})
	if endpoint.Scheme == ogrenet.SchemeWSS {
		tlsCfg, err := e.cfg.serverTLSConfig()
		if err != nil {
			cancel()
			_ = rawLn.Close()
			return nil, err
		}
		serveLn = newGatedTLSListener(lctx, serveLn, e, tlsCfg)
	}

	l := &wsListener{
		engine:   e,
		endpoint: bound,
		ln:       serveLn,
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

		remote := parseRemoteAddr(r.RemoteAddr)
		connLease, err := e.acquireOpening(remote)
		if err != nil {
			w.Header().Set("Connection", "close")
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}
		transferred := false
		defer func() {
			if !transferred {
				connLease.release()
			}
		}()

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
		ws.SetReadLimit(int64(e.cfg.maxMessageBytes))
		cipher, err := e.cfg.newCipher()
		if err != nil {
			_ = ws.CloseNow()
			return
		}
		s := e.newWSSession(ws, bound, serveLn.Addr(), remote, h, cipher)
		if err := e.addWebSocketWithLease(s, connLease); err != nil {
			_ = ws.CloseNow()
			return
		}
		transferred = true
		s.start()
	})
	l.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: e.cfg.ws.HandshakeTimeout,
		WriteTimeout:      e.cfg.ws.HandshakeTimeout,
		IdleTimeout:       e.cfg.ws.HandshakeTimeout,
		MaxHeaderBytes:    32 << 10,
	}
	if err := e.addWSListener(l); err != nil {
		cancel()
		_ = serveLn.Close()
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

func (e *Engine) dialWebSocket(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.Handler) (ogrenet.Session, error) {
	if e.cfg.framerFactory != nil {
		return nil, ErrFramerNotSupported
	}
	hctx, cancel := context.WithTimeout(ctx, e.cfg.ws.HandshakeTimeout)
	defer cancel()

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

	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	ws, _, err := websocket.Dial(hctx, endpoint.URL(), &websocket.DialOptions{
		HTTPClient:      client,
		Subprotocols:    e.cfg.ws.Subprotocols,
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		return nil, err
	}
	ws.SetReadLimit(int64(e.cfg.maxMessageBytes))
	cipher, err := e.cfg.newCipher()
	if err != nil {
		_ = ws.CloseNow()
		return nil, err
	}
	lease, local, remote := state.take()
	if lease == nil {
		_ = ws.CloseNow()
		return nil, errors.New("transport: websocket dial completed without connection admission")
	}
	transferred := false
	defer func() {
		if !transferred {
			lease.release()
		}
	}()
	if local == nil {
		local = staticAddr{network: endpoint.Scheme.String(), value: "unknown"}
	}
	if remote == nil {
		remote = staticAddr{network: endpoint.Scheme.String(), value: endpoint.Address()}
	}
	s := e.newWSSession(ws, endpoint, local, remote, h, cipher)
	if err := e.addWebSocketWithLease(s, lease); err != nil {
		_ = ws.CloseNow()
		return nil, err
	}
	transferred = true
	s.start()
	return s, nil
}

func (e *Engine) newWSSession(ws *websocket.Conn, endpoint ogrenet.Endpoint, local, remote net.Addr, h ogrenet.Handler, cipher secure.Cipher) *wsSession {
	return &wsSession{
		engine:     e,
		id:         e.nextID.Add(1),
		protocol:   endpoint.Scheme,
		endpoint:   endpoint,
		ws:         ws,
		local:      local,
		remote:     remote,
		handler:    h,
		cipher:     cipher,
		maxMessage: e.cfg.maxMessageBytes,
		writeTO:    e.cfg.ws.WriteTimeout,
		pingEvery:  e.cfg.ws.PingInterval,
		pongTO:     e.cfg.ws.PongTimeout,
		queue:      make(chan wsOutbound, e.cfg.writeQueue),
		quota:      newByteQuota(e.cfg.maxQueuedBytes),
		gate:       newSendGate(),
		frameSlots: make(chan struct{}, e.cfg.writeQueue+1),
		encodeSlot: make(chan struct{}, 1),
		closing:    make(chan struct{}),
		done:       make(chan struct{}),
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
