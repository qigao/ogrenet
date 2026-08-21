package transport

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
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
	engine        *Engine
	id            uint64
	protocol      ogrenet.Scheme
	endpoint      ogrenet.Endpoint
	ws            *websocket.Conn
	physical      io.Closer
	local         net.Addr
	remote        net.Addr
	handler       ogrenet.Handler
	cipher        secure.Cipher
	maxMessage    int
	writeTO       time.Duration
	readIdle      time.Duration
	activity      *activityClock
	writeState    wsWriteState
	pingEvery     time.Duration
	pongTO        time.Duration
	queue         chan wsOutbound
	quota         *byteQuota
	gate          *sendGate
	frameSlots    chan struct{}
	encodeSlot    chan struct{}
	life          *sessionLifecycle
	closing       chan struct{}
	writerDrained chan struct{}
	done          chan struct{}

	closeOnce         sync.Once
	finalOnce         sync.Once
	writerDrainedOnce sync.Once
	loops             sync.WaitGroup
	errMu             sync.RWMutex
	err               error
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
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if !s.gate.enter() {
		return s.operationalError(OpSend, ErrClosed, hintNone)
	}
	defer s.gate.leave()
	if err := msg.Validate(); err != nil {
		return err
	}

	if err := s.acquireFrameSlot(ctx); err != nil {
		return s.sendError(ctx, err)
	}
	held := true
	defer func() {
		if held {
			s.releaseFrameSlot()
		}
	}()
	if err := s.acquireEncoder(ctx); err != nil {
		return s.sendError(ctx, err)
	}
	typ, payload, err := s.encode(msg)
	s.releaseEncoder()
	if err != nil {
		return s.sendError(ctx, err)
	}
	if err := s.quota.acquire(ctx, s.closing, len(payload)); err != nil {
		return s.sendError(ctx, err)
	}
	held = false

	ack := make(chan error, 1)
	req := wsOutbound{typ: typ, payload: payload, ack: ack, bytes: len(payload)}
	select {
	case <-ctx.Done():
		s.quota.release(req.bytes)
		s.releaseFrameSlot()
		return context.Cause(ctx)
	case <-s.closing:
		s.quota.release(req.bytes)
		s.releaseFrameSlot()
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		if terminal := s.Err(); terminal != nil {
			return terminal
		}
		return s.operationalError(OpSend, ErrClosed, hintNone)
	case s.queue <- req:
	}

	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.closing:
		select {
		case err := <-ack:
			return err
		default:
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
			if terminal := s.Err(); terminal != nil {
				return terminal
			}
			return s.operationalError(OpSend, ErrClosed, hintNone)
		}
	}
}

func (s *wsSession) TrySend(msg ogrenet.Message) error {
	if !s.gate.enter() {
		return s.operationalError(OpSend, ErrClosed, hintNone)
	}
	defer s.gate.leave()
	if err := msg.Validate(); err != nil {
		return err
	}
	if err := s.tryAcquireFrameSlot(); err != nil {
		return s.operationalError(OpSend, err, hintNone)
	}
	held := true
	defer func() {
		if held {
			s.releaseFrameSlot()
		}
	}()
	if err := s.tryAcquireEncoder(); err != nil {
		return s.operationalError(OpSend, err, hintNone)
	}
	typ, payload, err := s.encode(msg)
	s.releaseEncoder()
	if err != nil {
		return s.operationalError(OpSend, err, hintNone)
	}
	if err := s.quota.tryAcquire(len(payload)); err != nil {
		return s.operationalError(OpSend, err, hintNone)
	}
	held = false
	req := wsOutbound{typ: typ, payload: payload, bytes: len(payload)}
	select {
	case <-s.closing:
		s.quota.release(req.bytes)
		s.releaseFrameSlot()
		if terminal := s.Err(); terminal != nil {
			return terminal
		}
		return s.operationalError(OpSend, ErrClosed, hintNone)
	case s.queue <- req:
		return nil
	default:
		s.quota.release(req.bytes)
		s.releaseFrameSlot()
		return s.operationalError(OpSend, ErrWouldBlock, hintNone)
	}
}

func (s *wsSession) Close() error {
	select {
	case <-s.done:
		return nil
	default:
	}
	s.abort(abortExplicit, nil)
	return nil
}

func (s *wsSession) start() {
	loopCount := 2
	if s.pingEvery > 0 {
		loopCount++
	}
	if s.activity != nil {
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
	if s.activity != nil {
		go func() {
			defer s.loops.Done()
			s.activity.run(s.closing, func(kind TimeoutKind) {
				s.initiateClose(s.operationalError(OpRead, &TimeoutError{Kind: kind}, hintNone))
			})
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
		s.markWriterDrained()
	}()

	graceful := false
	for {
		if graceful {
			select {
			case <-s.closing:
				return
			case req := <-s.queue:
				if !s.handleOutbound(req) {
					return
				}
			case <-s.gate.done():
				for {
					select {
					case req := <-s.queue:
						if !s.handleOutbound(req) {
							return
						}
					default:
						s.markWriterDrained()
						err := s.ws.Close(websocket.StatusNormalClosure, "")
						s.finishLocalGraceful(err)
						return
					}
				}
			}
			continue
		}

		select {
		case <-s.closing:
			return
		case <-s.life.fullRequested():
			graceful = true
		case req := <-s.queue:
			if !s.handleOutbound(req) {
				return
			}
		}
	}
}

func (s *wsSession) handleOutbound(req wsOutbound) bool {
	ctx, cancel := context.WithTimeout(context.Background(), s.writeTO)
	s.writeState.begin(ctx)
	err := s.ws.Write(ctx, req.typ, req.payload)
	timeoutCause := s.writeState.timeoutCause()
	pendingReadErr, pendingRead := s.writeState.end()
	cancel()

	if err == nil {
		if s.activity != nil {
			s.activity.touch()
		}
		s.quota.release(req.bytes)
		s.releaseFrameSlot()
		if req.ack != nil {
			req.ack <- nil
		}
		if pendingRead {
			if normalized := normalizeWSError(pendingReadErr); normalized != nil {
				s.initiateClose(s.operationalError(OpRead, normalized, hintNone))
			} else {
				s.initiateClose(nil)
			}
			return false
		}
		return true
	}

	s.quota.release(req.bytes)
	s.releaseFrameSlot()

	var opErr error
	switch {
	case timeoutCause != nil || isTimeoutFailure(err):
		opErr = s.operationalError(OpWrite, &TimeoutError{Kind: TimeoutWrite, Cause: err}, hintNone)
	case pendingRead:
		if normalized := normalizeWSError(pendingReadErr); normalized != nil {
			opErr = s.operationalError(OpRead, normalized, hintNone)
		}
	default:
		opErr = s.operationalError(OpWrite, fmt.Errorf("transport: websocket write: %w", err), hintNone)
	}

	won := s.abort(abortFailure, opErr)
	sendErr := ErrClosed
	if won && opErr != nil {
		sendErr = opErr
	} else if terminal := s.Err(); terminal != nil {
		sendErr = terminal
	}
	if req.ack != nil {
		req.ack <- sendErr
	}
	return false
}

func (s *wsSession) failPending(err error) {
	pendingErr := err
	if errors.Is(err, ErrClosed) {
		if terminal := s.Err(); terminal != nil {
			pendingErr = terminal
		} else {
			pendingErr = s.operationalError(OpSend, ErrClosed, hintNone)
		}
	}
	for {
		select {
		case req := <-s.queue:
			s.quota.release(req.bytes)
			s.releaseFrameSlot()
			if req.ack != nil {
				req.ack <- pendingErr
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
		readCtx := context.Background()
		cancel := func() {}
		if s.readIdle > 0 {
			readCtx, cancel = context.WithTimeout(context.Background(), s.readIdle)
		}
		typ, payload, err := s.ws.Read(readCtx)
		cancel()
		if err != nil {
			if isNormalWSClose(err) && isClosedSignal(s.life.fullRequested()) && !isClosedSignal(s.life.aborted()) {
				s.life.markReadClosed()
				return
			}
			if s.readIdle > 0 && isTimeoutFailure(err) && !s.isClosing() {
				s.initiateClose(s.operationalError(OpRead, &TimeoutError{Kind: TimeoutReadIdle, Cause: err}, hintNone))
			} else if cause := s.writeState.timeoutCause(); cause != nil {
				s.initiateClose(s.operationalError(OpWrite, &TimeoutError{Kind: TimeoutWrite, Cause: cause}, hintNone))
			} else if s.writeState.deferRead(err) {
				// The read failure may be a side effect of coder/websocket
				// terminating the shared connection while a bounded write is in
				// flight. Let the writer arbitrate timeout vs genuine peer/read
				// failure after the write returns.
				return
			} else if normalized := normalizeWSError(err); normalized != nil {
				s.initiateClose(s.operationalError(OpRead, normalized, hintNone))
			} else {
				s.initiateClose(nil)
			}
			return
		}
		if s.activity != nil {
			s.activity.touch()
		}
		msg, err := s.decode(typ, payload)
		if err != nil {
			s.initiateClose(s.operationalError(OpRead, err, hintMessageDecode))
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
		case <-s.life.fullRequested():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), s.pongTO)
			err := s.ws.Ping(ctx)
			cancel()
			if err != nil {
				s.initiateClose(s.operationalError(OpWrite, fmt.Errorf("transport: websocket ping: %w", err), hintNone))
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
	s.abort(abortFailure, cause)
}

func (s *wsSession) finalize() {
	s.finalOnce.Do(func() {
		if !s.isClosing() {
			s.initiateClose(nil)
		}
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
