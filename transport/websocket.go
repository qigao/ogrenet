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
	readIdle   time.Duration
	activity   *activityClock
	writeState wsWriteState
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
			return s.writeTimeoutOrClosed()
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
				s.initiateClose(&TimeoutError{Kind: kind})
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
	}()
	for {
		select {
		case <-s.closing:
			return
		case req := <-s.queue:
			ctx, cancel := context.WithTimeout(context.Background(), s.writeTO)
			s.writeState.begin(ctx)
			err := s.ws.Write(ctx, req.typ, req.payload)
			cancel()
			if err == nil {
				s.writeState.end()
				if s.activity != nil {
					s.activity.touch()
				}
			}
			if err != nil && isTimeoutFailure(err) {
				err = &TimeoutError{Kind: TimeoutWrite, Cause: err}
			} else if err != nil {
				err = fmt.Errorf("transport: websocket write: %w", err)
			}
			s.quota.release(req.bytes)
			s.releaseFrameSlot()
			sendErr := err
			if err != nil && s.isClosing() && !errors.Is(err, ErrTimeout) {
				sendErr = ErrClosed
			}
			if req.ack != nil {
				req.ack <- sendErr
			}
			if err != nil {
				s.initiateClose(err)
				s.writeState.end()
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
		readCtx := context.Background()
		cancel := func() {}
		if s.readIdle > 0 {
			readCtx, cancel = context.WithTimeout(context.Background(), s.readIdle)
		}
		typ, payload, err := s.ws.Read(readCtx)
		cancel()
		if err != nil {
			if s.readIdle > 0 && isTimeoutFailure(err) && !s.isClosing() {
				s.initiateClose(&TimeoutError{Kind: TimeoutReadIdle, Cause: err})
			} else if cause := s.writeState.timeoutCause(); cause != nil {
				s.initiateClose(&TimeoutError{Kind: TimeoutWrite, Cause: cause})
			} else {
				s.initiateClose(normalizeWSError(err))
			}
			return
		}
		if s.activity != nil {
			s.activity.touch()
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
