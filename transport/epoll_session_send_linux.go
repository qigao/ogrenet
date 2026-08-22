//go:build linux

package transport

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/epoll"
	"golang.org/x/sys/unix"
)

func (s *epollSession) initNativeSendState() {
	if s == nil || s.engine == nil || s.queue != nil {
		return
	}
	s.queue = make(chan outbound, s.engine.cfg.writeQueue)
	s.quota = newByteQuota(s.engine.cfg.maxQueuedBytes)
	s.quota.setParent(s.engine.admission.bytes)
	s.gate = newSendGate()
	s.frameSlots = make(chan struct{}, s.engine.cfg.writeQueue+1)
	s.codecSlot = make(chan struct{}, 1)
	s.stats = newSessionCounters()
	s.activity = newActivityClock(s.engine.cfg.timeouts.ConnectionIdle, s.engine.cfg.timeouts.MaxLifetime)
}

func (s *epollSession) nativeOperationalError(op Op, cause error) error {
	if s == nil {
		return classifyOperational(op, ogrenet.SchemeTCP, nil, nil, cause, hintNone)
	}
	return classifyOperational(op, ogrenet.SchemeTCP, cloneTCPAddr(s.local), cloneTCPAddr(s.remote), cause, hintNone)
}

func (s *epollSession) nativeSendError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
	}
	return s.nativeOperationalError(OpSend, err)
}

func (s *epollSession) observeNative(kind ogrenet.EventKind, bytes uint64, err error) {
	if s == nil || s.engine == nil || s.engine.observer == nil {
		return
	}
	s.engine.observer.emit(ogrenet.Event{
		Kind:       kind,
		Resource:   ogrenet.ResourceSession,
		ResourceID: s.id,
		Protocol:   ogrenet.SchemeTCP,
		Local:      cloneTCPAddr(s.local),
		Remote:     cloneTCPAddr(s.remote),
		Bytes:      bytes,
		Err:        err,
	})
}

func (s *epollSession) acquireNativeFrameSlot(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.done:
		return ErrClosed
	case s.frameSlots <- struct{}{}:
	}
	select {
	case <-s.done:
		<-s.frameSlots
		return ErrClosed
	default:
		return nil
	}
}

func (s *epollSession) tryAcquireNativeFrameSlot() error {
	select {
	case <-s.done:
		return ErrClosed
	case s.frameSlots <- struct{}{}:
		select {
		case <-s.done:
			<-s.frameSlots
			return ErrClosed
		default:
			return nil
		}
	default:
		return ErrWouldBlock
	}
}

func (s *epollSession) releaseNativeFrameSlot() {
	<-s.frameSlots
}

func (s *epollSession) acquireCodec(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.done:
		return ErrClosed
	case s.codecSlot <- struct{}{}:
	}
	select {
	case <-s.done:
		<-s.codecSlot
		return ErrClosed
	default:
		return nil
	}
}

func (s *epollSession) tryAcquireCodec() error {
	select {
	case <-s.done:
		return ErrClosed
	case s.codecSlot <- struct{}{}:
		select {
		case <-s.done:
			<-s.codecSlot
			return ErrClosed
		default:
			return nil
		}
	default:
		return ErrWouldBlock
	}
}

func (s *epollSession) releaseCodec() {
	if s == nil || s.codecSlot == nil {
		return
	}
	<-s.codecSlot
	if s.decodeWaiting.Swap(false) && s.reactor != nil {
		s.reactor.signal(s)
	}
}

func (s *epollSession) encodeNative(msg ogrenet.Message) ([]byte, error) {
	frame, err := s.framer.Encode(msg)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), frame...), nil
}

func (s *epollSession) prepareNativeSend(ctx context.Context, msg ogrenet.Message) ([]byte, error) {
	if err := s.acquireNativeFrameSlot(ctx); err != nil {
		return nil, err
	}
	frameSlotHeld := true
	defer func() {
		if frameSlotHeld {
			s.releaseNativeFrameSlot()
		}
	}()

	if err := s.acquireCodec(ctx); err != nil {
		return nil, err
	}
	frame, err := s.encodeNative(msg)
	s.releaseCodec()
	if err != nil {
		return nil, err
	}
	if err := s.quota.acquire(ctx, s.done, len(frame)); err != nil {
		return nil, err
	}
	frameSlotHeld = false
	return frame, nil
}

func (s *epollSession) prepareNativeTrySend(msg ogrenet.Message) ([]byte, error) {
	if err := s.tryAcquireNativeFrameSlot(); err != nil {
		return nil, err
	}
	frameSlotHeld := true
	defer func() {
		if frameSlotHeld {
			s.releaseNativeFrameSlot()
		}
	}()

	if err := s.tryAcquireCodec(); err != nil {
		return nil, err
	}
	frame, err := s.encodeNative(msg)
	s.releaseCodec()
	if err != nil {
		return nil, err
	}
	if err := s.quota.tryAcquire(len(frame)); err != nil {
		return nil, err
	}
	frameSlotHeld = false
	return frame, nil
}

func (s *epollSession) recordNativeBackpressure(err error) error {
	opErr := s.nativeOperationalError(OpSend, err)
	if s != nil && s.stats != nil {
		s.stats.backpressure.Add(1)
	}
	s.observeNative(ogrenet.EventBackpressure, 0, opErr)
	return opErr
}

// Send performs validation/admission/encoding on the caller, then transfers the
// encoded frame to the owning reactor. Caller cancellation after queue transfer
// does not revoke reactor ownership of the admitted frame.
func (s *epollSession) Send(ctx context.Context, msg ogrenet.Message) error {
	if ctx == nil {
		return ErrNilContext
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if s == nil || s.gate == nil || !s.gate.enter() {
		if s == nil {
			return classifyOperational(OpSend, ogrenet.SchemeTCP, nil, nil, ErrClosed, hintNone)
		}
		return s.nativeOperationalError(OpSend, ErrClosed)
	}
	defer s.gate.leave()
	if err := msg.Validate(); err != nil {
		return err
	}

	frame, err := s.prepareNativeSend(ctx, msg)
	if err != nil {
		return s.nativeSendError(ctx, err)
	}
	req := outbound{
		frame:        frame,
		ack:          make(chan error, 1),
		bytes:        len(frame),
		payloadBytes: len(msg.Data),
	}
	select {
	case <-ctx.Done():
		s.quota.release(req.bytes)
		s.releaseNativeFrameSlot()
		return context.Cause(ctx)
	case <-s.done:
		s.quota.release(req.bytes)
		s.releaseNativeFrameSlot()
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		return s.nativeOperationalError(OpSend, ErrClosed)
	case s.queue <- req:
	}

	if s.testAfterSendQueueTransfer != nil {
		s.testAfterSendQueueTransfer()
	}
	if s.reactor != nil {
		s.reactor.signal(s)
	}

	select {
	case err := <-req.ack:
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.done:
		select {
		case err := <-req.ack:
			return err
		default:
		}
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		return s.nativeOperationalError(OpSend, ErrClosed)
	}
}

// TrySend performs the same caller-side validation/admission/encoding as Send,
// but it never waits for queue capacity or reactor/network progress.
func (s *epollSession) TrySend(msg ogrenet.Message) error {
	if s == nil || s.gate == nil || !s.gate.enter() {
		if s == nil {
			return classifyOperational(OpSend, ogrenet.SchemeTCP, nil, nil, ErrClosed, hintNone)
		}
		return s.nativeOperationalError(OpSend, ErrClosed)
	}
	defer s.gate.leave()
	if err := msg.Validate(); err != nil {
		return err
	}

	frame, err := s.prepareNativeTrySend(msg)
	if err != nil {
		if errors.Is(err, ErrWouldBlock) {
			return s.recordNativeBackpressure(err)
		}
		return s.nativeOperationalError(OpSend, err)
	}
	req := outbound{frame: frame, bytes: len(frame), payloadBytes: len(msg.Data)}
	select {
	case <-s.done:
		s.quota.release(req.bytes)
		s.releaseNativeFrameSlot()
		return s.nativeOperationalError(OpSend, ErrClosed)
	case s.queue <- req:
		if s.testAfterSendQueueTransfer != nil {
			s.testAfterSendQueueTransfer()
		}
		if s.reactor != nil {
			s.reactor.signal(s)
		}
		return nil
	default:
		s.quota.release(req.bytes)
		s.releaseNativeFrameSlot()
		return s.recordNativeBackpressure(ErrWouldBlock)
	}
}

func (s *epollSession) driveNativeWrite(r *epollReactor) {
	if s == nil || r == nil || s.fd < 0 || (s.state != epollSessionOpening && s.state != epollSessionActive) {
		return
	}
	if s.writeBlocked {
		return
	}

	bytesBudget := r.cfg.ioBudgetBytes
	opsBudget := r.cfg.ioBudgetOps
	for bytesBudget > 0 && opsBudget > 0 {
		if !s.writeActive {
			select {
			case req := <-s.queue:
				s.writeCurrent = req
				s.writeOffset = 0
				s.writeActive = true
				s.writeGen++
				if timeout := s.engine.cfg.timeouts.Write; timeout > 0 {
					r.scheduleDeadline(s.id, epollDeadlineWrite, s.writeGen, time.Now().Add(timeout))
				}
			default:
				s.disableNativeWriteInterest(r)
				return
			}
		}

		remaining := s.writeCurrent.frame[s.writeOffset:]
		n, err := unix.Write(s.fd, remaining)
		opsBudget--
		if n > 0 {
			s.writeOffset += n
			bytesBudget -= n
			if bytesBudget < 0 {
				bytesBudget = 0
			}
			if s.activity != nil {
				s.activity.touch()
			}
			if s.writeOffset < len(s.writeCurrent.frame) && s.testAfterWriteProgress != nil {
				s.testAfterWriteProgress(s)
			}
		}

		if err != nil {
			switch {
			case errors.Is(err, unix.EINTR):
				continue
			case errors.Is(err, unix.EAGAIN), errors.Is(err, unix.EWOULDBLOCK):
				if enableErr := s.enableNativeWriteInterest(r); enableErr != nil {
					s.failNativeWrite(r, s.nativeOperationalError(OpWrite, enableErr))
				}
				return
			default:
				s.failNativeWrite(r, s.nativeOperationalError(OpWrite, err))
				return
			}
		}
		if n == 0 {
			s.failNativeWrite(r, s.nativeOperationalError(OpWrite, io.ErrNoProgress))
			return
		}
		if s.writeOffset == len(s.writeCurrent.frame) {
			s.completeNativeWrite()
			continue
		}
	}

	if s.writeActive || len(s.queue) > 0 {
		r.requeue(s)
		return
	}
	s.disableNativeWriteInterest(r)
}

func (s *epollSession) enableNativeWriteInterest(r *epollReactor) error {
	if s.writeInterested {
		s.writeBlocked = true
		return nil
	}
	if err := r.poller.Mod(s.fd, epoll.Readable|epoll.Writable|epoll.PeerClosed|epoll.Error|epoll.EdgeTriggered, s.id); err != nil {
		return err
	}
	s.writeInterested = true
	s.writeBlocked = true
	return nil
}

func (s *epollSession) disableNativeWriteInterest(r *epollReactor) {
	if s == nil || r == nil || !s.writeInterested || s.fd < 0 || !s.registered {
		s.writeBlocked = false
		return
	}
	if err := r.poller.Mod(s.fd, epoll.Readable|epoll.PeerClosed|epoll.Error|epoll.EdgeTriggered, s.id); err != nil {
		s.failNativeWrite(r, s.nativeOperationalError(OpWrite, err))
		return
	}
	s.writeInterested = false
	s.writeBlocked = false
}

func (s *epollSession) completeNativeWrite() {
	req := s.writeCurrent
	s.writeCurrent = outbound{}
	s.writeOffset = 0
	s.writeActive = false
	s.writeBlocked = false
	s.writeGen++
	s.quota.release(req.bytes)
	s.releaseNativeFrameSlot()
	if s.stats != nil {
		s.stats.bytesTX.Add(uint64(req.payloadBytes))
		s.stats.messagesTX.Add(1)
	}
	s.observeNative(ogrenet.EventWrite, uint64(req.payloadBytes), nil)
	if req.ack != nil {
		req.ack <- nil
	}
}

func (s *epollSession) failNativeWrite(r *epollReactor, err error) {
	if s == nil || s.state == epollSessionClosed || s.state == epollSessionTerminal {
		return
	}
	s.releaseNativeWriteOwnership(err)
	s.state = epollSessionTerminal
	s.finalizeReactor(r)
}

func (s *epollSession) releaseNativeWriteOwnership(err error) {
	if s == nil || s.queue == nil {
		return
	}
	if err == nil {
		err = s.nativeOperationalError(OpSend, ErrClosed)
	}
	if s.writeActive {
		req := s.writeCurrent
		s.writeCurrent = outbound{}
		s.writeOffset = 0
		s.writeActive = false
		s.writeBlocked = false
		s.writeInterested = false
		s.writeGen++
		if s.quota != nil {
			s.quota.release(req.bytes)
		}
		if s.frameSlots != nil {
			s.releaseNativeFrameSlot()
		}
		if req.ack != nil {
			req.ack <- err
		}
	}
	for {
		select {
		case req := <-s.queue:
			if s.quota != nil {
				s.quota.release(req.bytes)
			}
			if s.frameSlots != nil {
				s.releaseNativeFrameSlot()
			}
			if req.ack != nil {
				req.ack <- err
			}
		default:
			return
		}
	}
}

func (s *epollSession) onNativeWritable(r *epollReactor) {
	if s == nil {
		return
	}
	s.writeBlocked = false
	s.driveNativeWrite(r)
}

func (s *epollSession) onNativeWriteDeadline(r *epollReactor, generation uint64) {
	if s == nil || !s.writeActive || generation != s.writeGen || (s.state != epollSessionOpening && s.state != epollSessionActive) {
		return
	}
	timeout := &TimeoutError{Kind: TimeoutWrite, Cause: context.DeadlineExceeded}
	s.failNativeWrite(r, s.nativeOperationalError(OpWrite, timeout))
}
