//go:build linux

package transport

import (
	"errors"
	"fmt"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/wire"
	"golang.org/x/sys/unix"
)

func (s *epollSession) nativeReadError(cause error, hint classifyHint) error {
	return classifyOperational(OpRead, ogrenet.SchemeTCP, cloneTCPAddr(s.local), cloneTCPAddr(s.remote), cause, hint)
}

func (s *epollSession) releaseNativeReadAttempt(codecHeld, reservationHeld bool) {
	if codecHeld {
		s.releaseCodec()
	}
	if reservationHeld && s.engine != nil && s.engine.callbacks != nil {
		s.engine.callbacks.releaseReserved()
	}
}

func (s *epollSession) failNativeDecode(r *epollReactor, cause error, hint classifyHint) {
	if s.stats != nil {
		s.stats.decodeErrors.Add(1)
	}
	s.failNativeLifecycle(r, s.nativeReadError(cause, hint))
}

func (s *epollSession) compactNativeRead(consumed int) {
	if consumed <= 0 || consumed > len(s.readPending) {
		return
	}
	if consumed == len(s.readPending) {
		s.readPending = s.readPending[:0]
		if cap(s.readPending) > retainedReadCapacity(len(s.readScratch)) {
			s.readPending = make([]byte, 0, len(s.readScratch))
		}
		return
	}
	copy(s.readPending, s.readPending[consumed:])
	s.readPending = s.readPending[:len(s.readPending)-consumed]
}

func (s *epollSession) driveNativeRead(r *epollReactor) {
	if s == nil || r == nil || s.engine == nil || s.engine.callbacks == nil || !s.readReady || s.callbackState != epollCallbackIdle || s.fd < 0 {
		return
	}
	if !s.engine.callbacks.tryReserve() {
		r.blockOnWorker(s)
		return
	}
	reservationHeld := true

	// Publish codec contention before the non-blocking acquire so a concurrent
	// encoder release cannot be lost between the failed acquire and waiter flag.
	s.decodeWaiting.Store(true)
	if err := s.tryAcquireCodec(); err != nil {
		if !errors.Is(err, ErrWouldBlock) && !errors.Is(err, ErrClosed) {
			s.engine.callbacks.releaseReserved()
			s.failNativeLifecycle(r, s.nativeReadError(err, hintNone))
			return
		}
		s.engine.callbacks.releaseReserved()
		return
	}
	s.decodeWaiting.Store(false)
	codecHeld := true

	byteBudget := s.reactor.cfg.ioBudgetBytes
	opBudget := s.reactor.cfg.ioBudgetOps
	bytesUsed := 0
	opsUsed := 0

	for {
		if len(s.readPending) != 0 {
			msg, consumed, err := s.framer.DecodeOne(s.readPending)
			if errors.Is(err, wire.ErrNeedMore) {
				if consumed != 0 {
					s.releaseNativeReadAttempt(codecHeld, reservationHeld)
					s.failNativeDecode(r, ErrInvalidFramer, hintNone)
					return
				}
			} else if err != nil {
				s.releaseNativeReadAttempt(codecHeld, reservationHeld)
				hint := hintNone
				if s.wireFramer {
					hint = hintWireDecode
				}
				s.failNativeDecode(r, fmt.Errorf("transport: decode frame: %w", err), hint)
				return
			} else {
				if consumed <= 0 || consumed > len(s.readPending) {
					s.releaseNativeReadAttempt(codecHeld, reservationHeld)
					s.failNativeDecode(r, ErrInvalidFramer, hintNone)
					return
				}
				if err := msg.Validate(); err != nil {
					s.releaseNativeReadAttempt(codecHeld, reservationHeld)
					s.failNativeDecode(r, fmt.Errorf("transport: invalid message: %w", err), hintMessageDecode)
					return
				}
				msg.Data = append([]byte(nil), msg.Data...)
				s.compactNativeRead(consumed)
				s.releaseCodec()
				codecHeld = false
				s.disarmNativeReadIdle()
				if s.stats != nil {
					s.stats.bytesRX.Add(uint64(len(msg.Data)))
					s.stats.messagesRX.Add(1)
				}
				s.observeNative(ogrenet.EventRead, uint64(len(msg.Data)), nil)
				s.callbackState = epollCallbackMessageInFlight
				s.engine.callbacks.submitReserved(&epollSessionCallbackTask{
					session: s,
					state:   epollCallbackMessageInFlight,
					message: msg,
				})
				reservationHeld = false
				return
			}
		}

		if len(s.readPending) > s.engine.cfg.maxBufferedRead {
			s.releaseNativeReadAttempt(codecHeld, reservationHeld)
			s.failNativeDecode(r, ErrReadBufferFull, hintNone)
			return
		}
		if opsUsed >= opBudget || bytesUsed >= byteBudget {
			s.ensureNativeReadIdleDeadline(r)
			s.releaseNativeReadAttempt(codecHeld, reservationHeld)
			r.requeue(s)
			return
		}

		n, err := unix.Read(s.fd, s.readScratch)
		opsUsed++
		if n > 0 {
			bytesUsed += n
			s.disarmNativeReadIdle()
			if s.activity != nil {
				s.activity.touch()
			}
			if len(s.readPending)+n > s.engine.cfg.maxBufferedRead {
				s.releaseNativeReadAttempt(codecHeld, reservationHeld)
				s.failNativeDecode(r, ErrReadBufferFull, hintNone)
				return
			}
			s.readPending = append(s.readPending, s.readScratch[:n]...)
			continue
		}
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
				s.readReady = false
				s.ensureNativeReadIdleDeadline(r)
				s.releaseNativeReadAttempt(codecHeld, reservationHeld)
				return
			}
			s.releaseNativeReadAttempt(codecHeld, reservationHeld)
			s.failNativeLifecycle(r, s.nativeReadError(err, hintNone))
			return
		}
		if n == 0 {
			s.readReady = false
			s.releaseNativeReadAttempt(codecHeld, reservationHeld)
			if s.life != nil {
				s.life.markReadClosed()
			}
			s.driveNativeLifecycle(r)
			return
		}
	}
}
