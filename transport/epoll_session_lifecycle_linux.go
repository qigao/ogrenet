//go:build linux

package transport

import (
	"context"
	"errors"
	"net"

	"github.com/qigao/ogrenet"
	"golang.org/x/sys/unix"
)

func (s *epollSession) ID() uint64 { return s.id }

func (s *epollSession) Protocol() ogrenet.Scheme { return ogrenet.SchemeTCP }

func (s *epollSession) Endpoint() ogrenet.Endpoint { return s.endpoint }

func (s *epollSession) LocalAddr() net.Addr {
	if s == nil {
		return nil
	}
	return cloneTCPAddr(s.local)
}

func (s *epollSession) RemoteAddr() net.Addr {
	if s == nil {
		return nil
	}
	return cloneTCPAddr(s.remote)
}

func (s *epollSession) Stats() ogrenet.SessionStats {
	if s == nil {
		return ogrenet.SessionStats{}
	}
	out := sessionStatsSnapshot(s.id, ogrenet.SchemeTCP, s.LocalAddr(), s.RemoteAddr(), s.stats)
	if s.frameSlots != nil {
		out.QueuedFrames = uint64(len(s.frameSlots))
	}
	if s.quota != nil {
		out.QueuedBytes = nonNegativeUint64(s.quota.current())
	}
	return out
}

func (s *epollSession) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.done
}

func (s *epollSession) ReadClosed() <-chan struct{} {
	if s == nil || s.life == nil {
		return nil
	}
	return s.life.readDone()
}

func (s *epollSession) Err() error {
	if s == nil {
		return nil
	}
	s.errMu.RLock()
	defer s.errMu.RUnlock()
	return s.err
}

func (s *epollSession) setNativeTerminalError(err error) {
	s.errMu.Lock()
	s.err = err
	s.errMu.Unlock()
}

func (s *epollSession) signalNativeLifecycle() {
	if s != nil && s.reactor != nil {
		s.reactor.signal(s)
	}
}

func (s *epollSession) publishNativeAbort(reason abortReason, cause error) bool {
	if s == nil || s.life == nil || reason == abortNone {
		return false
	}
	won := s.life.abortWith(reason, func() { s.setNativeTerminalError(cause) })
	if !won {
		return false
	}
	if s.gate != nil {
		s.gate.close()
	}
	s.signalNativeLifecycle()
	return true
}

func (s *epollSession) requestNativeWriteClose() bool {
	if s == nil || s.life == nil {
		return false
	}
	owner, _ := s.life.requestWithPrevious(closeGoalWrite)
	if owner {
		if s.gate != nil {
			s.gate.close()
		}
		s.signalNativeLifecycle()
	}
	return owner
}

func (s *epollSession) requestNativeShutdown() (owner bool, ownsWrite bool) {
	if s == nil || s.life == nil {
		return false, false
	}
	owner, previous := s.life.requestWithPrevious(closeGoalFull)
	if owner {
		if s.gate != nil {
			s.gate.close()
		}
		s.signalNativeLifecycle()
	}
	return owner, previous == closeGoalRunning
}

func (s *epollSession) CloseWrite(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	owner := s.requestNativeWriteClose()
	for {
		select {
		case <-s.life.writeDone():
			return s.nativeLifecycleResult()
		case <-s.life.aborted():
			return s.nativeLifecycleResult()
		case <-ctx.Done():
			if owner {
				s.publishNativeAbort(abortCaller, nil)
			}
			return context.Cause(ctx)
		}
	}
}

func (s *epollSession) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	owner, ownsWrite := s.requestNativeShutdown()

	select {
	case <-s.life.writeDone():
	case <-s.life.aborted():
		return s.nativeLifecycleResult()
	case <-ctx.Done():
		if owner && ownsWrite {
			s.publishNativeAbort(abortCaller, nil)
		}
		return context.Cause(ctx)
	}

	for {
		select {
		case <-s.done:
			return s.nativeLifecycleResult()
		case <-s.life.aborted():
			return s.nativeLifecycleResult()
		case <-ctx.Done():
			if owner {
				s.publishNativeAbort(abortCaller, nil)
			}
			return context.Cause(ctx)
		}
	}
}

func (s *epollSession) Close() error {
	if s == nil {
		return nil
	}
	select {
	case <-s.done:
		return nil
	default:
	}
	s.publishNativeAbort(abortExplicit, nil)
	return nil
}

func (s *epollSession) nativeLifecycleResult() error {
	if err := s.Err(); err != nil {
		return err
	}
	if s.life == nil {
		return ErrClosed
	}
	switch s.life.reason() {
	case abortExplicit, abortCaller, abortFailure:
		return ErrClosed
	default:
		return nil
	}
}

func (s *epollSession) nativeWriteGateDrained() bool {
	if s == nil || s.gate == nil {
		return true
	}
	select {
	case <-s.gate.done():
		return true
	default:
		return false
	}
}

func (s *epollSession) nativeWriteHalfClosed() bool {
	if s == nil || s.life == nil {
		return false
	}
	return isClosedSignal(s.life.writeDone())
}

func (s *epollSession) nativeReadHalfClosed() bool {
	if s == nil || s.life == nil {
		return false
	}
	return isClosedSignal(s.life.readDone())
}

func (s *epollSession) driveNativeLifecycle(r *epollReactor) {
	if s == nil || r == nil || s.state == epollSessionClosed {
		return
	}
	if s.life != nil && isClosedSignal(s.life.aborted()) {
		s.finalizeNativeEstablished(r)
		return
	}
	if !s.nativeWriteHalfClosed() {
		s.driveNativeWrite(r)
		if s.state == epollSessionClosed || (s.life != nil && isClosedSignal(s.life.aborted())) {
			return
		}
	}
	if s.life != nil && isClosedSignal(s.life.writeRequested()) && !s.nativeWriteHalfClosed() && s.nativeWriteGateDrained() && !s.writeActive && len(s.queue) == 0 {
		if err := unix.Shutdown(s.fd, unix.SHUT_WR); err != nil && !errors.Is(err, unix.ENOTCONN) {
			s.failNativeLifecycle(r, s.nativeOperationalError(OpClose, err))
			return
		}
		s.life.markWriteClosed()
	}
	if s.nativeReadHalfClosed() && s.nativeWriteHalfClosed() {
		if s.life.tryMarkTerminal() {
			s.finalizeNativeEstablished(r)
		}
	}
}

func (s *epollSession) detectNativePeerFIN(r *epollReactor) {
	if s == nil || r == nil || s.fd < 0 || s.nativeReadHalfClosed() {
		return
	}
	var one [1]byte
	n, _, err := unix.Recvfrom(s.fd, one[:], unix.MSG_PEEK|unix.MSG_DONTWAIT)
	if err != nil {
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EINTR) {
			return
		}
		s.failNativeLifecycle(r, s.nativeOperationalError(OpRead, err))
		return
	}
	if n > 0 {
		return
	}
	s.life.markReadClosed()
	if s.nativeWriteHalfClosed() && s.life.tryMarkTerminal() {
		s.finalizeNativeEstablished(r)
	}
}

func (s *epollSession) failNativeLifecycle(r *epollReactor, err error) {
	if s == nil || r == nil || s.state == epollSessionClosed {
		return
	}
	if !s.established.Load() {
		s.state = epollSessionTerminal
		s.finalizeReactor(r)
		return
	}
	if s.life != nil {
		if s.life.abortWith(abortFailure, func() { s.setNativeTerminalError(err) }) && s.gate != nil {
			s.gate.close()
		}
	}
	s.finalizeNativeEstablished(r)
}

func (s *epollSession) finalizeNativeEstablished(r *epollReactor) {
	if s == nil || r == nil || s.state == epollSessionClosed || s.codecSetupOutstanding() {
		return
	}
	if !s.terminalPrepared {
		s.state = epollSessionTerminal
		if s.gate != nil {
			s.gate.close()
		}
		pendingErr := s.Err()
		if pendingErr == nil {
			pendingErr = s.nativeOperationalError(OpSend, ErrClosed)
		}
		s.releaseNativeWriteOwnership(pendingErr)
		s.writeGen++
		s.invalidateNativeRuntimeDeadlines()

		if s.registered {
			_ = r.poller.Del(s.fd)
			s.registered = false
		}
		if s.fd >= 0 {
			if s.testBeforePhysicalClose != nil {
				s.testBeforePhysicalClose(s)
			}
			_ = unix.Close(s.fd)
			s.fd = -1
		}
		if s.life != nil {
			s.life.markReadClosed()
			s.life.markWriteClosed()
			s.life.markTerminal()
		}
		if s.stats != nil {
			s.stats.age.freeze()
		}
		s.observeNative(ogrenet.EventClose, 0, s.Err())
		s.terminalPrepared = true
	}

	s.driveNativeCallbackState(r)
	if s.callbackState != epollCallbackClosed {
		return
	}
	if r.resources[s.id] == s {
		delete(r.resources, s.id)
	}
	s.state = epollSessionClosed
	s.doneOnce.Do(func() { close(s.done) })
	if s.lease != nil {
		s.lease.release()
		s.lease = nil
	}
	if s.dial != nil {
		s.dial.finish(nil, ErrClosed)
	}
	if s.engine != nil {
		s.engine.removeManaged(s.id)
	}
}

var _ ogrenet.Session = (*epollSession)(nil)
var _ ogrenet.HalfCloseSession = (*epollSession)(nil)
