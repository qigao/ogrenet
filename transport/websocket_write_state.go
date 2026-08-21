package transport

import (
	"context"
	"errors"
	"sync"
	"time"
)

// wsWriteState lets the reader attribute connection failures observed while a
// WebSocket write is in flight without racing the writer for terminal-cause
// ownership. coder/websocket may surface a derived read-side close before the
// blocked writer returns, so the reader defers that failure to the writer's
// bounded operation unless the write deadline is already known to have fired.
type wsWriteState struct {
	mu sync.RWMutex

	ctx context.Context

	pendingReadObserved bool
	pendingReadErr      error
}

func (s *wsWriteState) begin(ctx context.Context) {
	s.mu.Lock()
	s.ctx = ctx
	s.pendingReadObserved = false
	s.pendingReadErr = nil
	s.mu.Unlock()
}

func (s *wsWriteState) pendingRead() (error, bool) {
	s.mu.RLock()
	pendingErr := s.pendingReadErr
	observed := s.pendingReadObserved
	s.mu.RUnlock()
	return pendingErr, observed
}

func (s *wsWriteState) end() (error, bool) {
	s.mu.Lock()
	pendingErr := s.pendingReadErr
	observed := s.pendingReadObserved
	s.ctx = nil
	s.pendingReadObserved = false
	s.pendingReadErr = nil
	s.mu.Unlock()
	return pendingErr, observed
}

func (s *wsWriteState) deferRead(err error) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx == nil {
		return false
	}
	if !s.pendingReadObserved {
		s.pendingReadObserved = true
		s.pendingReadErr = err
	}
	return true
}

func (s *wsWriteState) timeoutCause() error {
	s.mu.RLock()
	ctx := s.ctx
	s.mu.RUnlock()
	if ctx == nil {
		return nil
	}
	cause := context.Cause(ctx)
	if errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

func (s *wsSession) writeTimeoutOrClosed() error {
	err := s.Err()
	var timeout *TimeoutError
	if errors.As(err, &timeout) && timeout.Kind == TimeoutWrite {
		return err
	}
	return ErrClosed
}
