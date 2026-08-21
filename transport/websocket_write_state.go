package transport

import (
	"context"
	"errors"
	"sync"
	"time"
)

// wsWriteState lets the reader attribute a connection close caused by
// coder/websocket write-context expiration back to the write timeout that
// triggered it. coder/websocket may close the whole connection when an active
// I/O context expires, so the reader can observe the derived close before the
// writer returns.
type wsWriteState struct {
	mu  sync.RWMutex
	ctx context.Context
}

func (s *wsWriteState) begin(ctx context.Context) {
	s.mu.Lock()
	s.ctx = ctx
	s.mu.Unlock()
}

func (s *wsWriteState) end() {
	s.mu.Lock()
	s.ctx = nil
	s.mu.Unlock()
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
