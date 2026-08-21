package transport

import (
	"context"
	"errors"
	"time"

	"github.com/coder/websocket"
)

func (s *wsSession) requestShutdown() bool {
	owner, _ := s.life.requestWithPrevious(closeGoalFull)
	if owner {
		s.gate.close()
		go s.watchCloseTimeout()
	}
	return owner
}

func (s *wsSession) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	owner := s.requestShutdown()
	for {
		select {
		case <-s.done:
			return s.lifecycleResult()
		case <-s.life.aborted():
			return s.lifecycleResult()
		case <-ctx.Done():
			if owner && s.abort(abortCaller, nil) {
				return context.Cause(ctx)
			}
			return context.Cause(ctx)
		}
	}
}

func (s *wsSession) watchCloseTimeout() {
	select {
	case <-s.writerDrained:
	case <-s.life.aborted():
		return
	case <-s.done:
		return
	}

	timer := time.NewTimer(s.engine.cfg.ws.CloseTimeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		cause := &TimeoutError{Kind: TimeoutClose, Cause: context.DeadlineExceeded}
		s.abort(abortFailure, s.operationalError(OpClose, cause, hintNone))
	case <-s.life.aborted():
	case <-s.done:
	}
}

func (s *wsSession) abort(reason abortReason, cause error) bool {
	// Failure causes reaching this point have already been normalized and
	// classified by the operation that owns them. Normalizing again here would
	// incorrectly erase typed errors whose underlying cause also matches
	// net.ErrClosed (notably WebSocket write timeouts).
	won := s.life.abortWith(reason, func() {
		if reason == abortFailure && cause != nil {
			s.errMu.Lock()
			s.err = cause
			s.errMu.Unlock()
		}
	})
	if !won {
		return false
	}
	s.closeOnce.Do(func() {
		s.gate.close()
		close(s.closing)
		if s.physical != nil {
			_ = s.physical.Close()
		} else {
			_ = s.ws.CloseNow()
		}
	})
	return true
}

func (s *wsSession) lifecycleResult() error {
	if err := s.Err(); err != nil {
		return err
	}
	switch s.life.reason() {
	case abortExplicit, abortCaller, abortFailure:
		return ErrClosed
	default:
		return nil
	}
}

func (s *wsSession) finishLocalGraceful(err error) {
	if normalized := normalizeWSError(err); normalized != nil {
		s.initiateClose(s.operationalError(OpClose, normalized, hintNone))
		return
	}
	s.life.markReadClosed()
	s.life.markWriteClosed()
	s.life.tryMarkTerminal()
	s.closeOnce.Do(func() {
		s.gate.close()
		close(s.closing)
		_ = s.ws.CloseNow()
	})
}

func (s *wsSession) markWriterDrained() {
	s.writerDrainedOnce.Do(func() { close(s.writerDrained) })
}

func isNormalWSClose(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return err == nil
	}
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}
