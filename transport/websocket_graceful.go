package transport

import (
	"context"
	"errors"

	"github.com/coder/websocket"
)

func (s *wsSession) requestShutdown() bool {
	owner, _ := s.life.requestWithPrevious(closeGoalFull)
	if owner {
		s.gate.close()
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

func (s *wsSession) abort(reason abortReason, cause error) bool {
	cause = normalizeWSError(cause)
	if !s.life.abort(reason) {
		return false
	}
	s.closeOnce.Do(func() {
		s.errMu.Lock()
		s.err = cause
		s.errMu.Unlock()
		s.gate.close()
		close(s.closing)
		_ = s.ws.CloseNow()
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
		s.initiateClose(normalized)
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
