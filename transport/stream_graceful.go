package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

type writeHalfCloser interface {
	CloseWrite() error
}

func physicalStreamCloser(raw net.Conn) io.Closer {
	if tc, ok := raw.(*tls.Conn); ok {
		return tc.NetConn()
	}
	return raw
}

func (c *conn) ReadClosed() <-chan struct{} { return c.life.readDone() }

func (c *conn) requestWriteClose() bool {
	owner, _ := c.life.requestWithPrevious(closeGoalWrite)
	if owner {
		c.gate.close()
	}
	return owner
}

func (c *conn) requestShutdown() (owner bool, ownsWrite bool) {
	owner, previous := c.life.requestWithPrevious(closeGoalFull)
	if owner {
		c.gate.close()
	}
	return owner, previous == closeGoalRunning
}

func (c *conn) CloseWrite(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	owner := c.requestWriteClose()
	for {
		select {
		case <-c.life.writeDone():
			return c.lifecycleResult()
		case <-c.life.aborted():
			return c.lifecycleResult()
		case <-ctx.Done():
			if owner && c.abort(abortCaller, nil) {
				return context.Cause(ctx)
			}
			return context.Cause(ctx)
		}
	}
}

func (c *conn) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	owner, ownsWrite := c.requestShutdown()

	select {
	case <-c.life.writeDone():
	case <-c.life.aborted():
		return c.lifecycleResult()
	case <-ctx.Done():
		if owner && ownsWrite && c.abort(abortCaller, nil) {
			return context.Cause(ctx)
		}
		return context.Cause(ctx)
	}

	for {
		select {
		case <-c.done:
			return c.lifecycleResult()
		case <-c.life.aborted():
			return c.lifecycleResult()
		case <-ctx.Done():
			if owner && c.abort(abortCaller, nil) {
				return context.Cause(ctx)
			}
			return context.Cause(ctx)
		}
	}
}

func (c *conn) closeProtocolWrite() error {
	if tc, ok := c.raw.(*tls.Conn); ok {
		return c.closeTLSWrite(tc)
	}
	cw, ok := c.raw.(writeHalfCloser)
	if !ok {
		return fmt.Errorf("transport: %s stream does not support write half-close", c.protocol)
	}
	return cw.CloseWrite()
}

func (c *conn) closeTLSWrite(tc *tls.Conn) error {
	if err := tc.SetWriteDeadline(time.Now().Add(c.timeouts.Write)); err != nil {
		return fmt.Errorf("transport: set TLS close deadline: %w", err)
	}
	err := tc.CloseWrite()
	clearErr := tc.SetWriteDeadline(time.Time{})
	if err != nil {
		if isTimeoutFailure(err) {
			return &TimeoutError{Kind: TimeoutWrite, Cause: err}
		}
		return fmt.Errorf("transport: TLS close write: %w", err)
	}
	if clearErr != nil {
		return fmt.Errorf("transport: clear TLS close deadline: %w", clearErr)
	}
	return nil
}

func (c *conn) abort(reason abortReason, cause error) bool {
	if cause != nil {
		var typed *Error
		if !errors.As(cause, &typed) {
			cause = normalizeConnError(cause)
		}
	}
	won := c.life.abortWith(reason, func() {
		c.errMu.Lock()
		c.err = cause
		c.errMu.Unlock()
	})
	if !won {
		return false
	}
	c.closeOnce.Do(func() {
		c.gate.close()
		close(c.closing)
		if c.physical != nil {
			_ = c.physical.Close()
		} else {
			_ = c.raw.Close()
		}
	})
	return true
}

func (c *conn) lifecycleResult() error {
	if err := c.Err(); err != nil {
		return err
	}
	switch c.life.reason() {
	case abortExplicit, abortCaller, abortFailure:
		return ErrClosed
	default:
		return nil
	}
}

func (c *conn) maybeFinishGraceful() {
	select {
	case <-c.life.readDone():
	default:
		return
	}
	select {
	case <-c.life.writeDone():
	default:
		return
	}
	if !c.life.tryMarkTerminal() {
		return
	}
	c.closeOnce.Do(func() {
		c.gate.close()
		close(c.closing)
		_ = c.raw.Close()
	})
}

func (c *conn) markWriterDrained() {
	c.writerDrainedOnce.Do(func() { close(c.writerDrained) })
}

func (c *conn) initiateClose(cause error) {
	if cause == nil {
		c.abort(abortFailure, nil)
		return
	}
	c.abort(abortFailure, cause)
}

func isClosedSignal(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func joinCloseResult(primary, secondary error) error {
	if primary == nil {
		return secondary
	}
	if secondary == nil || errors.Is(secondary, net.ErrClosed) {
		return primary
	}
	return errors.Join(primary, secondary)
}
