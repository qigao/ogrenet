package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/wire"
)

type outbound struct {
	frame []byte
	ack   chan error
	bytes int
}

type conn struct {
	engine  *Engine
	id      uint64
	raw     net.Conn
	framer  wire.Framer
	handler ogrenet.Handler
	queue   chan outbound
	quota   *byteQuota

	readSize int
	maxRead  int

	framerMu  sync.Mutex
	closeOnce sync.Once
	finalOnce sync.Once
	closing   chan struct{}
	done      chan struct{}

	errMu sync.RWMutex
	err   error
}

func (c *conn) ID() uint64 { return c.id }

func (c *conn) LocalAddr() net.Addr { return c.raw.LocalAddr() }

func (c *conn) RemoteAddr() net.Addr { return c.raw.RemoteAddr() }

func (c *conn) Done() <-chan struct{} { return c.done }

func (c *conn) Err() error {
	c.errMu.RLock()
	defer c.errMu.RUnlock()
	return c.err
}

// Send waits for byte/queue admission and then for the frame's socket write to
// complete. If ctx is canceled after admission, the frame may still be written
// by the writer goroutine even though Send returns ctx.Err().
func (c *conn) Send(ctx context.Context, msg ogrenet.Message) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.isClosing() {
		return ErrClosed
	}

	frame, err := c.encode(msg)
	if err != nil {
		return err
	}
	if err := c.quota.acquire(ctx, c.closing, len(frame)); err != nil {
		return err
	}

	ack := make(chan error, 1)
	req := outbound{frame: frame, ack: ack, bytes: len(frame)}
	select {
	case <-ctx.Done():
		c.quota.release(req.bytes)
		return ctx.Err()
	case <-c.closing:
		c.quota.release(req.bytes)
		return ErrClosed
	case c.queue <- req:
	}

	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closing:
		select {
		case err := <-ack:
			return err
		default:
			return ErrClosed
		}
	}
}

// TrySend queues a frame without blocking. It returns ErrWouldBlock if either
// the frame-count queue or queued-byte budget is currently full.
func (c *conn) TrySend(msg ogrenet.Message) error {
	if c.isClosing() {
		return ErrClosed
	}
	frame, err := c.encode(msg)
	if err != nil {
		return err
	}
	if err := c.quota.tryAcquire(len(frame)); err != nil {
		return err
	}
	req := outbound{frame: frame, bytes: len(frame)}
	select {
	case <-c.closing:
		c.quota.release(req.bytes)
		return ErrClosed
	case c.queue <- req:
		if c.isClosing() {
			return ErrClosed
		}
		return nil
	default:
		c.quota.release(req.bytes)
		return ErrWouldBlock
	}
}

func (c *conn) Close() error {
	c.initiateClose(nil)
	return nil
}

func (c *conn) start() {
	go c.writerLoop()
	go c.readerLoop()
}

func (c *conn) encode(msg ogrenet.Message) ([]byte, error) {
	c.framerMu.Lock()
	frame, err := c.framer.Encode(msg)
	c.framerMu.Unlock()
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), frame...), nil
}

func (c *conn) writerLoop() {
	defer c.failPending(ErrClosed)
	for {
		select {
		case <-c.closing:
			return
		default:
		}

		select {
		case <-c.closing:
			return
		case req := <-c.queue:
			err := writeAll(c.raw, req.frame)
			c.quota.release(req.bytes)
			if req.ack != nil {
				req.ack <- err
			}
			if err != nil {
				c.initiateClose(fmt.Errorf("transport: write: %w", err))
				return
			}
		}
	}
}

func (c *conn) failPending(err error) {
	for {
		select {
		case req := <-c.queue:
			c.quota.release(req.bytes)
			if req.ack != nil {
				req.ack <- err
			}
		default:
			return
		}
	}
}

func (c *conn) readerLoop() {
	defer c.finalize()
	c.handler.OnOpen(c)
	if c.isClosing() {
		return
	}

	readBuf := make([]byte, c.readSize)
	pending := make([]byte, 0, c.readSize)

	for {
		n, readErr := c.raw.Read(readBuf)
		if n > 0 {
			pending = append(pending, readBuf[:n]...)
			for len(pending) > 0 {
				c.framerMu.Lock()
				msg, consumed, err := c.framer.DecodeOne(pending)
				c.framerMu.Unlock()
				if errors.Is(err, wire.ErrNeedMore) {
					break
				}
				if err != nil {
					c.initiateClose(fmt.Errorf("transport: decode frame: %w", err))
					return
				}
				if consumed <= 0 || consumed > len(pending) {
					c.initiateClose(ErrInvalidFramer)
					return
				}
				if err := msg.Validate(); err != nil {
					c.initiateClose(fmt.Errorf("transport: invalid message: %w", err))
					return
				}

				c.handler.OnMessage(c, msg)
				pending = pending[consumed:]
				if c.isClosing() {
					return
				}
			}

			if len(pending) > c.maxRead {
				c.initiateClose(ErrReadBufferFull)
				return
			}
			if len(pending) == 0 {
				pending = pending[:0]
			}
		}

		if readErr != nil {
			c.initiateClose(normalizeConnError(readErr))
			return
		}
		if c.isClosing() {
			return
		}
	}
}

func (c *conn) initiateClose(cause error) {
	c.closeOnce.Do(func() {
		cause = normalizeConnError(cause)
		c.errMu.Lock()
		c.err = cause
		c.errMu.Unlock()
		close(c.closing)
		_ = c.raw.Close()
	})
}

func (c *conn) finalize() {
	c.finalOnce.Do(func() {
		c.initiateClose(nil)
		c.engine.removeConn(c)
		close(c.done)
		c.handler.OnClose(c, c.Err())
	})
}

func (c *conn) isClosing() bool {
	select {
	case <-c.closing:
		return true
	default:
		return false
	}
}

func normalizeConnError(err error) error {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if n > 0 {
			p = p[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

var _ ogrenet.Conn = (*conn)(nil)
