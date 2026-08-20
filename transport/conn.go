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
	engine     *Engine
	id         uint64
	protocol   ogrenet.Scheme
	endpoint   ogrenet.Endpoint
	raw        net.Conn
	framer     wire.Framer
	handler    ogrenet.Handler
	queue      chan outbound
	quota      *byteQuota
	gate       *sendGate
	frameSlots chan struct{}
	encodeSlot chan struct{}

	readSize int
	maxRead  int

	framerMu  sync.Mutex
	closeOnce sync.Once
	finalOnce sync.Once
	loops     sync.WaitGroup
	closing   chan struct{}
	done      chan struct{}

	errMu sync.RWMutex
	err   error
}

func (c *conn) ID() uint64                 { return c.id }
func (c *conn) Protocol() ogrenet.Scheme   { return c.protocol }
func (c *conn) Endpoint() ogrenet.Endpoint { return c.endpoint }
func (c *conn) LocalAddr() net.Addr        { return c.raw.LocalAddr() }
func (c *conn) RemoteAddr() net.Addr       { return c.raw.RemoteAddr() }
func (c *conn) Done() <-chan struct{}      { return c.done }

func (c *conn) Err() error {
	c.errMu.RLock()
	defer c.errMu.RUnlock()
	return c.err
}

// Send waits for frame-count and byte-budget admission, then waits for the
// frame's socket write to complete. If ctx is canceled after queue admission,
// the frame may still be written by the writer goroutine even though Send
// returns ctx.Err().
func (c *conn) Send(ctx context.Context, msg ogrenet.Message) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !c.gate.enter() {
		return ErrClosed
	}
	defer c.gate.leave()

	frame, err := c.prepareSend(ctx, msg)
	if err != nil {
		return err
	}

	ack := make(chan error, 1)
	req := outbound{frame: frame, ack: ack, bytes: len(frame)}
	select {
	case <-ctx.Done():
		c.quota.release(req.bytes)
		c.releaseFrameSlot()
		return ctx.Err()
	case <-c.closing:
		c.quota.release(req.bytes)
		c.releaseFrameSlot()
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

// TrySend never waits for queue or network I/O. Encoding/encryption still runs
// synchronously after admission. It returns ErrWouldBlock when admission cannot
// be obtained immediately.
func (c *conn) TrySend(msg ogrenet.Message) error {
	if !c.gate.enter() {
		return ErrClosed
	}
	defer c.gate.leave()

	frame, err := c.prepareTrySend(msg)
	if err != nil {
		return err
	}
	req := outbound{frame: frame, bytes: len(frame)}
	select {
	case <-c.closing:
		c.quota.release(req.bytes)
		c.releaseFrameSlot()
		return ErrClosed
	case c.queue <- req:
		return nil
	default:
		c.quota.release(req.bytes)
		c.releaseFrameSlot()
		return ErrWouldBlock
	}
}

func (c *conn) Close() error {
	c.initiateClose(nil)
	return nil
}

func (c *conn) start() {
	c.loops.Add(2)
	go func() {
		defer c.loops.Done()
		c.writerLoop()
	}()
	go func() {
		defer c.loops.Done()
		c.readerLoop()
	}()
	go func() {
		c.loops.Wait()
		c.finalize()
	}()
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
	defer func() {
		<-c.gate.done()
		c.failPending(ErrClosed)
	}()
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
			c.releaseFrameSlot()
			sendErr := err
			if err != nil && c.isClosing() {
				sendErr = ErrClosed
			}
			if req.ack != nil {
				req.ack <- sendErr
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
			c.releaseFrameSlot()
			if req.ack != nil {
				req.ack <- err
			}
		default:
			return
		}
	}
}

func (c *conn) readerLoop() {
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
			consumedTotal := 0
			for consumedTotal < len(pending) {
				remaining := pending[consumedTotal:]
				c.framerMu.Lock()
				msg, consumed, err := c.framer.DecodeOne(remaining)
				c.framerMu.Unlock()
				if errors.Is(err, wire.ErrNeedMore) {
					break
				}
				if err != nil {
					c.initiateClose(fmt.Errorf("transport: decode frame: %w", err))
					return
				}
				if consumed <= 0 || consumed > len(remaining) {
					c.initiateClose(ErrInvalidFramer)
					return
				}
				if err := msg.Validate(); err != nil {
					c.initiateClose(fmt.Errorf("transport: invalid message: %w", err))
					return
				}

				c.handler.OnMessage(c, msg)
				consumedTotal += consumed
				if c.isClosing() {
					return
				}
			}

			if consumedTotal > 0 {
				if consumedTotal == len(pending) {
					pending = pending[:0]
				} else {
					copy(pending, pending[consumedTotal:])
					pending = pending[:len(pending)-consumedTotal]
				}
			}

			if len(pending) > c.maxRead {
				c.initiateClose(ErrReadBufferFull)
				return
			}
			if len(pending) == 0 && cap(pending) > retainedReadCapacity(c.readSize) {
				pending = make([]byte, 0, c.readSize)
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

func retainedReadCapacity(readSize int) int {
	maxInt := int(^uint(0) >> 1)
	if readSize > maxInt/4 {
		return readSize
	}
	return readSize * 4
}

func (c *conn) initiateClose(cause error) {
	c.closeOnce.Do(func() {
		cause = normalizeConnError(cause)
		c.errMu.Lock()
		c.err = cause
		c.errMu.Unlock()
		c.gate.close()
		close(c.closing)
		_ = c.raw.Close()
	})
}

func (c *conn) finalize() {
	c.finalOnce.Do(func() {
		c.initiateClose(nil)
		defer func() {
			close(c.done)
			c.engine.removeStream(c)
		}()
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

var _ ogrenet.Session = (*conn)(nil)
