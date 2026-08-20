package transport

import (
	"context"

	"github.com/qigao/ogrenet"
)

func (c *conn) acquireFrameSlot(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closing:
		return ErrClosed
	case c.frameSlots <- struct{}{}:
		return nil
	}
}

func (c *conn) tryAcquireFrameSlot() error {
	select {
	case <-c.closing:
		return ErrClosed
	case c.frameSlots <- struct{}{}:
		return nil
	default:
		return ErrWouldBlock
	}
}

func (c *conn) releaseFrameSlot() {
	<-c.frameSlots
}

func (c *conn) acquireEncoder(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closing:
		return ErrClosed
	case c.encodeSlot <- struct{}{}:
		if c.isClosing() {
			<-c.encodeSlot
			return ErrClosed
		}
		return nil
	}
}

func (c *conn) tryAcquireEncoder() error {
	select {
	case <-c.closing:
		return ErrClosed
	case c.encodeSlot <- struct{}{}:
		if c.isClosing() {
			<-c.encodeSlot
			return ErrClosed
		}
		return nil
	default:
		return ErrWouldBlock
	}
}

func (c *conn) releaseEncoder() {
	<-c.encodeSlot
}

// prepareSend bounds library-owned frame memory before it can accumulate behind
// queue or byte-budget waits. On success the returned frame owns one frame slot
// and len(frame) bytes of quota; the caller transfers both to the writer or
// releases them on pre-queue failure.
func (c *conn) prepareSend(ctx context.Context, msg ogrenet.Message) ([]byte, error) {
	if err := c.acquireFrameSlot(ctx); err != nil {
		return nil, err
	}
	frameSlotHeld := true
	defer func() {
		if frameSlotHeld {
			c.releaseFrameSlot()
		}
	}()

	if err := c.acquireEncoder(ctx); err != nil {
		return nil, err
	}
	defer c.releaseEncoder()

	frame, err := c.encode(msg)
	if err != nil {
		return nil, err
	}
	if err := c.quota.acquire(ctx, c.closing, len(frame)); err != nil {
		return nil, err
	}

	frameSlotHeld = false
	return frame, nil
}

// prepareTrySend is the non-blocking counterpart of prepareSend.
func (c *conn) prepareTrySend(msg ogrenet.Message) ([]byte, error) {
	if err := c.tryAcquireFrameSlot(); err != nil {
		return nil, err
	}
	frameSlotHeld := true
	defer func() {
		if frameSlotHeld {
			c.releaseFrameSlot()
		}
	}()

	if err := c.tryAcquireEncoder(); err != nil {
		return nil, err
	}
	defer c.releaseEncoder()

	frame, err := c.encode(msg)
	if err != nil {
		return nil, err
	}
	if err := c.quota.tryAcquire(len(frame)); err != nil {
		return nil, err
	}

	frameSlotHeld = false
	return frame, nil
}
