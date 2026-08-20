package transport

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/wire"
)

type countingFramer struct {
	calls     atomic.Int32
	frameSize int
}

func (f *countingFramer) Encode(ogrenet.Message) ([]byte, error) {
	f.calls.Add(1)
	return make([]byte, f.frameSize), nil
}

func (*countingFramer) DecodeOne([]byte) (ogrenet.Message, int, error) {
	return ogrenet.Message{}, 0, wire.ErrNeedMore
}

func TestFrameAdmissionPrecedesEncoding(t *testing.T) {
	framer := &countingFramer{frameSize: 32}
	e, err := New(
		WithWriteQueue(1),
		WithMaxQueuedBytes(1<<20),
		WithFramerFactory(func() wire.Framer { return framer }),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	raw := newBlockingConn()
	c, err := e.adopt(raw, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	if err := c.TrySend(ogrenet.Text("one")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-raw.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("writer did not start")
	}
	if err := c.TrySend(ogrenet.Text("two")); err != nil {
		t.Fatal(err)
	}
	if got := framer.calls.Load(); got != 2 {
		t.Fatalf("Encode calls = %d, want 2", got)
	}

	if err := c.TrySend(ogrenet.Text("three")); !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("third TrySend = %v, want ErrWouldBlock", err)
	}
	if got := framer.calls.Load(); got != 2 {
		t.Fatalf("full frame admission still encoded: calls = %d", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := c.Send(ctx, ogrenet.Text("four")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked Send = %v, want context deadline", err)
	}
	if got := framer.calls.Load(); got != 2 {
		t.Fatalf("blocked Send encoded before admission: calls = %d", got)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("connection did not close")
	}
}

func TestOnlyOneEncodedFrameCanWaitOutsideByteQuota(t *testing.T) {
	framer := &countingFramer{frameSize: 32}
	c := &conn{
		framer:     framer,
		quota:      newByteQuota(32),
		frameSlots: make(chan struct{}, 3),
		encodeSlot: make(chan struct{}, 1),
		closing:    make(chan struct{}),
	}
	if err := c.quota.tryAcquire(32); err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 2)
	go func() {
		_, err := c.prepareSend(context.Background(), ogrenet.Text("first"))
		results <- err
	}()

	deadline := time.Now().Add(time.Second)
	for framer.calls.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("first sender did not reach encoding")
		}
		time.Sleep(time.Millisecond)
	}

	go func() {
		_, err := c.prepareSend(context.Background(), ogrenet.Text("second"))
		results <- err
	}()

	deadline = time.Now().Add(time.Second)
	for len(c.frameSlots) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("second sender did not reach frame admission")
		}
		time.Sleep(time.Millisecond)
	}
	if got := framer.calls.Load(); got != 1 {
		t.Fatalf("second sender encoded while first waited for byte quota: calls = %d", got)
	}
	if got := len(c.encodeSlot); got != 1 {
		t.Fatalf("encoder slot occupancy = %d, want 1", got)
	}

	close(c.closing)
	for i := 0; i < 2; i++ {
		select {
		case err := <-results:
			if !errors.Is(err, ErrClosed) {
				t.Fatalf("prepareSend = %v, want ErrClosed", err)
			}
		case <-time.After(time.Second):
			t.Fatal("blocked sender did not exit on close")
		}
	}
	if got := len(c.frameSlots); got != 0 {
		t.Fatalf("frame slots after close = %d, want 0", got)
	}
	c.quota.release(32)
}
