package transport

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestConcurrentSendAndClose(t *testing.T) {
	for iteration := 0; iteration < 50; iteration++ {
		e, err := New(WithWriteQueue(16), WithMaxQueuedBytes(4096))
		if err != nil {
			t.Fatal(err)
		}

		raw := newBlockingConn()
		c, err := e.adopt(raw, ogrenet.HandlerFuncs{})
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if i%2 == 0 {
					ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
					defer cancel()
					err := c.Send(ctx, ogrenet.Text("payload"))
					if err != nil &&
						!errors.Is(err, ErrClosed) &&
						!errors.Is(err, context.DeadlineExceeded) &&
						!errors.Is(err, context.Canceled) {
						t.Errorf("Send: %v", err)
					}
					return
				}

				err := c.TrySend(ogrenet.Text("payload"))
				if err != nil && !errors.Is(err, ErrClosed) && !errors.Is(err, ErrWouldBlock) {
					t.Errorf("TrySend: %v", err)
				}
			}(i)
		}

		time.Sleep(time.Millisecond)
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		wg.Wait()

		select {
		case <-c.Done():
		case <-time.After(time.Second):
			t.Fatal("connection did not finish shutdown")
		}
		if got := c.quota.current(); got != 0 {
			t.Fatalf("queued-byte budget after Done = %d, want 0", got)
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
