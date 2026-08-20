package transport

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestCloseDrainsQueuedByteBudget(t *testing.T) {
	e, err := New(WithWriteQueue(8), WithMaxQueuedBytes(1024))
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
	if err := c.TrySend(ogrenet.Text("three")); err != nil {
		t.Fatal(err)
	}

	if got := c.quota.current(); got == 0 {
		t.Fatal("queued-byte budget was not reserved")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("connection did not finish shutdown")
	}
	if got := c.quota.current(); got != 0 {
		t.Fatalf("queued-byte budget after Done = %d, want 0", got)
	}
	if err := c.TrySend(ogrenet.Text("late")); !errors.Is(err, ErrClosed) {
		t.Fatalf("TrySend after Done = %v, want ErrClosed", err)
	}
}

func TestDoneClosesAfterOnClose(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	local, peer := net.Pipe()
	defer func() { _ = peer.Close() }()

	onCloseDone := make(chan struct{})
	c, err := e.adopt(local, ogrenet.HandlerFuncs{
		Close: func(ogrenet.Conn, error) {
			close(onCloseDone)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("connection Done did not close")
	}
	select {
	case <-onCloseDone:
	default:
		t.Fatal("Done closed before OnClose returned")
	}
}
