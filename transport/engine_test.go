package transport

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/secure"
)

func TestEncryptedTextAndBinaryEcho(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	serverCipher, err := secure.NewAESGCM(key)
	if err != nil {
		t.Fatal(err)
	}
	clientCipher, err := secure.NewAESGCM(key)
	if err != nil {
		t.Fatal(err)
	}

	server, err := New(WithCipher(serverCipher))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	listener, err := server.Listen(ctx, "tcp", "127.0.0.1:0", ogrenet.HandlerFuncs{
		Message: func(c ogrenet.Conn, msg ogrenet.Message) {
			if err := c.Send(context.Background(), msg); err != nil && !errors.Is(err, ErrClosed) {
				t.Errorf("server echo: %v", err)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	client, err := New(WithCipher(clientCipher))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	received := make(chan ogrenet.Message, 4)
	conn, err := client.Dial(ctx, "tcp", listener.Addr().String(), ogrenet.HandlerFuncs{
		Message: func(_ ogrenet.Conn, msg ogrenet.Message) {
			received <- ogrenet.Message{Type: msg.Type, Data: append([]byte(nil), msg.Data...)}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []ogrenet.Message{
		ogrenet.Text("hello 世界"),
		ogrenet.Bin([]byte{0x00, 0x01, 0xff, 0x00}),
		ogrenet.Text(""),
		ogrenet.Bin(nil),
	}
	for _, msg := range want {
		if err := conn.Send(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}

	for _, expected := range want {
		select {
		case got := <-received:
			if got.Type != expected.Type || !bytes.Equal(got.Data, expected.Data) {
				t.Fatalf("got %+v, want %+v", got, expected)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-conn.Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestTrySendBackpressure(t *testing.T) {
	e, err := New(WithWriteQueue(1))
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
	if err := c.TrySend(ogrenet.Text("three")); !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("got %v, want ErrWouldBlock", err)
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

func TestQueuedByteBackpressure(t *testing.T) {
	e, err := New(WithWriteQueue(8), WithMaxQueuedBytes(20))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	raw := newBlockingConn()
	c, err := e.adopt(raw, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	// The default frame for Text("one") is 10 header bytes + 3 payload bytes.
	if err := c.TrySend(ogrenet.Text("one")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-raw.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("writer did not start")
	}

	if err := c.TrySend(ogrenet.Text("two")); !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("got %v, want ErrWouldBlock from byte budget", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := c.Send(ctx, ogrenet.Text("two")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}

	if err := c.TrySend(ogrenet.Text("01234567890")); !errors.Is(err, ErrFrameExceedsQueueBudget) {
		t.Fatalf("got %v, want ErrFrameExceedsQueueBudget", err)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

type blockingConn struct {
	writeStarted chan struct{}
	closed       chan struct{}
	writeOnce    sync.Once
	closeOnce    sync.Once
}

func newBlockingConn() *blockingConn {
	return &blockingConn{
		writeStarted: make(chan struct{}),
		closed:       make(chan struct{}),
	}
}

func (c *blockingConn) Read([]byte) (int, error) {
	<-c.closed
	return 0, net.ErrClosed
}

func (c *blockingConn) Write([]byte) (int, error) {
	c.writeOnce.Do(func() { close(c.writeStarted) })
	<-c.closed
	return 0, net.ErrClosed
}

func (c *blockingConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (*blockingConn) LocalAddr() net.Addr              { return stubAddr("local") }
func (*blockingConn) RemoteAddr() net.Addr             { return stubAddr("remote") }
func (*blockingConn) SetDeadline(time.Time) error      { return nil }
func (*blockingConn) SetReadDeadline(time.Time) error  { return nil }
func (*blockingConn) SetWriteDeadline(time.Time) error { return nil }

type stubAddr string

func (a stubAddr) Network() string { return "stub" }
func (a stubAddr) String() string  { return string(a) }
