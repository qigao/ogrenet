package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestTransportErrorRaceTimeoutVsDerivedClose(t *testing.T) {
	e, err := New(WithTimeouts(Timeouts{Write: 30 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	left, right := net.Pipe()
	c, err := e.adoptStream(left, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer right.Close()

	err = c.Send(context.Background(), ogrenet.Bin([]byte("timeout-owner")))
	assertTransportError(t, err, OpWrite, ogrenet.SchemeTCP, ErrorTimeout)
	var timeout *TimeoutError
	if !errors.As(err, &timeout) || timeout.Kind != TimeoutWrite || !errors.Is(err, ErrTimeout) {
		t.Fatalf("write timeout chain = %#v", err)
	}
	terminal := c.Err()
	if terminal != err {
		t.Fatalf("Session.Err identity mismatch before fallout: got %v want %v", terminal, err)
	}

	waitClosed(t, c.Done(), "timeout-owned TCP session")
	_ = right.Close()
	_ = c.Close()
	if c.Err() != terminal {
		t.Fatalf("derived close replaced timeout owner: got %v want %v", c.Err(), terminal)
	}
}

func TestTransportErrorRaceResetVsExplicitClose(t *testing.T) {
	t.Run("reset-first", func(t *testing.T) {
		e, err := New()
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()

		left, right := net.Pipe()
		defer right.Close()
		raw := errors.New("synthetic reset")
		failing := newControlledWriteErrorConn(left, categorized(ErrConnectionReset, raw))
		c, err := e.adoptStream(failing, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
		if err != nil {
			t.Fatal(err)
		}

		result := make(chan error, 1)
		go func() { result <- c.Send(context.Background(), ogrenet.Bin([]byte("reset-first"))) }()
		waitClosed(t, failing.entered, "reset writer entry")
		failing.releaseWrite()

		err = <-result
		assertTransportError(t, err, OpWrite, ogrenet.SchemeTCP, ErrorReset)
		if !errors.Is(err, ErrConnectionReset) || !errors.Is(err, raw) {
			t.Fatalf("reset chain lost cause: %v", err)
		}
		terminal := c.Err()
		if terminal != err {
			t.Fatalf("Session.Err identity mismatch: got %v want %v", terminal, err)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		waitClosed(t, c.Done(), "reset-owned TCP session")
		if c.Err() != terminal {
			t.Fatalf("explicit Close replaced reset owner: got %v want %v", c.Err(), terminal)
		}
	})

	t.Run("explicit-close-first", func(t *testing.T) {
		e, err := New()
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()

		left, right := net.Pipe()
		defer right.Close()
		failing := newControlledWriteErrorConn(left, categorized(ErrConnectionReset, errors.New("late reset")))
		c, err := e.adoptStream(failing, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
		if err != nil {
			t.Fatal(err)
		}

		result := make(chan error, 1)
		go func() { result <- c.Send(context.Background(), ogrenet.Bin([]byte("close-first"))) }()
		waitClosed(t, failing.entered, "close-first writer entry")
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		if c.Err() != nil {
			t.Fatalf("explicit Close populated Session.Err before derived failure: %v", c.Err())
		}
		failing.releaseWrite()
		select {
		case sendErr := <-result:
			if sendErr == nil {
				t.Fatal("blocked Send unexpectedly succeeded after explicit Close")
			}
		case <-time.After(time.Second):
			t.Fatal("blocked Send did not finish after explicit Close")
		}
		waitClosed(t, c.Done(), "explicit-close-owned TCP session")
		if c.Err() != nil {
			t.Fatalf("derived reset replaced explicit Close ownership: %v", c.Err())
		}
	})
}

func TestTransportErrorRaceShutdownDeadlineVsPhysicalClose(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	left, right := net.Pipe()
	defer right.Close()
	blocked := newBlockingWriteConn(left)
	c, err := e.adoptStream(blocked, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	sendResult := make(chan error, 1)
	go func() { sendResult <- c.Send(context.Background(), ogrenet.Bin([]byte("shutdown-deadline"))) }()
	waitClosed(t, blocked.entered, "shutdown-deadline writer entry")

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	err = c.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want caller deadline", err)
	}
	if c.Err() != nil {
		t.Fatalf("caller-owned Shutdown abort populated Session.Err: %v", c.Err())
	}

	blocked.releaseWrite()
	select {
	case <-sendResult:
	case <-time.After(time.Second):
		t.Fatal("blocked Send did not finish after caller-owned physical close")
	}
	waitClosed(t, c.Done(), "caller-aborted TCP session")
	if c.Err() != nil {
		t.Fatalf("physical-close fallout replaced caller ownership: %v", c.Err())
	}
}

func TestTransportErrorRaceWebSocketCloseTimeoutVsPhysicalClose(t *testing.T) {
	endpoint, waitAccepted := startNonResponsiveWebSocketServer(t)
	e, err := New(WithWebSocketConfig(testWebSocketCloseConfig(50 * time.Millisecond)))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	s, err := e.Dial(context.Background(), endpoint, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	waitAccepted()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = s.Shutdown(ctx)
	assertTransportError(t, err, OpClose, ogrenet.SchemeWS, ErrorTimeout)
	var timeout *TimeoutError
	if !errors.As(err, &timeout) || timeout.Kind != TimeoutClose || !errors.Is(err, ErrTimeout) {
		t.Fatalf("WebSocket close timeout chain = %#v", err)
	}
	terminal := s.Err()
	if terminal != err {
		t.Fatalf("Session.Err identity mismatch before physical-close fallout: got %v want %v", terminal, err)
	}

	waitClosed(t, s.Done(), "WebSocket close-timeout session")
	if closeErr := s.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if s.Err() != terminal {
		t.Fatalf("physical-close fallout replaced WebSocket close timeout: got %v want %v", s.Err(), terminal)
	}
}

type controlledWriteErrorConn struct {
	net.Conn
	err     error
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newControlledWriteErrorConn(conn net.Conn, err error) *controlledWriteErrorConn {
	return &controlledWriteErrorConn{
		Conn:    conn,
		err:     err,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (c *controlledWriteErrorConn) Write([]byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return 0, c.err
}

func (c *controlledWriteErrorConn) releaseWrite() {
	select {
	case <-c.release:
	default:
		close(c.release)
	}
}
