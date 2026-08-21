package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func assertTransportError(t *testing.T, err error, op Op, protocol ogrenet.Scheme, kind ErrorKind) *Error {
	t.Helper()
	var te *Error
	if !errors.As(err, &te) {
		t.Fatalf("error is not *transport.Error: %T %v", err, err)
	}
	if te.Op != op || te.Protocol != protocol || te.Kind != kind {
		t.Fatalf("envelope = %+v, want op=%v protocol=%v kind=%v", te, op, protocol, kind)
	}
	return te
}

func TestTransportErrorTCPSendBackpressureUsesOpSend(t *testing.T) {
	e, err := New(WithWriteQueue(1))
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	blocked := newBlockingWriteConn(left)
	c, err := e.adoptStream(blocked, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		blocked.releaseWrite()
		_ = right.Close()
		_ = c.Close()
		_ = e.Close()
	}()

	if err := c.TrySend(ogrenet.Bin([]byte("first"))); err != nil {
		t.Fatalf("first TrySend: %v", err)
	}
	waitClosed(t, blocked.entered, "blocked TCP writer")
	if err := c.TrySend(ogrenet.Bin([]byte("second"))); err != nil {
		t.Fatalf("second TrySend: %v", err)
	}
	err = c.TrySend(ogrenet.Bin([]byte("third")))
	assertTransportError(t, err, OpSend, ogrenet.SchemeTCP, ErrorBackpressure)
	if !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("TrySend error does not match ErrWouldBlock: %v", err)
	}
}

func TestTransportErrorTCPMessageTooLargeUsesOpSend(t *testing.T) {
	e, err := New(WithMaxMessageBytes(1))
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	c, err := e.adoptStream(left, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = right.Close()
		_ = c.Close()
		_ = e.Close()
	}()

	err = c.TrySend(ogrenet.Bin([]byte{1, 2}))
	assertTransportError(t, err, OpSend, ogrenet.SchemeTCP, ErrorTooLarge)
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("TrySend error does not match ErrMessageTooLarge: %v", err)
	}
}

func TestTransportErrorTCPWriteFailureUsesOpWriteAndOwnsSessionErr(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	rawCause := errors.New("synthetic reset root")
	failing := &typedFailWriteConn{Conn: left, err: categorized(ErrConnectionReset, rawCause)}
	c, err := e.adoptStream(failing, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = right.Close()
		_ = e.Close()
	}()

	err = c.Send(context.Background(), ogrenet.Bin([]byte("write-reset")))
	assertTransportError(t, err, OpWrite, ogrenet.SchemeTCP, ErrorReset)
	if !errors.Is(err, ErrConnectionReset) || !errors.Is(err, rawCause) {
		t.Fatalf("write error chain lost reset/root cause: %v", err)
	}
	waitClosed(t, c.Done(), "reset TCP session")
	if c.Err() != err {
		t.Fatalf("Session.Err identity = %p %v, Send error = %p %v", c.Err(), c.Err(), err, err)
	}
}

func TestTransportErrorTCPWriteTimeoutUsesOpWrite(t *testing.T) {
	e, err := New(WithTimeouts(Timeouts{Write: 30 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	c, err := e.adoptStream(left, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = right.Close()
		_ = e.Close()
	}()

	err = c.Send(context.Background(), ogrenet.Bin([]byte("timeout")))
	assertTransportError(t, err, OpWrite, ogrenet.SchemeTCP, ErrorTimeout)
	var timeout *TimeoutError
	if !errors.As(err, &timeout) || timeout.Kind != TimeoutWrite || !errors.Is(err, ErrTimeout) {
		t.Fatalf("write timeout chain = %#v", err)
	}
	waitClosed(t, c.Done(), "timed-out TCP session")
	if c.Err() != err {
		t.Fatalf("Session.Err identity mismatch: got %v want %v", c.Err(), err)
	}
}

func TestTransportErrorTCPDialRefusedUsesOpDial(t *testing.T) {
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	addr := *ln.Addr().(*net.TCPAddr)
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	_, err = e.Dial(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: addr.IP.String(), Port: uint16(addr.Port)}, ogrenet.HandlerFuncs{})
	assertTransportError(t, err, OpDial, ogrenet.SchemeTCP, ErrorRefused)
	if !errors.Is(err, ErrConnectionRefused) {
		t.Fatalf("Dial error does not match ErrConnectionRefused: %v", err)
	}
}

func TestTransportErrorTCPListenFailureUsesOpListen(t *testing.T) {
	held, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	addr := held.Addr().(*net.TCPAddr)

	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	_, err = e.Listen(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: addr.IP.String(), Port: uint16(addr.Port)}, ogrenet.HandlerFuncs{})
	assertTransportError(t, err, OpListen, ogrenet.SchemeTCP, ErrorUnknown)
}

func TestTransportErrorTCPCleanFINRemainsErrorFree(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTCP)
	defer p.close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := requireHalfClose(t, p.server).CloseWrite(ctx); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, requireHalfClose(t, p.client).ReadClosed(), "client read half")
	if err := p.client.Err(); err != nil {
		t.Fatalf("clean FIN populated Session.Err: %v", err)
	}
}

type typedFailWriteConn struct {
	net.Conn
	err error
}

func (c *typedFailWriteConn) Write([]byte) (int, error) { return 0, c.err }
