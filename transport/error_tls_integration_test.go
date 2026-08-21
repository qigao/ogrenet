package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestTransportErrorTLSCertificateFailureUsesOpHandshake(t *testing.T) {
	serverTLS, _ := testTLSConfigs(t)
	server, err := New(WithTLSServerConfig(serverTLS))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	listener, err := server.Listen(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeTLS, Host: "127.0.0.1", Port: 0}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	clientTLS := &tls.Config{MinVersion: tls.VersionTLS13}
	client, err := New(WithTLSClientConfig(clientTLS))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = client.Dial(ctx, listener.Endpoint(), ogrenet.HandlerFuncs{})
	assertTransportError(t, err, OpHandshake, ogrenet.SchemeTLS, ErrorTLS)
	if !errors.Is(err, ErrTLS) {
		t.Fatalf("handshake error does not match ErrTLS: %v", err)
	}
	var verification *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	if !errors.As(err, &verification) && !errors.As(err, &unknownAuthority) {
		t.Fatalf("typed certificate cause not reachable: %T %v", err, err)
	}
}

func TestTransportErrorTLSCloseWriteTimeoutUsesOpClose(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	rawTimeout := &TimeoutError{Kind: TimeoutWrite, Cause: context.DeadlineExceeded}
	wrapped := &closeWriteErrorConn{Conn: left, err: rawTimeout}
	c, err := e.adoptStream(wrapped, ogrenet.Endpoint{Scheme: ogrenet.SchemeTLS, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = right.Close()
		_ = e.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = c.CloseWrite(ctx)
	assertTransportError(t, err, OpClose, ogrenet.SchemeTLS, ErrorTimeout)
	var timeout *TimeoutError
	if !errors.As(err, &timeout) || timeout.Kind != TimeoutWrite || !errors.Is(err, ErrTimeout) {
		t.Fatalf("TLS close timeout chain = %#v", err)
	}
	waitClosed(t, c.Done(), "TLS close-timeout session")
	if c.Err() != err {
		t.Fatalf("Session.Err identity mismatch: got %v want %v", c.Err(), err)
	}
}

func TestTransportErrorTLSCleanCloseNotifyRemainsErrorFree(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTLS)
	defer p.close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := requireHalfClose(t, p.server).CloseWrite(ctx); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, requireHalfClose(t, p.client).ReadClosed(), "client TLS read half")
	if err := p.client.Err(); err != nil {
		t.Fatalf("clean close_notify populated Session.Err: %v", err)
	}
}

type closeWriteErrorConn struct {
	net.Conn
	err error
}

func (c *closeWriteErrorConn) CloseWrite() error { return c.err }
