package transport

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestTLSCloseWriteKeepsReadSideOpen(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTLS)
	defer p.close()

	clientHalf := requireHalfClose(t, p.client)
	serverHalf := requireHalfClose(t, p.server)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := clientHalf.CloseWrite(ctx); err != nil {
		t.Fatalf("client CloseWrite: %v", err)
	}
	waitClosed(t, serverHalf.ReadClosed(), "server TLS read half")
	assertStillOpen(t, p.client.Done(), "client TLS session")

	want := ogrenet.Text("final-server-response")
	if err := p.server.Send(ctx, want); err != nil {
		t.Fatalf("server Send after peer close_notify: %v", err)
	}
	select {
	case got := <-p.clientMsgs:
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("client got %+v, want %+v", got, want)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	if err := serverHalf.CloseWrite(ctx); err != nil {
		t.Fatalf("server CloseWrite: %v", err)
	}
	waitClosed(t, clientHalf.ReadClosed(), "client TLS read half")
	waitClosed(t, p.client.Done(), "client TLS session")
	if err := p.client.Err(); err != nil {
		t.Fatalf("client Err = %v, want nil", err)
	}
}

func TestTLSPeerCloseNotifyKeepsWriteSideOpen(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTLS)
	defer p.close()

	clientHalf := requireHalfClose(t, p.client)
	serverHalf := requireHalfClose(t, p.server)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := serverHalf.CloseWrite(ctx); err != nil {
		t.Fatalf("server CloseWrite: %v", err)
	}
	waitClosed(t, clientHalf.ReadClosed(), "client TLS read half")
	assertStillOpen(t, p.client.Done(), "client TLS session")

	want := ogrenet.Text("response-after-close-notify")
	if err := p.client.Send(ctx, want); err != nil {
		t.Fatalf("client Send after peer close_notify: %v", err)
	}
	select {
	case got := <-p.serverMsgs:
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("server got %+v, want %+v", got, want)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestTLSShutdownCallerDeadlineAbortsPhysicalTransport(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTLS)
	defer p.close()

	clientGraceful := requireGracefulShutdown(t, p.client)
	serverHalf := requireHalfClose(t, p.server)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	result := make(chan error, 1)
	go func() { result <- clientGraceful.Shutdown(ctx) }()
	waitClosed(t, serverHalf.ReadClosed(), "server TLS read half")

	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown = %v, want context deadline", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not honor caller deadline")
	}
	waitClosed(t, p.client.Done(), "client TLS session")
	if err := p.client.Err(); err != nil {
		t.Fatalf("client Err = %v, want nil after caller-owned abort", err)
	}
}

func TestTLSCloseInterruptsGracefulShutdown(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTLS)
	defer p.close()

	clientGraceful := requireGracefulShutdown(t, p.client)
	serverHalf := requireHalfClose(t, p.server)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result := make(chan error, 1)
	go func() { result <- clientGraceful.Shutdown(ctx) }()
	waitClosed(t, serverHalf.ReadClosed(), "server TLS read half")

	if err := p.client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Shutdown after Close = %v, want ErrClosed", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Close did not interrupt TLS graceful shutdown promptly")
	}
	waitClosed(t, p.client.Done(), "client TLS session")
	if err := p.client.Err(); err != nil {
		t.Fatalf("client Err = %v, want nil after explicit abort", err)
	}
}
