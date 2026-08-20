package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/secure"
)

func TestEngineShutdownWaitsForOnClose(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}

	local, peer := net.Pipe()
	defer func() { _ = peer.Close() }()

	onCloseEntered := make(chan struct{})
	releaseOnClose := make(chan struct{})
	c, err := e.adopt(local, ogrenet.HandlerFuncs{
		Close: func(ogrenet.Conn, error) {
			close(onCloseEntered)
			<-releaseOnClose
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- e.Shutdown(ctx) }()

	select {
	case <-onCloseEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	select {
	case <-e.Done():
		t.Fatal("Engine.Done closed before OnClose returned")
	default:
	}
	select {
	case err := <-result:
		t.Fatalf("Shutdown returned before OnClose: %v", err)
	default:
	}

	close(releaseOnClose)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	select {
	case <-c.Done():
	default:
		t.Fatal("connection Done not closed when Shutdown returned")
	}
	select {
	case <-e.Done():
	default:
		t.Fatal("Engine.Done not closed when Shutdown returned")
	}
}

func TestEngineShutdownContextOnlyBoundsWait(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}

	local, peer := net.Pipe()
	defer func() { _ = peer.Close() }()

	onCloseEntered := make(chan struct{})
	releaseOnClose := make(chan struct{})
	_, err = e.adopt(local, ogrenet.HandlerFuncs{
		Close: func(ogrenet.Conn, error) {
			close(onCloseEntered)
			<-releaseOnClose
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- e.Shutdown(ctx) }()

	select {
	case <-onCloseEntered:
	case <-time.After(time.Second):
		t.Fatal("OnClose did not start")
	}

	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown = %v, want context deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not honor context deadline")
	}

	select {
	case <-e.Done():
		t.Fatal("Engine.Done closed while OnClose was still blocked")
	default:
	}

	close(releaseOnClose)
	select {
	case <-e.Done():
	case <-time.After(time.Second):
		t.Fatal("Engine did not finish after blocked OnClose returned")
	}
}

func TestEngineShutdownWaitsForInFlightDialSetup(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()

	factoryEntered := make(chan struct{})
	releaseFactory := make(chan struct{})
	e, err := New(WithCipherFactory(func() (secure.Cipher, error) {
		close(factoryEntered)
		<-releaseFactory
		return secure.NewAESGCM([]byte("0123456789abcdef0123456789abcdef"))
	}))
	if err != nil {
		t.Fatal(err)
	}

	dialResult := make(chan error, 1)
	go func() {
		_, dialErr := e.Dial(context.Background(), "tcp", ln.Addr().String(), nil)
		dialResult <- dialErr
	}()

	select {
	case <-factoryEntered:
	case <-time.After(time.Second):
		t.Fatal("Dial did not reach cipher factory")
	}

	var serverConn net.Conn
	select {
	case serverConn = <-accepted:
		defer func() { _ = serverConn.Close() }()
	case <-time.After(time.Second):
		t.Fatal("server did not accept Dial connection")
	}

	shutdownResult := make(chan error, 1)
	go func() { shutdownResult <- e.Shutdown(context.Background()) }()

	select {
	case <-e.Done():
		t.Fatal("Engine.Done closed while Dial setup was still active")
	case err := <-shutdownResult:
		t.Fatalf("Shutdown returned while Dial setup was still active: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseFactory)
	select {
	case err := <-dialResult:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Dial after concurrent shutdown = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Dial did not finish after cipher factory returned")
	}

	select {
	case err := <-shutdownResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish after in-flight Dial cleanup")
	}
	select {
	case <-e.Done():
	default:
		t.Fatal("Engine.Done not closed after in-flight Dial cleanup")
	}
}

func TestEngineDoneClosesWhenEmptyEngineCloses(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-e.Done():
	default:
		t.Fatal("empty Engine.Done did not close synchronously")
	}
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
