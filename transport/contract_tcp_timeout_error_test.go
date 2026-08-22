package transport_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/transport"
)

type rawTCPContractPeer struct {
	ctx      context.Context
	cancel   context.CancelFunc
	engine   ogrenet.Engine
	session  ogrenet.Session
	peer     net.Conn
	listener net.Listener
}

func newRawTCPContractPeer(t *testing.T, f engineFactory, handler ogrenet.Handler, opts ...transport.Option) *rawTCPContractPeer {
	t.Helper()
	rawListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := rawListener.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()

	ctx, cancel := contractContext(t)
	e := f.new(t, opts...)
	capture := newTCPContractCapture(handler)
	addr := rawListener.Addr().(*net.TCPAddr)
	session, err := e.Dial(ctx, ogrenet.Endpoint{
		Scheme: ogrenet.SchemeTCP,
		Host:   addr.IP.String(),
		Port:   uint16(addr.Port),
	}, capture)
	if err != nil {
		_ = rawListener.Close()
		cancel()
		t.Fatal(err)
	}
	peer := recvContract(t, ctx, accepted, "raw TCP contract peer")
	_ = recvContract(t, ctx, capture.opened, "raw TCP contract OnOpen")
	p := &rawTCPContractPeer{ctx: ctx, cancel: cancel, engine: e, session: session, peer: peer, listener: rawListener}
	t.Cleanup(func() {
		_ = p.session.Close()
		_ = p.peer.Close()
		_ = p.listener.Close()
		p.cancel()
	})
	return p
}

func requireTimeoutKind(t *testing.T, err error, kind transport.TimeoutKind) *transport.TimeoutError {
	t.Helper()
	if !errors.Is(err, transport.ErrTimeout) {
		t.Fatalf("error=%v, want ErrTimeout", err)
	}
	var timeoutErr *transport.TimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("error=%T %v, want *TimeoutError", err, err)
	}
	if timeoutErr.Kind != kind {
		t.Fatalf("timeout kind=%v, want %v", timeoutErr.Kind, kind)
	}
	return timeoutErr
}

func waitSessionTimeout(t *testing.T, session ogrenet.Session, kind transport.TimeoutKind) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	waitContractDone(t, ctx, session.Done(), "TCP timeout session Done")
	requireTimeoutKind(t, session.Err(), kind)
}

func runTCPTimeoutErrorContracts(t *testing.T, f engineFactory) {
	t.Helper()

	t.Run("read-idle", func(t *testing.T) {
		p := newRawTCPContractPeer(t, f, nil, transport.WithTimeouts(transport.Timeouts{ReadIdle: 50 * time.Millisecond}))
		waitSessionTimeout(t, p.session, transport.TimeoutReadIdle)
	})

	t.Run("connection-idle", func(t *testing.T) {
		p := newRawTCPContractPeer(t, f, nil, transport.WithTimeouts(transport.Timeouts{ConnectionIdle: 50 * time.Millisecond}))
		waitSessionTimeout(t, p.session, transport.TimeoutConnectionIdle)
	})

	t.Run("max-lifetime", func(t *testing.T) {
		p := newRawTCPContractPeer(t, f, nil, transport.WithTimeouts(transport.Timeouts{MaxLifetime: 60 * time.Millisecond}))
		waitSessionTimeout(t, p.session, transport.TimeoutMaxLifetime)
	})

	t.Run("read-idle-suspended-during-handler", func(t *testing.T) {
		ctx, cancel := contractContext(t)
		defer cancel()
		server := f.new(t)
		serverSide := newTCPContractCapture(nil)
		ln, err := server.Listen(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, serverSide)
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()

		entered := make(chan struct{})
		release := make(chan struct{})
		clientHandler := ogrenet.HandlerFuncs{Message: func(ogrenet.Session, ogrenet.Message) {
			close(entered)
			<-release
		}}
		client := f.new(t, transport.WithTimeouts(transport.Timeouts{ReadIdle: 50 * time.Millisecond}))
		clientSide := newTCPContractCapture(clientHandler)
		clientSession, err := client.Dial(ctx, ln.Endpoint(), clientSide)
		if err != nil {
			t.Fatal(err)
		}
		defer clientSession.Close()
		serverSession := recvContract(t, ctx, serverSide.opened, "read-idle suspension server OnOpen")
		defer serverSession.Close()
		_ = recvContract(t, ctx, clientSide.opened, "read-idle suspension client OnOpen")

		if err := serverSession.Send(ctx, ogrenet.Text("hold-handler")); err != nil {
			t.Fatal(err)
		}
		waitContractDone(t, ctx, entered, "read-idle Handler entry")
		time.Sleep(150 * time.Millisecond)
		assertContractStillOpen(t, clientSession.Done(), "client during blocked Handler ReadIdle suspension")
		close(release)
		waitSessionTimeout(t, clientSession, transport.TimeoutReadIdle)
	})

	t.Run("write-timeout", func(t *testing.T) {
		p := newRawTCPContractPeer(t, f, nil,
			transport.WithTimeouts(transport.Timeouts{Write: 50 * time.Millisecond}),
			transport.WithTCPConfig(transport.TCPConfig{NoDelay: true, WriteBufferBytes: 1024}),
		)
		if tcp, ok := p.peer.(*net.TCPConn); ok {
			_ = tcp.SetReadBuffer(1024)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err := p.session.Send(ctx, ogrenet.Bin(make([]byte, 12<<20)))
		requireTimeoutKind(t, err, transport.TimeoutWrite)
		waitContractDone(t, ctx, p.session.Done(), "write-timeout Session Done")
		requireTimeoutKind(t, p.session.Err(), transport.TimeoutWrite)
	})

	t.Run("refused-is-typed", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().(*net.TCPAddr)
		if err := ln.Close(); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := contractContext(t)
		defer cancel()
		e := f.new(t)
		_, err = e.Dial(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: addr.IP.String(), Port: uint16(addr.Port)}, ogrenet.HandlerFuncs{})
		if err == nil {
			t.Fatal("refused Dial unexpectedly succeeded")
		}
		var opErr *transport.Error
		if !errors.As(err, &opErr) || opErr.Op != transport.OpDial || opErr.Protocol != ogrenet.SchemeTCP || opErr.Kind != transport.ErrorRefused {
			t.Fatalf("refused Dial error=%#v", err)
		}
	})

	t.Run("caller-cancellation-is-not-runtime-timeout", func(t *testing.T) {
		server := newTCPContractListenerHarness(t, f)
		e := f.new(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := e.Dial(ctx, server.listener.Endpoint(), ogrenet.HandlerFuncs{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Dial=%v, want context.Canceled", err)
		}
		var timeoutErr *transport.TimeoutError
		if errors.As(err, &timeoutErr) {
			t.Fatalf("caller cancellation wrapped as runtime timeout: %#v", timeoutErr)
		}
	})

	t.Run("reset-is-typed", func(t *testing.T) {
		p := newRawTCPContractPeer(t, f, nil)
		if err := p.session.Send(p.ctx, ogrenet.Text("unread-before-reset")); err != nil {
			t.Fatal(err)
		}
		tcp, ok := p.peer.(*net.TCPConn)
		if !ok {
			t.Fatalf("raw peer=%T, want *net.TCPConn", p.peer)
		}
		if err := tcp.SetLinger(0); err != nil {
			t.Fatal(err)
		}
		if err := tcp.Close(); err != nil {
			t.Fatal(err)
		}
		waitContractDone(t, p.ctx, p.session.Done(), "reset Session Done")
		var opErr *transport.Error
		if !errors.As(p.session.Err(), &opErr) || opErr.Kind != transport.ErrorReset {
			t.Fatalf("reset Session.Err=%#v", p.session.Err())
		}
	})

	t.Run("first-terminal-owner-preserved", func(t *testing.T) {
		p := newRawTCPContractPeer(t, f, nil)
		if err := p.session.Close(); err != nil {
			t.Fatal(err)
		}
		tcp, ok := p.peer.(*net.TCPConn)
		if !ok {
			t.Fatalf("raw peer=%T, want *net.TCPConn", p.peer)
		}
		_ = tcp.SetLinger(0)
		_ = tcp.Close()
		waitContractDone(t, p.ctx, p.session.Done(), "explicit-close Session Done")
		if err := p.session.Err(); err != nil {
			t.Fatalf("explicit close terminal owner overwritten: %v", err)
		}
	})
}

func TestPortableTCPTimeoutErrorContracts(t *testing.T) {
	runTCPTimeoutErrorContracts(t, portableFactory())
}
