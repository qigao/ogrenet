package transport_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/transport"
)

type tcpContractCapture struct {
	inner    ogrenet.Handler
	opened   chan ogrenet.Session
	messages chan ogrenet.Message
}

func newTCPContractCapture(inner ogrenet.Handler) *tcpContractCapture {
	return &tcpContractCapture{
		inner:    inner,
		opened:   make(chan ogrenet.Session, 1),
		messages: make(chan ogrenet.Message, 256),
	}
}

func (h *tcpContractCapture) OnOpen(s ogrenet.Session) {
	select {
	case h.opened <- s:
	default:
	}
	if h.inner != nil {
		h.inner.OnOpen(s)
	}
}

func (h *tcpContractCapture) OnMessage(s ogrenet.Session, msg ogrenet.Message) {
	h.messages <- msg
	if h.inner != nil {
		h.inner.OnMessage(s, msg)
	}
}

func (h *tcpContractCapture) OnClose(s ogrenet.Session, err error) {
	if h.inner != nil {
		h.inner.OnClose(s, err)
	}
}

type tcpContractPair struct {
	ctx        context.Context
	cancel     context.CancelFunc
	engine     ogrenet.Engine
	listener   ogrenet.Listener
	client     ogrenet.Session
	server     ogrenet.Session
	clientSide *tcpContractCapture
	serverSide *tcpContractCapture
}

func newTCPContractPair(t *testing.T, f engineFactory, clientInner, serverInner ogrenet.Handler) *tcpContractPair {
	t.Helper()
	ctx, cancel := contractContext(t)
	e := f.new(t)
	serverSide := newTCPContractCapture(serverInner)
	clientSide := newTCPContractCapture(clientInner)

	ln, err := e.Listen(ctx, ogrenet.Endpoint{
		Scheme: ogrenet.SchemeTCP,
		Host:   "127.0.0.1",
		Port:   0,
	}, serverSide)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	client, err := e.Dial(ctx, ln.Endpoint(), clientSide)
	if err != nil {
		_ = ln.Close()
		cancel()
		t.Fatal(err)
	}
	server := recvContract(t, ctx, serverSide.opened, "accepted tcp graceful session")
	_ = recvContract(t, ctx, clientSide.opened, "client tcp graceful OnOpen")

	p := &tcpContractPair{
		ctx:        ctx,
		cancel:     cancel,
		engine:     e,
		listener:   ln,
		client:     client,
		server:     server,
		clientSide: clientSide,
		serverSide: serverSide,
	}
	t.Cleanup(func() {
		_ = p.client.Close()
		_ = p.server.Close()
		_ = p.listener.Close()
		p.cancel()
	})
	return p
}

func requireTCPHalfClose(t *testing.T, session ogrenet.Session) ogrenet.HalfCloseSession {
	t.Helper()
	half, ok := session.(ogrenet.HalfCloseSession)
	if !ok {
		t.Fatalf("%T does not implement HalfCloseSession", session)
	}
	return half
}

func assertContractStillOpen(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
		t.Fatalf("%s closed unexpectedly", what)
	default:
	}
}

func runTCPGracefulContracts(t *testing.T, f engineFactory) {
	t.Helper()

	t.Run("peer-fin-keeps-write-open", func(t *testing.T) {
		p := newTCPContractPair(t, f, nil, nil)
		serverHalf := requireTCPHalfClose(t, p.server)
		clientHalf := requireTCPHalfClose(t, p.client)

		if err := serverHalf.CloseWrite(p.ctx); err != nil {
			t.Fatal(err)
		}
		waitContractDone(t, p.ctx, clientHalf.ReadClosed(), "client tcp read half")
		assertContractStillOpen(t, p.client.Done(), "client tcp session")
		if err := p.client.Err(); err != nil {
			t.Fatalf("client Err after clean peer FIN = %v", err)
		}

		want := ogrenet.Text("response-after-fin")
		if err := p.client.Send(p.ctx, want); err != nil {
			t.Fatalf("Send after peer FIN: %v", err)
		}
		got := recvContract(t, p.ctx, p.serverSide.messages, "server response after peer FIN")
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("server got %+v, want %+v", got, want)
		}
	})

	t.Run("close-write-drains-admitted-frames", func(t *testing.T) {
		p := newTCPContractPair(t, f, nil, nil)
		clientHalf := requireTCPHalfClose(t, p.client)
		serverHalf := requireTCPHalfClose(t, p.server)

		accepted := make([]string, 0, 64)
		for i := 0; i < 64; i++ {
			text := fmt.Sprintf("msg-%02d", i)
			err := p.client.TrySend(ogrenet.Text(text))
			switch {
			case err == nil:
				accepted = append(accepted, text)
			case errors.Is(err, transport.ErrWouldBlock):
				continue
			default:
				t.Fatalf("TrySend(%d): %v", i, err)
			}
		}
		if len(accepted) == 0 {
			t.Fatal("no messages admitted before CloseWrite")
		}

		if err := clientHalf.CloseWrite(p.ctx); err != nil {
			t.Fatalf("CloseWrite: %v", err)
		}
		if err := p.client.Send(p.ctx, ogrenet.Text("late")); !errors.Is(err, transport.ErrClosed) {
			t.Fatalf("Send after CloseWrite = %v, want ErrClosed", err)
		}
		if err := p.client.TrySend(ogrenet.Text("late")); !errors.Is(err, transport.ErrClosed) {
			t.Fatalf("TrySend after CloseWrite = %v, want ErrClosed", err)
		}

		got := make([]string, 0, len(accepted))
		for len(got) < len(accepted) {
			msg := recvContract(t, p.ctx, p.serverSide.messages, "drained tcp message")
			got = append(got, string(msg.Data))
		}
		waitContractDone(t, p.ctx, serverHalf.ReadClosed(), "server tcp read half after drain")
		if !reflect.DeepEqual(got, accepted) {
			t.Fatalf("drain order = %v, want %v", got, accepted)
		}
	})

	t.Run("shutdown-waits-for-peer-fin", func(t *testing.T) {
		p := newTCPContractPair(t, f, nil, nil)
		clientHalf := requireTCPHalfClose(t, p.client)
		serverHalf := requireTCPHalfClose(t, p.server)
		result := make(chan error, 1)
		go func() { result <- p.client.Shutdown(p.ctx) }()

		waitContractDone(t, p.ctx, serverHalf.ReadClosed(), "server tcp read half after client Shutdown")
		assertContractStillOpen(t, clientHalf.ReadClosed(), "client tcp read half before peer FIN")
		select {
		case err := <-result:
			t.Fatalf("Shutdown returned before peer FIN: %v", err)
		default:
		}

		if err := serverHalf.CloseWrite(p.ctx); err != nil {
			t.Fatal(err)
		}
		if err := recvContract(t, p.ctx, result, "client tcp Shutdown completion"); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		waitContractDone(t, p.ctx, p.client.Done(), "client tcp Done after peer FIN")
		if err := p.client.Err(); err != nil {
			t.Fatalf("client terminal Err = %v, want nil", err)
		}
	})

	t.Run("done-waits-for-onclose", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		var closeCount atomic.Int32
		clientHandler := ogrenet.HandlerFuncs{Close: func(ogrenet.Session, error) {
			closeCount.Add(1)
			close(entered)
			<-release
		}}
		p := newTCPContractPair(t, f, clientHandler, nil)
		if err := p.client.Close(); err != nil {
			t.Fatal(err)
		}
		waitContractDone(t, p.ctx, entered, "client tcp OnClose entry")
		assertContractStillOpen(t, p.client.Done(), "client Done while OnClose blocked")
		close(release)
		waitContractDone(t, p.ctx, p.client.Done(), "client Done after OnClose release")
		if got := closeCount.Load(); got != 1 {
			t.Fatalf("OnClose count=%d, want 1", got)
		}
	})
}

func TestPortableTCPGracefulContracts(t *testing.T) {
	runTCPGracefulContracts(t, portableFactory())
}
