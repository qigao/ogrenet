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

func TestGracefulRaceSendVsShutdown(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTCP)
	defer p.close()

	start := make(chan struct{})
	type sendResult struct {
		id  byte
		err error
	}
	results := make(chan sendResult, 32)
	for i := 0; i < 32; i++ {
		id := byte(i)
		go func() {
			<-start
			results <- sendResult{id: id, err: p.client.Send(context.Background(), ogrenet.Bin([]byte{id}))}
		}()
	}
	shutdownResult := make(chan error, 1)
	go func() {
		<-start
		shutdownResult <- p.client.Shutdown(context.Background())
	}()
	close(start)

	waitClosed(t, requireHalfClose(t, p.server).ReadClosed(), "server read-half")
	accepted := make(map[byte]bool)
	for i := 0; i < 32; i++ {
		r := <-results
		switch {
		case r.err == nil:
			accepted[r.id] = true
		case errors.Is(r.err, ErrClosed):
		default:
			t.Fatalf("Send(%d) = %v", r.id, r.err)
		}
	}
	got := make(map[byte]bool)
	for len(got) < len(accepted) {
		select {
		case msg := <-p.serverMsgs:
			if len(msg.Data) == 1 {
				got[msg.Data[0]] = true
			}
		case <-time.After(time.Second):
			t.Fatalf("delivered %d/%d accepted sends", len(got), len(accepted))
		}
	}
	for id := range accepted {
		if !got[id] {
			t.Fatalf("accepted Send(%d) was not delivered", id)
		}
	}
	if err := requireHalfClose(t, p.server).CloseWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-shutdownResult; err != nil {
		t.Fatalf("Shutdown = %v", err)
	}
	assertGracefulAccountingZero(t, p.clientEngine)
}

func TestGracefulRaceTrySendVsShutdown(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTCP)
	defer p.close()

	start := make(chan struct{})
	type tryResult struct {
		id  byte
		err error
	}
	results := make(chan tryResult, 32)
	for i := 0; i < 32; i++ {
		id := byte(i)
		go func() {
			<-start
			results <- tryResult{id: id, err: p.client.TrySend(ogrenet.Bin([]byte{id}))}
		}()
	}
	shutdownResult := make(chan error, 1)
	go func() {
		<-start
		shutdownResult <- p.client.Shutdown(context.Background())
	}()
	close(start)
	waitClosed(t, requireHalfClose(t, p.server).ReadClosed(), "server read-half")

	accepted := make(map[byte]bool)
	for i := 0; i < 32; i++ {
		r := <-results
		switch {
		case r.err == nil:
			accepted[r.id] = true
		case errors.Is(r.err, ErrClosed), errors.Is(r.err, ErrWouldBlock):
		default:
			t.Fatalf("TrySend(%d) = %v", r.id, r.err)
		}
	}
	got := make(map[byte]bool)
	for len(got) < len(accepted) {
		select {
		case msg := <-p.serverMsgs:
			if len(msg.Data) == 1 {
				got[msg.Data[0]] = true
			}
		case <-time.After(time.Second):
			t.Fatalf("delivered %d/%d accepted TrySend calls", len(got), len(accepted))
		}
	}
	if err := requireHalfClose(t, p.server).CloseWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-shutdownResult; err != nil {
		t.Fatalf("Shutdown = %v", err)
	}
	assertGracefulAccountingZero(t, p.clientEngine)
}

func TestGracefulRaceSendContextCancelAfterEnqueue(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	left, right := net.Pipe()
	blocked := newBlockingWriteConn(left)
	c, err := e.adoptStream(blocked, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer right.Close()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- c.Send(ctx, ogrenet.Bin([]byte("cancel-after-enqueue"))) }()
	waitClosed(t, blocked.entered, "blocked writer entry")
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Send = %v, want context.Canceled", err)
	}

	readDone := make(chan struct{})
	go func() {
		buf := make([]byte, 4096)
		_, _ = right.Read(buf)
		close(readDone)
	}()
	blocked.releaseWrite()
	waitClosed(t, readDone, "peer read")
	_ = c.Close()
	waitClosed(t, c.Done(), "canceled-send session")
	assertGracefulAccountingZero(t, e)
}

func TestGracefulRaceCloseWriteVsShutdown(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTCP)
	defer p.close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	closeWriteResult := make(chan error, 1)
	shutdownResult := make(chan error, 1)
	start := make(chan struct{})
	go func() { <-start; closeWriteResult <- requireHalfClose(t, p.client).CloseWrite(ctx) }()
	go func() { <-start; shutdownResult <- p.client.Shutdown(ctx) }()
	close(start)
	waitClosed(t, requireHalfClose(t, p.server).ReadClosed(), "server read-half")
	if err := requireHalfClose(t, p.server).CloseWrite(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-closeWriteResult; err != nil {
		t.Fatalf("CloseWrite = %v", err)
	}
	if err := <-shutdownResult; err != nil {
		t.Fatalf("Shutdown = %v", err)
	}
	assertGracefulAccountingZero(t, p.clientEngine)
}

func TestGracefulRaceShutdownVsClose(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTCP)
	defer p.serverEngine.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- p.client.Shutdown(ctx) }()
	waitClosed(t, requireHalfClose(t, p.server).ReadClosed(), "server read-half")
	if err := p.client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatalf("Shutdown racing Close = %v, want ErrClosed", err)
	}
	waitClosed(t, p.client.Done(), "closed session")
	assertGracefulAccountingZero(t, p.clientEngine)
}

func TestGracefulRacePeerFINVsCloseWrite(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTCP)
	defer p.close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := make(chan struct{})
	clientResult := make(chan error, 1)
	serverResult := make(chan error, 1)
	go func() { <-start; clientResult <- requireHalfClose(t, p.client).CloseWrite(ctx) }()
	go func() { <-start; serverResult <- requireHalfClose(t, p.server).CloseWrite(ctx) }()
	close(start)
	if err := <-clientResult; err != nil {
		t.Fatalf("client CloseWrite = %v", err)
	}
	if err := <-serverResult; err != nil {
		t.Fatalf("server CloseWrite = %v", err)
	}
	waitClosed(t, p.client.Done(), "client session")
	waitClosed(t, p.server.Done(), "server session")
	assertGracefulAccountingZero(t, p.clientEngine)
	assertGracefulAccountingZero(t, p.serverEngine)
}

func TestGracefulRaceTimeoutVsDrain(t *testing.T) {
	e, err := New(WithTimeouts(Timeouts{Write: 40 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	left, right := net.Pipe()
	defer right.Close()
	c, err := e.adoptStream(left, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.TrySend(ogrenet.Bin([]byte("timeout-during-drain"))); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = c.CloseWrite(ctx)
	var te *TimeoutError
	if !errors.As(err, &te) || te.Kind != TimeoutWrite {
		t.Fatalf("CloseWrite = %#v, want TimeoutWrite", err)
	}
	waitClosed(t, c.Done(), "timed-out drain")
	assertGracefulAccountingZero(t, e)
}

func TestGracefulRaceWebSocketCloseVsWriteTimeout(t *testing.T) {
	endpoint := startNonReadingWebSocketEndpoint(t)
	e, err := New(WithTimeouts(Timeouts{Write: 30 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	session, err := e.Dial(context.Background(), endpoint, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	ws := session.(*wsSession)
	sendResult := make(chan error, 1)
	go func() { sendResult <- session.Send(context.Background(), ogrenet.Bin(make([]byte, 4<<20))) }()
	waitWSWriteActive(t, ws)
	shutdownResult := make(chan error, 1)
	go func() { shutdownResult <- session.Shutdown(context.Background()) }()

	var te *TimeoutError
	if err := <-sendResult; !errors.As(err, &te) || te.Kind != TimeoutWrite {
		t.Fatalf("Send = %#v, want TimeoutWrite", err)
	}
	if err := <-shutdownResult; !errors.As(err, &te) || te.Kind != TimeoutWrite {
		t.Fatalf("Shutdown = %#v, want TimeoutWrite", err)
	}
	waitClosed(t, session.Done(), "WebSocket timeout session")
	assertGracefulAccountingZero(t, e)
}

func assertGracefulAccountingZero(t *testing.T, e *Engine) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap := e.admissionSnapshot()
		if snap.OpeningConnections == 0 && snap.ActiveConnections == 0 && snap.DrainingConnections == 0 && snap.GlobalQueuedBytes == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("leaked accounting: %+v", e.admissionSnapshot())
}

type blockingWriteConn struct {
	net.Conn
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingWriteConn(conn net.Conn) *blockingWriteConn {
	return &blockingWriteConn{Conn: conn, entered: make(chan struct{}), release: make(chan struct{})}
}

func (c *blockingWriteConn) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.Conn.Write(p)
}

func (c *blockingWriteConn) releaseWrite() {
	select {
	case <-c.release:
	default:
		close(c.release)
	}
}
