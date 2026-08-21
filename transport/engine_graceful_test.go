package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestEngineShutdownWaitsForPeerGracefulClose(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTCP)
	defer p.close()

	accepted := make([]string, 0, 16)
	for i := 0; i < 16; i++ {
		text := string(rune('a' + i))
		err := p.client.TrySend(ogrenet.Text(text))
		switch {
		case err == nil:
			accepted = append(accepted, text)
		case errors.Is(err, ErrWouldBlock):
			continue
		default:
			t.Fatalf("TrySend(%d): %v", i, err)
		}
	}
	if len(accepted) == 0 {
		t.Fatal("no TCP messages accepted")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- p.clientEngine.Shutdown(ctx) }()

	serverHalf := requireHalfClose(t, p.server)
	waitClosed(t, serverHalf.ReadClosed(), "server read-half during Engine Shutdown")

	select {
	case err := <-result:
		t.Fatalf("Engine.Shutdown returned before peer write close: %v", err)
	default:
	}

	got := make([]string, 0, len(accepted))
	for len(got) < len(accepted) {
		select {
		case msg := <-p.serverMsgs:
			got = append(got, string(msg.Data))
		case <-ctx.Done():
			t.Fatalf("received %d/%d accepted messages: %v", len(got), len(accepted), ctx.Err())
		}
	}
	for i := range accepted {
		if got[i] != accepted[i] {
			t.Fatalf("message[%d] = %q, want %q", i, got[i], accepted[i])
		}
	}

	if err := serverHalf.CloseWrite(ctx); err != nil {
		t.Fatalf("server CloseWrite: %v", err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Engine.Shutdown = %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitClosed(t, p.clientEngine.Done(), "client Engine")
}

func TestEngineShutdownWaiterDeadlineDoesNotAbortOwner(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTCP)
	defer p.close()

	ownerCtx, ownerCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer ownerCancel()
	ownerResult := make(chan error, 1)
	go func() { ownerResult <- p.clientEngine.Shutdown(ownerCtx) }()

	serverHalf := requireHalfClose(t, p.server)
	waitClosed(t, serverHalf.ReadClosed(), "server read-half for owner shutdown")

	waiterCtx, waiterCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer waiterCancel()
	waiterErr := p.clientEngine.Shutdown(waiterCtx)
	if !errors.Is(waiterErr, context.DeadlineExceeded) {
		t.Fatalf("waiter Shutdown = %v, want context deadline", waiterErr)
	}

	select {
	case err := <-ownerResult:
		t.Fatalf("owner Shutdown was aborted by waiter: %v", err)
	default:
	}

	if err := serverHalf.CloseWrite(ownerCtx); err != nil {
		t.Fatalf("server CloseWrite: %v", err)
	}
	select {
	case err := <-ownerResult:
		if err != nil {
			t.Fatalf("owner Shutdown = %v", err)
		}
	case <-ownerCtx.Done():
		t.Fatal(ownerCtx.Err())
	}
}

func TestEngineShutdownOwnerDeadlineAbortsAndReleasesAccounting(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTCP)
	defer p.serverEngine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := p.clientEngine.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Engine.Shutdown = %v, want caller deadline", err)
	}
	waitClosed(t, p.clientEngine.Done(), "deadline-aborted Engine")
	waitClosed(t, p.client.Done(), "deadline-aborted Session")
	if err := p.client.Err(); err != nil {
		t.Fatalf("Session.Err after caller-owned Engine abort = %v, want nil", err)
	}
	snap := p.clientEngine.admissionSnapshot()
	if snap.OpeningConnections != 0 || snap.ActiveConnections != 0 || snap.DrainingConnections != 0 || snap.GlobalQueuedBytes != 0 {
		t.Fatalf("leaked accounting after owner deadline: %+v", snap)
	}
}

func TestEngineCloseInterruptsGracefulShutdown(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTCP)
	defer p.serverEngine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- p.clientEngine.Shutdown(ctx) }()
	waitClosed(t, requireHalfClose(t, p.server).ReadClosed(), "server read-half before Engine.Close")

	if err := p.clientEngine.Close(); err != nil {
		t.Fatalf("Engine.Close: %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Shutdown racing Close = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Engine.Close did not interrupt graceful Shutdown")
	}
	waitClosed(t, p.clientEngine.Done(), "explicitly aborted Engine")
}

func TestEngineShutdownIgnoresChildFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	server, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	accepted := make(chan ogrenet.Session, 2)
	ln, err := server.Listen(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, ogrenet.HandlerFuncs{
		Open: func(s ogrenet.Session) { accepted <- s },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	client, err := New()
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.Dial(ctx, ln.Endpoint(), ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.Dial(ctx, ln.Endpoint(), ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	serverA := <-accepted
	serverB := <-accepted
	serverFirst, serverSecond := pairAcceptedSessions(t, first, second, serverA, serverB)

	result := make(chan error, 1)
	go func() { result <- client.Shutdown(ctx) }()
	waitClosed(t, requireHalfClose(t, serverFirst).ReadClosed(), "first server read-half")
	waitClosed(t, requireHalfClose(t, serverSecond).ReadClosed(), "second server read-half")

	childErr := errors.New("synthetic child failure")
	first.(*conn).initiateClose(childErr)
	waitClosed(t, first.Done(), "failed child")
	if !errors.Is(first.Err(), childErr) {
		t.Fatalf("failed child Err = %v, want synthetic failure", first.Err())
	}

	if err := requireHalfClose(t, serverSecond).CloseWrite(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Engine.Shutdown surfaced child failure: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitClosed(t, second.Done(), "clean child")
	_ = serverFirst.Close()
}

func pairAcceptedSessions(t *testing.T, first, second, serverA, serverB ogrenet.Session) (ogrenet.Session, ogrenet.Session) {
	t.Helper()
	servers := []ogrenet.Session{serverA, serverB}
	var firstPeer, secondPeer ogrenet.Session
	for _, serverSession := range servers {
		remote := serverSession.RemoteAddr()
		if remote == nil {
			t.Fatalf("server session %T has nil remote address", serverSession)
		}
		switch remote.String() {
		case first.LocalAddr().String():
			firstPeer = serverSession
		case second.LocalAddr().String():
			secondPeer = serverSession
		}
	}
	if firstPeer == nil || secondPeer == nil {
		t.Fatalf("could not pair accepted sessions: first local=%v second local=%v serverA remote=%v serverB remote=%v", first.LocalAddr(), second.LocalAddr(), serverA.RemoteAddr(), serverB.RemoteAddr())
	}
	return firstPeer, secondPeer
}
