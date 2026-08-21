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
