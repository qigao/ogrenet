package transport

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestWebSocketShutdownDrainsAcceptedMessagesBeforeClose(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeWS)
	defer p.close()

	graceful := requireGracefulShutdown(t, p.client)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	accepted := make([]string, 0, 32)
	for i := 0; i < 32; i++ {
		text := fmt.Sprintf("ws-%02d", i)
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
		t.Fatal("no WebSocket messages accepted")
	}

	if err := graceful.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := p.client.Send(ctx, ogrenet.Text("late")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Send after Shutdown = %v, want ErrClosed", err)
	}

	got := make([]string, 0, len(accepted))
	for len(got) < len(accepted) {
		select {
		case msg := <-p.serverMsgs:
			got = append(got, string(msg.Data))
		case <-ctx.Done():
			t.Fatalf("received %d/%d messages: %v", len(got), len(accepted), ctx.Err())
		}
	}
	if !reflect.DeepEqual(got, accepted) {
		t.Fatalf("drain order = %v, want %v", got, accepted)
	}
	waitClosed(t, p.client.Done(), "client WebSocket session")
	if err := p.client.Err(); err != nil {
		t.Fatalf("client Err = %v, want nil", err)
	}
}
