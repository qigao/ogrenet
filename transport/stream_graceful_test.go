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

func TestTCPPeerFINClosesReadHalfButKeepsWriteOpen(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTCP)
	defer p.close()

	serverHalf := requireHalfClose(t, p.server)
	clientHalf := requireHalfClose(t, p.client)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := serverHalf.CloseWrite(ctx); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, clientHalf.ReadClosed(), "client read half")
	assertStillOpen(t, p.client.Done(), "client session")

	want := ogrenet.Text("response-after-fin")
	if err := p.client.Send(ctx, want); err != nil {
		t.Fatalf("send after peer FIN: %v", err)
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

func TestTCPCloseWriteDrainsAcceptedFramesBeforeFIN(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTCP)
	defer p.close()

	clientHalf := requireHalfClose(t, p.client)
	serverHalf := requireHalfClose(t, p.server)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	accepted := make([]string, 0, 64)
	for i := 0; i < 64; i++ {
		text := fmt.Sprintf("msg-%02d", i)
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
		t.Fatal("no messages accepted")
	}

	if err := clientHalf.CloseWrite(ctx); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	if err := p.client.Send(ctx, ogrenet.Text("late")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Send after CloseWrite = %v, want ErrClosed", err)
	}
	if err := p.client.TrySend(ogrenet.Text("late")); !errors.Is(err, ErrClosed) {
		t.Fatalf("TrySend after CloseWrite = %v, want ErrClosed", err)
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
	waitClosed(t, serverHalf.ReadClosed(), "server read half")
	if !reflect.DeepEqual(got, accepted) {
		t.Fatalf("drain order = %v, want %v", got, accepted)
	}
}

func TestTCPShutdownWaitsForPeerFIN(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTCP)
	defer p.close()

	clientGraceful := requireGracefulShutdown(t, p.client)
	clientHalf := requireHalfClose(t, p.client)
	serverHalf := requireHalfClose(t, p.server)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result := make(chan error, 1)
	go func() { result <- clientGraceful.Shutdown(ctx) }()

	waitClosed(t, serverHalf.ReadClosed(), "server read half")
	assertStillOpen(t, clientHalf.ReadClosed(), "client read half")
	select {
	case err := <-result:
		t.Fatalf("Shutdown returned before peer FIN: %v", err)
	default:
	}

	if err := serverHalf.CloseWrite(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitClosed(t, p.client.Done(), "client session")
	if err := p.client.Err(); err != nil {
		t.Fatalf("client Err = %v, want nil", err)
	}
}
