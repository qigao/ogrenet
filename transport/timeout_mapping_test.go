package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestMapOperationTimeoutConnectRuntimeTimeout(t *testing.T) {
	parent := context.Background()
	op, cancel := context.WithTimeout(parent, time.Nanosecond)
	defer cancel()
	<-op.Done()

	root := errors.New("dial failed")
	err := mapOperationTimeout(parent, op, TimeoutConnect, root)
	var te *TimeoutError
	if !errors.As(err, &te) || te.Kind != TimeoutConnect {
		t.Fatalf("mapOperationTimeout = %#v, want TimeoutConnect", err)
	}
	if !errors.Is(err, root) {
		t.Fatalf("mapped timeout lost root cause: %v", err)
	}
}

func TestMapOperationTimeoutConnectCallerCancelWins(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	op, opCancel := context.WithCancel(parent)
	defer opCancel()

	err := mapOperationTimeout(parent, op, TimeoutConnect, errors.New("dial failed"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("mapOperationTimeout = %#v, want context.Canceled", err)
	}
	var te *TimeoutError
	if errors.As(err, &te) {
		t.Fatalf("caller cancellation was wrapped as runtime timeout: %#v", te)
	}
}

func TestWebSocketWriteTimeoutIsTyped(t *testing.T) {
	listener, closeServer := startTimeoutWSServer(t, ogrenet.SchemeWS, false)
	defer closeServer()

	client, err := New(WithTimeouts(Timeouts{Write: time.Nanosecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := client.Dial(ctx, listener.Endpoint(), ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	sendCtx, sendCancel := context.WithTimeout(context.Background(), time.Second)
	err = s.Send(sendCtx, ogrenet.Text("timeout"))
	sendCancel()
	var te *TimeoutError
	if !errors.As(err, &te) || te.Kind != TimeoutWrite {
		t.Fatalf("Send error = %#v, want TimeoutWrite", err)
	}
	waitSessionTimeoutKind(t, s, TimeoutWrite)
}
