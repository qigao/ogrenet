package transport

import (
	"context"
	"errors"
	"testing"
	"time"
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

func TestMapWebSocketWriteErrorTimeout(t *testing.T) {
	err := mapWebSocketWriteError(context.DeadlineExceeded, false)
	var te *TimeoutError
	if !errors.As(err, &te) || te.Kind != TimeoutWrite {
		t.Fatalf("mapWebSocketWriteError = %#v, want TimeoutWrite", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("mapped write timeout lost root cause: %v", err)
	}
}

func TestMapWebSocketWriteErrorNonTimeoutPreservesCause(t *testing.T) {
	root := errors.New("write failed")
	err := mapWebSocketWriteError(root, false)
	if !errors.Is(err, root) {
		t.Fatalf("mapped websocket write lost root cause: %v", err)
	}
	var te *TimeoutError
	if errors.As(err, &te) {
		t.Fatalf("non-timeout write was classified as timeout: %#v", te)
	}
}
