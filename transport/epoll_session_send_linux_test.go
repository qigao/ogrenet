//go:build linux

package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/wire"
)

func newEpollNativeSendSession(t *testing.T, opts ...Option) (*epollEngine, *epollSession, net.Conn) {
	t.Helper()
	_, endpoint, accepted := newNativeDialTarget(t)
	e := newEpollTestEngine(t, 1, opts...)
	s, err := e.dialNativeTCP(context.Background(), endpoint, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	peer := waitNativeDialAccepted(t, accepted)
	return e, s, peer
}

func epollNativeMessage(payload []byte) ogrenet.Message {
	return ogrenet.Message{Type: ogrenet.PayloadBinary, Data: payload}
}

func encodedNativeFrame(t *testing.T, msg ogrenet.Message) []byte {
	t.Helper()
	frame, err := wire.New(nil).Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func readNativeFrame(t *testing.T, peer net.Conn, want []byte) {
	t.Helper()
	if err := peer.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(peer, got); err != nil {
		t.Fatalf("read native frame: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("native frame mismatch: got=%d bytes want=%d", len(got), len(want))
	}
}

func waitNativeSendSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", what, context.Cause(ctx))
	}
}

func waitNativeSendResult(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", what, context.Cause(ctx))
		return nil
	}
}

func observerEventsThroughBarrier(t *testing.T, e *epollEngine, o *epollTestObserver) []ogrenet.Event {
	t.Helper()
	const barrierKind = ogrenet.EventKind(255)
	const barrierID = ^uint64(0)
	if !e.observer.emit(ogrenet.Event{Kind: barrierKind, ResourceID: barrierID}) {
		t.Fatal("observer barrier was dropped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var events []ogrenet.Event
	for {
		select {
		case event := <-o.events:
			if event.Kind == barrierKind && event.ResourceID == barrierID {
				return events
			}
			events = append(events, event)
		case <-ctx.Done():
			t.Fatalf("waiting for observer barrier: %v", context.Cause(ctx))
		}
	}
}

func countEpollEventKind(events []ogrenet.Event, kind ogrenet.EventKind) int {
	count := 0
	for _, event := range events {
		if event.Kind == kind {
			count++
		}
	}
	return count
}

func requireNativeOp(t *testing.T, err error, op Op) *Error {
	t.Helper()
	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatalf("error type=%T err=%v, want *transport.Error", err, err)
	}
	if typed.Op != op || typed.Protocol != ogrenet.SchemeTCP {
		t.Fatalf("typed error=%+v, want op=%v protocol=tcp", typed, op)
	}
	return typed
}

func TestEpollNativeTrySendCodecContentionIsTypedBackpressureOnce(t *testing.T) {
	observer := newEpollTestObserver()
	e, s, _ := newEpollNativeSendSession(t, WithObserver(observer))
	_ = waitEpollEvent(t, observer, ogrenet.EventConnect)

	s.codecSlot <- struct{}{}
	defer func() { <-s.codecSlot }()

	err := s.TrySend(epollNativeMessage([]byte("codec-contention")))
	if !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("TrySend err=%v, want ErrWouldBlock", err)
	}
	typed := requireNativeOp(t, err, OpSend)
	if typed.Kind != ErrorBackpressure {
		t.Fatalf("TrySend kind=%v, want backpressure", typed.Kind)
	}
	if got := s.stats.backpressure.Load(); got != 1 {
		t.Fatalf("backpressure=%d, want 1", got)
	}
	events := observerEventsThroughBarrier(t, e, observer)
	if got := countEpollEventKind(events, ogrenet.EventBackpressure); got != 1 {
		t.Fatalf("backpressure events=%d, want 1: %+v", got, events)
	}
}

func TestEpollNativeTrySendQueuePressureIsCountedOnce(t *testing.T) {
	observer := newEpollTestObserver()
	e, s, peer := newEpollNativeSendSession(t, WithObserver(observer), WithWriteQueue(1))
	_ = waitEpollEvent(t, observer, ogrenet.EventConnect)

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	blocker := newTestInboxItem(func(*epollReactor) {
		close(entered)
		<-release
	})
	s.reactor.signal(blocker)
	waitNativeSendSignal(t, entered, "reactor blocker")

	first := epollNativeMessage([]byte("first"))
	if err := s.TrySend(first); err != nil {
		t.Fatalf("first TrySend: %v", err)
	}
	second := epollNativeMessage([]byte("second"))
	err := s.TrySend(second)
	if !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("second TrySend err=%v, want ErrWouldBlock", err)
	}
	typed := requireNativeOp(t, err, OpSend)
	if typed.Kind != ErrorBackpressure {
		t.Fatalf("second TrySend kind=%v, want backpressure", typed.Kind)
	}
	if got := s.stats.backpressure.Load(); got != 1 {
		t.Fatalf("backpressure=%d, want 1", got)
	}

	releaseOnce.Do(func() { close(release) })
	readNativeFrame(t, peer, encodedNativeFrame(t, first))
	events := observerEventsThroughBarrier(t, e, observer)
	if got := countEpollEventKind(events, ogrenet.EventBackpressure); got != 1 {
		t.Fatalf("backpressure events=%d, want 1: %+v", got, events)
	}
}

func TestEpollNativeSendCancellationAfterQueueTransferKeepsFrameOwned(t *testing.T) {
	observer := newEpollTestObserver()
	_, s, peer := newEpollNativeSendSession(t, WithObserver(observer))
	_ = waitEpollEvent(t, observer, ogrenet.EventConnect)

	transferred := make(chan struct{})
	release := make(chan struct{})
	s.testAfterSendQueueTransfer = func() {
		close(transferred)
		<-release
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	callerCause := errors.New("caller canceled after native queue transfer")
	msg := epollNativeMessage([]byte("admitted-frame-survives-cancel"))
	result := make(chan error, 1)
	go func() { result <- s.Send(ctx, msg) }()

	waitNativeSendSignal(t, transferred, "native queue transfer")
	cancel(callerCause)
	close(release)
	if err := waitNativeSendResult(t, result, "canceled Send"); err != callerCause {
		t.Fatalf("Send err=%v, want exact caller cause %v", err, callerCause)
	}

	readNativeFrame(t, peer, encodedNativeFrame(t, msg))
	event := waitEpollEvent(t, observer, ogrenet.EventWrite)
	if event.Err != nil || event.Bytes != uint64(len(msg.Data)) {
		t.Fatalf("write event=%+v", event)
	}
	if got := s.stats.messagesTX.Load(); got != 1 {
		t.Fatalf("messagesTX=%d, want 1", got)
	}
	if got := s.stats.bytesTX.Load(); got != uint64(len(msg.Data)) {
		t.Fatalf("bytesTX=%d, want %d", got, len(msg.Data))
	}
}

func TestEpollNativePartialWriteRetainsAdmissionAndYieldsReactor(t *testing.T) {
	observer := newEpollTestObserver()
	_, s, peer := newEpollNativeSendSession(t,
		WithObserver(observer),
		WithTCPConfig(TCPConfig{NoDelay: true, WriteBufferBytes: 4096}),
		WithTimeouts(Timeouts{Write: 3 * time.Second}),
	)
	_ = waitEpollEvent(t, observer, ogrenet.EventConnect)

	payload := bytes.Repeat([]byte{0x5a}, 8<<20)
	msg := epollNativeMessage(payload)
	progress := make(chan struct{})
	var progressOnce sync.Once
	var firstGeneration uint64
	var generationChanged atomic.Bool
	var progressCalls atomic.Int32
	s.testAfterWriteProgress = func(session *epollSession) {
		progressCalls.Add(1)
		if firstGeneration == 0 {
			firstGeneration = session.writeGen
		} else if session.writeGen != firstGeneration {
			generationChanged.Store(true)
		}
		progressOnce.Do(func() { close(progress) })
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.Send(ctx, msg) }()
	waitNativeSendSignal(t, progress, "first native partial write")

	if got := s.quota.current(); got <= 0 {
		t.Fatalf("quota released during partial write: %d", got)
	}
	if got := len(s.frameSlots); got != 1 {
		t.Fatalf("frame slots during partial write=%d, want 1", got)
	}

	independent := make(chan struct{})
	s.reactor.signal(newTestInboxItem(func(*epollReactor) { close(independent) }))
	waitNativeSendSignal(t, independent, "same-reactor independent item")

	readNativeFrame(t, peer, encodedNativeFrame(t, msg))
	if err := waitNativeSendResult(t, result, "partial Send completion"); err != nil {
		t.Fatalf("Send: %v", err)
	}
	_ = waitEpollEvent(t, observer, ogrenet.EventWrite)
	if got := s.quota.current(); got != 0 {
		t.Fatalf("quota after write=%d, want 0", got)
	}
	if got := len(s.frameSlots); got != 0 {
		t.Fatalf("frame slots after write=%d, want 0", got)
	}
	if progressCalls.Load() < 2 {
		t.Fatalf("partial progress calls=%d, want at least 2", progressCalls.Load())
	}
	if generationChanged.Load() {
		t.Fatal("write deadline generation changed across partial progress")
	}
	if got := s.stats.messagesTX.Load(); got != 1 {
		t.Fatalf("messagesTX=%d, want 1", got)
	}
}

func TestEpollNativeWriteDeadlineFailsBlockedFrameAndReleasesAdmission(t *testing.T) {
	_, s, peer := newEpollNativeSendSession(t,
		WithTCPConfig(TCPConfig{NoDelay: true, WriteBufferBytes: 4096}),
		WithTimeouts(Timeouts{Write: 150 * time.Millisecond}),
	)

	progress := make(chan struct{})
	var progressOnce sync.Once
	s.testAfterWriteProgress = func(*epollSession) {
		progressOnce.Do(func() { close(progress) })
	}
	msg := epollNativeMessage(bytes.Repeat([]byte{0x33}, 8<<20))
	result := make(chan error, 1)
	go func() { result <- s.Send(context.Background(), msg) }()
	waitNativeSendSignal(t, progress, "write progress before fixed deadline")

	err := waitNativeSendResult(t, result, "fixed write deadline")
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("Send err=%v, want ErrTimeout", err)
	}
	typed := requireNativeOp(t, err, OpWrite)
	if typed.Kind != ErrorTimeout {
		t.Fatalf("write error kind=%v, want timeout", typed.Kind)
	}
	var timeoutErr *TimeoutError
	if !errors.As(err, &timeoutErr) || timeoutErr.Kind != TimeoutWrite {
		t.Fatalf("timeout error=%T %v, want TimeoutWrite", err, err)
	}
	if got := s.quota.current(); got != 0 {
		t.Fatalf("quota after timeout=%d, want 0", got)
	}
	if got := len(s.frameSlots); got != 0 {
		t.Fatalf("frame slots after timeout=%d, want 0", got)
	}
	waitPeerClosed(t, peer)
}
