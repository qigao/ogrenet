//go:build linux

package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func newEpollNativeReadEngine(t *testing.T, cfg EpollConfig, opts ...Option) *epollEngine {
	t.Helper()
	raw, err := NewEpoll(cfg, opts...)
	if err != nil {
		t.Fatal(err)
	}
	e := raw.(*epollEngine)
	t.Cleanup(func() {
		_ = e.Close()
		waitEpollEngineDone(t, e.Done())
	})
	return e
}

func dialEpollNativeReadSession(t *testing.T, e *epollEngine, h ogrenet.Handler) (*epollSession, net.Conn) {
	t.Helper()
	_, endpoint, accepted := newNativeDialTarget(t)
	s, err := e.dialNativeTCP(context.Background(), endpoint, h, nil)
	if err != nil {
		t.Fatal(err)
	}
	peer := waitNativeDialAccepted(t, accepted)
	t.Cleanup(func() { _ = peer.Close() })
	return s, peer
}

func writeNativeTestAll(t *testing.T, conn net.Conn, data []byte) {
	t.Helper()
	for len(data) != 0 {
		n, err := conn.Write(data)
		if err != nil {
			t.Fatalf("peer Write: %v", err)
		}
		data = data[n:]
	}
}

type epollNativeRetainHandler struct {
	first    chan struct{}
	done     chan struct{}
	retained ogrenet.Message
	count    int
}

func (h *epollNativeRetainHandler) OnOpen(ogrenet.Session) {}
func (h *epollNativeRetainHandler) OnClose(ogrenet.Session, error) {}
func (h *epollNativeRetainHandler) OnMessage(_ ogrenet.Session, msg ogrenet.Message) {
	h.count++
	if h.count == 1 {
		h.retained = msg
		close(h.first)
	}
	if h.count == 65 {
		close(h.done)
	}
}

func TestEpollNativeDecodedMessageOwnsRetainedBytes(t *testing.T) {
	h := &epollNativeRetainHandler{first: make(chan struct{}), done: make(chan struct{})}
	_, _, peer := newEpollNativeReadSession(t, h)

	firstPayload := bytes.Repeat([]byte("retained-"), 64)
	writeNativeTestAll(t, peer, encodedNativeFrame(t, epollNativeMessage(firstPayload)))
	waitNativeSendSignal(t, h.first, "first retained native message")

	var later []byte
	for i := 0; i < 64; i++ {
		payload := bytes.Repeat([]byte{byte(i + 1)}, 257)
		later = append(later, encodedNativeFrame(t, epollNativeMessage(payload))...)
	}
	writeNativeTestAll(t, peer, later)
	waitNativeSendSignal(t, h.done, "later native messages")
	if !bytes.Equal(h.retained.Data, firstPayload) {
		t.Fatalf("retained message mutated: got %q want %q", h.retained.Data, firstPayload)
	}
}

type epollNativeStatsObserverHandler struct {
	readObserved <-chan ogrenet.Event
	result       chan error
}

func (h *epollNativeStatsObserverHandler) OnOpen(ogrenet.Session) {}
func (h *epollNativeStatsObserverHandler) OnClose(ogrenet.Session, error) {}
func (h *epollNativeStatsObserverHandler) OnMessage(s ogrenet.Session, msg ogrenet.Message) {
	stats := s.Stats()
	if stats.MessagesRX != 1 || stats.BytesRX != uint64(len(msg.Data)) {
		h.result <- fmt.Errorf("RX stats before handler = (%d messages, %d bytes), want (1, %d)", stats.MessagesRX, stats.BytesRX, len(msg.Data))
		return
	}
	select {
	case event := <-h.readObserved:
		if event.Kind != ogrenet.EventRead || event.ResourceID != s.ID() || event.Bytes != uint64(len(msg.Data)) {
			h.result <- fmt.Errorf("read event = %+v", event)
			return
		}
		h.result <- nil
	case <-time.After(time.Second):
		h.result <- fmt.Errorf("EventRead was not observed during OnMessage")
	}
}

func TestEpollNativeStatsAndObserverReadPrecedeHandlerCompletion(t *testing.T) {
	observed := make(chan ogrenet.Event, 1)
	e := newEpollNativeReadEngine(t, EpollConfig{Pollers: 1, CallbackWorkers: 1, CallbackQueue: 4}, WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) {
		if event.Kind == ogrenet.EventRead {
			select {
			case observed <- event:
			default:
			}
		}
	})))
	h := &epollNativeStatsObserverHandler{readObserved: observed, result: make(chan error, 1)}
	_, peer := dialEpollNativeReadSession(t, e, h)
	payload := []byte("stats-observer-before-handler")
	writeNativeTestAll(t, peer, encodedNativeFrame(t, epollNativeMessage(payload)))
	select {
	case err := <-h.result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnMessage did not complete")
	}
}

type epollNativeBlockingMessageHandler struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *epollNativeBlockingMessageHandler) OnOpen(ogrenet.Session) {}
func (h *epollNativeBlockingMessageHandler) OnClose(ogrenet.Session, error) {}
func (h *epollNativeBlockingMessageHandler) OnMessage(ogrenet.Session, ogrenet.Message) {
	h.once.Do(func() { close(h.entered) })
	<-h.release
}

type epollNativeEchoHandler struct {
	result chan error
}

func (h *epollNativeEchoHandler) OnOpen(ogrenet.Session) {}
func (h *epollNativeEchoHandler) OnClose(ogrenet.Session, error) {}
func (h *epollNativeEchoHandler) OnMessage(s ogrenet.Session, msg ogrenet.Message) {
	h.result <- s.Send(context.Background(), msg)
}

func TestEpollNativeBlockedHandlerDoesNotBlockSameReactorSession(t *testing.T) {
	e := newEpollNativeReadEngine(t, EpollConfig{Pollers: 1, CallbackWorkers: 2, CallbackQueue: 4})
	blocker := &epollNativeBlockingMessageHandler{entered: make(chan struct{}), release: make(chan struct{})}
	_, peerA := dialEpollNativeReadSession(t, e, blocker)
	echo := &epollNativeEchoHandler{result: make(chan error, 1)}
	_, peerB := dialEpollNativeReadSession(t, e, echo)

	writeNativeTestAll(t, peerA, encodedNativeFrame(t, epollNativeMessage([]byte("block-A"))))
	waitNativeSendSignal(t, blocker.entered, "blocking Session A handler")

	want := epollNativeMessage([]byte("echo-B"))
	frame := encodedNativeFrame(t, want)
	writeNativeTestAll(t, peerB, frame)
	if err := peerB.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(frame))
	if _, err := io.ReadFull(peerB, got); err != nil {
		t.Fatalf("Session B echo while Session A blocked: %v", err)
	}
	if !bytes.Equal(got, frame) {
		t.Fatalf("Session B echo = %x, want %x", got, frame)
	}
	select {
	case err := <-echo.result:
		if err != nil {
			t.Fatalf("Session B Send: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Session B handler did not complete")
	}
	close(blocker.release)
}

type epollNativeBlockingWorkerTask struct {
	entered chan struct{}
	release chan struct{}
}

func (t *epollNativeBlockingWorkerTask) runEpollWorkerTask() {
	close(t.entered)
	<-t.release
}

type epollNativeOpenMessageHandler struct {
	opened   chan struct{}
	messages chan ogrenet.Message
	once     sync.Once
}

func (h *epollNativeOpenMessageHandler) OnOpen(ogrenet.Session) {
	h.once.Do(func() { close(h.opened) })
}
func (h *epollNativeOpenMessageHandler) OnClose(ogrenet.Session, error) {}
func (h *epollNativeOpenMessageHandler) OnMessage(_ ogrenet.Session, msg ogrenet.Message) {
	h.messages <- msg
}

func TestEpollNativeCallbackCapacityReleaseRetriesReadableSession(t *testing.T) {
	e := newEpollNativeReadEngine(t, EpollConfig{Pollers: 1, CallbackWorkers: 1, CallbackQueue: 1})
	h := &epollNativeOpenMessageHandler{opened: make(chan struct{}), messages: make(chan ogrenet.Message, 1)}
	s, peer := dialEpollNativeReadSession(t, e, h)
	waitNativeSendSignal(t, h.opened, "native OnOpen before worker saturation")

	first := &epollNativeBlockingWorkerTask{entered: make(chan struct{}), release: make(chan struct{})}
	second := &epollNativeBlockingWorkerTask{entered: make(chan struct{}), release: make(chan struct{})}
	if !e.callbacks.tryReserve() {
		t.Fatal("reserve running worker task")
	}
	e.callbacks.submitReserved(first)
	waitNativeSendSignal(t, first.entered, "running worker task")
	if !e.callbacks.tryReserve() {
		t.Fatal("reserve queued worker task")
	}
	e.callbacks.submitReserved(second)
	if got := e.callbacks.queuedCount(); got != 1 {
		t.Fatalf("queued worker tasks=%d, want 1", got)
	}

	want := epollNativeMessage([]byte("retry-after-capacity"))
	writeNativeTestAll(t, peer, encodedNativeFrame(t, want))
	deadline := time.Now().Add(2 * time.Second)
	for !s.reactor.hasWorkerBlocked.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !s.reactor.hasWorkerBlocked.Load() {
		t.Fatal("readable Session never entered workerBlocked")
	}

	close(first.release)
	waitNativeSendSignal(t, second.entered, "queued worker task promotion")
	close(second.release)

	select {
	case got := <-h.messages:
		if got.Type != want.Type || !bytes.Equal(got.Data, want.Data) {
			t.Fatalf("retried callback got=%+v, want=%+v", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker capacity release did not retry readable Session")
	}
}
