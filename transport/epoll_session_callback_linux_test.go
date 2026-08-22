//go:build linux

package transport

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

type epollNativeOpenBarrierHandler struct {
	openEntered chan struct{}
	releaseOpen chan struct{}
	messages    chan ogrenet.Message
}

func (h *epollNativeOpenBarrierHandler) OnOpen(ogrenet.Session) {
	close(h.openEntered)
	<-h.releaseOpen
}

func (h *epollNativeOpenBarrierHandler) OnMessage(_ ogrenet.Session, msg ogrenet.Message) {
	h.messages <- msg
}

func (h *epollNativeOpenBarrierHandler) OnClose(ogrenet.Session, error) {}

func TestEpollNativeOnOpenPrecedesFirstDecodedMessage(t *testing.T) {
	h := &epollNativeOpenBarrierHandler{
		openEntered: make(chan struct{}),
		releaseOpen: make(chan struct{}),
		messages:    make(chan ogrenet.Message, 1),
	}
	_, _, peer := newEpollNativeReadSession(t, h)
	waitNativeSendSignal(t, h.openEntered, "native OnOpen entry")

	want := epollNativeMessage([]byte("after-open"))
	if _, err := peer.Write(encodedNativeFrame(t, want)); err != nil {
		t.Fatalf("peer Write: %v", err)
	}

	select {
	case got := <-h.messages:
		t.Fatalf("OnMessage entered before OnOpen release: %+v", got)
	case <-time.After(100 * time.Millisecond):
	}
	close(h.releaseOpen)

	select {
	case got := <-h.messages:
		if got.Type != want.Type || string(got.Data) != string(want.Data) {
			t.Fatalf("OnMessage got=%+v, want=%+v", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnMessage did not run after OnOpen release")
	}
}

type epollNativeSerializedHandler struct {
	inCallback atomic.Bool
	overlapped atomic.Bool
	count      atomic.Int32
	done       chan struct{}
	doneOnce   sync.Once
}

func (h *epollNativeSerializedHandler) OnOpen(ogrenet.Session) {}

func (h *epollNativeSerializedHandler) OnMessage(_ ogrenet.Session, _ ogrenet.Message) {
	if !h.inCallback.CompareAndSwap(false, true) {
		h.overlapped.Store(true)
	}
	time.Sleep(time.Millisecond)
	if h.count.Add(1) == 100 {
		h.doneOnce.Do(func() { close(h.done) })
	}
	h.inCallback.Store(false)
}

func (h *epollNativeSerializedHandler) OnClose(ogrenet.Session, error) {}

func TestEpollNativeOneSessionCallbacksNeverOverlap(t *testing.T) {
	h := &epollNativeSerializedHandler{done: make(chan struct{})}
	_, _, peer := newEpollNativeReadSession(t, h)

	frames := make([]byte, 0, 4096)
	for i := 0; i < 100; i++ {
		frames = append(frames, encodedNativeFrame(t, epollNativeMessage([]byte{byte(i)}))...)
	}
	if err := writeAll(peer, frames, nil); err != nil {
		t.Fatalf("peer write frames: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	select {
	case <-h.done:
	case <-ctx.Done():
		t.Fatalf("waiting for 100 native callbacks: %v", context.Cause(ctx))
	}
	if h.overlapped.Load() {
		t.Fatal("same-session OnMessage callbacks overlapped")
	}
	if got := h.count.Load(); got != 100 {
		t.Fatalf("callback count=%d, want 100", got)
	}
}

type epollNativeCloseBarrierHandler struct {
	closeEntered chan struct{}
	releaseClose chan struct{}
}

func (h *epollNativeCloseBarrierHandler) OnOpen(ogrenet.Session) {}
func (h *epollNativeCloseBarrierHandler) OnMessage(ogrenet.Session, ogrenet.Message) {
}
func (h *epollNativeCloseBarrierHandler) OnClose(ogrenet.Session, error) {
	close(h.closeEntered)
	<-h.releaseClose
}

func TestEpollNativeDoneWaitsForOnClose(t *testing.T) {
	h := &epollNativeCloseBarrierHandler{
		closeEntered: make(chan struct{}),
		releaseClose: make(chan struct{}),
	}
	_, s, _ := newEpollNativeReadSession(t, h)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitNativeSendSignal(t, h.closeEntered, "native OnClose entry")

	select {
	case <-s.Done():
		t.Fatal("Done closed before OnClose returned")
	default:
	}
	close(h.releaseClose)
	waitNativeSendSignal(t, s.Done(), "native Done after OnClose")
}
