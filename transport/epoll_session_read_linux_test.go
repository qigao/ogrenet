//go:build linux

package transport

import (
	"context"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

type epollNativeReadHandler struct {
	messages chan ogrenet.Message
}

func (h *epollNativeReadHandler) OnOpen(ogrenet.Session) {}
func (h *epollNativeReadHandler) OnMessage(_ ogrenet.Session, msg ogrenet.Message) {
	h.messages <- msg
}
func (h *epollNativeReadHandler) OnClose(ogrenet.Session, error) {}

func TestEpollNativeReadDecodesFrameAndDeliversMessage(t *testing.T) {
	_, s, peer := newEpollNativeSendSession(t)
	h := &epollNativeReadHandler{messages: make(chan ogrenet.Message, 1)}
	s.handler = h

	want := epollNativeMessage([]byte("native-read"))
	frame := encodedNativeFrame(t, want)
	if _, err := peer.Write(frame); err != nil {
		t.Fatalf("peer Write: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case got := <-h.messages:
		if got.Type != want.Type || string(got.Data) != string(want.Data) {
			t.Fatalf("OnMessage got=%+v, want=%+v", got, want)
		}
	case <-ctx.Done():
		t.Fatalf("waiting for native OnMessage: %v", context.Cause(ctx))
	}
}
