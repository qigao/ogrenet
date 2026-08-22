//go:build linux

package transport

import (
	"context"
	"net"
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

func newEpollNativeReadSession(t *testing.T, handler ogrenet.Handler, opts ...Option) (*epollEngine, *epollSession, net.Conn) {
	t.Helper()
	_, endpoint, accepted := newNativeDialTarget(t)
	e := newEpollTestEngine(t, 1, opts...)
	s, err := e.dialNativeTCP(context.Background(), endpoint, handler, nil)
	if err != nil {
		t.Fatal(err)
	}
	peer := waitNativeDialAccepted(t, accepted)
	return e, s, peer
}

func TestEpollNativeReadDecodesFrameAndDeliversMessage(t *testing.T) {
	h := &epollNativeReadHandler{messages: make(chan ogrenet.Message, 1)}
	_, _, peer := newEpollNativeReadSession(t, h)

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
