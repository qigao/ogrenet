//go:build linux

package transport

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/wire"
)

func TestEpollNativeReadableEdgeDuringCodecSetupIsPreserved(t *testing.T) {
	setupEntered := make(chan struct{})
	setupRelease := make(chan struct{})
	var setupOnce sync.Once
	releaseSetup := func() { setupOnce.Do(func() { close(setupRelease) }) }
	t.Cleanup(releaseSetup)

	raw, err := NewEpoll(
		EpollConfig{Pollers: 1, CallbackWorkers: 1, CallbackQueue: 4},
		WithFramerFactory(func() wire.Framer {
			close(setupEntered)
			<-setupRelease
			return wire.New(nil)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	e := raw.(*epollEngine)
	t.Cleanup(func() {
		_ = e.Close()
		waitEpollEngineDone(t, e.Done())
	})

	opened := make(chan struct{})
	messages := make(chan ogrenet.Message, 1)
	ln, err := e.Listen(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeTCP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.HandlerFuncs{
		Open: func(ogrenet.Session) { close(opened) },
		Message: func(_ ogrenet.Session, msg ogrenet.Message) {
			messages <- msg
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	peer, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	waitEpollEngineSignal(t, setupEntered, "accepted Session codec setup entry")

	// Install the wait hook from the reactor itself so the test does not race
	// the reactor-owned hook field. The first arm means the blocked setup has
	// left the reactor idle in epoll_wait.
	armed := make(chan struct{}, 4)
	e.reactors[0].signal(newTestInboxItem(func(r *epollReactor) {
		r.testWaitArmed = func() {
			select {
			case armed <- struct{}{}:
			default:
			}
		}
	}))
	waitEpollEngineSignal(t, armed, "reactor wait before pre-open write")

	want := ogrenet.Text("pre-open-readable")
	frame, err := wire.New(nil).Encode(want)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Write(frame); err != nil {
		t.Fatal(err)
	}

	// With the codec factory still blocked, a second arm proves the reactor
	// returned from epoll_wait and dispatched the readable edge while the
	// Session was still in epollSessionCodecSetup.
	waitEpollEngineSignal(t, armed, "reactor wait after pre-open readable edge")
	releaseSetup()
	waitEpollEngineSignal(t, opened, "accepted Session OnOpen")

	select {
	case got := <-messages:
		if got.Type != want.Type || string(got.Data) != string(want.Data) {
			t.Fatalf("OnMessage=%+v, want=%+v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("pre-open readable edge was lost after codec setup completed")
	}
}
