//go:build linux

package transport

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestEpollNativeEngineShutdownDrainsAdmittedPacketBeforeClose(t *testing.T) {
	events := make(chan ogrenet.Event, 16)
	e, _, client := newEpollPacketPair(t,
		WithWriteQueue(2),
		WithMaxQueuedBytes(128),
		WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) {
			if event.Resource != ogrenet.ResourcePacketConn {
				return
			}
			select {
			case events <- event:
			default:
			}
		})),
	)

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	client.reactor.signal(newTestInboxItem(func(*epollReactor) {
		close(entered)
		<-release
	}))
	waitNativeSendSignal(t, entered, "packet reactor blocker before Engine Shutdown")

	first := ogrenet.Packet{Data: []byte("admitted-before-drain")}
	if err := client.TrySend(first); err != nil {
		t.Fatalf("first TrySend: %v", err)
	}
	stats := client.Stats()
	if stats.QueuedPackets != 1 || stats.QueuedBytes != uint64(len(first.Data)) {
		t.Fatalf("pre-drain packet ownership=%+v, want one admitted datagram", stats)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	shutdownResult := make(chan error, 1)
	go func() { shutdownResult <- e.Shutdown(ctx) }()

	waitEpollEngineCondition(t, "Engine to enter draining for UDP", func() bool {
		state, reason := epollEngineLifecycleState(e)
		return state == engineDraining && reason == abortNone
	})
	waitEpollEngineCondition(t, "UDP send gate to stop new admission", func() bool {
		return isClosedSignal(client.gate.done())
	})

	if err := client.TrySend(ogrenet.Packet{Data: []byte("rejected-after-drain")}); !errors.Is(err, ErrClosed) {
		t.Fatalf("TrySend after Engine drain started=%v, want ErrClosed", err)
	}
	stats = client.Stats()
	if stats.QueuedPackets != 1 || stats.QueuedBytes != uint64(len(first.Data)) {
		t.Fatalf("post-drain rejection changed admitted ownership: %+v", stats)
	}

	releaseOnce.Do(func() { close(release) })

	seenWrite := false
	for {
		select {
		case event := <-events:
			if event.ResourceID != client.id {
				continue
			}
			switch event.Kind {
			case ogrenet.EventWrite:
				if event.Err != nil || event.Bytes != uint64(len(first.Data)) {
					t.Fatalf("admitted datagram EventWrite=%+v", event)
				}
				seenWrite = true
			case ogrenet.EventClose:
				if !seenWrite {
					t.Fatal("PacketConn closed before pre-drain admitted datagram was physically written")
				}
				if event.Err != nil {
					t.Fatalf("graceful UDP EventClose=%+v, want clean close", event)
				}
				goto observedClose
			}
		case <-ctx.Done():
			t.Fatalf("waiting for graceful UDP write/close ordering: %v", context.Cause(ctx))
		}
	}

observedClose:
	waitEpollPacketDone(t, client.Done(), "UDP PacketConn Done after graceful Engine drain")
	if err := client.Err(); err != nil {
		t.Fatalf("gracefully drained PacketConn Err=%v, want nil", err)
	}
	select {
	case err := <-shutdownResult:
		if err != nil {
			t.Fatalf("Engine.Shutdown=%v", err)
		}
	case <-ctx.Done():
		t.Fatalf("waiting for Engine.Shutdown after UDP drain: %v", context.Cause(ctx))
	}
	waitEpollEngineSignal(t, e.Done(), "Engine.Done after UDP graceful drain")
	stats = client.Stats()
	if stats.QueuedPackets != 0 || stats.QueuedBytes != 0 {
		t.Fatalf("final PacketConn queue ownership=%+v", stats)
	}
	if got := e.Stats().GlobalQueuedBytes; got != 0 {
		t.Fatalf("final Engine GlobalQueuedBytes=%d, want 0", got)
	}
	assertEpollEngineZeroInvariants(t, e)
}
