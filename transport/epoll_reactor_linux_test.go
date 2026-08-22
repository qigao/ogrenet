//go:build linux

package transport

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qigao/ogrenet/epoll"
)

type testInboxItem struct {
	node  epollInboxNode
	calls atomic.Int32
	run   func(*epollReactor)
}

func newTestInboxItem(run func(*epollReactor)) *testInboxItem {
	x := &testInboxItem{run: run}
	x.node.owner = x
	return x
}

func (x *testInboxItem) inboxNode() *epollInboxNode { return &x.node }

func (x *testInboxItem) onReactorInbox(r *epollReactor) {
	x.calls.Add(1)
	if x.run != nil {
		x.run(r)
	}
}

func newTestEpollReactor(t *testing.T) *epollReactor {
	t.Helper()
	p, err := epoll.Open()
	if err != nil {
		t.Fatal(err)
	}
	r := &epollReactor{
		cfg:       resolvedEpollConfig{eventBatch: 8, ioBudgetBytes: 1024, ioBudgetOps: 8},
		poller:    p,
		events:    make([]epoll.Event, 8),
		resources: make(map[uint64]epollEventResource),
	}
	t.Cleanup(func() { _ = p.Close() })
	return r
}

func waitTestSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", what, context.Cause(ctx))
	}
}

func TestEpollReactorSignalDeduplicatesQueuedItem(t *testing.T) {
	r := newTestEpollReactor(t)
	item := newTestInboxItem(nil)
	for i := 0; i < 100; i++ {
		r.signal(item)
	}
	r.drainInbox()
	if got := item.calls.Load(); got != 1 {
		t.Fatalf("calls=%d, want 1", got)
	}
}

func TestEpollReactorSignalWakesBlockedWait(t *testing.T) {
	r := newTestEpollReactor(t)
	waitArmed := make(chan struct{}, 4)
	processed := make(chan struct{})
	r.testWaitArmed = func() {
		select {
		case waitArmed <- struct{}{}:
		default:
		}
	}
	item := newTestInboxItem(func(*epollReactor) { close(processed) })
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.run()
	}()

	waitTestSignal(t, waitArmed, "reactor wait arm")
	r.signal(item)
	waitTestSignal(t, processed, "reactor inbox item")
	r.signalControl(epollControlStop)
	waitTestSignal(t, done, "reactor stop")
}

func TestEpollReactorControlWakeCannotBeLost(t *testing.T) {
	r := newTestEpollReactor(t)
	waitArmed := make(chan struct{}, 1)
	r.testWaitArmed = func() { waitArmed <- struct{}{} }
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.run()
	}()

	waitTestSignal(t, waitArmed, "reactor wait arm")
	r.signalControl(epollControlStop)
	waitTestSignal(t, done, "reactor control stop")
}
