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

type testEventResource struct {
	node     epollInboxNode
	id       uint64
	fd       int
	events   atomic.Int32
	runs     atomic.Int32
	runUntil int32
}

func newTestEventResource(id uint64, fd int, runUntil int32) *testEventResource {
	x := &testEventResource{id: id, fd: fd, runUntil: runUntil}
	x.node.owner = x
	return x
}

func (x *testEventResource) inboxNode() *epollInboxNode   { return &x.node }
func (x *testEventResource) onReactorInbox(*epollReactor) {}
func (x *testEventResource) resourceID() uint64           { return x.id }
func (x *testEventResource) resourceFD() int              { return x.fd }
func (x *testEventResource) onReactorEvent(*epollReactor, epoll.Events) {
	x.events.Add(1)
}
func (x *testEventResource) onReactorRunnable(r *epollReactor) {
	if n := x.runs.Add(1); n < x.runUntil {
		r.requeue(x)
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

func TestEpollReactorIgnoresStaleEventData(t *testing.T) {
	r := newTestEpollReactor(t)
	res := newTestEventResource(42, -1, 1)
	r.dispatch(epoll.Event{Data: 42, Events: epoll.Readable})
	if got := res.events.Load(); got != 0 {
		t.Fatalf("stale event dispatched: %d", got)
	}
}

func TestEpollReactorResourceIDRegistryRejectsDuplicate(t *testing.T) {
	r := newTestEpollReactor(t)
	a := newTestEventResource(7, -1, 1)
	b := newTestEventResource(7, -1, 1)
	if err := r.registerResource(a); err != nil {
		t.Fatal(err)
	}
	if err := r.registerResource(b); err == nil {
		t.Fatal("duplicate resource ID replaced owner")
	}
	if got := r.resources[7]; got != a {
		t.Fatal("duplicate registration changed existing owner")
	}
}

func TestEpollReactorRunnableContinuesWithoutSecondEdge(t *testing.T) {
	r := newTestEpollReactor(t)
	res := newTestEventResource(9, -1, 3)
	r.requeue(res)
	r.drainRunnable()
	if got := res.runs.Load(); got != 3 {
		t.Fatalf("runs=%d, want 3", got)
	}
}
