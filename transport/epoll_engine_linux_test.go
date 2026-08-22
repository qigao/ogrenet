//go:build linux

package transport

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/epoll"
)

func TestNewEpollStartsConfiguredReactorCount(t *testing.T) {
	e, err := NewEpoll(EpollConfig{Pollers: 3, CallbackWorkers: 2, CallbackQueue: 4})
	if err != nil {
		t.Fatal(err)
	}
	ne := e.(*epollEngine)
	if got := len(ne.reactors); got != 3 {
		t.Fatalf("reactors=%d, want 3", got)
	}
	if ne.callbacks == nil {
		t.Fatal("callback executor was not created")
	}
	if err := ne.Close(); err != nil {
		t.Fatal(err)
	}
	waitEpollEngineDone(t, ne.Done())
}

func TestEpollEngineResourceIDNeverReturnsZeroOrReservedWakeValue(t *testing.T) {
	e, err := NewEpoll(EpollConfig{Pollers: 1, CallbackWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	ne := e.(*epollEngine)
	defer func() {
		_ = ne.Close()
		waitEpollEngineDone(t, ne.Done())
	}()

	ne.nextID.Store(math.MaxUint64 - 2)
	id, err := ne.nextResourceID()
	if err != nil || id != math.MaxUint64-1 {
		t.Fatalf("id=%d err=%v, want id=%d", id, err, uint64(math.MaxUint64-1))
	}
	if id == 0 || id == math.MaxUint64 {
		t.Fatalf("allocated reserved id=%d", id)
	}
	if _, err := ne.nextResourceID(); !errors.Is(err, errNativeResourceIDExhausted) {
		t.Fatalf("exhaustion err=%v", err)
	}
	if got := ne.nextID.Load(); got != math.MaxUint64-1 {
		t.Fatalf("allocator wrapped or advanced: %d", got)
	}
}

func TestEpollEngineCloseStopsEmptyReactorsAndWorkers(t *testing.T) {
	e, err := NewEpoll(EpollConfig{Pollers: 2, CallbackWorkers: 2, CallbackQueue: 2})
	if err != nil {
		t.Fatal(err)
	}
	ne := e.(*epollEngine)
	if err := ne.Close(); err != nil {
		t.Fatal(err)
	}
	waitEpollEngineDone(t, ne.Done())

	for i, r := range ne.reactors {
		if err := r.poller.Wake(); !errors.Is(err, epoll.ErrClosed) {
			t.Fatalf("reactor %d poller still open: %v", i, err)
		}
	}
	if ne.callbacks.tryReserve() {
		t.Fatal("callback executor accepted reservation after Engine.Done")
	}
}

func TestEpollEngineStatsUsesAdmissionAndObserverOwners(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	e, err := NewEpoll(
		EpollConfig{Pollers: 1, CallbackWorkers: 1},
		WithObserverBuffer(1),
		WithObserver(ogrenet.ObserverFunc(func(ogrenet.Event) {
			once.Do(func() { close(entered) })
			<-release
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	ne := e.(*epollEngine)

	lease, err := ne.admission.acquireOpening("")
	if err != nil {
		t.Fatal(err)
	}
	if !ne.observer.emit(ogrenet.Event{Kind: ogrenet.EventRead}) {
		t.Fatal("first observer event not accepted")
	}
	waitEpollEngineDone(t, entered)
	if !ne.observer.emit(ogrenet.Event{Kind: ogrenet.EventWrite}) {
		t.Fatal("buffered observer event not accepted")
	}
	if ne.observer.emit(ogrenet.Event{Kind: ogrenet.EventClose}) {
		t.Fatal("overflow observer event unexpectedly accepted")
	}

	stats := ne.Stats()
	if stats.OpeningConnections != 1 {
		t.Fatalf("opening=%d, want 1", stats.OpeningConnections)
	}
	if stats.ObserverDroppedEvents != 1 {
		t.Fatalf("observer dropped=%d, want 1", stats.ObserverDroppedEvents)
	}

	lease.release()
	close(release)
	if err := ne.Close(); err != nil {
		t.Fatal(err)
	}
	waitEpollEngineDone(t, ne.Done())
}

func waitEpollEngineDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("epoll engine did not reach Done")
	}
}
