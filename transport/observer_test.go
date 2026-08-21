package transport

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestObserverDisabledCreatesNoDispatcher(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if e.observer != nil {
		t.Fatal("observer dispatcher exists with no observer configured")
	}
}

func TestObserverBufferRejectsNonPositiveSize(t *testing.T) {
	for _, size := range []int{0, -1} {
		_, err := New(WithObserverBuffer(size))
		if !errors.Is(err, ErrInvalidObserverBuffer) {
			t.Fatalf("WithObserverBuffer(%d): got %v, want ErrInvalidObserverBuffer", size, err)
		}
	}
}

func TestObserverQueueOverflowDoesNotBlockProducer(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	d := newObserverDispatcher(ogrenet.ObserverFunc(func(ogrenet.Event) {
		once.Do(func() { close(entered) })
		<-release
	}), 1)
	defer func() {
		close(release)
		d.stop()
	}()

	if !d.emit(ogrenet.Event{Kind: ogrenet.EventRead}) {
		t.Fatal("first event was not accepted")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("observer worker did not start")
	}

	if !d.emit(ogrenet.Event{Kind: ogrenet.EventWrite}) {
		t.Fatal("queue event was not accepted")
	}
	start := time.Now()
	if d.emit(ogrenet.Event{Kind: ogrenet.EventClose}) {
		t.Fatal("overflow event unexpectedly accepted")
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("overflow emission blocked producer")
	}
	if got := d.dropped.Load(); got != 1 {
		t.Fatalf("dropped=%d, want 1", got)
	}
}

func TestObserverPanicIsRecoveredAndCounted(t *testing.T) {
	var calls atomic.Uint64
	second := make(chan struct{})
	d := newObserverDispatcher(ogrenet.ObserverFunc(func(ogrenet.Event) {
		n := calls.Add(1)
		if n == 1 {
			panic("observer boom")
		}
		if n == 2 {
			close(second)
		}
	}), 4)
	defer d.stop()

	if !d.emit(ogrenet.Event{Kind: ogrenet.EventRead}) || !d.emit(ogrenet.Event{Kind: ogrenet.EventWrite}) {
		t.Fatal("failed to enqueue observer events")
	}
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("observer worker did not continue after panic")
	}
	if got := d.panics.Load(); got != 1 {
		t.Fatalf("panics=%d, want 1", got)
	}
}

func TestObserverStopDoesNotCloseProducerQueue(t *testing.T) {
	d := newObserverDispatcher(ogrenet.ObserverFunc(func(ogrenet.Event) {}), 8)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				d.emit(ogrenet.Event{Kind: ogrenet.EventRead})
			}
		}()
	}
	d.stop()
	wg.Wait()
	if d.emit(ogrenet.Event{Kind: ogrenet.EventRead}) {
		t.Fatal("stopped dispatcher accepted event")
	}
}

func TestObserverStopDoesNotWaitForBlockedCallback(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	d := newObserverDispatcher(ogrenet.ObserverFunc(func(ogrenet.Event) {
		close(entered)
		<-release
	}), 1)
	if !d.emit(ogrenet.Event{Kind: ogrenet.EventRead}) {
		t.Fatal("event not accepted")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("observer callback did not start")
	}

	done := make(chan struct{})
	go func() {
		d.stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("dispatcher stop waited for blocked callback")
	}
	close(release)
}
