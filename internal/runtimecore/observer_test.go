package runtimecore

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestObserverDispatcherOverflowIsNonBlockingAndCounted(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Uint64
	d := NewObserverDispatcher(ogrenet.ObserverFunc(func(ogrenet.Event) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
	}), 1)
	defer func() {
		close(release)
		d.Stop()
	}()

	if !d.Emit(ogrenet.Event{Kind: ogrenet.EventRead}) {
		t.Fatal("first event was not enqueued")
	}
	<-started
	if !d.Emit(ogrenet.Event{Kind: ogrenet.EventWrite}) {
		t.Fatal("second event did not fill bounded queue")
	}
	if d.Emit(ogrenet.Event{Kind: ogrenet.EventClose}) {
		t.Fatal("overflow event was accepted")
	}
	if got := d.Dropped(); got != 1 {
		t.Fatalf("dropped=%d want=1", got)
	}
}

func TestObserverDispatcherRecoversPanicsAndContinues(t *testing.T) {
	continued := make(chan struct{})
	var calls atomic.Uint64
	d := NewObserverDispatcher(ogrenet.ObserverFunc(func(ogrenet.Event) {
		switch calls.Add(1) {
		case 1:
			panic("observer panic")
		case 2:
			close(continued)
		}
	}), 4)
	defer d.Stop()

	if !d.Emit(ogrenet.Event{Kind: ogrenet.EventRead}) || !d.Emit(ogrenet.Event{Kind: ogrenet.EventWrite}) {
		t.Fatal("events were not enqueued")
	}
	select {
	case <-continued:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not continue after observer panic")
	}
	if got := d.Panics(); got != 1 {
		t.Fatalf("panics=%d want=1", got)
	}
}

func TestObserverDispatcherStopMakesFutureEmitNoop(t *testing.T) {
	d := NewObserverDispatcher(ogrenet.ObserverFunc(func(ogrenet.Event) {}), 1)
	d.Stop()
	if d.Emit(ogrenet.Event{Kind: ogrenet.EventRead}) {
		t.Fatal("emit succeeded after stop")
	}
	if got := d.Dropped(); got != 0 {
		t.Fatalf("stopped emit counted as overflow: %d", got)
	}
}

func TestObserverDispatcherStopDoesNotWaitForBlockedCallback(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	d := NewObserverDispatcher(ogrenet.ObserverFunc(func(ogrenet.Event) {
		close(started)
		<-release
	}), 1)
	if !d.Emit(ogrenet.Event{Kind: ogrenet.EventRead}) {
		t.Fatal("event was not enqueued")
	}
	<-started

	returned := make(chan struct{})
	go func() {
		d.Stop()
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("stop waited for blocked observer callback")
	}
	close(release)
}
