package runtimecore

import (
	"sync"
	"sync/atomic"

	"github.com/qigao/ogrenet"
)

type ObserverDispatcher struct {
	observer ogrenet.Observer
	queue    chan ogrenet.Event
	stopCh   chan struct{}
	stopped  atomic.Bool
	stopOnce sync.Once
	dropped  atomic.Uint64
	panics   atomic.Uint64
}

func NewObserverDispatcher(observer ogrenet.Observer, size int) *ObserverDispatcher {
	if observer == nil {
		return nil
	}
	d := &ObserverDispatcher{
		observer: observer,
		queue:    make(chan ogrenet.Event, size),
		stopCh:   make(chan struct{}),
	}
	go d.run()
	return d
}

func (d *ObserverDispatcher) Emit(event ogrenet.Event) bool {
	if d == nil || d.stopped.Load() {
		return false
	}
	select {
	case d.queue <- event:
		return true
	default:
		d.dropped.Add(1)
		return false
	}
}

func (d *ObserverDispatcher) Stop() {
	if d == nil {
		return
	}
	d.stopped.Store(true)
	d.stopOnce.Do(func() { close(d.stopCh) })
}

func (d *ObserverDispatcher) Dropped() uint64 {
	if d == nil {
		return 0
	}
	return d.dropped.Load()
}

func (d *ObserverDispatcher) Panics() uint64 {
	if d == nil {
		return 0
	}
	return d.panics.Load()
}

func (d *ObserverDispatcher) run() {
	for {
		select {
		case <-d.stopCh:
			return
		case event := <-d.queue:
			d.observe(event)
		}
	}
}

func (d *ObserverDispatcher) observe(event ogrenet.Event) {
	defer func() {
		if recover() != nil {
			d.panics.Add(1)
		}
	}()
	d.observer.Observe(event)
}
