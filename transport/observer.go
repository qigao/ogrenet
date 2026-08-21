package transport

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qigao/ogrenet"
)

const defaultObserverBuffer = 1024

type configuredObserver = ogrenet.Observer

type observerDispatcher struct {
	observer ogrenet.Observer
	queue    chan ogrenet.Event
	stopCh   chan struct{}
	stopped  atomic.Bool
	stopOnce sync.Once
	dropped  atomic.Uint64
	panics   atomic.Uint64
}

func newObserverDispatcher(observer ogrenet.Observer, size int) *observerDispatcher {
	if observer == nil {
		return nil
	}
	d := &observerDispatcher{
		observer: observer,
		queue:    make(chan ogrenet.Event, size),
		stopCh:   make(chan struct{}),
	}
	go d.run()
	return d
}

func (d *observerDispatcher) emit(event ogrenet.Event) bool {
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

func (d *observerDispatcher) stop() {
	if d == nil {
		return
	}
	d.stopped.Store(true)
	d.stopOnce.Do(func() { close(d.stopCh) })
}

func (d *observerDispatcher) run() {
	for {
		select {
		case <-d.stopCh:
			return
		case event := <-d.queue:
			d.observe(event)
		}
	}
}

func (d *observerDispatcher) observe(event ogrenet.Event) {
	defer func() {
		if recover() != nil {
			d.panics.Add(1)
		}
	}()
	d.observer.Observe(event)
}

func (e *Engine) observeSetup(kind ogrenet.EventKind, resourceID, parentID uint64, protocol ogrenet.Scheme, local, remote net.Addr, duration time.Duration, err error) {
	if e == nil || e.observer == nil {
		return
	}
	e.observer.emit(ogrenet.Event{
		Kind:       kind,
		Resource:   ogrenet.ResourceSession,
		ResourceID: resourceID,
		ParentID:   parentID,
		Protocol:   protocol,
		Local:      local,
		Remote:     remote,
		Duration:   duration,
		Err:        err,
	})
}

func WithObserver(observer ogrenet.Observer) Option {
	return func(c *config) error {
		c.observer = observer
		return nil
	}
}

func WithObserverBuffer(size int) Option {
	return func(c *config) error {
		if size <= 0 {
			return ErrInvalidObserverBuffer
		}
		c.observerBuffer = size
		return nil
	}
}
