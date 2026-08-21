package transport

import (
	"net"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/internal/runtimecore"
)

const defaultObserverBuffer = 1024

type configuredObserver = ogrenet.Observer

type observerDispatcher struct {
	core *runtimecore.ObserverDispatcher
}

func newObserverDispatcher(observer ogrenet.Observer, size int) *observerDispatcher {
	core := runtimecore.NewObserverDispatcher(observer, size)
	if core == nil {
		return nil
	}
	return &observerDispatcher{core: core}
}

func (d *observerDispatcher) emit(event ogrenet.Event) bool {
	if d == nil {
		return false
	}
	return d.core.Emit(event)
}

func (d *observerDispatcher) stop() {
	if d != nil {
		d.core.Stop()
	}
}

func (d *observerDispatcher) droppedCount() uint64 {
	if d == nil {
		return 0
	}
	return d.core.Dropped()
}

func (d *observerDispatcher) panicCount() uint64 {
	if d == nil {
		return 0
	}
	return d.core.Panics()
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
