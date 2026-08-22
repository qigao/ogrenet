//go:build linux

package transport

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qigao/ogrenet/epoll"
)

type epollEventResource interface {
	epollInboxItem
	resourceID() uint64
	resourceFD() int
	onReactorEvent(*epollReactor, epoll.Events)
	onReactorRunnable(*epollReactor)
}

const epollControlStop uint32 = 1 << 0

var (
	errEpollNilResource         = errors.New("transport: nil epoll resource")
	errEpollInvalidResourceID   = errors.New("transport: invalid epoll resource id")
	errEpollDuplicateResourceID = errors.New("transport: duplicate epoll resource id")
)

type epollReactor struct {
	index  int
	cfg    resolvedEpollConfig
	poller *epoll.Poller
	events []epoll.Event

	resources map[uint64]epollEventResource
	deadlines epollDeadlineHeap

	inboxMu     sync.Mutex
	inboxHead   *epollInboxNode
	inboxTail   *epollInboxNode
	waiting     bool
	wakePending bool

	controlFlags atomic.Uint32
	runnable     []*epollInboxNode
	runnableHead int
	onFatal      func(error)

	// Package tests use this nil-by-default hook to synchronize on the exact
	// lost-wake boundary after waiting=true is committed and before Wait starts.
	testWaitArmed func()
}

func (r *epollReactor) registerResource(resource epollEventResource) error {
	if resource == nil {
		return errEpollNilResource
	}
	id := resource.resourceID()
	if id == 0 {
		return errEpollInvalidResourceID
	}
	if r.resources == nil {
		r.resources = make(map[uint64]epollEventResource)
	}
	if _, exists := r.resources[id]; exists {
		return errEpollDuplicateResourceID
	}
	r.resources[id] = resource
	return nil
}

func (r *epollReactor) dispatch(event epoll.Event) {
	resource := r.resources[event.Data]
	if resource == nil {
		return
	}
	resource.onReactorEvent(r, event.Events)
}

func (r *epollReactor) requeue(resource epollEventResource) {
	if resource == nil {
		return
	}
	n := resource.inboxNode()
	if n == nil || n.owner == nil {
		panic("transport: epoll runnable resource has no owner")
	}
	if n.runnableQueued {
		return
	}
	n.runnableQueued = true
	r.runnable = append(r.runnable, n)
}

func (r *epollReactor) hasRunnable() bool {
	return r.runnableHead < len(r.runnable)
}

func (r *epollReactor) drainRunnable() {
	for r.hasRunnable() {
		n := r.runnable[r.runnableHead]
		r.runnable[r.runnableHead] = nil
		r.runnableHead++
		n.runnableQueued = false
		resource, ok := n.owner.(epollEventResource)
		if !ok || resource == nil {
			continue
		}
		resource.onReactorRunnable(r)
	}
	r.runnable = r.runnable[:0]
	r.runnableHead = 0
}

func (r *epollReactor) run() {
	for {
		r.drainInbox()
		r.runExpiredDeadlines(time.Now())
		r.drainRunnable()
		if r.shouldStop() {
			return
		}
		timeout := r.nextWaitTimeout(time.Now())
		if !r.armWait() {
			continue
		}
		if r.testWaitArmed != nil {
			r.testWaitArmed()
		}
		n, err := r.poller.Wait(r.events, timeout)
		r.disarmWait()
		if err != nil {
			if errors.Is(err, epoll.ErrClosed) {
				return
			}
			if r.onFatal != nil {
				r.onFatal(err)
			}
			return
		}
		for i := 0; i < n; i++ {
			r.dispatch(r.events[i])
		}
	}
}

func (r *epollReactor) shouldStop() bool {
	if r.controlFlags.Load()&epollControlStop == 0 {
		return false
	}
	r.inboxMu.Lock()
	idle := r.inboxHead == nil && !r.hasRunnable()
	r.inboxMu.Unlock()
	return idle && len(r.resources) == 0
}
