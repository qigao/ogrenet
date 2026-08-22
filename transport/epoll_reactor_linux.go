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

type epollReactor struct {
	index  int
	cfg    resolvedEpollConfig
	poller *epoll.Poller
	events []epoll.Event

	resources map[uint64]epollEventResource

	inboxMu     sync.Mutex
	inboxHead   *epollInboxNode
	inboxTail   *epollInboxNode
	waiting     bool
	wakePending bool

	controlFlags atomic.Uint32
	runnable     []*epollInboxNode
	onFatal      func(error)

	// Package tests use this nil-by-default hook to synchronize on the exact
	// lost-wake boundary after waiting=true is committed and before Wait starts.
	testWaitArmed func()
}

func (r *epollReactor) run() {
	for {
		r.drainInbox()
		if r.shouldStop() {
			return
		}
		if !r.armWait() {
			continue
		}
		if r.testWaitArmed != nil {
			r.testWaitArmed()
		}
		_, err := r.poller.Wait(r.events, -1*time.Nanosecond)
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
	}
}

func (r *epollReactor) shouldStop() bool {
	if r.controlFlags.Load()&epollControlStop == 0 {
		return false
	}
	r.inboxMu.Lock()
	idle := r.inboxHead == nil && len(r.runnable) == 0
	r.inboxMu.Unlock()
	return idle && len(r.resources) == 0
}
