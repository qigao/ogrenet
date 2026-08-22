//go:build linux

package transport

import "github.com/qigao/ogrenet/epoll"

type epollInboxItem interface {
	inboxNode() *epollInboxNode
	onReactorInbox(*epollReactor)
}

type epollInboxNode struct {
	owner         epollInboxItem
	next          *epollInboxNode
	queued        bool
	runnableQueued bool
	workerBlocked bool
}

type epollTestInboxItem struct {
	node epollInboxNode
	fn   func(*epollReactor)
}

func newTestInboxItem(fn func(*epollReactor)) *epollTestInboxItem {
	item := &epollTestInboxItem{fn: fn}
	item.node.owner = item
	return item
}

func (i *epollTestInboxItem) inboxNode() *epollInboxNode { return &i.node }
func (i *epollTestInboxItem) onReactorInbox(r *epollReactor) {
	if i != nil && i.fn != nil {
		i.fn(r)
	}
}

func (r *epollReactor) signal(item epollInboxItem) {
	if item == nil {
		return
	}
	n := item.inboxNode()
	if n == nil || n.owner == nil {
		panic("transport: epoll inbox item has no owner")
	}

	r.inboxMu.Lock()
	if !n.queued {
		n.queued = true
		n.next = nil
		if r.inboxTail == nil {
			r.inboxHead = n
		} else {
			r.inboxTail.next = n
		}
		r.inboxTail = n
	}
	shouldWake := r.waiting && !r.wakePending
	if shouldWake {
		r.wakePending = true
	}
	r.inboxMu.Unlock()

	if shouldWake {
		_ = r.poller.Wake()
	}
}

func (r *epollReactor) signalControl(flag uint32) {
	r.controlFlags.Or(flag)

	r.inboxMu.Lock()
	shouldWake := r.waiting && !r.wakePending
	if shouldWake {
		r.wakePending = true
	}
	r.inboxMu.Unlock()
	if shouldWake {
		_ = r.poller.Wake()
	}
}

func (r *epollReactor) armWait() bool {
	r.inboxMu.Lock()
	defer r.inboxMu.Unlock()
	if r.inboxHead != nil || r.controlFlags.Load() != 0 || r.hasRunnable() {
		return false
	}
	r.waiting = true
	return true
}

func (r *epollReactor) disarmWait() {
	r.inboxMu.Lock()
	r.waiting = false
	r.wakePending = false
	r.inboxMu.Unlock()
}

func (r *epollReactor) drainInbox() {
	for {
		r.inboxMu.Lock()
		n := r.inboxHead
		if n == nil {
			r.inboxMu.Unlock()
			return
		}
		r.inboxHead = n.next
		if r.inboxHead == nil {
			r.inboxTail = nil
		}
		n.next = nil
		n.queued = false
		r.inboxMu.Unlock()

		n.owner.onReactorInbox(r)
	}
}

var _ = epoll.Readable
