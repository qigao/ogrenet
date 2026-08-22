//go:build linux

package transport

type epollInboxItem interface {
	inboxNode() *epollInboxNode
	onReactorInbox(*epollReactor)
}

type epollInboxNode struct {
	owner          epollInboxItem
	next           *epollInboxNode
	queued         bool
	runnableQueued bool
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
	wake := r.waiting && !r.wakePending
	if wake {
		r.wakePending = true
	}
	r.inboxMu.Unlock()

	if wake {
		_ = r.poller.Wake()
	}
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

func (r *epollReactor) signalControl(mask uint32) {
	for {
		old := r.controlFlags.Load()
		if old&mask == mask || r.controlFlags.CompareAndSwap(old, old|mask) {
			break
		}
	}

	r.inboxMu.Lock()
	wake := r.waiting && !r.wakePending
	if wake {
		r.wakePending = true
	}
	r.inboxMu.Unlock()
	if wake {
		_ = r.poller.Wake()
	}
}

func (r *epollReactor) armWait() bool {
	r.inboxMu.Lock()
	defer r.inboxMu.Unlock()
	if r.inboxHead != nil || r.hasRunnable() || r.controlFlags.Load() != 0 {
		return false
	}
	r.waiting = true
	r.wakePending = false
	return true
}

func (r *epollReactor) disarmWait() {
	r.inboxMu.Lock()
	r.waiting = false
	r.wakePending = false
	r.inboxMu.Unlock()
}
