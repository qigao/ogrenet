package transport

import (
	"context"
	"sync"
)

type byteQuota struct {
	mu      sync.Mutex
	limit   int
	used    int
	changed chan struct{}
	parent  *globalByteQuota
}

func newByteQuota(limit int) *byteQuota {
	return &byteQuota{limit: limit}
}

func (q *byteQuota) setParent(parent *globalByteQuota) {
	q.parent = parent
}

func (q *byteQuota) acquire(ctx context.Context, closing <-chan struct{}, n int) error {
	if err := q.acquireLocal(ctx, closing, n); err != nil {
		return err
	}
	if err := q.parent.acquire(ctx, closing, n); err != nil {
		q.releaseLocal(n)
		return err
	}
	return nil
}

func (q *byteQuota) acquireLocal(ctx context.Context, closing <-chan struct{}, n int) error {
	if n > q.limit {
		return ErrFrameExceedsQueueBudget
	}
	for {
		q.mu.Lock()
		if q.used+n <= q.limit {
			q.used += n
			q.mu.Unlock()
			return nil
		}
		if q.changed == nil {
			q.changed = make(chan struct{})
		}
		changed := q.changed
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-closing:
			return ErrClosed
		case <-changed:
		}
	}
}

func (q *byteQuota) tryAcquire(n int) error {
	if err := q.tryAcquireLocal(n); err != nil {
		return err
	}
	if err := q.parent.tryAcquire(n); err != nil {
		q.releaseLocal(n)
		return err
	}
	return nil
}

func (q *byteQuota) tryAcquireLocal(n int) error {
	if n > q.limit {
		return ErrFrameExceedsQueueBudget
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.used+n > q.limit {
		return ErrWouldBlock
	}
	q.used += n
	return nil
}

func (q *byteQuota) release(n int) {
	q.releaseLocal(n)
	q.parent.release(n)
}

func (q *byteQuota) releaseLocal(n int) {
	if n <= 0 {
		return
	}
	q.mu.Lock()
	q.used -= n
	if q.used < 0 {
		q.used = 0
	}
	if q.changed != nil {
		close(q.changed)
		q.changed = nil
	}
	q.mu.Unlock()
}

func (q *byteQuota) current() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.used
}
