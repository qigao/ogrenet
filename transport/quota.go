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
}

func newByteQuota(limit int) *byteQuota {
	return &byteQuota{limit: limit, changed: make(chan struct{})}
}

func (q *byteQuota) acquire(ctx context.Context, closing <-chan struct{}, n int) error {
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
	if n <= 0 {
		return
	}
	q.mu.Lock()
	q.used -= n
	if q.used < 0 {
		q.used = 0
	}
	close(q.changed)
	q.changed = make(chan struct{})
	q.mu.Unlock()
}

func (q *byteQuota) current() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.used
}
